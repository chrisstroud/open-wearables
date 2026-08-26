import logging
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session

from app.integrations.celery.tasks.project_sdk_sleep_inbox_task import project_sdk_sleep_inbox
from app.models import EventRecord, SDKSleepInbox
from app.schemas.providers.mobile_sdk import SleepRecord, SyncWindowManifest
from app.services.sdk_batch_receipt_service import SDKBatchReceiptService
from app.services.sdk_sleep_inbox_service import sdk_sleep_inbox_service
from app.services.sdk_sync_window_receipt_service import SDKSyncWindowReceiptService
from tests.factories import DataSourceFactory, EventRecordFactory, UserFactory


def session_context(db: Session) -> MagicMock:
    context = MagicMock()
    context.__enter__.return_value = db
    context.__exit__.return_value = False
    return context


def sleep_record(external_id: str) -> SleepRecord:
    return SleepRecord(
        id=external_id,
        stage="light",
        startDate="2026-08-23T22:00:00Z",
        endDate="2026-08-23T23:00:00Z",
    )


class TestProjectSDKSleepInboxTask:
    def test_schedule_failure_log_does_not_include_user_identifier(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        user_id = uuid4()
        with (
            patch(
                "app.integrations.celery.tasks.project_sdk_sleep_inbox_task.project_sdk_sleep_inbox.delay",
                side_effect=RuntimeError("broker unavailable"),
            ),
            caplog.at_level(logging.WARNING),
        ):
            sdk_sleep_inbox_service.schedule_projection(user_id=user_id, provider="apple")

        matching = [
            record
            for record in caplog.records
            if record.getMessage() == "Could not immediately schedule durable sleep projection"
        ]
        assert len(matching) == 1
        assert str(user_id) not in matching[0].getMessage()
        assert "user_id" not in matching[0].__dict__

    def test_lock_failure_remains_retryable_then_materializes_from_durable_inbox(self, db: Session) -> None:
        user = UserFactory()
        external_id = "56565656-5656-5656-5656-565656565656"
        batch_id = uuid4()
        batch_service = SDKBatchReceiptService()
        batch_service.prepare_submission(
            db,
            batch_id=batch_id,
            user_id=user.id,
            provider="apple",
            payload_sha256=sha256(b"sleep-payload").hexdigest(),
        )
        outcome = sdk_sleep_inbox_service.stage(
            db,
            user_id=user.id,
            provider="apple",
            batch_id=batch_id,
            records=[sleep_record(external_id)],
        )
        assert outcome.error_code is None
        db.commit()
        claim = batch_service.claim_for_processing(db, batch_id)
        assert claim.attempt_count is not None
        batch_service.mark_succeeded(
            db,
            batch_id=batch_id,
            attempt_count=claim.attempt_count,
            result={"status_code": 200, "sleep_saved": 1, "dropped_count": 0},
        )
        window_service = SDKSyncWindowReceiptService()
        window_id = uuid4()
        batch_service.prepare_submission(
            db,
            batch_id=window_id,
            user_id=user.id,
            provider="apple",
            payload_sha256=sha256(b"window-payload").hexdigest(),
        )
        window_manifest = SyncWindowManifest(
            windowId=window_id,
            purpose="activation",
            windowVersion=2,
            lowerBoundInclusive="2026-08-01T00:00:00Z",
            upperBoundExclusive="2026-08-25T00:00:00Z",
            batchIds=[batch_id],
        )
        unrelated_external_id = "78787878-7878-7878-7878-787878787878"
        data_source = DataSourceFactory(user=user, provider="apple")
        EventRecordFactory(
            data_source=data_source,
            external_id=unrelated_external_id,
            category="sleep",
            type_="sleep_session",
            start_datetime=datetime(2026, 8, 23, 21, 0, tzinfo=timezone.utc),
            end_datetime=datetime(2026, 8, 24, 0, 0, tzinfo=timezone.utc),
        )
        db.commit()

        redis_client = MagicMock()
        lock = MagicMock()
        redis_client.lock.return_value = lock
        redis_client.get.return_value = None

        with (
            patch(
                "app.integrations.celery.tasks.project_sdk_sleep_inbox_task.SessionLocal",
                return_value=session_context(db),
            ),
            patch(
                "app.services.apple.healthkit.sleep_service.get_redis_client",
                return_value=redis_client,
            ),
            patch(
                "app.integrations.celery.tasks.finalize_stale_sleep_task.finalize_stale_sleeps.delay",
            ),
        ):
            lock.acquire.return_value = False
            first = project_sdk_sleep_inbox(user_id=str(user.id), provider="apple")

            row = db.query(SDKSleepInbox).filter(SDKSleepInbox.external_id == external_id).one()
            assert first == {"leased": 1, "materialized": 0}
            assert row.status == "projected"
            assert row.materialized_at is None
            assert db.query(EventRecord).filter(EventRecord.external_id == external_id).count() == 0
            assert db.query(EventRecord).filter(EventRecord.external_id == unrelated_external_id).count() == 1
            pending_window = window_service.accept(
                db,
                user_id=user.id,
                provider="apple",
                terminal_batch_id=window_id,
                manifest=window_manifest,
            )
            assert pending_window.accepted is False
            assert pending_window.error_code == "window_sleep_projection_pending"

            row.next_attempt_at = datetime.now(timezone.utc) - timedelta(seconds=1)
            db.commit()
            lock.acquire.return_value = True
            second = project_sdk_sleep_inbox(user_id=str(user.id), provider="apple")

        db.expire_all()
        row = db.query(SDKSleepInbox).filter(SDKSleepInbox.external_id == external_id).one()
        assert second == {"leased": 1, "materialized": 1}
        assert row.status == "materialized"
        assert row.materialized_at is not None
        assert db.query(EventRecord).filter(EventRecord.external_id == external_id).count() == 1
        accepted_window = window_service.accept(
            db,
            user_id=user.id,
            provider="apple",
            terminal_batch_id=window_id,
            manifest=window_manifest,
        )
        assert accepted_window.accepted is True
