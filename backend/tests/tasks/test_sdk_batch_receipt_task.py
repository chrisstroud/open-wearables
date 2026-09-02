import json
from datetime import date, datetime, timezone
from hashlib import sha256
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest
from sqlalchemy.orm import Session

from app.integrations.celery.tasks.process_sdk_upload_task import process_sdk_upload
from app.models import AppleHealthDailySummary, SDKUploadInbox
from app.schemas.model_crud.credentials.sdk_client_installation import SDKClientRegistration
from app.schemas.responses.upload import SDKBatchReceiptStatus, UploadDataResponse
from app.services.sdk_batch_receipt_service import SDKBatchReceiptService, sdk_batch_receipt_service
from app.services.sdk_client_installation_service import sdk_client_installation_service
from app.services.sdk_upload_inbox_service import sdk_upload_inbox_service
from tests.factories import UserFactory
from tests.services.test_apple_health_daily_summary_service import daily_summary_envelope, summary_payload


def session_context(db: Session) -> MagicMock:
    context = MagicMock()
    context.__enter__.return_value = db
    context.__exit__.return_value = False
    return context


def queue_receipt(db: Session, batch_id: UUID, user_id: UUID) -> None:
    SDKBatchReceiptService().prepare_submission(
        db,
        batch_id=batch_id,
        user_id=user_id,
        provider="apple",
        payload_sha256=sha256(b"payload").hexdigest(),
    )
    sdk_upload_inbox_service.put(
        db,
        batch_id=batch_id,
        user_id=user_id,
        installation_id=None,
        installation_generation=None,
        health_evidence_generation=None,
        provider="apple",
        payload_sha256=sha256(b"payload").hexdigest(),
        content_type="application/json",
        content="payload",
    )


class TestSDKBatchReceiptTask:
    def test_malformed_daily_summary_never_publishes_a_terminal_success(self, db: Session) -> None:
        user = UserFactory()
        installation = sdk_client_installation_service.activate(
            db,
            user_id=user.id,
            registration=SDKClientRegistration(
                installation_id=uuid4(),
                bundle_id="fitness.dashboard.app",
                app_version="1.0.0",
                build_number="1",
                protocol_version=3,
            ),
        )
        shifted = summary_payload()
        shifted["day_start_inclusive"] = "2026-08-31T12:00:00-04:00"
        shifted["day_end_exclusive"] = "2026-09-01T12:00:00-04:00"
        payload = daily_summary_envelope(daily_summaries=[shifted], sleep=[], workouts=[])
        content = json.dumps(payload, separators=(",", ":"), sort_keys=True)
        payload_digest = sha256(content.encode()).hexdigest()
        batch_id = uuid4()
        SDKBatchReceiptService().prepare_submission(
            db,
            batch_id=batch_id,
            user_id=user.id,
            installation_id=installation.id,
            installation_generation=installation.generation,
            health_evidence_generation=user.health_evidence_generation,
            provider="apple",
            payload_sha256=payload_digest,
        )
        sdk_upload_inbox_service.put(
            db,
            batch_id=batch_id,
            user_id=user.id,
            installation_id=installation.id,
            installation_generation=installation.generation,
            health_evidence_generation=user.health_evidence_generation,
            provider="apple",
            payload_sha256=payload_digest,
            content_type="application/json",
            content=content,
        )

        with (
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.SessionLocal",
                return_value=session_context(db),
            ),
            patch("app.integrations.celery.tasks.process_sdk_upload_task.started"),
            patch("app.integrations.celery.tasks.process_sdk_upload_task.completed") as completed,
            patch("app.integrations.celery.tasks.process_sdk_upload_task.failed") as failed,
        ):
            result = process_sdk_upload(
                batch_id=str(batch_id),
                require_terminal_receipt=True,
            )

        receipt = sdk_batch_receipt_service.get_for_user(db, batch_id, user.id)
        assert result["status_code"] == 400
        assert db.query(AppleHealthDailySummary).filter_by(batch_id=batch_id).count() == 0
        assert receipt is not None
        assert receipt.status == SDKBatchReceiptStatus.FAILED
        assert receipt.daily_summaries_saved == 0
        assert receipt.revision_set_digest is None
        assert receipt.error_code == "validation_failed"
        assert sdk_batch_receipt_service.to_response(receipt).accepted is False
        completed.assert_not_called()
        failed.assert_called_once()

    def test_daily_summary_receipt_uses_authoritative_digest_reparsed_from_inbox(
        self,
        db: Session,
    ) -> None:
        user = UserFactory()
        batch_id = uuid4()
        queue_receipt(db, batch_id, user.id)
        authoritative_digest = sha256(b"authoritative-revision-set").hexdigest()
        response = UploadDataResponse(
            status_code=200,
            response="Import successful",
            user_id=str(user.id),
            daily_summaries_saved=1,
        )

        with (
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.SessionLocal",
                return_value=session_context(db),
            ),
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.sdk_import_service.import_data_from_request",
                return_value=response,
            ),
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.validated_content_coverage",
                return_value={
                    "covered_type_identifiers": ["HKWorkoutType"],
                    "content_lower_bound_inclusive": "2026-08-31T07:49:00-04:00",
                    "content_upper_bound_exclusive": "2026-08-31T08:17:00-04:00",
                    "revision_set_digest": authoritative_digest,
                },
            ),
            patch("app.integrations.celery.tasks.process_sdk_upload_task.started"),
            patch("app.integrations.celery.tasks.process_sdk_upload_task.completed") as completed,
            patch("app.integrations.celery.tasks.process_sdk_upload_task.failed") as failed,
        ):
            result = process_sdk_upload(
                content="payload",
                content_type="application/json",
                user_id=str(user.id),
                provider="apple",
                batch_id=str(batch_id),
                require_terminal_receipt=True,
            )

        receipt = sdk_batch_receipt_service.get_for_user(db, batch_id, user.id)
        assert result["revision_set_digest"] == authoritative_digest
        assert receipt is not None
        assert receipt.status == SDKBatchReceiptStatus.SUCCEEDED
        assert receipt.revision_set_digest == authoritative_digest
        assert completed.call_args.kwargs["metadata"]["revision_set_digest"] == authoritative_digest
        failed.assert_not_called()

    def test_invalid_authoritative_digest_rolls_back_compact_rows_before_receipt_failure(
        self,
        db: Session,
    ) -> None:
        user = UserFactory()
        installation = sdk_client_installation_service.activate(
            db,
            user_id=user.id,
            registration=SDKClientRegistration(
                installation_id=uuid4(),
                bundle_id="fitness.dashboard.app",
                app_version="1.0.0",
                build_number="1",
                protocol_version=3,
            ),
        )
        batch_id = uuid4()
        payload_digest = sha256(b"payload").hexdigest()
        SDKBatchReceiptService().prepare_submission(
            db,
            batch_id=batch_id,
            user_id=user.id,
            installation_id=installation.id,
            installation_generation=installation.generation,
            health_evidence_generation=user.health_evidence_generation,
            provider="apple",
            payload_sha256=payload_digest,
        )
        sdk_upload_inbox_service.put(
            db,
            batch_id=batch_id,
            user_id=user.id,
            installation_id=installation.id,
            installation_generation=installation.generation,
            health_evidence_generation=user.health_evidence_generation,
            provider="apple",
            payload_sha256=payload_digest,
            content_type="application/json",
            content="payload",
        )
        response = UploadDataResponse(
            status_code=200,
            response="Import successful",
            user_id=str(user.id),
            daily_summaries_saved=1,
        )

        def import_with_pending_summary(db_session: Session, *_args: object, **_kwargs: object) -> UploadDataResponse:
            db_session.add(
                AppleHealthDailySummary(
                    id=uuid4(),
                    user_id=user.id,
                    installation_id=installation.id,
                    installation_generation=installation.generation,
                    health_evidence_generation=user.health_evidence_generation,
                    batch_id=batch_id,
                    summary_kind="workout",
                    stable_key=sha256(b"workout-key").hexdigest(),
                    schema_version="apple-health-workout-summary.v1",
                    revision_id=sha256(b"workout-revision").hexdigest(),
                    supersedes_revision_id=None,
                    local_date=date(2026, 8, 31),
                    timezone="America/Toronto",
                    timezone_boundary_version="session-start-day.v1",
                    series_type="tennis",
                    contributor_set_digest=sha256(b"contributors").hexdigest(),
                    input_set_digest=sha256(b"inputs").hexdigest(),
                    computed_at=datetime(2026, 9, 1, tzinfo=timezone.utc),
                    payload={"coverage_status": "observed"},
                    is_current=True,
                )
            )
            db_session.flush()
            return response

        with (
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.SessionLocal",
                return_value=session_context(db),
            ),
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.sdk_import_service.import_data_from_request",
                side_effect=import_with_pending_summary,
            ),
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.validated_content_coverage",
                return_value={
                    "covered_type_identifiers": ["HKWorkoutType"],
                    "content_lower_bound_inclusive": "2026-08-31T07:49:00-04:00",
                    "content_upper_bound_exclusive": "2026-08-31T08:17:00-04:00",
                },
            ),
            patch("app.integrations.celery.tasks.process_sdk_upload_task.started"),
            patch("app.integrations.celery.tasks.process_sdk_upload_task.completed") as completed,
            patch("app.integrations.celery.tasks.process_sdk_upload_task.failed") as failed,
        ):
            process_sdk_upload(
                content="payload",
                content_type="application/json",
                user_id=str(user.id),
                provider="apple",
                batch_id=str(batch_id),
                require_terminal_receipt=True,
            )

        receipt = sdk_batch_receipt_service.get_for_user(db, batch_id, user.id)
        assert db.query(AppleHealthDailySummary).filter_by(batch_id=batch_id).count() == 0
        assert receipt is not None
        assert receipt.status == SDKBatchReceiptStatus.FAILED
        assert receipt.daily_summaries_saved == 0
        assert receipt.revision_set_digest is None
        assert receipt.error_code == "daily_summary_revision_set_digest_invalid"
        completed.assert_not_called()
        failed.assert_called_once()

    def test_non_compact_result_cannot_publish_a_revision_set_digest(self, db: Session) -> None:
        user = UserFactory()
        batch_id = uuid4()
        queue_receipt(db, batch_id, user.id)
        response = UploadDataResponse(
            status_code=200,
            response="Import successful",
            user_id=str(user.id),
            records_saved=1,
            revision_set_digest=sha256(b"unexpected-digest").hexdigest(),
        )

        with (
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.SessionLocal",
                return_value=session_context(db),
            ),
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.sdk_import_service.import_data_from_request",
                return_value=response,
            ),
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.validated_content_coverage",
                return_value={
                    "covered_type_identifiers": ["HKQuantityTypeIdentifierStepCount"],
                    "content_lower_bound_inclusive": "2026-08-31T07:49:00-04:00",
                    "content_upper_bound_exclusive": "2026-08-31T08:17:00-04:00",
                },
            ),
            patch("app.integrations.celery.tasks.process_sdk_upload_task.started"),
            patch("app.integrations.celery.tasks.process_sdk_upload_task.completed") as completed,
            patch("app.integrations.celery.tasks.process_sdk_upload_task.failed") as failed,
        ):
            process_sdk_upload(
                content="payload",
                content_type="application/json",
                user_id=str(user.id),
                provider="apple",
                batch_id=str(batch_id),
                require_terminal_receipt=True,
            )

        receipt = sdk_batch_receipt_service.get_for_user(db, batch_id, user.id)
        assert receipt is not None
        assert receipt.status == SDKBatchReceiptStatus.FAILED
        assert receipt.revision_set_digest is None
        assert receipt.error_code == "daily_summary_revision_set_digest_invalid"
        completed.assert_not_called()
        failed.assert_called_once()

    def test_receipt_worker_loads_durable_inbox_without_health_data_in_task_message(
        self,
        db: Session,
    ) -> None:
        user = UserFactory()
        batch_id = uuid4()
        content = json.dumps(
            {
                "provider": "apple",
                "sdkVersion": "2.0.0",
                "syncTimestamp": "2026-08-26T00:00:00Z",
                "data": {},
            }
        )
        payload_sha256 = sha256(content.encode()).hexdigest()
        SDKBatchReceiptService().prepare_submission(
            db,
            batch_id=batch_id,
            user_id=user.id,
            provider="apple",
            payload_sha256=payload_sha256,
        )
        sdk_upload_inbox_service.put(
            db,
            batch_id=batch_id,
            user_id=user.id,
            installation_id=None,
            installation_generation=None,
            health_evidence_generation=None,
            provider="apple",
            payload_sha256=payload_sha256,
            content_type="application/json",
            content=content,
        )
        response = UploadDataResponse(
            status_code=200,
            response="Import successful",
            user_id=str(user.id),
            records_saved=1,
            dropped_count=0,
        )

        with (
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.SessionLocal",
                return_value=session_context(db),
            ),
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.sdk_import_service.import_data_from_request",
                return_value=response,
            ) as importer,
            patch("app.integrations.celery.tasks.process_sdk_upload_task.started"),
            patch("app.integrations.celery.tasks.process_sdk_upload_task.completed"),
        ):
            result = process_sdk_upload(
                batch_id=str(batch_id),
                require_terminal_receipt=True,
            )

        assert result["status_code"] == 200
        assert importer.call_args.args[1:4] == (content, "application/json", str(user.id))
        receipt = sdk_batch_receipt_service.get_for_user(db, batch_id, user.id)
        assert receipt is not None
        assert receipt.status == SDKBatchReceiptStatus.SUCCEEDED
        assert db.query(SDKUploadInbox).filter_by(id=batch_id).one_or_none() is None

    def test_delayed_upload_cannot_cross_health_evidence_generation(self, db: Session) -> None:
        user = UserFactory(
            health_evidence_generation=3,
            health_source_policy="apple-mobile-v2-only",
        )
        installation = sdk_client_installation_service.activate(
            db,
            user_id=user.id,
            registration=SDKClientRegistration(
                installation_id=uuid4(),
                bundle_id="fitness.dashboard.app",
                app_version="1.0.0",
                build_number="1",
                protocol_version=2,
            ),
        )
        batch_id = uuid4()
        SDKBatchReceiptService().prepare_submission(
            db,
            batch_id=batch_id,
            user_id=user.id,
            installation_id=installation.id,
            installation_generation=installation.generation,
            health_evidence_generation=3,
            provider="apple",
            payload_sha256=sha256(b"payload").hexdigest(),
        )
        sdk_upload_inbox_service.put(
            db,
            batch_id=batch_id,
            user_id=user.id,
            installation_id=installation.id,
            installation_generation=installation.generation,
            health_evidence_generation=3,
            provider="apple",
            payload_sha256=sha256(b"payload").hexdigest(),
            content_type="application/json",
            content="payload",
        )
        user.health_evidence_generation = 4
        user.health_write_state = "fenced"
        db.commit()

        with (
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.SessionLocal",
                return_value=session_context(db),
            ),
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.sdk_import_service.import_data_from_request"
            ) as importer,
        ):
            result = process_sdk_upload(batch_id=str(batch_id), require_terminal_receipt=True)

        assert result["reason"] == "health_write_fenced"
        importer.assert_not_called()
        receipt = sdk_batch_receipt_service.get_for_user(db, batch_id, user.id)
        assert receipt is not None
        assert receipt.status == SDKBatchReceiptStatus.FAILED
        assert receipt.retryable is False
        assert db.query(SDKUploadInbox).filter_by(id=batch_id).one_or_none() is not None

    def test_repairing_same_phone_fences_its_prior_installation_generation(self, db: Session) -> None:
        user = UserFactory()
        installation_id = uuid4()
        registration = SDKClientRegistration(
            installation_id=installation_id,
            bundle_id="fitness.dashboard.app",
            app_version="1.0.0",
            build_number="1",
            protocol_version=2,
        )
        installation = sdk_client_installation_service.activate(db, user_id=user.id, registration=registration)
        prior_generation = installation.generation
        batch_id = uuid4()
        SDKBatchReceiptService().prepare_submission(
            db,
            batch_id=batch_id,
            user_id=user.id,
            installation_id=installation.id,
            installation_generation=prior_generation,
            health_evidence_generation=0,
            provider="apple",
            payload_sha256=sha256(b"payload").hexdigest(),
        )
        sdk_upload_inbox_service.put(
            db,
            batch_id=batch_id,
            user_id=user.id,
            installation_id=installation.id,
            installation_generation=prior_generation,
            health_evidence_generation=0,
            provider="apple",
            payload_sha256=sha256(b"payload").hexdigest(),
            content_type="application/json",
            content="payload",
        )
        repaired = sdk_client_installation_service.activate(db, user_id=user.id, registration=registration)
        assert repaired.generation == prior_generation + 1
        db.commit()

        with (
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.SessionLocal",
                return_value=session_context(db),
            ),
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.sdk_import_service.import_data_from_request"
            ) as importer,
        ):
            result = process_sdk_upload(batch_id=str(batch_id), require_terminal_receipt=True)

        assert result["reason"] == "installation_generation_mismatch"
        importer.assert_not_called()
        receipt = sdk_batch_receipt_service.get_for_user(db, batch_id, user.id)
        assert receipt is not None
        assert receipt.status == SDKBatchReceiptStatus.FAILED
        assert receipt.retryable is False

    def test_receipt_and_inbox_scope_mismatch_fails_before_import(self, db: Session) -> None:
        user = UserFactory()
        batch_id = uuid4()
        queue_receipt(db, batch_id, user.id)
        inbox = db.query(SDKUploadInbox).filter_by(id=batch_id).one()
        inbox.content = "tampered-payload"
        db.commit()

        with (
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.SessionLocal",
                return_value=session_context(db),
            ),
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.sdk_import_service.import_data_from_request"
            ) as importer,
        ):
            result = process_sdk_upload(batch_id=str(batch_id), require_terminal_receipt=True)

        assert result["reason"] == "upload_inbox_scope_mismatch"
        importer.assert_not_called()
        receipt = sdk_batch_receipt_service.get_for_user(db, batch_id, user.id)
        assert receipt is not None
        assert receipt.status == SDKBatchReceiptStatus.FAILED
        assert receipt.retryable is False
        assert receipt.error_code == "upload_inbox_scope_mismatch"
        assert db.query(SDKUploadInbox).filter_by(id=batch_id).one_or_none() is not None

    def test_worker_202_remains_nonterminal_and_retryable(self, db: Session) -> None:
        user = UserFactory()
        batch_id = uuid4()
        queue_receipt(db, batch_id, user.id)
        response = UploadDataResponse(
            status_code=202,
            response="Projection still queued",
            user_id=str(user.id),
            records_saved=0,
            dropped_count=0,
        )

        with (
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.SessionLocal",
                return_value=session_context(db),
            ),
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.sdk_import_service.import_data_from_request",
                return_value=response,
            ),
            patch("app.integrations.celery.tasks.process_sdk_upload_task.started"),
            patch("app.integrations.celery.tasks.process_sdk_upload_task.completed") as completed,
            patch("app.integrations.celery.tasks.process_sdk_upload_task.failed") as failed,
        ):
            process_sdk_upload(
                content="payload",
                content_type="application/json",
                user_id=str(user.id),
                provider="apple",
                batch_id=str(batch_id),
                require_terminal_receipt=True,
            )

        receipt = sdk_batch_receipt_service.get_for_user(db, batch_id, user.id)
        assert receipt is not None
        assert receipt.status == SDKBatchReceiptStatus.FAILED
        assert receipt.retryable is True
        assert receipt.error_code == "processing_not_terminal"
        completed.assert_not_called()
        failed.assert_called_once()

    def test_delayed_drop_publishes_terminal_failure_not_success(self, db: Session) -> None:
        user = UserFactory()
        batch_id = uuid4()
        queue_receipt(db, batch_id, user.id)
        response = UploadDataResponse(
            status_code=200,
            response="Import successful",
            user_id=str(user.id),
            records_saved=8,
            dropped_count=1,
        )

        with (
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.SessionLocal",
                return_value=session_context(db),
            ),
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.sdk_import_service.import_data_from_request",
                return_value=response,
            ),
            patch("app.integrations.celery.tasks.process_sdk_upload_task.started"),
            patch("app.integrations.celery.tasks.process_sdk_upload_task.completed") as completed,
            patch("app.integrations.celery.tasks.process_sdk_upload_task.failed") as failed,
        ):
            result = process_sdk_upload(
                content="payload",
                content_type="application/json",
                user_id=str(user.id),
                provider="apple",
                batch_id=str(batch_id),
                require_terminal_receipt=True,
            )

        receipt = sdk_batch_receipt_service.get_for_user(db, batch_id, user.id)
        assert result["dropped_count"] == 1
        assert receipt is not None
        assert receipt.status == SDKBatchReceiptStatus.FAILED
        assert receipt.error_code == "dropped_records"
        assert receipt.dropped_count == 1
        completed.assert_not_called()
        failed.assert_called_once()

    def test_worker_exception_is_retryable_and_never_acknowledged(self, db: Session) -> None:
        user = UserFactory()
        batch_id = uuid4()
        queue_receipt(db, batch_id, user.id)

        with (
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.SessionLocal",
                return_value=session_context(db),
            ),
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.sdk_import_service.import_data_from_request",
                side_effect=RuntimeError("delayed worker failure"),
            ),
            patch("app.integrations.celery.tasks.process_sdk_upload_task.started"),
            pytest.raises(RuntimeError, match="delayed worker failure"),
        ):
            process_sdk_upload(
                content="payload",
                content_type="application/json",
                user_id=str(user.id),
                provider="apple",
                batch_id=str(batch_id),
                require_terminal_receipt=True,
            )

        receipt = sdk_batch_receipt_service.get_for_user(db, batch_id, user.id)
        assert receipt is not None
        assert receipt.status == SDKBatchReceiptStatus.FAILED
        assert receipt.retryable is True
        assert receipt.error_code == "worker_exception"
        assert receipt.completed_at is not None

    def test_invalid_tombstone_keeps_typed_terminal_failure_even_when_validation_dropped_it(
        self,
        db: Session,
    ) -> None:
        user = UserFactory()
        batch_id = uuid4()
        queue_receipt(db, batch_id, user.id)
        response = UploadDataResponse(
            status_code=409,
            response="HealthKit deletion quarantined until end-to-end deletion projection is supported",
            user_id=str(user.id),
            dropped_count=1,
            tombstones_received=1,
            tombstones_unresolved=1,
            tombstone_error_code="invalid_tombstone",
        )

        with (
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.SessionLocal",
                return_value=session_context(db),
            ),
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.sdk_import_service.import_data_from_request",
                return_value=response,
            ),
            patch("app.integrations.celery.tasks.process_sdk_upload_task.started"),
            patch("app.integrations.celery.tasks.process_sdk_upload_task.completed"),
            patch("app.integrations.celery.tasks.process_sdk_upload_task.failed"),
        ):
            process_sdk_upload(
                content="payload",
                content_type="application/json",
                user_id=str(user.id),
                provider="apple",
                batch_id=str(batch_id),
                require_terminal_receipt=True,
            )

        receipt = sdk_batch_receipt_service.get_for_user(db, batch_id, user.id)
        assert receipt is not None
        assert receipt.status == SDKBatchReceiptStatus.FAILED
        assert receipt.error_code == "invalid_tombstone"
        assert receipt.dropped_count == 1
        assert receipt.tombstones_unresolved == 1

    def test_durable_processing_quarantine_keeps_typed_terminal_failure(self, db: Session) -> None:
        user = UserFactory()
        batch_id = uuid4()
        queue_receipt(db, batch_id, user.id)
        response = UploadDataResponse(
            status_code=409,
            response="SDK item quarantined because durable processing is unavailable",
            user_id=str(user.id),
            dropped_count=1,
            processing_error_code="sleep_durable_receipt_unavailable",
        )

        with (
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.SessionLocal",
                return_value=session_context(db),
            ),
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.sdk_import_service.import_data_from_request",
                return_value=response,
            ),
            patch("app.integrations.celery.tasks.process_sdk_upload_task.started"),
            patch("app.integrations.celery.tasks.process_sdk_upload_task.completed"),
            patch("app.integrations.celery.tasks.process_sdk_upload_task.failed"),
        ):
            process_sdk_upload(
                content="payload",
                content_type="application/json",
                user_id=str(user.id),
                provider="apple",
                batch_id=str(batch_id),
                require_terminal_receipt=True,
            )

        receipt = sdk_batch_receipt_service.get_for_user(db, batch_id, user.id)
        assert receipt is not None
        assert receipt.status == SDKBatchReceiptStatus.FAILED
        assert receipt.error_code == "sleep_durable_receipt_unavailable"
        assert receipt.dropped_count == 1

    def test_window_waiting_for_sleep_projection_remains_retryable(self, db: Session) -> None:
        user = UserFactory()
        batch_id = uuid4()
        queue_receipt(db, batch_id, user.id)
        response = UploadDataResponse(
            status_code=409,
            response="SDK item quarantined because durable processing is unavailable",
            user_id=str(user.id),
            dropped_count=1,
            processing_error_code="window_sleep_projection_pending",
        )

        with (
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.SessionLocal",
                return_value=session_context(db),
            ),
            patch(
                "app.integrations.celery.tasks.process_sdk_upload_task.sdk_import_service.import_data_from_request",
                return_value=response,
            ),
            patch("app.integrations.celery.tasks.process_sdk_upload_task.started"),
            patch("app.integrations.celery.tasks.process_sdk_upload_task.completed"),
            patch("app.integrations.celery.tasks.process_sdk_upload_task.failed"),
        ):
            process_sdk_upload(
                content="payload",
                content_type="application/json",
                user_id=str(user.id),
                provider="apple",
                batch_id=str(batch_id),
                require_terminal_receipt=True,
            )

        receipt = sdk_batch_receipt_service.get_for_user(db, batch_id, user.id)
        assert receipt is not None
        assert receipt.status == SDKBatchReceiptStatus.FAILED
        assert receipt.error_code == "window_sleep_projection_pending"
        assert receipt.retryable is True

        retry = sdk_batch_receipt_service.prepare_submission(
            db,
            batch_id=batch_id,
            user_id=user.id,
            provider="apple",
            payload_sha256=sha256(b"payload").hexdigest(),
        )
        assert retry.http_status == 202
        assert retry.should_dispatch is True
