from hashlib import sha256
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest
from sqlalchemy.orm import Session

from app.integrations.celery.tasks.process_sdk_upload_task import process_sdk_upload
from app.schemas.responses.upload import SDKBatchReceiptStatus, UploadDataResponse
from app.services.sdk_batch_receipt_service import SDKBatchReceiptService, sdk_batch_receipt_service
from tests.factories import UserFactory


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


class TestSDKBatchReceiptTask:
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
