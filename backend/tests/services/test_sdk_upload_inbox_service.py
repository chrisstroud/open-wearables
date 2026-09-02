from datetime import datetime, timedelta, timezone
from hashlib import sha256
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest
from sqlalchemy.exc import IntegrityError, SQLAlchemyError
from sqlalchemy.orm import Session

from app.config import settings
from app.integrations.celery.tasks.prune_sdk_upload_inbox_task import prune_sdk_upload_inbox
from app.models import SDKBatchReceipt, SDKUploadInbox
from app.repositories.sdk_upload_inbox_repository import sdk_upload_inbox_repository
from app.services.sdk_batch_receipt_service import SDKBatchReceiptService
from app.services.sdk_upload_inbox_service import (
    SDKUploadInboxConflictError,
    SDKUploadInboxService,
    SDKUploadInboxStorageError,
    SDKUploadInboxTooLargeError,
    sdk_upload_inbox_service,
)
from tests.factories import UserFactory


def prepare_receipt(db: Session, *, user_id: UUID, content: str = "payload") -> UUID:
    batch_id = uuid4()
    SDKBatchReceiptService().prepare_submission(
        db,
        batch_id=batch_id,
        user_id=user_id,
        provider="apple",
        payload_sha256=sha256(content.encode()).hexdigest(),
    )
    return batch_id


def put(db: Session, *, batch_id: UUID, user_id: UUID, content: str = "payload") -> SDKUploadInbox:
    return sdk_upload_inbox_service.put(
        db,
        batch_id=batch_id,
        user_id=user_id,
        installation_id=None,
        installation_generation=None,
        health_evidence_generation=None,
        provider="apple",
        payload_sha256=sha256(content.encode()).hexdigest(),
        content_type="application/json",
        content=content,
    )


def test_inbox_size_and_retention_are_bounded(db: Session) -> None:
    user = UserFactory()
    with patch.object(settings, "sdk_upload_max_size_bytes", 4), pytest.raises(SDKUploadInboxTooLargeError):
        sdk_upload_inbox_service.validate_content_size("12345")

    batch_id = prepare_receipt(db, user_id=user.id)
    before = datetime.now(timezone.utc)
    with patch.object(settings, "sdk_upload_inbox_retention_days", 3):
        row = put(db, batch_id=batch_id, user_id=user.id)
    after = datetime.now(timezone.utc)

    assert before + timedelta(days=3) <= row.expires_at <= after + timedelta(days=3)
    assert row.content_size_bytes == len(b"payload")


def test_expiry_terminally_fences_receipt_before_payload_deletion(db: Session) -> None:
    user = UserFactory()
    batch_id = prepare_receipt(db, user_id=user.id)
    row = put(db, batch_id=batch_id, user_id=user.id)
    row.expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    db.commit()

    assert sdk_upload_inbox_service.prune_expired(db) == 1

    receipt = db.query(SDKBatchReceipt).filter_by(id=batch_id).one()
    assert receipt.status == "failed"
    assert receipt.retryable is False
    assert receipt.error_code == "upload_inbox_expired"
    assert db.query(SDKUploadInbox).filter_by(id=batch_id).one_or_none() is None


def test_periodic_prune_exhausts_more_than_one_bounded_transaction(db: Session) -> None:
    user = UserFactory()
    batch_ids = [prepare_receipt(db, user_id=user.id, content=f"payload-{index}") for index in range(5)]
    for index, batch_id in enumerate(batch_ids):
        row = put(db, batch_id=batch_id, user_id=user.id, content=f"payload-{index}")
        row.expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    db.commit()

    session_context = MagicMock()
    session_context.__enter__.return_value = db
    session_context.__exit__.return_value = False
    with patch(
        "app.integrations.celery.tasks.prune_sdk_upload_inbox_task.SessionLocal",
        return_value=session_context,
    ) as session_local:
        result = prune_sdk_upload_inbox(limit=2)

    assert result == {"pruned": 5}
    assert session_local.call_count == 3
    assert db.query(SDKUploadInbox).filter(SDKUploadInbox.id.in_(batch_ids)).count() == 0
    receipts = db.query(SDKBatchReceipt).filter(SDKBatchReceipt.id.in_(batch_ids)).all()
    assert {receipt.status for receipt in receipts} == {"failed"}
    assert {receipt.error_code for receipt in receipts} == {"upload_inbox_expired"}


def test_prune_rejects_nonpositive_transaction_bound(db: Session) -> None:
    with pytest.raises(ValueError, match="positive"):
        sdk_upload_inbox_service.prune_expired(db, limit=0)


def test_concurrent_identical_insert_recovers_existing_row() -> None:
    service = SDKUploadInboxService()
    db = MagicMock()
    batch_id = uuid4()
    user_id = uuid4()
    content = "payload"
    digest = sha256(content.encode()).hexdigest()
    concurrent = MagicMock(spec=SDKUploadInbox)
    concurrent.user_id = user_id
    concurrent.installation_id = None
    concurrent.installation_generation = None
    concurrent.health_evidence_generation = None
    concurrent.provider = "apple"
    concurrent.payload_sha256 = digest
    concurrent.content_type = "application/json"
    concurrent.content_size_bytes = len(content.encode())
    concurrent.content = content
    db.flush.side_effect = IntegrityError("insert", {}, RuntimeError("duplicate"))

    with patch.object(sdk_upload_inbox_repository, "get", side_effect=[None, concurrent]):
        recovered = service.put(
            db,
            batch_id=batch_id,
            user_id=user_id,
            installation_id=None,
            installation_generation=None,
            health_evidence_generation=None,
            provider="apple",
            payload_sha256=digest,
            content_type="application/json",
            content=content,
        )

    assert recovered is concurrent
    db.commit.assert_not_called()


def test_concurrent_conflicting_insert_fails_closed() -> None:
    service = SDKUploadInboxService()
    db = MagicMock()
    batch_id = uuid4()
    user_id = uuid4()
    content = "payload"
    digest = sha256(content.encode()).hexdigest()
    concurrent = MagicMock(spec=SDKUploadInbox)
    concurrent.user_id = user_id
    concurrent.installation_id = None
    concurrent.installation_generation = None
    concurrent.health_evidence_generation = None
    concurrent.provider = "apple"
    concurrent.payload_sha256 = sha256(b"different").hexdigest()
    concurrent.content_type = "application/json"
    concurrent.content_size_bytes = len(content.encode())
    concurrent.content = "different"
    db.flush.side_effect = IntegrityError("insert", {}, RuntimeError("duplicate"))

    with (
        patch.object(sdk_upload_inbox_repository, "get", side_effect=[None, concurrent]),
        pytest.raises(SDKUploadInboxConflictError),
    ):
        service.put(
            db,
            batch_id=batch_id,
            user_id=user_id,
            installation_id=None,
            installation_generation=None,
            health_evidence_generation=None,
            provider="apple",
            payload_sha256=digest,
            content_type="application/json",
            content=content,
        )


def test_database_errors_are_sanitized_without_payload_or_driver_details() -> None:
    service = SDKUploadInboxService()
    db = MagicMock()
    sentinel = "private-health-value-and-database-host"
    with (
        patch.object(
            sdk_upload_inbox_repository,
            "get",
            side_effect=SQLAlchemyError(sentinel),
        ),
        pytest.raises(SDKUploadInboxStorageError) as exc_info,
    ):
        service.put(
            db,
            batch_id=uuid4(),
            user_id=uuid4(),
            installation_id=None,
            installation_generation=None,
            health_evidence_generation=None,
            provider="apple",
            payload_sha256=sha256(sentinel.encode()).hexdigest(),
            content_type="application/json",
            content=sentinel,
        )

    assert sentinel not in str(exc_info.value)
    assert str(exc_info.value) == "Unable to durably stage SDK upload"
    db.rollback.assert_called_once()
