from datetime import datetime, timedelta, timezone
from uuid import UUID

from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from app.config import settings
from app.database import DbSession
from app.models import SDKBatchReceipt, SDKUploadInbox
from app.repositories.sdk_upload_inbox_repository import sdk_upload_inbox_repository


class SDKUploadInboxConflictError(ValueError):
    """The durable batch payload differs from the existing inbox item."""


class SDKUploadInboxTooLargeError(ValueError):
    """The payload exceeds the bounded durable-inbox contract."""


class SDKUploadInboxStorageError(RuntimeError):
    """Durable staging failed without exposing database or payload details."""


class SDKUploadInboxService:
    @staticmethod
    def content_size(content: str) -> int:
        return len(content.encode("utf-8"))

    def validate_content_size(self, content: str) -> int:
        size = self.content_size(content)
        if size <= 0 or size > settings.sdk_upload_max_size_bytes:
            raise SDKUploadInboxTooLargeError("SDK upload exceeds the durable inbox size limit")
        return size

    def put(
        self,
        db_session: DbSession,
        *,
        batch_id: UUID,
        user_id: UUID,
        installation_id: UUID | None,
        installation_generation: int | None,
        health_evidence_generation: int | None,
        provider: str,
        payload_sha256: str,
        content_type: str,
        content: str,
    ) -> SDKUploadInbox:
        content_size_bytes = self.validate_content_size(content)
        try:
            existing = sdk_upload_inbox_repository.get(db_session, batch_id)
        except SQLAlchemyError:
            db_session.rollback()
            raise SDKUploadInboxStorageError("Unable to durably stage SDK upload") from None
        if existing is not None:
            if (
                existing.user_id != user_id
                or existing.installation_id != installation_id
                or existing.installation_generation != installation_generation
                or existing.health_evidence_generation != health_evidence_generation
                or existing.provider != provider
                or existing.payload_sha256 != payload_sha256
                or existing.content_type != content_type
                or existing.content_size_bytes != content_size_bytes
                or existing.content != content
            ):
                raise SDKUploadInboxConflictError("Batch inbox already belongs to a different payload")
            return existing
        row = SDKUploadInbox(
            id=batch_id,
            user_id=user_id,
            installation_id=installation_id,
            installation_generation=installation_generation,
            health_evidence_generation=health_evidence_generation,
            provider=provider,
            payload_sha256=payload_sha256,
            content_type=content_type,
            content_size_bytes=content_size_bytes,
            expires_at=datetime.now(timezone.utc) + timedelta(days=settings.sdk_upload_inbox_retention_days),
            content=content,
        )
        try:
            with db_session.begin_nested():
                db_session.add(row)
                db_session.flush()
            db_session.commit()
            return row
        except IntegrityError:
            try:
                concurrent = sdk_upload_inbox_repository.get(db_session, batch_id)
            except SQLAlchemyError:
                db_session.rollback()
                raise SDKUploadInboxStorageError("Unable to durably stage SDK upload") from None
            if concurrent is None:
                raise SDKUploadInboxStorageError("Unable to durably stage SDK upload") from None
            if (
                concurrent.user_id != user_id
                or concurrent.installation_id != installation_id
                or concurrent.installation_generation != installation_generation
                or concurrent.health_evidence_generation != health_evidence_generation
                or concurrent.provider != provider
                or concurrent.payload_sha256 != payload_sha256
                or concurrent.content_type != content_type
                or concurrent.content_size_bytes != content_size_bytes
                or concurrent.content != content
            ):
                raise SDKUploadInboxConflictError("Batch inbox already belongs to a different payload") from None
            return concurrent
        except SQLAlchemyError:
            db_session.rollback()
            raise SDKUploadInboxStorageError("Unable to durably stage SDK upload") from None

    def prune_expired(self, db_session: DbSession, *, limit: int = 500) -> int:
        """Remove expired raw payloads and terminally fence unfinished receipts."""
        if limit <= 0:
            raise ValueError("prune batch limit must be positive")
        now = datetime.now(timezone.utc)
        rows = sdk_upload_inbox_repository.list_expired_for_update(db_session, now=now, limit=limit)
        for row in rows:
            receipt = (
                db_session.query(SDKBatchReceipt).filter(SDKBatchReceipt.id == row.id).with_for_update().one_or_none()
            )
            if receipt is not None and receipt.status not in {"succeeded", "failed"}:
                receipt.status = "failed"
                receipt.retryable = False
                receipt.error_code = "upload_inbox_expired"
                receipt.updated_at = now
                receipt.completed_at = now
            sdk_upload_inbox_repository.delete(db_session, row.id)
        db_session.commit()
        return len(rows)


sdk_upload_inbox_service = SDKUploadInboxService()
