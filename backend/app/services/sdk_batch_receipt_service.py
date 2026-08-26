from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from logging import getLogger
from uuid import UUID

from sqlalchemy.exc import IntegrityError

from app.database import DbSession
from app.models import SDKBatchReceipt
from app.repositories import SDKBatchReceiptRepository
from app.schemas.responses.upload import SDKBatchReceiptResponse, SDKBatchReceiptStatus

logger = getLogger(__name__)


@dataclass(frozen=True)
class SubmissionDecision:
    receipt: SDKBatchReceipt
    should_dispatch: bool
    http_status: int


@dataclass(frozen=True)
class ClaimDecision:
    receipt_exists: bool
    should_process: bool
    attempt_count: int | None = None


class BatchReceiptConflictError(ValueError):
    """The idempotency key was reused for a different upload."""


class SDKBatchReceiptService:
    stale_processing_after = timedelta(minutes=15)

    def __init__(self) -> None:
        self.crud = SDKBatchReceiptRepository()

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    def prepare_submission(
        self,
        db_session: DbSession,
        *,
        batch_id: UUID,
        user_id: UUID,
        provider: str,
        payload_sha256: str,
    ) -> SubmissionDecision:
        receipt = self.crud.get_for_update(db_session, batch_id)
        if receipt is None:
            now = self._now()
            receipt = SDKBatchReceipt(
                id=batch_id,
                user_id=user_id,
                provider=provider,
                payload_sha256=payload_sha256,
                status=SDKBatchReceiptStatus.QUEUED,
                retryable=False,
                attempt_count=0,
                dropped_count=0,
                records_saved=0,
                workouts_saved=0,
                sleep_saved=0,
                tombstones_received=0,
                tombstones_applied=0,
                tombstones_unresolved=0,
                tombstone_rows_deleted=0,
                error_code=None,
                updated_at=now,
                processing_started_at=None,
                completed_at=None,
            )
            try:
                self.crud.add(db_session, receipt)
                return SubmissionDecision(self.crud.save(db_session, receipt), True, 202)
            except IntegrityError:
                db_session.rollback()
                receipt = self.crud.get_for_update(db_session, batch_id)
                if receipt is None:
                    raise

        if receipt.user_id != user_id or receipt.provider != provider or receipt.payload_sha256 != payload_sha256:
            db_session.rollback()
            raise BatchReceiptConflictError("Batch ID already belongs to a different payload")

        status = SDKBatchReceiptStatus(receipt.status)
        if status == SDKBatchReceiptStatus.SUCCEEDED:
            db_session.rollback()
            return SubmissionDecision(receipt, False, 200)

        if status == SDKBatchReceiptStatus.FAILED and not receipt.retryable:
            db_session.rollback()
            return SubmissionDecision(receipt, False, 409)

        now = self._now()
        if status == SDKBatchReceiptStatus.PROCESSING:
            started_at = receipt.processing_started_at or receipt.updated_at
            if started_at > now - self.stale_processing_after:
                db_session.rollback()
                return SubmissionDecision(receipt, False, 202)

        # Queued receipts are safe to dispatch more than once: the worker claim is
        # serialized. Retryable failures and stale workers are explicitly requeued.
        receipt.status = SDKBatchReceiptStatus.QUEUED
        receipt.retryable = False
        receipt.error_code = None
        receipt.updated_at = now
        receipt.processing_started_at = None
        receipt.completed_at = None
        return SubmissionDecision(self.crud.save(db_session, receipt), True, 202)

    def claim_for_processing(self, db_session: DbSession, batch_id: UUID) -> ClaimDecision:
        receipt = self.crud.get_for_update(db_session, batch_id)
        if receipt is None:
            db_session.rollback()
            return ClaimDecision(receipt_exists=False, should_process=True, attempt_count=None)
        if SDKBatchReceiptStatus(receipt.status) != SDKBatchReceiptStatus.QUEUED:
            db_session.rollback()
            return ClaimDecision(receipt_exists=True, should_process=False, attempt_count=None)

        now = self._now()
        receipt.status = SDKBatchReceiptStatus.PROCESSING
        receipt.attempt_count += 1
        receipt.updated_at = now
        receipt.processing_started_at = now
        self.crud.save(db_session, receipt)
        return ClaimDecision(
            receipt_exists=True,
            should_process=True,
            attempt_count=receipt.attempt_count,
        )

    def mark_succeeded(
        self,
        db_session: DbSession,
        *,
        batch_id: UUID,
        attempt_count: int,
        result: dict,
    ) -> None:
        dropped_count = int(result.get("dropped_count", 0) or 0)
        tombstones_unresolved = int(result.get("tombstones_unresolved", 0) or 0)
        if dropped_count or tombstones_unresolved:
            self.mark_failed(
                db_session,
                batch_id=batch_id,
                attempt_count=attempt_count,
                error_code="dropped_records" if dropped_count else "tombstones_unresolved",
                retryable=False,
                result=result,
            )
            return

        status_code = result.get("status_code")
        processing_error_code = result.get("processing_error_code")
        if status_code != 200 or processing_error_code:
            if processing_error_code:
                error_code = str(processing_error_code)
            elif status_code == 202:
                error_code = "processing_not_terminal"
            else:
                error_code = "worker_processing_failed"
            self.mark_failed(
                db_session,
                batch_id=batch_id,
                attempt_count=attempt_count,
                error_code=error_code,
                retryable=status_code == 202 or not isinstance(status_code, int) or status_code >= 500,
                result=result,
            )
            return

        receipt = self.crud.get_for_update(db_session, batch_id)
        if receipt is None:
            db_session.rollback()
            return
        if (
            SDKBatchReceiptStatus(receipt.status) != SDKBatchReceiptStatus.PROCESSING
            or receipt.attempt_count != attempt_count
        ):
            # A stale worker must never publish over a newer attempt's outcome.
            db_session.rollback()
            return
        now = self._now()
        self._copy_counts(receipt, result)
        receipt.status = SDKBatchReceiptStatus.SUCCEEDED
        receipt.retryable = False
        receipt.error_code = None
        receipt.updated_at = now
        receipt.completed_at = now
        self.crud.save(db_session, receipt)

    def mark_failed(
        self,
        db_session: DbSession,
        *,
        batch_id: UUID,
        attempt_count: int | None = None,
        error_code: str,
        retryable: bool,
        result: dict | None = None,
    ) -> None:
        receipt = self.crud.get_for_update(db_session, batch_id)
        if receipt is None or SDKBatchReceiptStatus(receipt.status) == SDKBatchReceiptStatus.SUCCEEDED:
            db_session.rollback()
            return
        if attempt_count is None and SDKBatchReceiptStatus(receipt.status) != SDKBatchReceiptStatus.QUEUED:
            # The only unfenced writer is the API process reporting that broker
            # dispatch failed before any worker claim. Every worker publication
            # must carry its claimed generation.
            db_session.rollback()
            return
        if attempt_count is not None and (
            SDKBatchReceiptStatus(receipt.status) != SDKBatchReceiptStatus.PROCESSING
            or receipt.attempt_count != attempt_count
        ):
            # The receipt has been requeued and claimed by a newer worker.
            db_session.rollback()
            return
        if result:
            self._copy_counts(receipt, result)
        now = self._now()
        receipt.status = SDKBatchReceiptStatus.FAILED
        receipt.retryable = retryable
        receipt.error_code = error_code[:100]
        receipt.updated_at = now
        receipt.completed_at = now
        self.crud.save(db_session, receipt)

    @staticmethod
    def _copy_counts(receipt: SDKBatchReceipt, result: dict) -> None:
        for field in (
            "dropped_count",
            "records_saved",
            "workouts_saved",
            "sleep_saved",
            "tombstones_received",
            "tombstones_applied",
            "tombstones_unresolved",
            "tombstone_rows_deleted",
        ):
            setattr(receipt, field, int(result.get(field, 0) or 0))

    def get_for_user(self, db_session: DbSession, batch_id: UUID, user_id: UUID) -> SDKBatchReceipt | None:
        receipt = self.crud.get(db_session, batch_id)
        if receipt is None or receipt.user_id != user_id:
            return None
        return receipt

    @staticmethod
    def to_response(receipt: SDKBatchReceipt) -> SDKBatchReceiptResponse:
        status = SDKBatchReceiptStatus(receipt.status)
        accepted = (
            status == SDKBatchReceiptStatus.SUCCEEDED
            and receipt.dropped_count == 0
            and receipt.tombstones_unresolved == 0
        )
        return SDKBatchReceiptResponse(
            batch_id=receipt.id,
            status=status,
            terminal=status in (SDKBatchReceiptStatus.SUCCEEDED, SDKBatchReceiptStatus.FAILED),
            accepted=accepted,
            retryable=receipt.retryable,
            dropped_count=receipt.dropped_count,
            records_saved=receipt.records_saved,
            workouts_saved=receipt.workouts_saved,
            sleep_saved=receipt.sleep_saved,
            tombstones_received=receipt.tombstones_received,
            tombstones_applied=receipt.tombstones_applied,
            tombstones_unresolved=receipt.tombstones_unresolved,
            tombstone_rows_deleted=receipt.tombstone_rows_deleted,
            error_code=receipt.error_code,
            created_at=receipt.created_at,
            updated_at=receipt.updated_at,
            completed_at=receipt.completed_at,
        )


sdk_batch_receipt_service = SDKBatchReceiptService()
