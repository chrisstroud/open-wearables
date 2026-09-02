from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from logging import getLogger
from uuid import UUID

from sqlalchemy.exc import IntegrityError

from app.database import DbSession
from app.models import SDKBatchReceipt, User
from app.repositories import SDKBatchReceiptRepository
from app.repositories.sdk_upload_inbox_repository import sdk_upload_inbox_repository
from app.schemas.responses.upload import SDKBatchReceiptResponse, SDKBatchReceiptStatus
from app.services.sdk_client_installation_service import sdk_client_installation_service

logger = getLogger(__name__)
MAX_COVERED_TYPE_IDENTIFIERS = 256
LOWERCASE_HEX_DIGITS = frozenset("0123456789abcdef")


def is_revision_set_digest(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(character in LOWERCASE_HEX_DIGITS for character in value)


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
        installation_id: UUID | None = None,
        installation_generation: int | None = None,
        health_evidence_generation: int | None = None,
        provider: str,
        payload_sha256: str,
    ) -> SubmissionDecision:
        receipt = self.crud.get_for_update(db_session, batch_id)
        if receipt is None:
            now = self._now()
            receipt = SDKBatchReceipt(
                id=batch_id,
                user_id=user_id,
                installation_id=installation_id,
                installation_generation=installation_generation,
                health_evidence_generation=health_evidence_generation,
                provider=provider,
                payload_sha256=payload_sha256,
                content_lower_bound_inclusive=None,
                content_upper_bound_exclusive=None,
                covered_type_identifiers=[],
                status=SDKBatchReceiptStatus.QUEUED,
                retryable=False,
                attempt_count=0,
                dropped_count=0,
                records_saved=0,
                daily_summaries_saved=0,
                revision_set_digest=None,
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

        if (
            receipt.user_id != user_id
            or receipt.installation_id != installation_id
            or receipt.installation_generation != installation_generation
            or receipt.health_evidence_generation != health_evidence_generation
            or receipt.provider != provider
            or receipt.payload_sha256 != payload_sha256
        ):
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
        receipt.revision_set_digest = None
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
        commit: bool = True,
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
                commit=commit,
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
                commit=commit,
            )
            return

        daily_summaries_saved = int(result.get("daily_summaries_saved", 0) or 0)
        revision_set_digest = result.get("revision_set_digest")
        if (daily_summaries_saved > 0 and not is_revision_set_digest(revision_set_digest)) or (
            daily_summaries_saved == 0 and revision_set_digest is not None
        ):
            # The importer and receipt publication share a transaction. Roll it
            # back before fencing the receipt so a digest regression cannot
            # commit summary rows under a failed acknowledgement.
            db_session.rollback()
            self.mark_failed(
                db_session,
                batch_id=batch_id,
                attempt_count=attempt_count,
                error_code="daily_summary_revision_set_digest_invalid",
                retryable=False,
                commit=commit,
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
        user = db_session.query(User).filter(User.id == receipt.user_id).with_for_update().one_or_none()
        write_error = (
            "user_not_found"
            if user is None
            else sdk_client_installation_service.health_write_error(
                db_session,
                user=user,
                installation_id=receipt.installation_id,
                installation_generation=receipt.installation_generation,
                health_evidence_generation=receipt.health_evidence_generation,
            )
        )
        now = self._now()
        if write_error is not None:
            receipt.status = SDKBatchReceiptStatus.FAILED
            receipt.retryable = False
            receipt.error_code = write_error
            receipt.updated_at = now
            receipt.completed_at = now
            self.crud.save(db_session, receipt, commit=commit)
            return

        self._copy_counts(receipt, result)
        self._copy_content_coverage(receipt, result)
        receipt.revision_set_digest = revision_set_digest if isinstance(revision_set_digest, str) else None
        receipt.status = SDKBatchReceiptStatus.SUCCEEDED
        receipt.retryable = False
        receipt.error_code = None
        receipt.updated_at = now
        receipt.completed_at = now
        if receipt.installation_id is not None:
            installation = sdk_client_installation_service.crud.get_for_update(db_session, receipt.installation_id)
            assert installation is not None
            installation.last_terminal_receipt_at = now
            if user is not None and user.health_write_state == "activating":
                user.health_write_state = "active"
                user.health_reset_operation_id = None
        sdk_upload_inbox_repository.delete(db_session, batch_id)
        self.crud.save(db_session, receipt, commit=commit)

    def mark_failed(
        self,
        db_session: DbSession,
        *,
        batch_id: UUID,
        attempt_count: int | None = None,
        error_code: str,
        retryable: bool,
        result: dict | None = None,
        commit: bool = True,
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
        # Compact summary acceptance is all-or-nothing; failed receipts never
        # claim that summary rows were durably accepted.
        receipt.daily_summaries_saved = 0
        receipt.revision_set_digest = None
        now = self._now()
        receipt.status = SDKBatchReceiptStatus.FAILED
        receipt.retryable = retryable
        receipt.error_code = error_code[:100]
        receipt.updated_at = now
        receipt.completed_at = now
        self.crud.save(db_session, receipt, commit=commit)

    @staticmethod
    def _copy_counts(receipt: SDKBatchReceipt, result: dict) -> None:
        for field in (
            "dropped_count",
            "records_saved",
            "daily_summaries_saved",
            "workouts_saved",
            "sleep_saved",
            "tombstones_received",
            "tombstones_applied",
            "tombstones_unresolved",
            "tombstone_rows_deleted",
        ):
            setattr(receipt, field, int(result.get(field, 0) or 0))

    @staticmethod
    def _copy_content_coverage(receipt: SDKBatchReceipt, result: dict) -> None:
        """Persist only bounded, worker-validated metadata; never health values."""
        raw_types = result.get("covered_type_identifiers") or []
        bounded_types = sorted({value for value in raw_types if isinstance(value, str) and 0 < len(value) <= 255})
        receipt.covered_type_identifiers = bounded_types if len(bounded_types) <= MAX_COVERED_TYPE_IDENTIFIERS else []
        lower_raw = result.get("content_lower_bound_inclusive")
        upper_raw = result.get("content_upper_bound_exclusive")
        try:
            lower = datetime.fromisoformat(lower_raw) if isinstance(lower_raw, str) else None
            upper = datetime.fromisoformat(upper_raw) if isinstance(upper_raw, str) else None
        except ValueError:
            lower = None
            upper = None
        if lower is None or upper is None or lower.tzinfo is None or upper.tzinfo is None or lower > upper:
            receipt.content_lower_bound_inclusive = None
            receipt.content_upper_bound_exclusive = None
            return
        receipt.content_lower_bound_inclusive = lower
        receipt.content_upper_bound_exclusive = upper

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
            and (receipt.daily_summaries_saved == 0 or is_revision_set_digest(receipt.revision_set_digest))
        )
        return SDKBatchReceiptResponse(
            batch_id=receipt.id,
            status=status,
            terminal=status in (SDKBatchReceiptStatus.SUCCEEDED, SDKBatchReceiptStatus.FAILED),
            accepted=accepted,
            retryable=receipt.retryable,
            dropped_count=receipt.dropped_count,
            records_saved=receipt.records_saved,
            daily_summaries_saved=receipt.daily_summaries_saved,
            revision_set_digest=receipt.revision_set_digest,
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
