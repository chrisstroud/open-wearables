import hashlib
import uuid
from datetime import datetime, timezone
from logging import getLogger
from typing import Any
from uuid import UUID

from celery import shared_task

from app.database import SessionLocal
from app.models import User
from app.repositories.sdk_upload_inbox_repository import sdk_upload_inbox_repository
from app.repositories.user_connection_repository import UserConnectionRepository
from app.repositories.user_repository import UserRepository
from app.schemas.sync_status import SyncSource, SyncStatus
from app.services.apple.healthkit.import_service import (
    ImportService as SDKImportService,
)
from app.services.apple.healthkit.import_service import (
    import_service as sdk_import_service,
)
from app.services.apple.healthkit.import_service import validated_content_coverage
from app.services.sdk_batch_receipt_service import is_revision_set_digest, sdk_batch_receipt_service
from app.services.sdk_client_installation_service import sdk_client_installation_service
from app.services.sdk_sleep_inbox_service import sdk_sleep_inbox_service
from app.services.sync_status_service import completed, failed, started
from app.utils.structured_logging import log_structured

logger = getLogger(__name__)


def _get_import_service(provider: str) -> SDKImportService:
    if provider in ("apple", "samsung", "google"):
        return sdk_import_service
    raise ValueError(f"Unsupported provider: {provider}")


@shared_task(queue="sdk_sync")
def process_sdk_upload(
    content: str | None = None,
    content_type: str | None = None,
    user_id: str | None = None,
    provider: str | None = None,
    batch_id: str | None = None,
    require_terminal_receipt: bool = False,
) -> dict[str, Any]:
    """
    Process SDK data import asynchronously.

    Args:
        content: Legacy in-message payload. Receipt-backed calls load the
            content from the durable database inbox instead.
        content_type: Legacy in-message content type.
        user_id: Legacy in-message user ID.
        provider: Legacy in-message provider.
        batch_id: Unique batch identifier for tracking (optional for backwards compatibility)
        require_terminal_receipt: True only for the new durable-receipt route contract

    Returns:
        Dictionary with status_code and response message
    """
    # Generate batch_id if not provided (backwards compatibility)
    if not batch_id:
        batch_id = str(uuid.uuid4())

    batch_uuid: UUID | None = None
    receipt_exists = False
    receipt_attempt: int | None = None
    if require_terminal_receipt:
        try:
            batch_uuid = UUID(batch_id)
        except ValueError:
            return {"status": "error", "reason": "invalid_batch_id", "batch_id": batch_id}
        with SessionLocal() as db:
            claim = sdk_batch_receipt_service.claim_for_processing(db, batch_uuid)
        receipt_exists = claim.receipt_exists
        if not claim.receipt_exists:
            return {"status": "error", "reason": "receipt_missing", "batch_id": batch_id}
        if claim.receipt_exists and not claim.should_process:
            return {"status": "duplicate", "reason": "receipt_not_queued", "batch_id": batch_id}
        receipt_attempt = claim.attempt_count

        if content is None:
            with SessionLocal() as db:
                inbox = sdk_upload_inbox_repository.get(db, batch_uuid)
                if inbox is not None:
                    content = inbox.content
                    content_type = inbox.content_type
                    user_id = str(inbox.user_id)
                    provider = inbox.provider
            if content is None:
                assert receipt_attempt is not None
                with SessionLocal() as db:
                    sdk_batch_receipt_service.mark_failed(
                        db,
                        batch_id=batch_uuid,
                        attempt_count=receipt_attempt,
                        error_code="upload_inbox_missing",
                        retryable=False,
                    )
                return {"status": "error", "reason": "upload_inbox_missing", "batch_id": batch_id}

    if content is None or content_type is None or user_id is None or provider is None:
        return {"status": "error", "reason": "task_payload_missing", "batch_id": batch_id}

    # Validate user_id format
    try:
        user_uuid = UUID(user_id)
    except (TypeError, ValueError):
        log_structured(
            logger,
            "warning",
            "Invalid user_id format",
            provider=provider,
            action="validate_user_id",
            batch_id=batch_id,
            user_id=user_id,
        )
        return {"status": "error", "reason": "invalid_user_id", "batch_id": batch_id}

    # Log task start
    log_structured(
        logger,
        "info",
        f"{provider.capitalize()} sync batch processing started",
        action=f"{provider}_batch_processing_start",
        batch_id=batch_id,
        user_id=user_id,
        provider=provider,
    )

    started(
        user_uuid,
        provider,
        SyncSource.SDK,
        run_id=batch_id,
        message=f"Processing {provider} SDK batch",
        metadata={"batch_id": batch_id},
    )

    try:
        with SessionLocal() as db:
            # The user row is the account-wide health write lock. Validation
            # and every normalized write happen in this same transaction, so a
            # reset fence or replacement cannot interleave after validation.
            user = UserRepository(User).get(db, user_uuid)
            if user is None:
                log_structured(
                    logger,
                    "warning",
                    "Skipping import for non-existent user",
                    provider=provider,
                    action="validate_user_exists",
                    batch_id=batch_id,
                    user_id=user_id,
                )
                if receipt_exists:
                    assert receipt_attempt is not None
                    assert batch_uuid is not None
                    sdk_batch_receipt_service.mark_failed(
                        db,
                        batch_id=batch_uuid,
                        attempt_count=receipt_attempt,
                        error_code="user_not_found",
                        retryable=False,
                    )
                return {"status": "skipped", "reason": "user_not_found", "batch_id": batch_id}

            # The preliminary repository lookup preserves the task's legacy
            # user-not-found contract. This query is the authority boundary:
            # it locks the exact row, then refreshes the instance so a reset or
            # installation replacement that won the lock is observed before
            # any normalized write begins.
            db.query(User.id).filter(User.id == user_uuid).with_for_update().one_or_none()
            db.refresh(user)

            inbox = sdk_upload_inbox_repository.get(db, batch_uuid) if batch_uuid is not None else None
            if receipt_exists:
                assert batch_uuid is not None
                assert receipt_attempt is not None
                receipt = sdk_batch_receipt_service.crud.get_for_update(db, batch_uuid)
                inbox_digest = hashlib.sha256(content.encode("utf-8")).hexdigest()
                scope_matches = (
                    receipt is not None
                    and inbox is not None
                    and receipt.status == "processing"
                    and receipt.attempt_count == receipt_attempt
                    and inbox.user_id == receipt.user_id == user_uuid
                    and inbox.provider == receipt.provider == provider
                    and inbox.payload_sha256 == receipt.payload_sha256 == inbox_digest
                    and inbox.installation_id == receipt.installation_id
                    and inbox.installation_generation == receipt.installation_generation
                    and inbox.health_evidence_generation == receipt.health_evidence_generation
                    and inbox.content_type == content_type
                    and inbox.expires_at > datetime.now(timezone.utc)
                )
                if not scope_matches:
                    sdk_batch_receipt_service.mark_failed(
                        db,
                        batch_id=batch_uuid,
                        attempt_count=receipt_attempt,
                        error_code="upload_inbox_scope_mismatch",
                        retryable=False,
                        commit=False,
                    )
                    db.commit()
                    return {
                        "status": "skipped",
                        "reason": "upload_inbox_scope_mismatch",
                        "batch_id": batch_id,
                    }
            write_error = sdk_client_installation_service.health_write_error(
                db,
                user=user,
                installation_id=inbox.installation_id if inbox is not None else None,
                installation_generation=inbox.installation_generation if inbox is not None else None,
                health_evidence_generation=inbox.health_evidence_generation if inbox is not None else None,
            )
            if write_error is not None:
                log_structured(
                    logger,
                    "warning",
                    "Skipping SDK import because its write generation is no longer active",
                    provider=provider,
                    action="validate_health_write_generation",
                    batch_id=batch_id,
                    user_id=user_id,
                    error_code=write_error,
                )
                if receipt_exists:
                    assert receipt_attempt is not None
                    assert batch_uuid is not None
                    sdk_batch_receipt_service.mark_failed(
                        db,
                        batch_id=batch_uuid,
                        attempt_count=receipt_attempt,
                        error_code=write_error,
                        retryable=False,
                    )
                return {"status": "skipped", "reason": write_error, "batch_id": batch_id}

            if inbox is not None and inbox.installation_id is not None:
                db.info["health_write_authority"] = (
                    user_uuid,
                    inbox.health_evidence_generation,
                    inbox.installation_id,
                    inbox.installation_generation,
                )

            # Ensure SDK connection exists for this user (SDK-based, no OAuth tokens)
            connection_repo = UserConnectionRepository()
            connection_repo.ensure_sdk_connection(db, user_uuid, provider, commit=False)

            # Select the appropriate import service based on source
            import_service = _get_import_service(provider)

            result = import_service.import_data_from_request(
                db,
                content,
                content_type,
                user_id,
                batch_id=batch_id,
                require_terminal_receipt=require_terminal_receipt,
            ).model_dump()

            preliminary_terminal_success = (
                result.get("status_code", 200) == 200
                and int(result.get("dropped_count", 0) or 0) == 0
                and int(result.get("tombstones_unresolved", 0) or 0) == 0
                and not result.get("processing_error_code")
            )
            if receipt_exists and preliminary_terminal_success:
                # Re-parse the durable inbox content before publication. This
                # independently supplies the canonical compact revision digest
                # that is persisted on the terminal receipt.
                result.update(validated_content_coverage(content))

            # Log processing completion with results
            log_structured(
                logger,
                "info",
                f"{provider.capitalize()} sync batch processing completed",
                action=f"{provider}_batch_processing_complete",
                batch_id=batch_id,
                user_id=user_id,
                provider=provider,
                status_code=result.get("status_code"),
                response=result.get("response"),
                # Include counts from result if available
                records_saved=result.get("records_saved", 0),
                daily_summaries_saved=result.get("daily_summaries_saved", 0),
                revision_set_digest=result.get("revision_set_digest"),
                workouts_saved=result.get("workouts_saved", 0),
                sleep_saved=result.get("sleep_saved", 0),
                tombstones_applied=result.get("tombstones_applied", 0),
                tombstones_unresolved=result.get("tombstones_unresolved", 0),
            )

            status_code = result.get("status_code", 200)
            records_saved = int(result.get("records_saved", 0) or 0)
            daily_summaries_saved = int(result.get("daily_summaries_saved", 0) or 0)
            revision_set_digest = result.get("revision_set_digest")
            workouts_saved = int(result.get("workouts_saved", 0) or 0)
            sleep_saved = int(result.get("sleep_saved", 0) or 0)
            dropped_count = int(result.get("dropped_count", 0) or 0)
            tombstones_unresolved = int(result.get("tombstones_unresolved", 0) or 0)
            processing_error_code = result.get("processing_error_code")
            if (daily_summaries_saved > 0 and not is_revision_set_digest(revision_set_digest)) or (
                daily_summaries_saved == 0 and revision_set_digest is not None
            ):
                processing_error_code = "daily_summary_revision_set_digest_invalid"
                result["processing_error_code"] = processing_error_code
            types = result.get("types") or []
            items_total = records_saved + daily_summaries_saved + workouts_saved + sleep_saved
            terminal_success = (
                status_code == 200 and dropped_count == 0 and tombstones_unresolved == 0 and not processing_error_code
            )

            if terminal_success:
                if receipt_exists:
                    assert receipt_attempt is not None
                    assert batch_uuid is not None
                    sdk_batch_receipt_service.mark_succeeded(
                        db,
                        batch_id=batch_uuid,
                        attempt_count=receipt_attempt,
                        result=result,
                        commit=False,
                    )
                    db.commit()
                    if sleep_saved:
                        sdk_sleep_inbox_service.schedule_projection(user_id=user_uuid, provider=provider)
                completed(
                    user_uuid,
                    provider,
                    SyncSource.SDK,
                    run_id=batch_id,
                    status=SyncStatus.SUCCESS,
                    message=f"{provider.capitalize()} batch saved",
                    items_processed=items_total,
                    metadata={
                        "batch_id": batch_id,
                        "records_saved": records_saved,
                        "daily_summaries_saved": daily_summaries_saved,
                        "revision_set_digest": revision_set_digest,
                        "workouts_saved": workouts_saved,
                        "sleep_saved": sleep_saved,
                        "types": types,
                        "dropped_count": 0,
                        "tombstones_applied": result.get("tombstones_applied", 0),
                    },
                )
            else:
                if processing_error_code:
                    error_code = str(processing_error_code)
                    retryable = error_code == "window_sleep_projection_pending"
                elif tombstones_unresolved:
                    error_code = str(result.get("tombstone_error_code") or "tombstones_unresolved")
                    retryable = False
                elif dropped_count:
                    error_code = "dropped_records"
                    retryable = False
                elif status_code == 202:
                    error_code = "processing_not_terminal"
                    retryable = True
                elif status_code == 400 and str(result.get("response", "")).startswith("Validation failed"):
                    error_code = "validation_failed"
                    retryable = False
                else:
                    error_code = "worker_processing_failed"
                    retryable = True
                if receipt_exists:
                    assert receipt_attempt is not None
                    assert batch_uuid is not None
                    if error_code == "daily_summary_revision_set_digest_invalid":
                        # The importer may already have flushed compact rows in
                        # this transaction. Discard them before publishing the
                        # failed receipt in a fresh transaction.
                        db.rollback()
                    sdk_batch_receipt_service.mark_failed(
                        db,
                        batch_id=batch_uuid,
                        attempt_count=receipt_attempt,
                        error_code=error_code,
                        retryable=retryable,
                        result=result,
                        commit=False,
                    )
                    db.commit()
                failed(
                    user_uuid,
                    provider,
                    SyncSource.SDK,
                    run_id=batch_id,
                    error=error_code,
                    message=f"{provider.capitalize()} batch failed",
                    metadata={
                        "batch_id": batch_id,
                        "status_code": status_code,
                        "dropped_count": dropped_count,
                        "tombstones_unresolved": tombstones_unresolved,
                    },
                )

            return {**result, "batch_id": batch_id}
    except Exception:
        if receipt_exists:
            assert batch_uuid is not None
            with SessionLocal() as receipt_db:
                sdk_batch_receipt_service.mark_failed(
                    receipt_db,
                    batch_id=batch_uuid,
                    attempt_count=receipt_attempt,
                    error_code="worker_exception",
                    retryable=True,
                )
        raise
