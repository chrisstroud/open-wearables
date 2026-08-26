import uuid
from logging import getLogger
from typing import Any
from uuid import UUID

from celery import shared_task

from app.database import SessionLocal
from app.models import User
from app.repositories.user_connection_repository import UserConnectionRepository
from app.repositories.user_repository import UserRepository
from app.schemas.sync_status import SyncSource, SyncStatus
from app.services.apple.healthkit.import_service import (
    ImportService as SDKImportService,
)
from app.services.apple.healthkit.import_service import (
    import_service as sdk_import_service,
)
from app.services.sdk_batch_receipt_service import sdk_batch_receipt_service
from app.services.sync_status_service import completed, failed, started
from app.utils.structured_logging import log_structured

logger = getLogger(__name__)


def _get_import_service(provider: str) -> SDKImportService:
    if provider in ("apple", "samsung", "google"):
        return sdk_import_service
    raise ValueError(f"Unsupported provider: {provider}")


@shared_task(queue="sdk_sync")
def process_sdk_upload(
    content: str,
    content_type: str,
    user_id: str,
    provider: str,
    batch_id: str | None = None,
    require_terminal_receipt: bool = False,
) -> dict[str, Any]:
    """
    Process SDK data import asynchronously.

    Args:
        content: The request content as string (JSON or multipart data)
        content_type: The content type header value
        user_id: User ID to associate with the data
        provider: Import provider - "apple", "samsung", "google"
        batch_id: Unique batch identifier for tracking (optional for backwards compatibility)
        require_terminal_receipt: True only for the new durable-receipt route contract

    Returns:
        Dictionary with status_code and response message
    """
    # Generate batch_id if not provided (backwards compatibility)
    if not batch_id:
        batch_id = str(uuid.uuid4())

    # Validate user_id format
    try:
        user_uuid = UUID(user_id)
    except ValueError:
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

    # Validate user exists before processing
    with SessionLocal() as db:
        user_repo = UserRepository(User)
        if not user_repo.get(db, user_uuid):
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
            # Ensure SDK connection exists for this user (SDK-based, no OAuth tokens)
            connection_repo = UserConnectionRepository()
            connection_repo.ensure_sdk_connection(db, user_uuid, provider)

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
                workouts_saved=result.get("workouts_saved", 0),
                sleep_saved=result.get("sleep_saved", 0),
                tombstones_applied=result.get("tombstones_applied", 0),
                tombstones_unresolved=result.get("tombstones_unresolved", 0),
            )

            status_code = result.get("status_code", 200)
            records_saved = int(result.get("records_saved", 0) or 0)
            workouts_saved = int(result.get("workouts_saved", 0) or 0)
            sleep_saved = int(result.get("sleep_saved", 0) or 0)
            dropped_count = int(result.get("dropped_count", 0) or 0)
            tombstones_unresolved = int(result.get("tombstones_unresolved", 0) or 0)
            processing_error_code = result.get("processing_error_code")
            types = result.get("types") or []
            items_total = records_saved + workouts_saved + sleep_saved
            terminal_success = (
                status_code == 200 and dropped_count == 0 and tombstones_unresolved == 0 and not processing_error_code
            )

            if terminal_success:
                if receipt_exists:
                    assert receipt_attempt is not None
                    assert batch_uuid is not None
                    with SessionLocal() as receipt_db:
                        sdk_batch_receipt_service.mark_succeeded(
                            receipt_db,
                            batch_id=batch_uuid,
                            attempt_count=receipt_attempt,
                            result=result,
                        )
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
                    with SessionLocal() as receipt_db:
                        sdk_batch_receipt_service.mark_failed(
                            receipt_db,
                            batch_id=batch_uuid,
                            attempt_count=receipt_attempt,
                            error_code=error_code,
                            retryable=retryable,
                            result=result,
                        )
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
