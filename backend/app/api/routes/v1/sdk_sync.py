import hashlib
import json
from logging import getLogger
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException, Query, Request, Response, status
from pydantic import ValidationError

from app.config import settings
from app.database import DbSession
from app.integrations.celery.tasks.process_sdk_upload_task import process_sdk_upload
from app.schemas.providers.mobile_sdk import SyncRequest, SyncWindowManifest
from app.schemas.responses.upload import SDKBatchReceiptResponse, SDKSyncWindowReceiptResponse
from app.services.sdk_batch_receipt_service import BatchReceiptConflictError, sdk_batch_receipt_service
from app.services.sdk_client_installation_service import sdk_client_installation_service
from app.services.sdk_sync_window_receipt_service import sdk_sync_window_receipt_service
from app.services.sdk_upload_inbox_service import (
    SDKUploadInboxConflictError,
    SDKUploadInboxStorageError,
    SDKUploadInboxTooLargeError,
    sdk_upload_inbox_service,
)
from app.services.user_service import user_service
from app.utils.api_utils import inline_schema_defs
from app.utils.auth import SDKAuthDep
from app.utils.structured_logging import log_structured

router = APIRouter()


def _receipt_belongs_to_sdk_installation(receipt: object, auth: object) -> bool:
    """Keep one phone generation from observing another generation's receipts."""
    installation_id = getattr(receipt, "installation_id", None)
    installation_generation = getattr(receipt, "installation_generation", None)
    health_evidence_generation = getattr(receipt, "health_evidence_generation", None)
    auth_installation_id = getattr(auth, "installation_id", None)
    if auth_installation_id is None:
        return installation_id is None and installation_generation is None and health_evidence_generation is None
    return (
        installation_id == auth_installation_id
        and installation_generation == getattr(auth, "installation_generation", None)
        and health_evidence_generation == getattr(auth, "health_evidence_generation", None)
    )


logger = getLogger(__name__)


def _payload_too_large() -> HTTPException:
    return HTTPException(
        status_code=status.HTTP_413_CONTENT_TOO_LARGE,
        detail={"error_code": "sdk_upload_too_large", "retryable": False},
    )


async def _read_bounded_json_object(request: Request) -> dict:
    """Enforce the wire-byte limit before decoding or materializing JSON."""
    maximum = settings.sdk_upload_max_size_bytes
    content_length = request.headers.get("content-length")
    if content_length is not None:
        try:
            if int(content_length) > maximum:
                raise _payload_too_large()
        except ValueError:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Invalid Content-Length") from None

    chunks: list[bytes] = []
    received = 0
    async for chunk in request.stream():
        received += len(chunk)
        if received > maximum:
            raise _payload_too_large()
        chunks.append(chunk)

    try:
        body = json.loads(b"".join(chunks))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid JSON body") from exc
    if not isinstance(body, dict):
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="JSON body must be an object")
    return body


@router.post(
    "/sdk/users/{user_id}/sync",
    status_code=status.HTTP_202_ACCEPTED,
    # body is `dict` at runtime; keep the SyncRequest shape in the OpenAPI docs.
    openapi_extra={
        "requestBody": {
            "required": True,
            "content": {"application/json": {"schema": inline_schema_defs(SyncRequest.model_json_schema())}},
        }
    },
)
async def sync_sdk_data(
    user_id: str,
    request: Request,
    auth: SDKAuthDep,
    db: DbSession,
    response: Response,
    x_open_wearables_batch_id: Annotated[
        UUID | None,
        Header(alias="X-Open-Wearables-Batch-ID"),
    ] = None,
) -> SDKBatchReceiptResponse:
    """Import health data from SDK provider asynchronously via Celery.

    Supports Apple HealthKit and Samsung Health SDK formats (identical payloads):
    ```json
    {
        "provider": "apple",
        "sdkVersion": "1.0.0",
        "syncTimestamp": "2021-01-01T00:00:00Z",
        "data": {
            "records": [...],
            "sleep": [...],
            "workouts": [...]
        }
    }
    ```

    Args:
        user_id: SDK user identifier
        body: Health data payload
        auth: SDK authentication (Bearer token or API key)

    Returns:
        Durable batch receipt. HTTP 202 means queued/processing only; HTTP 200
        is returned only after terminal processing with zero dropped records.

    Raises:
        HTTPException: 403 if token doesn't match user_id, 400 if provider unsupported.
        Payload validation runs async in the worker, not here.
    """
    if auth.auth_type == "sdk_token" and (not auth.user_id or str(auth.user_id) != user_id):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Token does not match user_id",
        )

    # Backend-first rollout safety: legacy SDK builds treat every 2xx as a
    # terminal acknowledgement. Returning 425 makes those clients retain their
    # outbox/checkpoint until they upgrade to a build that supplies its durable
    # batch UUID and understands terminal receipts.
    if x_open_wearables_batch_id is None:
        raise HTTPException(
            status_code=status.HTTP_425_TOO_EARLY,
            detail={
                "error_code": "batch_id_required",
                "retryable": True,
                "message": "Upgrade the SDK to send X-Open-Wearables-Batch-ID",
            },
        )

    body = await _read_bounded_json_object(request)

    # Raw dict, not SyncRequest: schema-validating here would 400 the whole batch on one
    # bad record pre-dispatch. The worker validates and reports failures to Sentry.
    provider = str(body.get("provider") or "").lower()

    # Validate provider (routing decision — needed to select an import service)
    if provider not in ("apple", "samsung", "google"):
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Unsupported provider: {provider}. Supported: apple, samsung, google",
        )

    raw_window = body.get("syncWindow")
    if raw_window is not None:
        try:
            window = SyncWindowManifest.model_validate(raw_window)
        except ValidationError as exc:
            safe_errors = exc.errors(
                include_url=False,
                include_context=False,
                include_input=False,
            )
            raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail=safe_errors) from exc
        if window.windowId != x_open_wearables_batch_id:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={"error_code": "window_id_mismatch", "retryable": False},
            )

    try:
        user_uuid = UUID(user_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid user_id") from exc
    user = user_service.get(db, user_uuid, print_log=False)
    if user is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
    if auth.auth_type == "api_key" and (
        user.health_source_policy != "legacy-mixed"
        or bool(sdk_client_installation_service.crud.list_for_user(db, user_uuid))
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="API-key upload is disabled for a first-class mobile installation",
        )

    if body.get("schema_version") == "apple-health-daily-summary.v1" and auth.protocol_version != 3:
        raise HTTPException(
            status_code=status.HTTP_426_UPGRADE_REQUIRED,
            detail={
                "error_code": "daily_summary_protocol_version_required",
                "retryable": False,
                "required_protocol_version": 3,
            },
        )

    # The UUID already exists in the phone's protected outbox before upload.
    batch_uuid = x_open_wearables_batch_id

    # Extract and count data types from payload (best-effort; structure not yet validated)
    raw_data = body.get("data")
    data = raw_data if isinstance(raw_data, dict) else {}
    records = data.get("records")
    workouts = data.get("workouts")
    sleep = data.get("sleep")
    deletions = data.get("deletions")
    daily_summaries = data.get("daily_summaries")
    records_count = len(records) if isinstance(records, list) else 0
    workouts_count = len(workouts) if isinstance(workouts, list) else 0
    sleep_count = len(sleep) if isinstance(sleep, list) else 0
    deletions_count = len(deletions) if isinstance(deletions, list) else 0
    daily_summaries_count = len(daily_summaries) if isinstance(daily_summaries, list) else 0

    # Log initial batch receipt with counts
    log_structured(
        logger,
        "info",
        f"{provider.capitalize()} sync batch received",
        action=f"{provider}_sdk_batch_received",
        batch_id=str(batch_uuid),
        user_id=user_id,
        provider=provider,
        records_count=records_count,
        workouts_count=workouts_count,
        sleep_count=sleep_count,
        deletions_count=deletions_count,
        daily_summaries_count=daily_summaries_count,
        total_items=records_count + workouts_count + sleep_count + deletions_count + daily_summaries_count,
    )

    content_str = json.dumps(body, separators=(",", ":"), sort_keys=True)
    try:
        sdk_upload_inbox_service.validate_content_size(content_str)
    except SDKUploadInboxTooLargeError as exc:
        raise HTTPException(
            status_code=status.HTTP_413_CONTENT_TOO_LARGE,
            detail={"error_code": "sdk_upload_too_large", "retryable": False},
        ) from exc
    payload_sha256 = hashlib.sha256(content_str.encode("utf-8")).hexdigest()

    try:
        decision = sdk_batch_receipt_service.prepare_submission(
            db,
            batch_id=batch_uuid,
            user_id=user_uuid,
            installation_id=auth.installation_id,
            installation_generation=auth.installation_generation,
            health_evidence_generation=auth.health_evidence_generation,
            provider=provider,
            payload_sha256=payload_sha256,
        )
    except BatchReceiptConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc

    response.status_code = decision.http_status
    if not decision.should_dispatch:
        return sdk_batch_receipt_service.to_response(decision.receipt)

    try:
        sdk_upload_inbox_service.put(
            db,
            batch_id=batch_uuid,
            user_id=user_uuid,
            installation_id=auth.installation_id,
            installation_generation=auth.installation_generation,
            health_evidence_generation=auth.health_evidence_generation,
            provider=provider,
            payload_sha256=payload_sha256,
            content_type="application/json",
            content=content_str,
        )
    except SDKUploadInboxConflictError as exc:
        raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail=str(exc)) from exc
    except SDKUploadInboxTooLargeError as exc:
        raise HTTPException(
            status_code=status.HTTP_413_CONTENT_TOO_LARGE,
            detail={"error_code": "sdk_upload_too_large", "retryable": False},
        ) from exc
    except SDKUploadInboxStorageError as exc:
        sdk_batch_receipt_service.mark_failed(
            db,
            batch_id=batch_uuid,
            error_code="upload_inbox_unavailable",
            retryable=True,
        )
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail={"error_code": "upload_inbox_unavailable", "retryable": True},
        ) from exc

    try:
        process_sdk_upload.delay(
            batch_id=str(batch_uuid),
            require_terminal_receipt=True,
        )
    except Exception:
        logger.exception("Failed to dispatch SDK batch %s", batch_uuid)
        sdk_batch_receipt_service.mark_failed(
            db,
            batch_id=batch_uuid,
            error_code="dispatch_failed",
            retryable=True,
        )
        receipt = sdk_batch_receipt_service.get_for_user(db, batch_uuid, user_uuid)
        if receipt is None:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Import dispatch failed")
        response.status_code = status.HTTP_503_SERVICE_UNAVAILABLE
        return sdk_batch_receipt_service.to_response(receipt)

    return sdk_batch_receipt_service.to_response(decision.receipt)


@router.get(
    "/sdk/users/{user_id}/sync/{batch_id}",
)
def get_sdk_batch_receipt(
    user_id: str,
    batch_id: UUID,
    auth: SDKAuthDep,
    db: DbSession,
) -> SDKBatchReceiptResponse:
    """Return durable processing status for a previously submitted SDK batch."""
    if auth.auth_type == "sdk_token" and (not auth.user_id or str(auth.user_id) != user_id):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Token does not match user_id")
    try:
        user_uuid = UUID(user_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid user_id") from exc
    receipt = sdk_batch_receipt_service.get_for_user(db, batch_id, user_uuid)
    if receipt is None or (auth.auth_type == "sdk_token" and not _receipt_belongs_to_sdk_installation(receipt, auth)):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Batch receipt not found")
    return sdk_batch_receipt_service.to_response(receipt)


@router.get("/sdk/users/{user_id}/sync-windows/{window_id}")
def get_sdk_sync_window_receipt(
    user_id: str,
    window_id: UUID,
    auth: SDKAuthDep,
    db: DbSession,
) -> SDKSyncWindowReceiptResponse:
    """Return one accepted bounded sync-window authority."""
    if auth.auth_type == "sdk_token" and (not auth.user_id or str(auth.user_id) != user_id):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Token does not match user_id")
    try:
        user_uuid = UUID(user_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid user_id") from exc
    receipt = sdk_sync_window_receipt_service.get_for_user(
        db,
        user_id=user_uuid,
        window_id=window_id,
    )
    if receipt is None or (auth.auth_type == "sdk_token" and not _receipt_belongs_to_sdk_installation(receipt, auth)):
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Sync window receipt not found")
    return sdk_sync_window_receipt_service.to_response(receipt)


@router.get("/sdk/users/{user_id}/sync-windows")
def list_sdk_sync_window_receipts(
    user_id: str,
    auth: SDKAuthDep,
    db: DbSession,
    provider: str | None = None,
    limit: Annotated[int, Query(ge=1, le=200)] = 50,
) -> list[SDKSyncWindowReceiptResponse]:
    """List accepted bounded sync-window authorities newest first."""
    if auth.auth_type == "sdk_token" and (not auth.user_id or str(auth.user_id) != user_id):
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Token does not match user_id")
    try:
        user_uuid = UUID(user_id)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid user_id") from exc
    receipts = sdk_sync_window_receipt_service.list_for_user(
        db,
        user_id=user_uuid,
        provider=provider,
        limit=limit,
    )
    if auth.auth_type == "sdk_token":
        receipts = [receipt for receipt in receipts if _receipt_belongs_to_sdk_installation(receipt, auth)]
    return [sdk_sync_window_receipt_service.to_response(receipt) for receipt in receipts]
