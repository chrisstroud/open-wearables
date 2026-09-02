from typing import Literal, cast
from uuid import UUID

from fastapi import APIRouter, HTTPException, status

from app.database import DbSession
from app.models import SDKClientInstallation
from app.repositories.sdk_client_installation_repository import sdk_client_installation_repository
from app.schemas.model_crud.credentials import (
    SDKClientInstallationRead,
    SDKClientInstallationRevocationRead,
    SDKClientInstallationRevokeRequest,
    SDKHealthResetStateRead,
    SDKHealthResetTransitionRequest,
)
from app.services import ApiKeyDep, SourceResetApiKeyDep
from app.services.sdk_client_installation_service import sdk_client_installation_service
from app.services.sdk_source_reset_service import sdk_source_reset_service
from app.utils.auth import SDKRevocationAuthDep

router = APIRouter()


def _installation_read(db: DbSession, row: SDKClientInstallation) -> SDKClientInstallationRead:
    recent_ready_at, archive_frontier = sdk_client_installation_repository.readiness_for(db, row)
    return SDKClientInstallationRead(
        id=row.id,
        user_id=row.user_id,
        bundle_id=row.bundle_id,
        app_version=row.app_version,
        build_number=row.build_number,
        protocol_version=row.protocol_version,
        generation=row.generation,
        health_evidence_generation=row.health_evidence_generation,
        status=cast(Literal["active", "revoked"], row.status),
        connected_at=row.connected_at,
        last_contact_at=row.last_contact_at,
        last_terminal_receipt_at=row.last_terminal_receipt_at,
        recent_history_ready_at=recent_ready_at,
        archive_earliest_confirmed_at=archive_frontier,
        revoked_at=row.revoked_at,
    )


@router.get("/users/{user_id}/sdk-installations")
def list_sdk_client_installations(
    user_id: UUID,
    db: DbSession,
    _api_key: ApiKeyDep,
) -> list[SDKClientInstallationRead]:
    """List the privacy-safe mobile installations for one exact user."""
    return [_installation_read(db, row) for row in sdk_client_installation_repository.list_for_user(db, user_id)]


@router.post("/users/{user_id}/sdk-installations/{installation_id}/revoke")
def revoke_sdk_client_installation(
    user_id: UUID,
    installation_id: UUID,
    db: DbSession,
    _api_key: ApiKeyDep,
    payload: SDKClientInstallationRevokeRequest,
) -> SDKClientInstallationRead:
    """Idempotently fence one exact mobile installation without deleting evidence."""
    row = sdk_client_installation_service.revoke(
        db,
        user_id=user_id,
        installation_id=installation_id,
        expected_generation=payload.expected_generation,
        expected_health_evidence_generation=payload.expected_health_evidence_generation,
    )
    return _installation_read(db, row)


@router.post("/internal/source-resets/{user_id}/inventory")
def inventory_sdk_health_reset(
    user_id: UUID,
    payload: SDKHealthResetTransitionRequest,
    db: DbSession,
    _api_key: SourceResetApiKeyDep,
) -> SDKHealthResetStateRead:
    """Return exact PHI-free counts and a stable source-reset manifest digest."""
    return sdk_source_reset_service.inspect(db, user_id=user_id, request=payload)


@router.post("/internal/source-resets/{user_id}/fence")
def fence_sdk_health_reset(
    user_id: UUID,
    payload: SDKHealthResetTransitionRequest,
    db: DbSession,
    _api_key: SourceResetApiKeyDep,
) -> SDKHealthResetStateRead:
    """Fence every health writer and invalidate outstanding mobile authority."""
    return sdk_source_reset_service.fence(db, user_id=user_id, request=payload)


@router.post("/internal/source-resets/{user_id}/drain")
def drain_sdk_health_reset(
    user_id: UUID,
    payload: SDKHealthResetTransitionRequest,
    db: DbSession,
    _api_key: SourceResetApiKeyDep,
) -> SDKHealthResetStateRead:
    """Prove all durable SDK writers are terminal or quarantined."""
    return sdk_source_reset_service.drain(db, user_id=user_id, request=payload)


@router.post("/internal/source-resets/{user_id}/apply")
def apply_sdk_health_reset(
    user_id: UUID,
    payload: SDKHealthResetTransitionRequest,
    db: DbSession,
    _api_key: SourceResetApiKeyDep,
) -> SDKHealthResetStateRead:
    """Erase all user-bound health/provider evidence, preserve User identity, and advance authority."""
    return sdk_source_reset_service.apply(db, user_id=user_id, request=payload)


@router.post("/internal/source-resets/{user_id}/verify")
def verify_sdk_health_reset(
    user_id: UUID,
    payload: SDKHealthResetTransitionRequest,
    db: DbSession,
    _api_key: SourceResetApiKeyDep,
) -> SDKHealthResetStateRead:
    """Fail closed unless every governed Open Wearables plane is empty."""
    return sdk_source_reset_service.verify(
        db,
        user_id=user_id,
        request=payload,
    )


@router.post("/sdk/users/{user_id}/installation/revoke")
def revoke_current_sdk_client_installation(
    user_id: UUID,
    db: DbSession,
    auth: SDKRevocationAuthDep,
) -> SDKClientInstallationRevocationRead:
    """Allow an active phone to disconnect only its own installation."""
    if (
        auth.auth_type != "sdk_token"
        or auth.user_id != user_id
        or auth.installation_id is None
        or auth.installation_generation is None
        or auth.health_evidence_generation is None
    ):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Token does not identify an active mobile installation",
        )
    row = sdk_client_installation_service.revoke(
        db,
        user_id=user_id,
        installation_id=auth.installation_id,
        expected_generation=auth.installation_generation,
        expected_health_evidence_generation=auth.health_evidence_generation,
    )
    if row.status != "revoked" or row.revoked_at is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Mobile installation revocation did not become terminal",
        )
    return SDKClientInstallationRevocationRead(
        installation_id=row.id,
        status="revoked",
        revoked_at=row.revoked_at,
    )
