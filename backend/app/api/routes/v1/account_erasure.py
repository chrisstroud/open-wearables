"""Explicitly gated verified deletion; never the legacy DELETE response."""

from uuid import UUID

from fastapi import APIRouter, HTTPException, Response

from app.config import settings
from app.database import DbSession
from app.schemas.account_erasure import (
    AccountErasureOperationRequest,
    AccountErasureOperationResponse,
    AccountErasurePlanRequest,
    AccountErasurePlanResponse,
)
from app.services.account_erasure_service import account_erasure_service
from app.services.api_key_service import AccountErasureApiKeyDep

router = APIRouter()


def _admit(user_id: UUID, request: AccountErasurePlanRequest, response: Response) -> None:
    response.headers["Cache-Control"] = "no-store"
    if not settings.account_erasure_enabled:
        raise HTTPException(status_code=503, detail="Verified account erasure is not enabled")
    if str(user_id) != request.account.open_wearables_user_id:
        raise HTTPException(status_code=409, detail="Account path and request binding do not match")


def _status(response: Response, result: AccountErasurePlanResponse | AccountErasureOperationResponse) -> None:
    if result.status in {"planned", "deleted", "already_deleted"}:
        response.status_code = 200
    elif result.status in {"unavailable", "upstream_unavailable"}:
        response.status_code = 503
    elif result.status in {"identity_mismatch", "scope-rejected", "stale-plan"}:
        response.status_code = 409
    else:
        response.status_code = 202


@router.post("/users/{user_id}/account-erasure/plan")
def plan_account_erasure(
    user_id: UUID,
    request: AccountErasurePlanRequest,
    response: Response,
    db: DbSession,
    _key: AccountErasureApiKeyDep,
) -> AccountErasurePlanResponse:
    _admit(user_id, request, response)
    result = account_erasure_service.plan(db, request)
    _status(response, result)
    return result


@router.post("/users/{user_id}/account-erasure/erase")
def erase_account(
    user_id: UUID,
    request: AccountErasureOperationRequest,
    response: Response,
    db: DbSession,
    _key: AccountErasureApiKeyDep,
) -> AccountErasureOperationResponse:
    _admit(user_id, request, response)
    result = account_erasure_service.erase(db, request)
    _status(response, result)
    return result


@router.post("/users/{user_id}/account-erasure/verify")
def verify_account_erasure(
    user_id: UUID,
    request: AccountErasureOperationRequest,
    response: Response,
    db: DbSession,
    _key: AccountErasureApiKeyDep,
) -> AccountErasureOperationResponse:
    _admit(user_id, request, response)
    result = account_erasure_service.verify(db, request)
    _status(response, result)
    return result
