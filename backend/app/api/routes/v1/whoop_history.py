from uuid import UUID

from fastapi import APIRouter, status

from app.database import DbSession
from app.schemas.whoop_history import WhoopHistoryReceipt, WhoopHistoryRequest
from app.services import ApiKeyDep
from app.services.whoop_history_service import admit_history, get_history

router = APIRouter()


@router.post(
    "/whoop/users/{user_id}/connections/{connection_id}/sync/full-history",
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        400: {"description": "Invalid window or request"},
        409: {"description": "Authorization or request conflict"},
        "4XX": {"description": "Authentication or request rejected"},
    },
)
def start_history(
    user_id: UUID,
    connection_id: UUID,
    request: WhoopHistoryRequest,
    db: DbSession,
    _api_key: ApiKeyDep,
) -> WhoopHistoryReceipt:
    """Admit one idempotent, generation-bound WHOOP history import."""
    return admit_history(db, user_id, connection_id, request)


@router.get(
    "/whoop/users/{user_id}/sync/full-history/{dispatch_id}",
    responses={
        404: {"description": "No receipt for this user"},
        "4XX": {"description": "Authentication or request rejected"},
    },
)
def read_history(user_id: UUID, dispatch_id: UUID, db: DbSession, _api_key: ApiKeyDep) -> WhoopHistoryReceipt:
    """Read the durable receipt, including after a lost admission reply."""
    return get_history(db, user_id, dispatch_id)
