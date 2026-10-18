from contextlib import suppress
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy.exc import IntegrityError

from app.database import DbSession
from app.models import WhoopHistoryDispatch
from app.repositories.health_write_authority import HealthWriteAuthorityError
from app.repositories.whoop_history_repository import HistoryConflictError, WhoopHistoryRepository
from app.schemas.whoop_history import WhoopHistoryReceipt, WhoopHistoryRequest

repository = WhoopHistoryRepository()


def receipt(dispatch: WhoopHistoryDispatch) -> WhoopHistoryReceipt:
    return WhoopHistoryReceipt.model_validate(
        {
            "dispatch_id": dispatch.id,
            "user_id": dispatch.user_id,
            "connection_id": dispatch.connection_id,
            "authorization_generation": dispatch.authorization_generation,
            "task_id": dispatch.task_id,
            "status": dispatch.status,
            "requested_start_at": dispatch.requested_start_at,
            "requested_end_at": dispatch.requested_end_at,
            "completed_at": dispatch.completed_at,
            "error_code": dispatch.error_code,
        }
    )


def publish_history_dispatch(dispatch_id: UUID, task_id: UUID) -> None:
    # Imports stay local to avoid Celery task/service registration cycles.
    from app.integrations.celery.tasks.whoop_history_task import sync_whoop_history

    sync_whoop_history.apply_async(args=[str(dispatch_id)], task_id=str(task_id))


def admit_history(
    db: DbSession, user_id: UUID, connection_id: UUID, request: WhoopHistoryRequest
) -> WhoopHistoryReceipt:
    try:
        dispatch = repository.admit(
            db,
            user_id=user_id,
            connection_id=connection_id,
            generation=request.authorization_generation,
            dispatch_id=request.idempotency_key,
            start=request.requested_start_at,
            end=request.requested_end_at,
        )
    except HealthWriteAuthorityError as exc:
        db.rollback()
        raise HTTPException(status_code=409, detail="authorization_changed") from exc
    except (HistoryConflictError, IntegrityError) as exc:
        db.rollback()
        code = str(exc) if isinstance(exc, HistoryConflictError) else "idempotency_conflict"
        raise HTTPException(status_code=409, detail=code) from exc
    result = receipt(dispatch)
    if dispatch.status == "queued":
        # PostgreSQL is the durable outbox. Beat retries enqueueing; do not
        # falsely reject an already committed operation on broker timeout.
        with suppress(Exception):
            publish_history_dispatch(dispatch.id, dispatch.task_id)
    return result


def get_history(db: DbSession, user_id: UUID, dispatch_id: UUID) -> WhoopHistoryReceipt:
    dispatch = repository.get(db, user_id, dispatch_id)
    if dispatch is None:
        raise HTTPException(status_code=404, detail="history_not_found")
    return receipt(dispatch)
