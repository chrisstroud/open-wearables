from typing import cast
from uuid import UUID

from celery import shared_task

from app.database import SessionLocal
from app.models import WhoopHistoryDispatch
from app.repositories.health_write_authority import (
    HealthWriteAuthorityError,
    WhoopHistoryAuthority,
    require_health_write_authority,
)
from app.repositories.whoop_history_repository import WhoopHistoryRepository
from app.services.providers.factory import ProviderFactory
from app.services.providers.whoop.data_247 import Whoop247Data

repository = WhoopHistoryRepository()


@shared_task(soft_time_limit=840, time_limit=900, acks_late=True, reject_on_worker_lost=True)
def sync_whoop_history(dispatch_id: str) -> None:
    target = UUID(dispatch_id)
    with SessionLocal() as db:
        dispatch = repository.claim(db, target)
        if dispatch is None:
            return
        user_id, connection_id, start, end = (
            dispatch.user_id,
            dispatch.connection_id,
            dispatch.requested_start_at,
            dispatch.requested_end_at,
        )
        db.info["whoop_history_authority"] = WhoopHistoryAuthority(
            user_id,
            connection_id,
            dispatch.authorization_generation,
            dispatch.health_evidence_generation,
            target,
        )
        # Provider adapters retain their ordinary upserts and attribution. The
        # session-bound authority is checked at every provider request and write.
        try:
            require_health_write_authority(db, user_id=user_id, provider="whoop")
            db.commit()
            provider = ProviderFactory().get_provider("whoop")
            if provider.workouts is None or provider.data_247 is None:
                raise RuntimeError("WHOOP pull adapters are unavailable")
            provider.workouts.load_data(
                db,
                user_id,
                start_date=start,
                end_date=end,
                user_connection_id=connection_id,
            )
            cast(Whoop247Data, provider.data_247).load_and_save_all(
                db,
                user_id,
                start_time=start,
                end_time=end,
                user_connection_id=connection_id,
                include_body_measurement=False,
            )
            # This last guard also catches authorization changes during an empty
            # provider response. Success and final writes commit together.
            require_health_write_authority(db, user_id=user_id, provider="whoop")
            repository.finish(db, target, "succeeded", None)
        except Exception as exc:
            db.rollback()
            repository.finish(
                db,
                target,
                "superseded" if isinstance(exc, HealthWriteAuthorityError) else "failed",
                "authorization_changed" if isinstance(exc, HealthWriteAuthorityError) else "provider_sync_failed",
            )
        finally:
            db.info.pop("whoop_history_authority", None)


@shared_task
def recover_whoop_history() -> None:
    """Recover commit/enqueue gaps and give lost workers a durable outcome."""
    from app.services.whoop_history_service import publish_history_dispatch

    with SessionLocal() as db:
        for dispatch_id in repository.pending(db):
            dispatch = db.get(WhoopHistoryDispatch, dispatch_id)
            if dispatch is not None:
                publish_history_dispatch(dispatch.id, dispatch.task_id)
