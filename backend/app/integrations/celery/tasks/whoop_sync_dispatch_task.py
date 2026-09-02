from logging import getLogger
from threading import Event, Thread
from typing import Any
from uuid import UUID, uuid4

from celery import shared_task

from app.database import SessionLocal
from app.repositories.whoop_sync_dispatch_repository import WhoopSyncDispatchRepository
from app.schemas.whoop_sync_dispatch import WhoopSyncDispatchStatus
from app.services.providers.whoop.exact_sync_authority import (
    ExactWhoopSyncAuthority,
    scoped_exact_whoop_sync_authority,
)
from app.utils.structured_logging import log_structured

from .sync_vendor_data_task import sync_vendor_data

logger = getLogger(__name__)
WHOOP_AUTHORIZATION_HEARTBEAT_INTERVAL_SECONDS = 60.0


class _WhoopAuthorizationHeartbeat:
    """Renew a running worker's database-time lease during CPU and storage work."""

    def __init__(
        self,
        authority: ExactWhoopSyncAuthority,
        *,
        interval_seconds: float = WHOOP_AUTHORIZATION_HEARTBEAT_INTERVAL_SECONDS,
    ) -> None:
        self.authority = authority
        self.interval_seconds = interval_seconds
        self._stop = Event()
        self._thread = Thread(
            target=self._run,
            name=f"whoop-sync-lease-{authority.dispatch_id}",
            daemon=True,
        )

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=5.0)

    def _run(self) -> None:
        repository = WhoopSyncDispatchRepository()
        while not self._stop.wait(self.interval_seconds):
            try:
                with SessionLocal() as db:
                    renewed = repository.renew_runtime_authority(
                        db,
                        user_id=self.authority.user_id,
                        connection_id=self.authority.connection_id,
                        authorization_generation=self.authority.authorization_generation,
                        lease_token=self.authority.lease_token,
                    )
            except Exception as exc:
                log_structured(
                    logger,
                    "error",
                    "WHOOP exact-sync authorization heartbeat failed",
                    task="execute_whoop_sync_dispatch",
                    dispatch_id=str(self.authority.dispatch_id),
                    error=str(exc),
                )
                self.authority.lease_lost.set()
                return
            if not renewed:
                self.authority.lease_lost.set()
                return


def _whoop_sync_succeeded(result: dict[str, Any]) -> bool:
    provider_result = result.get("providers_synced", {}).get("whoop")
    return isinstance(provider_result, dict) and provider_result.get("success") is True and not result.get("errors")


@shared_task
def drain_whoop_sync_dispatch_outbox(limit: int = 100) -> dict[str, int]:
    """Redeliver queued receipts until an exact worker claims them."""
    repository = WhoopSyncDispatchRepository()
    with SessionLocal() as db:
        deliveries = repository.due_deliveries(db, limit=limit)

    published = 0
    failed = 0
    for delivery in deliveries:
        try:
            execute_whoop_sync_dispatch.apply_async(
                kwargs={"dispatch_id": str(delivery.dispatch_id)},
                task_id=str(delivery.task_id),
            )
            published += 1
        except Exception as exc:
            failed += 1
            log_structured(
                logger,
                "error",
                "Failed to publish durable WHOOP sync receipt",
                task="drain_whoop_sync_dispatch_outbox",
                dispatch_id=str(delivery.dispatch_id),
                error=str(exc),
            )
    return {"selected": len(deliveries), "published": published, "failed": failed}


@shared_task
def execute_whoop_sync_dispatch(dispatch_id: str) -> dict[str, Any]:
    """Execute one generation-bound WHOOP history receipt at most once."""
    try:
        dispatch_uuid = UUID(dispatch_id)
    except ValueError:
        return {"dispatch_id": dispatch_id, "status": "invalid"}

    repository = WhoopSyncDispatchRepository()
    with SessionLocal() as db:
        receipt = repository.get(db, dispatch_id=dispatch_uuid)
        if receipt is None:
            return {"dispatch_id": dispatch_id, "status": "missing"}
        if receipt.status != WhoopSyncDispatchStatus.QUEUED.value:
            return {"dispatch_id": dispatch_id, "status": receipt.status}
        user_id = receipt.user_id
        connection_id = receipt.connection_id
        generation = receipt.authorization_generation

    lease_token = uuid4()
    with SessionLocal() as db:
        acquired = repository.try_acquire_authorization_lease(
            db,
            user_id=user_id,
            connection_id=connection_id,
            authorization_generation=generation,
            lease_token=lease_token,
            lease_kind="full_history_sync",
        )
    if not acquired:
        with SessionLocal() as db:
            superseded = repository.supersede_if_authority_stale(
                db,
                dispatch_id=dispatch_uuid,
            )
        return {
            "dispatch_id": dispatch_id,
            "status": WhoopSyncDispatchStatus.SUPERSEDED.value if superseded else "deferred",
        }

    lease_released = False
    try:
        with SessionLocal() as db:
            claimed = repository.claim_execution(
                db,
                dispatch_id=dispatch_uuid,
                lease_token=lease_token,
            )
        if claimed is None:
            return {"dispatch_id": dispatch_id, "status": "not_claimed"}
        if claimed.status == WhoopSyncDispatchStatus.SUPERSEDED.value:
            return {"dispatch_id": dispatch_id, "status": claimed.status}

        authority = ExactWhoopSyncAuthority(
            dispatch_id=dispatch_uuid,
            user_id=user_id,
            connection_id=connection_id,
            authorization_generation=generation,
            lease_token=lease_token,
        )
        heartbeat = _WhoopAuthorizationHeartbeat(authority)
        heartbeat.start()
        try:
            with scoped_exact_whoop_sync_authority(authority):
                result = sync_vendor_data.run(
                    user_id=str(user_id),
                    start_date=claimed.requested_start_at.isoformat(),
                    end_date=claimed.requested_end_at.isoformat(),
                    providers=["whoop"],
                    is_historical=True,
                    _skip_linked_fan_out=True,
                    _exact_connection_id=str(connection_id),
                    _exact_authorization_generation=generation,
                )
            if authority.lease_lost.is_set():
                final_status = WhoopSyncDispatchStatus.FAILED
                error_code = "authorization_heartbeat_lost"
            else:
                succeeded = _whoop_sync_succeeded(result)
                final_status = WhoopSyncDispatchStatus.SUCCEEDED if succeeded else WhoopSyncDispatchStatus.FAILED
                error_code = None if succeeded else "provider_sync_failed"
        except Exception as exc:
            log_structured(
                logger,
                "error",
                "Exact WHOOP sync execution failed",
                task="execute_whoop_sync_dispatch",
                dispatch_id=dispatch_id,
                error=str(exc),
            )
            result = {"errors": {"general": str(exc)}}
            final_status = WhoopSyncDispatchStatus.FAILED
            error_code = "worker_exception"
        finally:
            heartbeat.stop()

        with SessionLocal() as db:
            finished = repository.finish_execution(
                db,
                dispatch_id=dispatch_uuid,
                lease_token=lease_token,
                status=final_status,
                error_code=error_code,
            )
        lease_released = finished
        return {
            "dispatch_id": dispatch_id,
            "status": final_status.value if finished else "lease_lost",
            "result": result,
        }
    finally:
        if not lease_released:
            with SessionLocal() as db:
                repository.release_authorization_lease(
                    db,
                    user_id=user_id,
                    lease_token=lease_token,
                )
