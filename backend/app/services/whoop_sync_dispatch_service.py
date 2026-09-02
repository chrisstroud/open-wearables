from logging import getLogger
from uuid import UUID

from app.database import DbSession
from app.models import WhoopSyncDispatchReceipt
from app.repositories.whoop_sync_dispatch_repository import WhoopSyncDispatchRepository
from app.schemas.whoop_sync_dispatch import WhoopFullHistorySyncCommand, WhoopSyncDispatchStatus
from app.utils.structured_logging import log_structured

logger = getLogger(__name__)


class WhoopSyncDispatchService:
    def __init__(self, repository: WhoopSyncDispatchRepository | None = None) -> None:
        self.repository = repository or WhoopSyncDispatchRepository()

    def request_full_history(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        connection_id: UUID,
        command: WhoopFullHistorySyncCommand,
    ) -> WhoopSyncDispatchReceipt:
        receipt = self.repository.create_or_get(
            db_session,
            user_id=user_id,
            connection_id=connection_id,
            command=command,
        )
        if receipt.status == WhoopSyncDispatchStatus.QUEUED.value:
            try:
                from app.integrations.celery.tasks.whoop_sync_dispatch_task import (
                    drain_whoop_sync_dispatch_outbox,
                )

                drain_whoop_sync_dispatch_outbox.delay()
            except Exception as exc:
                # The committed receipt remains due for the periodic outbox drainer.
                log_structured(
                    logger,
                    "warning",
                    "WHOOP sync receipt committed; immediate outbox nudge failed",
                    task="request_whoop_full_history",
                    dispatch_id=str(receipt.id),
                    error=str(exc),
                )
        return receipt

    def get(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        dispatch_id: UUID,
    ) -> WhoopSyncDispatchReceipt | None:
        return self.repository.get_for_user(
            db_session,
            user_id=user_id,
            dispatch_id=dispatch_id,
        )
