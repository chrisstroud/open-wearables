from logging import getLogger

from celery import shared_task

from app.database import SessionLocal
from app.services.sdk_upload_inbox_service import sdk_upload_inbox_service

logger = getLogger(__name__)


@shared_task(
    name="app.integrations.celery.tasks.prune_sdk_upload_inbox_task.prune_sdk_upload_inbox",
    queue="sdk_sync",
    acks_late=True,
)
def prune_sdk_upload_inbox(limit: int = 500) -> dict[str, int]:
    """Exhaust expired payloads in bounded transactions without logging values."""
    if limit <= 0:
        raise ValueError("prune batch limit must be positive")

    pruned = 0
    while True:
        # A fresh session keeps each delete transaction bounded while ensuring a
        # daily run cannot leave an arbitrarily large expired-PHI backlog behind.
        with SessionLocal() as db:
            batch_count = sdk_upload_inbox_service.prune_expired(db, limit=limit)
        pruned += batch_count
        if batch_count < limit:
            break

    logger.info("Pruned expired SDK upload inbox rows", extra={"pruned_count": pruned})
    return {"pruned": pruned}
