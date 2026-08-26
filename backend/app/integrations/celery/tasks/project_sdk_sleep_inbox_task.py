from collections import defaultdict
from datetime import datetime, timezone
from logging import getLogger
from uuid import UUID

from celery import shared_task

from app.database import SessionLocal
from app.schemas.providers.mobile_sdk import SleepRecord, SyncRequest, SyncRequestData
from app.services.apple.healthkit.sleep_service import handle_sleep_data
from app.services.sdk_sleep_inbox_service import sdk_sleep_inbox_service

logger = getLogger(__name__)


@shared_task(queue="sdk_sync", acks_late=True)
def project_sdk_sleep_inbox(
    user_id: str | None = None,
    provider: str | None = None,
    limit: int = 500,
) -> dict[str, int]:
    """Replay due durable sleep payloads into the existing sleep projection."""
    scoped_user = UUID(user_id) if user_id is not None else None
    with SessionLocal() as db:
        leased = sdk_sleep_inbox_service.lease_due(
            db,
            limit=limit,
            user_id=scoped_user,
            provider=provider,
        )
        items = [
            (
                row.id,
                row.user_id,
                row.provider,
                SleepRecord.model_validate(row.payload),
            )
            for row in leased
        ]

    groups: dict[tuple[UUID, str], list[tuple[UUID, SleepRecord]]] = defaultdict(list)
    for row_id, row_user_id, row_provider, record in items:
        groups[(row_user_id, row_provider)].append((row_id, record))

    materialized_total = 0
    for (row_user_id, row_provider), group in groups.items():
        row_ids = {row_id for row_id, _ in group}
        request = SyncRequest(
            provider=row_provider,
            sdkVersion="sleep-inbox-replay-v1",
            syncTimestamp=datetime.now(timezone.utc),
            data=SyncRequestData(sleep=[record for _, record in group]),
        )
        try:
            with SessionLocal() as projection_db:
                materialized_source_ids = handle_sleep_data(projection_db, request, str(row_user_id))
                projection_db.commit()

            materialized_ids = {
                row_id for row_id, record in group if record.id is not None and record.id in materialized_source_ids
            }

            with SessionLocal() as result_db:
                sdk_sleep_inbox_service.record_projection_result(
                    result_db,
                    row_ids=row_ids,
                    materialized_ids=materialized_ids,
                )
            materialized_total += len(materialized_ids)
        except Exception:
            logger.warning(
                "Durable sleep projection failed; inbox rows remain retryable",
                extra={"provider": row_provider},
                exc_info=True,
            )
            with SessionLocal() as result_db:
                sdk_sleep_inbox_service.record_projection_result(
                    result_db,
                    row_ids=row_ids,
                    materialized_ids=set(),
                    error_code="sleep_projection_failed",
                )

    return {"leased": len(items), "materialized": materialized_total}
