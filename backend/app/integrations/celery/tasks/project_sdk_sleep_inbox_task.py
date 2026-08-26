from collections import defaultdict
from datetime import datetime, timezone
from logging import getLogger
from uuid import UUID

from celery import shared_task

from app.database import DbSession, SessionLocal
from app.models import SDKSleepInbox, User
from app.schemas.providers.mobile_sdk import SleepRecord, SyncRequest, SyncRequestData
from app.services.apple.healthkit.sleep_service import handle_sleep_data
from app.services.sdk_client_installation_service import sdk_client_installation_service
from app.services.sdk_sleep_inbox_service import sdk_sleep_inbox_service

logger = getLogger(__name__)


def _health_write_error(db: DbSession, user: User, row: SDKSleepInbox) -> str | None:
    return sdk_client_installation_service.health_write_error(
        db,
        user=user,
        installation_id=row.installation_id,
        installation_generation=row.installation_generation,
        health_evidence_generation=row.health_evidence_generation,
    )


@shared_task(queue="sdk_sync", acks_late=True)
def project_sdk_sleep_inbox(
    user_id: str | None = None,
    provider: str | None = None,
    limit: int = 500,
) -> dict[str, int]:
    """Project sleep while one user-row lock fences reset/re-pair authority."""
    scoped_user = UUID(user_id) if user_id is not None else None
    with SessionLocal() as db:
        leased = sdk_sleep_inbox_service.lease_due(
            db,
            limit=limit,
            user_id=scoped_user,
            provider=provider,
        )
        items = [(row.id, row.user_id, row.provider, row.attempt_count) for row in leased]

    groups: dict[tuple[UUID, str], dict[UUID, int]] = defaultdict(dict)
    for row_id, row_user_id, row_provider, attempt_count in items:
        groups[(row_user_id, row_provider)][row_id] = attempt_count

    materialized_total = 0
    quarantined_total = 0
    for (row_user_id, row_provider), expected_attempts in groups.items():
        row_ids = set(expected_attempts)
        try:
            with SessionLocal() as projection_db:
                user = projection_db.query(User).filter(User.id == row_user_id).with_for_update().one_or_none()
                rows = sdk_sleep_inbox_service.crud.list_by_ids(
                    projection_db,
                    row_ids,
                    for_update=True,
                )
                claimed_rows = [
                    row
                    for row in rows
                    if row.status == "projecting" and row.attempt_count == expected_attempts.get(row.id)
                ]
                valid_rows: list[SDKSleepInbox] = []
                stale_by_error: dict[str, set[UUID]] = defaultdict(set)
                for row in claimed_rows:
                    error_code = (
                        "health_user_missing" if user is None else _health_write_error(projection_db, user, row)
                    )
                    if error_code is None:
                        valid_rows.append(row)
                    else:
                        stale_by_error[error_code].add(row.id)

                for error_code, stale_ids in stale_by_error.items():
                    sdk_sleep_inbox_service.quarantine(
                        projection_db,
                        row_ids=stale_ids,
                        expected_attempts=expected_attempts,
                        error_code=error_code,
                        commit=False,
                    )
                    quarantined_total += len(stale_ids)

                if valid_rows:
                    first = valid_rows[0]
                    projection_db.info["health_write_authority"] = (
                        row_user_id,
                        first.health_evidence_generation,
                        first.installation_id,
                        first.installation_generation,
                    )
                    request = SyncRequest(
                        provider=row_provider,
                        sdkVersion="sleep-inbox-replay-v2",
                        syncTimestamp=datetime.now(timezone.utc),
                        data=SyncRequestData(sleep=[SleepRecord.model_validate(row.payload) for row in valid_rows]),
                    )
                    materialized_source_ids = handle_sleep_data(
                        projection_db,
                        request,
                        str(row_user_id),
                        commit=False,
                    )
                    materialized_ids = {row.id for row in valid_rows if row.external_id in materialized_source_ids}
                    sdk_sleep_inbox_service.record_projection_result(
                        projection_db,
                        row_ids={row.id for row in valid_rows},
                        expected_attempts=expected_attempts,
                        materialized_ids=materialized_ids,
                        commit=False,
                    )
                    materialized_total += len(materialized_ids)
                projection_db.commit()
        except Exception:
            logger.warning(
                "Durable sleep projection failed; inbox rows remain retryable",
                extra={"provider": row_provider},
            )
            with SessionLocal() as result_db:
                user = result_db.query(User).filter(User.id == row_user_id).with_for_update().one_or_none()
                rows = sdk_sleep_inbox_service.crud.list_by_ids(result_db, row_ids, for_update=True)
                claimed_rows = [
                    row
                    for row in rows
                    if row.status == "projecting" and row.attempt_count == expected_attempts.get(row.id)
                ]
                retry_ids: set[UUID] = set()
                stale_by_error: dict[str, set[UUID]] = defaultdict(set)
                for row in claimed_rows:
                    error_code = "health_user_missing" if user is None else _health_write_error(result_db, user, row)
                    if error_code is None:
                        retry_ids.add(row.id)
                    else:
                        stale_by_error[error_code].add(row.id)
                for error_code, stale_ids in stale_by_error.items():
                    sdk_sleep_inbox_service.quarantine(
                        result_db,
                        row_ids=stale_ids,
                        expected_attempts=expected_attempts,
                        error_code=error_code,
                        commit=False,
                    )
                    quarantined_total += len(stale_ids)
                if retry_ids:
                    sdk_sleep_inbox_service.record_projection_result(
                        result_db,
                        row_ids=retry_ids,
                        expected_attempts=expected_attempts,
                        materialized_ids=set(),
                        error_code="sleep_projection_failed",
                        commit=False,
                    )
                result_db.commit()

    return {
        "leased": len(items),
        "materialized": materialized_total,
        "quarantined": quarantined_total,
    }
