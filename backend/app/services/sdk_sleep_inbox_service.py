import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from logging import getLogger
from uuid import UUID, uuid4

from app.config import settings
from app.constants.series_types.sdk import SleepPhase, get_apple_sleep_phase
from app.database import DbSession
from app.models import SDKSleepInbox
from app.repositories import SDKSleepInboxRepository
from app.schemas.providers.mobile_sdk import SleepRecord

logger = getLogger(__name__)


@dataclass(frozen=True)
class SleepInboxStageOutcome:
    staged_count: int
    row_ids: tuple[UUID, ...] = ()
    error_code: str | None = None


class SDKSleepInboxService:
    projection_lease = timedelta(minutes=15)
    projection_retry = timedelta(minutes=5)

    def __init__(self) -> None:
        self.crud = SDKSleepInboxRepository()

    @staticmethod
    def _payload(record: SleepRecord) -> dict:
        return record.model_dump(mode="json", by_alias=True)

    @classmethod
    def _payload_hash(cls, record: SleepRecord) -> str:
        encoded = json.dumps(cls._payload(record), separators=(",", ":"), sort_keys=True).encode()
        return hashlib.sha256(encoded).hexdigest()

    def stage(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        provider: str,
        batch_id: UUID,
        records: list[SleepRecord],
    ) -> SleepInboxStageOutcome:
        if not records:
            return SleepInboxStageOutcome(0)

        prepared: dict[str, tuple[SleepRecord, str, dict]] = {}
        for record in records:
            if not record.id:
                return SleepInboxStageOutcome(0, error_code="sleep_source_id_required")
            phase = get_apple_sleep_phase(str(record.stage))
            if phase is None or phase == SleepPhase.UNKNOWN:
                return SleepInboxStageOutcome(0, error_code="sleep_stage_unsupported")
            payload_hash = self._payload_hash(record)
            payload = self._payload(record)
            prior = prepared.get(record.id)
            if prior is not None and prior[1] != payload_hash:
                return SleepInboxStageOutcome(0, error_code="sleep_source_payload_conflict")
            prepared[record.id] = (record, payload_hash, payload)

        existing_by_id: dict[str, SDKSleepInbox] = {}
        for external_id, (_, payload_hash, _) in prepared.items():
            existing = self.crud.get(
                db_session,
                user_id=user_id,
                provider=provider,
                external_id=external_id,
                for_update=True,
            )
            if existing is not None:
                if existing.payload_sha256 != payload_hash:
                    return SleepInboxStageOutcome(0, error_code="sleep_source_payload_conflict")
                if batch_id not in existing.batch_ids:
                    existing.batch_ids = [*existing.batch_ids, batch_id]
                    existing.updated_at = datetime.now(timezone.utc)
                    self.crud.save(db_session, existing)
                existing_by_id[external_id] = existing

        now = datetime.now(timezone.utc)
        rows_by_external_id: dict[str, SDKSleepInbox] = dict(existing_by_id)
        for external_id, (_, payload_hash, payload) in prepared.items():
            if external_id in rows_by_external_id:
                continue
            row = SDKSleepInbox(
                id=uuid4(),
                user_id=user_id,
                provider=provider,
                external_id=external_id,
                batch_ids=[batch_id],
                payload_sha256=payload_hash,
                payload=payload,
                status="staged",
                attempt_count=0,
                next_attempt_at=now,
                last_attempt_at=None,
                materialized_at=None,
                last_error=None,
                updated_at=now,
            )
            self.crud.add(db_session, row)
            rows_by_external_id[external_id] = row

        return SleepInboxStageOutcome(
            staged_count=len(records),
            row_ids=tuple(row.id for row in rows_by_external_id.values()),
        )

    def lease_due(
        self,
        db_session: DbSession,
        *,
        limit: int,
        user_id: UUID | None = None,
        provider: str | None = None,
    ) -> list[SDKSleepInbox]:
        now = datetime.now(timezone.utc)
        rows = self.crud.list_due(
            db_session,
            now=now,
            limit=limit,
            user_id=user_id,
            provider=provider,
        )
        for row in rows:
            row.status = "projecting"
            row.attempt_count += 1
            row.last_attempt_at = now
            row.next_attempt_at = now + self.projection_lease
            row.last_error = None
            row.updated_at = now
            self.crud.save(db_session, row)
        db_session.commit()
        return rows

    def record_projection_result(
        self,
        db_session: DbSession,
        *,
        row_ids: set[UUID],
        materialized_ids: set[UUID],
        error_code: str | None = None,
    ) -> None:
        now = datetime.now(timezone.utc)
        rows = self.crud.list_by_ids(db_session, row_ids, for_update=True)
        for row in rows:
            if row.id in materialized_ids:
                row.status = "materialized"
                row.materialized_at = now
                row.next_attempt_at = now
                row.last_error = None
            else:
                row.status = "projected" if error_code is None else "staged"
                payload = SleepRecord.model_validate(row.payload)
                stale_at = payload.endDate + timedelta(minutes=settings.sleep_end_gap_minutes + 1)
                retry_at = now + self.projection_retry
                row.next_attempt_at = max(stale_at, retry_at)
                row.last_error = error_code[:100] if error_code else None
            row.updated_at = now
            self.crud.save(db_session, row)
        db_session.commit()

    @staticmethod
    def schedule_projection(*, user_id: UUID, provider: str) -> None:
        try:
            from app.integrations.celery.tasks.project_sdk_sleep_inbox_task import project_sdk_sleep_inbox

            project_sdk_sleep_inbox.delay(user_id=str(user_id), provider=provider)
        except Exception:
            # Beat scans the durable inbox, so a transient broker publish failure
            # cannot turn a staged sleep payload into data loss.
            logger.warning(
                "Could not immediately schedule durable sleep projection",
                extra={"provider": provider},
                exc_info=True,
            )


sdk_sleep_inbox_service = SDKSleepInboxService()
