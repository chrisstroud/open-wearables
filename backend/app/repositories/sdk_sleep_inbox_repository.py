from datetime import datetime
from uuid import UUID

from app.database import DbSession
from app.models import SDKSleepInbox


class SDKSleepInboxRepository:
    def get(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        provider: str,
        external_id: str,
        health_evidence_generation: int | None,
        for_update: bool = False,
    ) -> SDKSleepInbox | None:
        query = db_session.query(SDKSleepInbox).filter(
            SDKSleepInbox.user_id == user_id,
            SDKSleepInbox.provider == provider,
            SDKSleepInbox.external_id == external_id,
        )
        if health_evidence_generation is None:
            query = query.filter(SDKSleepInbox.health_evidence_generation.is_(None))
        else:
            query = query.filter(SDKSleepInbox.health_evidence_generation == health_evidence_generation)
        if for_update:
            query = query.with_for_update()
        return query.one_or_none()

    def add(self, db_session: DbSession, row: SDKSleepInbox) -> None:
        db_session.add(row)
        db_session.flush()

    def save(self, db_session: DbSession, row: SDKSleepInbox) -> None:
        db_session.add(row)
        db_session.flush()

    def list_due(
        self,
        db_session: DbSession,
        *,
        now: datetime,
        limit: int,
        user_id: UUID | None = None,
        provider: str | None = None,
    ) -> list[SDKSleepInbox]:
        query = db_session.query(SDKSleepInbox).filter(
            SDKSleepInbox.status.in_(("staged", "projecting", "projected")),
            SDKSleepInbox.next_attempt_at <= now,
        )
        if user_id is not None:
            query = query.filter(SDKSleepInbox.user_id == user_id)
        if provider is not None:
            query = query.filter(SDKSleepInbox.provider == provider)
        return (
            query.order_by(SDKSleepInbox.next_attempt_at, SDKSleepInbox.created_at)
            .with_for_update(skip_locked=True)
            .limit(limit)
            .all()
        )

    def list_by_ids(
        self,
        db_session: DbSession,
        row_ids: set[UUID],
        *,
        for_update: bool = False,
    ) -> list[SDKSleepInbox]:
        if not row_ids:
            return []
        query = db_session.query(SDKSleepInbox).filter(SDKSleepInbox.id.in_(row_ids))
        if for_update:
            query = query.with_for_update()
        return query.all()

    def has_unmaterialized_for_batches(
        self,
        db_session: DbSession,
        batch_ids: set[UUID],
    ) -> bool:
        if not batch_ids:
            return False
        return (
            db_session.query(SDKSleepInbox.id)
            .filter(
                SDKSleepInbox.status != "materialized",
                SDKSleepInbox.batch_ids.overlap(list(batch_ids)),
            )
            .first()
            is not None
        )
