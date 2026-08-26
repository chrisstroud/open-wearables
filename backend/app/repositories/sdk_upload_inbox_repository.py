from datetime import datetime
from uuid import UUID

from sqlalchemy import select

from app.database import DbSession
from app.models.sdk_upload_inbox import SDKUploadInbox


class SDKUploadInboxRepository:
    def get(self, db_session: DbSession, batch_id: UUID) -> SDKUploadInbox | None:
        return db_session.get(SDKUploadInbox, batch_id)

    def save(self, db_session: DbSession, row: SDKUploadInbox) -> SDKUploadInbox:
        db_session.add(row)
        db_session.commit()
        db_session.refresh(row)
        return row

    def delete(self, db_session: DbSession, batch_id: UUID) -> None:
        db_session.query(SDKUploadInbox).filter(SDKUploadInbox.id == batch_id).delete()

    def list_expired_for_update(
        self,
        db_session: DbSession,
        *,
        now: datetime,
        limit: int,
    ) -> list[SDKUploadInbox]:
        stmt = (
            select(SDKUploadInbox)
            .where(SDKUploadInbox.expires_at <= now)
            .order_by(SDKUploadInbox.expires_at)
            # Prune must prove the expired set is exhausted before the daily
            # run returns. Waiting for a concurrent owner avoids a false-empty
            # page that SKIP LOCKED could leave until the next day.
            .with_for_update()
            .limit(limit)
        )
        return list(db_session.execute(stmt).scalars().all())


sdk_upload_inbox_repository = SDKUploadInboxRepository()
