from uuid import UUID

from app.database import DbSession
from app.models import SDKSyncWindowReceipt


class SDKSyncWindowReceiptRepository:
    def get(self, db_session: DbSession, window_id: UUID, *, for_update: bool = False) -> SDKSyncWindowReceipt | None:
        query = db_session.query(SDKSyncWindowReceipt).filter(SDKSyncWindowReceipt.id == window_id)
        if for_update:
            query = query.with_for_update()
        return query.one_or_none()

    def add(self, db_session: DbSession, receipt: SDKSyncWindowReceipt) -> None:
        db_session.add(receipt)
        db_session.flush()

    def list_for_user(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        provider: str | None,
        limit: int,
    ) -> list[SDKSyncWindowReceipt]:
        query = db_session.query(SDKSyncWindowReceipt).filter(SDKSyncWindowReceipt.user_id == user_id)
        if provider is not None:
            query = query.filter(SDKSyncWindowReceipt.provider == provider)
        return (
            query.order_by(
                SDKSyncWindowReceipt.accepted_at.desc(),
                SDKSyncWindowReceipt.id.desc(),
            )
            .limit(limit)
            .all()
        )
