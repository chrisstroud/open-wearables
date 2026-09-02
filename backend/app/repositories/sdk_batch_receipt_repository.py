from uuid import UUID

from app.database import DbSession
from app.models import SDKBatchReceipt


class SDKBatchReceiptRepository:
    def get(self, db_session: DbSession, batch_id: UUID) -> SDKBatchReceipt | None:
        return db_session.query(SDKBatchReceipt).filter(SDKBatchReceipt.id == batch_id).one_or_none()

    def get_for_update(self, db_session: DbSession, batch_id: UUID) -> SDKBatchReceipt | None:
        return db_session.query(SDKBatchReceipt).filter(SDKBatchReceipt.id == batch_id).with_for_update().one_or_none()

    def add(self, db_session: DbSession, receipt: SDKBatchReceipt) -> None:
        db_session.add(receipt)
        db_session.flush()

    def list_by_ids(self, db_session: DbSession, batch_ids: set[UUID]) -> list[SDKBatchReceipt]:
        if not batch_ids:
            return []
        return db_session.query(SDKBatchReceipt).filter(SDKBatchReceipt.id.in_(batch_ids)).all()

    def save(
        self,
        db_session: DbSession,
        receipt: SDKBatchReceipt,
        *,
        commit: bool = True,
    ) -> SDKBatchReceipt:
        db_session.add(receipt)
        if commit:
            db_session.commit()
            db_session.refresh(receipt)
        else:
            db_session.flush()
        return receipt
