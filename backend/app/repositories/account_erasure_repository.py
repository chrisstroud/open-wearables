"""Transactional persistence for the exact whole-account operation only."""

from uuid import UUID

from sqlalchemy import delete, func, select, text

from app.database import BaseDbModel, DbSession
from app.models import AccountErasureOperation, User, WhoopAuthorizationLease


class AccountErasureRepository:
    def lock_operation(self, db: DbSession, operation_id: UUID) -> AccountErasureOperation | None:
        # Includes the not-yet-created case without a session lock surviving pool return.
        db.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
            {"key": f"account-erasure:{operation_id}"},
        )
        return (
            db.query(AccountErasureOperation)
            .filter(AccountErasureOperation.operation_id == operation_id)
            .populate_existing()
            .with_for_update()
            .one_or_none()
        )

    def user(self, db: DbSession, user_id: UUID, *, lock: bool = False) -> User | None:
        query = db.query(User).filter(User.id == user_id).populate_existing()
        return (query.with_for_update() if lock else query).one_or_none()

    def has_authorization_writer(self, db: DbSession, user_id: UUID) -> bool:
        return (
            db.query(WhoopAuthorizationLease)
            .filter(
                WhoopAuthorizationLease.user_id == user_id,
                WhoopAuthorizationLease.lease_expires_at > func.clock_timestamp(),
            )
            .first()
            is not None
        )

    def create(self, db: DbSession, operation: AccountErasureOperation) -> None:
        db.add(operation)
        db.flush()

    def owned_row_counts(self, db: DbSession, user_id: UUID) -> dict[str, int]:
        # Inspect every mapped direct owner, including future user_id tables.
        # Indirect source/event children are independently verified by source reset.
        return {
            table.name: db.execute(
                select(func.count()).select_from(table).where(table.c.user_id == user_id)
            ).scalar_one()
            for table in BaseDbModel.metadata.sorted_tables
            if "user_id" in table.c
        }

    def delete_user(self, db: DbSession, user_id: UUID) -> None:
        removed_id = db.scalar(delete(User).where(User.id == user_id).returning(User.id))
        if removed_id != user_id:
            raise RuntimeError("Exact account deletion was not applied")
        db.flush()


account_erasure_repository = AccountErasureRepository()
