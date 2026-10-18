from datetime import datetime
from uuid import UUID

from sqlalchemy import BigInteger, CheckConstraint, Index, text
from sqlalchemy.orm import Mapped, mapped_column

from app.database import BaseDbModel
from app.mappings import FKUser, PrimaryKey, str_32, str_100


class WhoopHistoryDispatch(BaseDbModel):
    """Durable admission and outcome for one exact, bounded WHOOP pull."""

    __tablename__ = "whoop_history_dispatch"
    __table_args__ = (
        CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed', 'superseded')",
            name="ck_whoop_history_status",
        ),
        CheckConstraint(
            "authorization_generation > 0 AND health_evidence_generation >= 0",
            name="ck_whoop_history_generations",
        ),
        CheckConstraint("requested_start_at < requested_end_at", name="ck_whoop_history_window"),
        CheckConstraint("status <> 'running' OR expires_at IS NOT NULL", name="ck_whoop_history_running_deadline"),
        CheckConstraint(
            "(status IN ('queued', 'running') AND completed_at IS NULL) OR "
            "(status IN ('succeeded', 'failed', 'superseded') AND completed_at IS NOT NULL)",
            name="ck_whoop_history_completion",
        ),
        Index(
            "uq_whoop_history_active_user", "user_id", unique=True,
            postgresql_where=text("status IN ('queued', 'running')"),
        ),
        Index("ix_whoop_history_status_created", "status", "created_at"),
    )

    id: Mapped[PrimaryKey[UUID]]
    user_id: Mapped[FKUser]
    # Keep the exact historical identifier even if the connection is deleted.
    connection_id: Mapped[UUID]
    authorization_generation: Mapped[int] = mapped_column(BigInteger)
    health_evidence_generation: Mapped[int]
    task_id: Mapped[UUID]
    requested_start_at: Mapped[datetime]
    requested_end_at: Mapped[datetime]
    status: Mapped[str_32]
    expires_at: Mapped[datetime | None]
    completed_at: Mapped[datetime | None]
    error_code: Mapped[str_100 | None]
