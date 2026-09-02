from datetime import datetime
from uuid import UUID

from sqlalchemy import CheckConstraint, ForeignKey, Index, UniqueConstraint
from sqlalchemy.orm import Mapped, mapped_column

from app.database import BaseDbModel
from app.mappings import FKUser, PrimaryKey, str_32, str_64, str_100


class WhoopSyncDispatchReceipt(BaseDbModel):
    """Durable, generation-bound authority for one exact WHOOP history pull."""

    __tablename__ = "whoop_sync_dispatch_receipt"
    __table_args__ = (
        CheckConstraint(
            "authorization_generation > 0",
            name="ck_whoop_sync_dispatch_authorization_generation",
        ),
        CheckConstraint(
            "requested_start_at < requested_end_at",
            name="ck_whoop_sync_dispatch_bounds",
        ),
        CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed', 'superseded')",
            name="ck_whoop_sync_dispatch_status",
        ),
        CheckConstraint(
            "enqueue_attempt_count >= 0 AND execution_attempt_count >= 0",
            name="ck_whoop_sync_dispatch_attempt_counts",
        ),
        CheckConstraint(
            "(status = 'running' AND lease_token IS NOT NULL AND processing_started_at IS NOT NULL) OR "
            "(status <> 'running' AND lease_token IS NULL)",
            name="ck_whoop_sync_dispatch_lease_state",
        ),
        UniqueConstraint(
            "connection_id",
            "authorization_generation",
            "requested_start_at",
            "requested_end_at",
            name="uq_whoop_sync_dispatch_exact_window",
        ),
        Index("ix_whoop_sync_dispatch_outbox", "status", "next_enqueue_at"),
        Index("ix_whoop_sync_dispatch_user_created", "user_id", "created_at"),
    )

    id: Mapped[PrimaryKey[UUID]]
    user_id: Mapped[FKUser]
    connection_id: Mapped[UUID] = mapped_column(
        ForeignKey("user_connection.id", ondelete="CASCADE"),
    )
    authorization_generation: Mapped[int]
    request_fingerprint: Mapped[str_64]
    requested_start_at: Mapped[datetime]
    requested_end_at: Mapped[datetime]
    task_id: Mapped[UUID]
    status: Mapped[str_32]
    enqueue_attempt_count: Mapped[int]
    execution_attempt_count: Mapped[int]
    next_enqueue_at: Mapped[datetime | None]
    enqueued_at: Mapped[datetime | None]
    lease_token: Mapped[UUID | None]
    processing_started_at: Mapped[datetime | None]
    completed_at: Mapped[datetime | None]
    error_code: Mapped[str_100 | None]
    updated_at: Mapped[datetime]
