from datetime import datetime
from uuid import UUID

from sqlalchemy import CheckConstraint, Index
from sqlalchemy.orm import Mapped

from app.database import BaseDbModel
from app.mappings import FKUser, PrimaryKey, str_32, str_64, str_100


class SDKBatchReceipt(BaseDbModel):
    """Durable terminal acknowledgement state for a mobile SDK batch."""

    __tablename__ = "sdk_batch_receipt"
    __table_args__ = (
        Index("ix_sdk_batch_receipt_user_status", "user_id", "status"),
        Index("ix_sdk_batch_receipt_updated_at", "updated_at"),
        CheckConstraint(
            "status IN ('queued', 'processing', 'succeeded', 'failed')",
            name="ck_sdk_batch_receipt_status",
        ),
        CheckConstraint(
            "attempt_count >= 0 AND dropped_count >= 0 AND records_saved >= 0 "
            "AND workouts_saved >= 0 AND sleep_saved >= 0 AND tombstones_received >= 0 "
            "AND tombstones_applied >= 0 AND tombstones_unresolved >= 0 "
            "AND tombstone_rows_deleted >= 0",
            name="ck_sdk_batch_receipt_nonnegative_counts",
        ),
        CheckConstraint(
            "status <> 'succeeded' OR (dropped_count = 0 AND tombstones_unresolved = 0 AND retryable = false)",
            name="ck_sdk_batch_receipt_success_is_accepted",
        ),
    )

    id: Mapped[PrimaryKey[UUID]]
    user_id: Mapped[FKUser]
    provider: Mapped[str_32]
    payload_sha256: Mapped[str_64]
    status: Mapped[str_32]
    retryable: Mapped[bool]
    attempt_count: Mapped[int]
    dropped_count: Mapped[int]
    records_saved: Mapped[int]
    workouts_saved: Mapped[int]
    sleep_saved: Mapped[int]
    tombstones_received: Mapped[int]
    tombstones_applied: Mapped[int]
    tombstones_unresolved: Mapped[int]
    tombstone_rows_deleted: Mapped[int]
    error_code: Mapped[str_100 | None]
    updated_at: Mapped[datetime]
    processing_started_at: Mapped[datetime | None]
    completed_at: Mapped[datetime | None]
