from datetime import datetime
from uuid import UUID

from sqlalchemy import CheckConstraint, ForeignKey, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

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
            "daily_summaries_saved >= 0",
            name="ck_sdk_batch_receipt_daily_summaries_saved",
        ),
        CheckConstraint(
            "status <> 'succeeded' OR (dropped_count = 0 AND tombstones_unresolved = 0 AND retryable = false)",
            name="ck_sdk_batch_receipt_success_is_accepted",
        ),
        CheckConstraint(
            "revision_set_digest IS NULL OR revision_set_digest ~ '^[0-9a-f]{64}$'",
            name="ck_sdk_batch_receipt_revision_set_digest_format",
        ),
        CheckConstraint(
            "(revision_set_digest IS NULL OR "
            "(provider = 'apple' AND status = 'succeeded' AND daily_summaries_saved > 0)) "
            "AND (daily_summaries_saved = 0 OR "
            "(provider = 'apple' AND status = 'succeeded' AND revision_set_digest IS NOT NULL))",
            name="ck_sdk_batch_receipt_revision_set_digest_state",
        ),
        CheckConstraint(
            "(installation_id IS NULL AND installation_generation IS NULL "
            "AND health_evidence_generation IS NULL) OR "
            "(installation_id IS NOT NULL AND installation_generation > 0 "
            "AND health_evidence_generation >= 0)",
            name="ck_sdk_batch_receipt_installation_scope",
        ),
        CheckConstraint(
            "(content_lower_bound_inclusive IS NULL AND content_upper_bound_exclusive IS NULL) OR "
            "(content_lower_bound_inclusive IS NOT NULL AND content_upper_bound_exclusive IS NOT NULL "
            "AND content_lower_bound_inclusive <= content_upper_bound_exclusive)",
            name="ck_sdk_batch_receipt_content_bounds",
        ),
    )

    id: Mapped[PrimaryKey[UUID]]
    user_id: Mapped[FKUser]
    installation_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("sdk_client_installation.id", ondelete="RESTRICT"),
        nullable=True,
    )
    installation_generation: Mapped[int | None]
    health_evidence_generation: Mapped[int | None]
    provider: Mapped[str_32]
    payload_sha256: Mapped[str_64]
    content_lower_bound_inclusive: Mapped[datetime | None]
    content_upper_bound_exclusive: Mapped[datetime | None]
    covered_type_identifiers: Mapped[list[str]] = mapped_column(JSONB, default=list)
    status: Mapped[str_32]
    retryable: Mapped[bool]
    attempt_count: Mapped[int]
    dropped_count: Mapped[int]
    records_saved: Mapped[int]
    daily_summaries_saved: Mapped[int]
    revision_set_digest: Mapped[str_64 | None]
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
