from datetime import datetime
from uuid import UUID

from sqlalchemy import CheckConstraint, ForeignKey, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.database import BaseDbModel
from app.mappings import FKUser, str_32, str_64


class SDKSyncWindowReceipt(BaseDbModel):
    """Immutable authority that every batch in one mobile sync window was accepted."""

    __tablename__ = "sdk_sync_window_receipt"
    __table_args__ = (
        Index(
            "ix_sdk_sync_window_receipt_user_provider_accepted",
            "user_id",
            "provider",
            "accepted_at",
        ),
        CheckConstraint(
            "purpose IN ('activation', 'archive', 'incremental')",
            name="ck_sdk_sync_window_receipt_purpose",
        ),
        CheckConstraint("window_version = 2", name="ck_sdk_sync_window_receipt_version"),
        CheckConstraint(
            "lower_bound_inclusive < upper_bound_exclusive",
            name="ck_sdk_sync_window_receipt_bounds",
        ),
        CheckConstraint(
            "(reconciliation_start_inclusive IS NULL AND reconciliation_end_exclusive IS NULL) "
            "OR (reconciliation_start_inclusive IS NOT NULL AND reconciliation_end_exclusive IS NOT NULL "
            "AND reconciliation_start_inclusive < reconciliation_end_exclusive)",
            name="ck_sdk_sync_window_receipt_reconciliation_bounds",
        ),
        CheckConstraint(
            "purpose <> 'incremental' OR reconciliation_start_inclusive IS NOT NULL",
            name="ck_sdk_sync_window_receipt_incremental_reconciliation",
        ),
        CheckConstraint(
            "jsonb_array_length(batch_ids) > 0 OR jsonb_array_length(empty_or_no_access_types) > 0",
            name="ck_sdk_sync_window_receipt_coverage",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        ForeignKey("sdk_batch_receipt.id", ondelete="CASCADE"),
        primary_key=True,
    )
    user_id: Mapped[FKUser]
    provider: Mapped[str_32]
    manifest_sha256: Mapped[str_64]
    purpose: Mapped[str_32]
    window_version: Mapped[int]
    lower_bound_inclusive: Mapped[datetime]
    upper_bound_exclusive: Mapped[datetime]
    batch_ids: Mapped[list[str]] = mapped_column(JSONB)
    empty_or_no_access_types: Mapped[list[str]] = mapped_column(JSONB)
    reconciliation_start_inclusive: Mapped[datetime | None]
    reconciliation_end_exclusive: Mapped[datetime | None]
    accepted_at: Mapped[datetime]
