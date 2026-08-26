"""sdk terminal receipts, durable sleep inbox, and sync windows

Revision ID: 8e3f1a9c2b47
Revises: b2c3d4e5f6a1
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "8e3f1a9c2b47"
down_revision: str | None = "b2c3d4e5f6a1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "sdk_batch_receipt",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("retryable", sa.Boolean(), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("dropped_count", sa.Integer(), nullable=False),
        sa.Column("records_saved", sa.Integer(), nullable=False),
        sa.Column("workouts_saved", sa.Integer(), nullable=False),
        sa.Column("sleep_saved", sa.Integer(), nullable=False),
        sa.Column("tombstones_received", sa.Integer(), nullable=False),
        sa.Column("tombstones_applied", sa.Integer(), nullable=False),
        sa.Column("tombstones_unresolved", sa.Integer(), nullable=False),
        sa.Column("tombstone_rows_deleted", sa.Integer(), nullable=False),
        sa.Column("error_code", sa.String(length=100), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("processing_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "status IN ('queued', 'processing', 'succeeded', 'failed')",
            name="ck_sdk_batch_receipt_status",
        ),
        sa.CheckConstraint(
            "attempt_count >= 0 AND dropped_count >= 0 AND records_saved >= 0 "
            "AND workouts_saved >= 0 AND sleep_saved >= 0 AND tombstones_received >= 0 "
            "AND tombstones_applied >= 0 AND tombstones_unresolved >= 0 "
            "AND tombstone_rows_deleted >= 0",
            name="ck_sdk_batch_receipt_nonnegative_counts",
        ),
        sa.CheckConstraint(
            "status <> 'succeeded' OR (dropped_count = 0 AND tombstones_unresolved = 0 AND retryable = false)",
            name="ck_sdk_batch_receipt_success_is_accepted",
        ),
        sa.ForeignKeyConstraint(["user_id"], ["user.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_sdk_batch_receipt_updated_at", "sdk_batch_receipt", ["updated_at"])
    op.create_index("ix_sdk_batch_receipt_user_status", "sdk_batch_receipt", ["user_id", "status"])

    op.create_table(
        "sdk_sleep_inbox",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("external_id", sa.String(length=100), nullable=False),
        sa.Column("batch_ids", postgresql.ARRAY(sa.UUID()), nullable=False),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("payload", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("attempt_count", sa.Integer(), nullable=False),
        sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("materialized_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_error", sa.String(length=100), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "status IN ('staged', 'projecting', 'projected', 'materialized')",
            name="ck_sdk_sleep_inbox_status",
        ),
        sa.CheckConstraint("attempt_count >= 0", name="ck_sdk_sleep_inbox_attempt_count"),
        sa.CheckConstraint("cardinality(batch_ids) > 0", name="ck_sdk_sleep_inbox_batch_ids"),
        sa.ForeignKeyConstraint(["user_id"], ["user.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "user_id",
            "provider",
            "external_id",
            name="uq_sdk_sleep_inbox_identity",
        ),
    )
    op.create_index(
        "ix_sdk_sleep_inbox_batch_ids",
        "sdk_sleep_inbox",
        ["batch_ids"],
        postgresql_using="gin",
    )
    op.create_index(
        "ix_sdk_sleep_inbox_due",
        "sdk_sleep_inbox",
        ["status", "next_attempt_at"],
    )
    op.create_index(
        "ix_sdk_sleep_inbox_user_provider",
        "sdk_sleep_inbox",
        ["user_id", "provider"],
    )

    op.create_table(
        "sdk_sync_window_receipt",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("manifest_sha256", sa.String(length=64), nullable=False),
        sa.Column("purpose", sa.String(length=32), nullable=False),
        sa.Column("window_version", sa.Integer(), nullable=False),
        sa.Column("lower_bound_inclusive", sa.DateTime(timezone=True), nullable=False),
        sa.Column("upper_bound_exclusive", sa.DateTime(timezone=True), nullable=False),
        sa.Column("batch_ids", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("empty_or_no_access_types", postgresql.JSONB(astext_type=sa.Text()), nullable=False),
        sa.Column("reconciliation_start_inclusive", sa.DateTime(timezone=True), nullable=True),
        sa.Column("reconciliation_end_exclusive", sa.DateTime(timezone=True), nullable=True),
        sa.Column("accepted_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "purpose IN ('activation', 'archive', 'incremental')",
            name="ck_sdk_sync_window_receipt_purpose",
        ),
        sa.CheckConstraint("window_version = 2", name="ck_sdk_sync_window_receipt_version"),
        sa.CheckConstraint(
            "lower_bound_inclusive < upper_bound_exclusive",
            name="ck_sdk_sync_window_receipt_bounds",
        ),
        sa.CheckConstraint(
            "(reconciliation_start_inclusive IS NULL AND reconciliation_end_exclusive IS NULL) "
            "OR (reconciliation_start_inclusive IS NOT NULL AND reconciliation_end_exclusive IS NOT NULL "
            "AND reconciliation_start_inclusive < reconciliation_end_exclusive)",
            name="ck_sdk_sync_window_receipt_reconciliation_bounds",
        ),
        sa.CheckConstraint(
            "purpose <> 'incremental' OR reconciliation_start_inclusive IS NOT NULL",
            name="ck_sdk_sync_window_receipt_incremental_reconciliation",
        ),
        sa.CheckConstraint(
            "jsonb_array_length(batch_ids) > 0 OR jsonb_array_length(empty_or_no_access_types) > 0",
            name="ck_sdk_sync_window_receipt_coverage",
        ),
        sa.ForeignKeyConstraint(["id"], ["sdk_batch_receipt.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["user.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "ix_sdk_sync_window_receipt_user_provider_accepted",
        "sdk_sync_window_receipt",
        ["user_id", "provider", "accepted_at"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_sdk_sync_window_receipt_user_provider_accepted",
        table_name="sdk_sync_window_receipt",
    )
    op.drop_table("sdk_sync_window_receipt")
    op.drop_index("ix_sdk_sleep_inbox_user_provider", table_name="sdk_sleep_inbox")
    op.drop_index("ix_sdk_sleep_inbox_due", table_name="sdk_sleep_inbox")
    op.drop_index("ix_sdk_sleep_inbox_batch_ids", table_name="sdk_sleep_inbox")
    op.drop_table("sdk_sleep_inbox")
    op.drop_index("ix_sdk_batch_receipt_user_status", table_name="sdk_batch_receipt")
    op.drop_index("ix_sdk_batch_receipt_updated_at", table_name="sdk_batch_receipt")
    op.drop_table("sdk_batch_receipt")
