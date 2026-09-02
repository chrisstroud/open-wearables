"""WHOOP exact generation-bound sync dispatch receipts

Revision ID: b5d7f9a1c3e4
Revises: a3c5e7f9b1d2
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b5d7f9a1c3e4"
down_revision: str | None = "a3c5e7f9b1d2"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "user_connection",
        sa.Column("authorization_generation", sa.Integer(), server_default="1", nullable=False),
    )
    op.create_check_constraint(
        "ck_user_connection_authorization_generation",
        "user_connection",
        "authorization_generation > 0",
    )

    op.create_table(
        "whoop_authorization_lease",
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("connection_id", sa.UUID(), nullable=True),
        sa.Column("authorization_generation", sa.Integer(), nullable=False),
        sa.Column("lease_token", sa.UUID(), nullable=False),
        sa.Column("lease_kind", sa.String(length=32), nullable=False),
        sa.Column("acquired_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("lease_expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "lease_kind IN ('oauth_callback', 'full_history_sync', 'disconnect', 'token_refresh')",
            name="ck_whoop_authorization_lease_kind",
        ),
        sa.CheckConstraint(
            "authorization_generation >= 0",
            name="ck_whoop_authorization_lease_generation",
        ),
        sa.CheckConstraint(
            "lease_expires_at > acquired_at",
            name="ck_whoop_authorization_lease_expiry",
        ),
        sa.ForeignKeyConstraint(["connection_id"], ["user_connection.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["user.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("user_id"),
    )

    op.create_table(
        "whoop_sync_dispatch_receipt",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("connection_id", sa.UUID(), nullable=False),
        sa.Column("authorization_generation", sa.Integer(), nullable=False),
        sa.Column("request_fingerprint", sa.String(length=64), nullable=False),
        sa.Column("requested_start_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("requested_end_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("task_id", sa.UUID(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("enqueue_attempt_count", sa.Integer(), nullable=False),
        sa.Column("execution_attempt_count", sa.Integer(), nullable=False),
        sa.Column("next_enqueue_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("enqueued_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("lease_token", sa.UUID(), nullable=True),
        sa.Column("processing_started_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_code", sa.String(length=100), nullable=True),
        sa.Column("updated_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "authorization_generation > 0",
            name="ck_whoop_sync_dispatch_authorization_generation",
        ),
        sa.CheckConstraint(
            "requested_start_at < requested_end_at",
            name="ck_whoop_sync_dispatch_bounds",
        ),
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed', 'superseded')",
            name="ck_whoop_sync_dispatch_status",
        ),
        sa.CheckConstraint(
            "enqueue_attempt_count >= 0 AND execution_attempt_count >= 0",
            name="ck_whoop_sync_dispatch_attempt_counts",
        ),
        sa.CheckConstraint(
            "(status = 'running' AND lease_token IS NOT NULL AND processing_started_at IS NOT NULL) OR "
            "(status <> 'running' AND lease_token IS NULL)",
            name="ck_whoop_sync_dispatch_lease_state",
        ),
        sa.ForeignKeyConstraint(["connection_id"], ["user_connection.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["user.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint(
            "connection_id",
            "authorization_generation",
            "requested_start_at",
            "requested_end_at",
            name="uq_whoop_sync_dispatch_exact_window",
        ),
    )
    op.create_index(
        "ix_whoop_sync_dispatch_outbox",
        "whoop_sync_dispatch_receipt",
        ["status", "next_enqueue_at"],
    )
    op.create_index(
        "ix_whoop_sync_dispatch_user_created",
        "whoop_sync_dispatch_receipt",
        ["user_id", "created_at"],
    )


def downgrade() -> None:
    op.drop_index("ix_whoop_sync_dispatch_user_created", table_name="whoop_sync_dispatch_receipt")
    op.drop_index("ix_whoop_sync_dispatch_outbox", table_name="whoop_sync_dispatch_receipt")
    op.drop_table("whoop_sync_dispatch_receipt")
    op.drop_table("whoop_authorization_lease")
    op.drop_constraint(
        "ck_user_connection_authorization_generation",
        "user_connection",
        type_="check",
    )
    op.drop_column("user_connection", "authorization_generation")
