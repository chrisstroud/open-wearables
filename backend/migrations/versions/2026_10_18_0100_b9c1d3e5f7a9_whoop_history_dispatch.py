"""Durable exact WHOOP history dispatch receipts.

Revision ID: b9c1d3e5f7a9
Revises: a8b0c2d4e6f8
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "b9c1d3e5f7a9"
down_revision: str | None = "a8b0c2d4e6f8"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "whoop_history_dispatch",
        sa.Column("id", sa.UUID(), primary_key=True),
        sa.Column("user_id", sa.UUID(), sa.ForeignKey("user.id", ondelete="CASCADE"), nullable=False),
        sa.Column("connection_id", sa.UUID(), nullable=False),
        sa.Column("authorization_generation", sa.BigInteger(), nullable=False),
        sa.Column("health_evidence_generation", sa.Integer(), nullable=False),
        sa.Column("task_id", sa.UUID(), nullable=False),
        sa.Column("requested_start_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("requested_end_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("status", sa.String(32), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("completed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("error_code", sa.String(100), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "status IN ('queued', 'running', 'succeeded', 'failed', 'superseded')", name="ck_whoop_history_status"
        ),
        sa.CheckConstraint(
            "authorization_generation > 0 AND health_evidence_generation >= 0", name="ck_whoop_history_generations"
        ),
        sa.CheckConstraint("requested_start_at < requested_end_at", name="ck_whoop_history_window"),
        sa.CheckConstraint("status <> 'running' OR expires_at IS NOT NULL", name="ck_whoop_history_running_deadline"),
        sa.CheckConstraint(
            "(status IN ('queued', 'running') AND completed_at IS NULL) OR "
            "(status IN ('succeeded', 'failed', 'superseded') AND completed_at IS NOT NULL)",
            name="ck_whoop_history_completion",
        ),
    )
    op.create_index(
        "uq_whoop_history_active_user",
        "whoop_history_dispatch",
        ["user_id"],
        unique=True,
        postgresql_where=sa.text("status IN ('queued', 'running')"),
    )
    op.create_index("ix_whoop_history_status_created", "whoop_history_dispatch", ["status", "created_at"])


def downgrade() -> None:
    raise RuntimeError(
        "Preserve history receipts; disable admission and drain workers before rolling application code back"
    )
