"""Durable whole-account erasure binding and permanent in-flight write fence.

Revision ID: c6e8a0b2d4f6
Revises: b5d7f9a1c3e4
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "c6e8a0b2d4f6"
down_revision: str | None = "b5d7f9a1c3e4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column("user", sa.Column("account_erasure_operation_id", sa.UUID(), nullable=True))
    op.create_check_constraint(
        "ck_user_account_erasure_fence",
        "user",
        "account_erasure_operation_id IS NULL OR health_write_state = 'fenced'",
    )
    op.create_table(
        "account_erasure_operation",
        sa.Column("operation_id", sa.UUID(), primary_key=True),
        sa.Column("open_wearables_user_id", sa.UUID(), nullable=False),
        sa.Column("binding_digest", sa.String(64), nullable=False),
        sa.Column("plan_fingerprint", sa.String(64), nullable=True),
        sa.Column("inventory_digest", sa.String(64), nullable=False),
        sa.Column("worker_set_digest", sa.String(64), nullable=False),
        sa.Column("health_evidence_generation", sa.Integer(), nullable=False),
        sa.Column("installation_generation", sa.Integer(), nullable=True),
        sa.Column("state", sa.String(16), nullable=False),
        sa.Column("identity_proof", postgresql.JSONB(), nullable=True),
        sa.Column("configuration_digest", sa.String(64), nullable=True),
        sa.Column("receipt_fingerprint", sa.String(64), nullable=True),
        sa.Column("recorded_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint("state IN ('planned', 'fenced', 'deleted')", name="ck_account_erasure_state"),
        sa.CheckConstraint("binding_digest ~ '^[0-9a-f]{64}$'", name="ck_account_erasure_binding"),
        sa.CheckConstraint(
            "plan_fingerprint IS NULL OR plan_fingerprint ~ '^[0-9a-f]{64}$'", name="ck_account_erasure_plan"
        ),
        sa.CheckConstraint(
            "(state = 'deleted' AND receipt_fingerprint IS NOT NULL AND recorded_at IS NOT NULL "
            "AND identity_proof IS NOT NULL AND configuration_digest IS NOT NULL AND plan_fingerprint IS NOT NULL) "
            "OR (state <> 'deleted' AND receipt_fingerprint IS NULL AND recorded_at IS NULL)",
            name="ck_account_erasure_terminal",
        ),
    )
    op.create_index(
        "ix_account_erasure_operation_open_wearables_user_id", "account_erasure_operation", ["open_wearables_user_id"]
    )


def downgrade() -> None:
    # Recovery receipts may be the only remaining proof after user deletion.
    # Do not silently destroy them or reopen an unfinished account fence.
    connection = op.get_bind()
    if connection.scalar(sa.text("SELECT EXISTS (SELECT 1 FROM account_erasure_operation)")):
        raise RuntimeError("Account-erasure recovery records require explicit disposition before downgrade")
    op.drop_table("account_erasure_operation")
    op.drop_constraint("ck_user_account_erasure_fence", "user", type_="check")
    op.drop_column("user", "account_erasure_operation_id")
