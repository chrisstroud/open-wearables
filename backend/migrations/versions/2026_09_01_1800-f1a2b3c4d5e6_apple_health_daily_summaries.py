"""apple health daily summaries

Revision ID: f1a2b3c4d5e6
Revises: e6f8a0b2c4d5
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "f1a2b3c4d5e6"
down_revision: str | None = "e6f8a0b2c4d5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "sdk_batch_receipt",
        sa.Column("daily_summaries_saved", sa.Integer(), server_default="0", nullable=False),
    )
    op.create_check_constraint(
        "ck_sdk_batch_receipt_daily_summaries_saved",
        "sdk_batch_receipt",
        "daily_summaries_saved >= 0",
    )
    op.add_column(
        "sdk_batch_receipt",
        sa.Column("revision_set_digest", sa.String(length=64), nullable=True),
    )
    op.create_check_constraint(
        "ck_sdk_batch_receipt_revision_set_digest_format",
        "sdk_batch_receipt",
        "revision_set_digest IS NULL OR revision_set_digest ~ '^[0-9a-f]{64}$'",
    )
    op.create_check_constraint(
        "ck_sdk_batch_receipt_revision_set_digest_state",
        "sdk_batch_receipt",
        "(revision_set_digest IS NULL OR "
        "(provider = 'apple' AND status = 'succeeded' AND daily_summaries_saved > 0)) "
        "AND (daily_summaries_saved = 0 OR "
        "(provider = 'apple' AND status = 'succeeded' AND revision_set_digest IS NOT NULL))",
    )

    op.create_table(
        "apple_health_daily_summary",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("installation_id", sa.UUID(), nullable=False),
        sa.Column("installation_generation", sa.Integer(), nullable=False),
        sa.Column("health_evidence_generation", sa.Integer(), nullable=False),
        sa.Column("batch_id", sa.UUID(), nullable=False),
        sa.Column("summary_kind", sa.String(length=32), nullable=False),
        sa.Column("stable_key", sa.String(length=64), nullable=False),
        sa.Column("schema_version", sa.String(length=64), nullable=False),
        sa.Column("revision_id", sa.String(length=64), nullable=False),
        sa.Column("supersedes_revision_id", sa.String(length=64), nullable=True),
        sa.Column("local_date", sa.Date(), nullable=False),
        sa.Column("timezone", sa.String(length=100), nullable=False),
        sa.Column("timezone_boundary_version", sa.String(length=64), nullable=False),
        sa.Column("series_type", sa.String(length=100), nullable=True),
        sa.Column("contributor_set_digest", sa.String(length=64), nullable=False),
        sa.Column("input_set_digest", sa.String(length=64), nullable=False),
        sa.Column("computed_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("payload", postgresql.JSONB(), nullable=False),
        sa.Column("is_current", sa.Boolean(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "stable_key ~ '^[0-9a-f]{64}$' AND revision_id ~ '^[0-9a-f]{64}$' "
            "AND (supersedes_revision_id IS NULL OR supersedes_revision_id ~ '^[0-9a-f]{64}$') "
            "AND contributor_set_digest ~ '^[0-9a-f]{64}$' AND input_set_digest ~ '^[0-9a-f]{64}$'",
            name="ck_apple_health_daily_summary_digests",
        ),
        sa.CheckConstraint(
            "(summary_kind = 'metric' AND schema_version = 'apple-health-daily-summary.v1' "
            "AND series_type IS NOT NULL) OR (summary_kind = 'sleep' "
            "AND schema_version = 'apple-health-sleep-summary.v1' AND series_type IS NULL) "
            "OR (summary_kind = 'workout' AND schema_version = 'apple-health-workout-summary.v1' "
            "AND series_type IS NOT NULL)",
            name="ck_apple_health_daily_summary_kind_schema",
        ),
        sa.CheckConstraint(
            "installation_generation > 0 AND health_evidence_generation >= 0",
            name="ck_apple_health_daily_summary_generations",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(payload) = 'object'",
            name="ck_apple_health_daily_summary_payload_object",
        ),
        sa.ForeignKeyConstraint(["batch_id"], ["sdk_batch_receipt.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["installation_id"], ["sdk_client_installation.id"], ondelete="RESTRICT"),
        sa.ForeignKeyConstraint(["user_id"], ["user.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "uq_apple_health_daily_summary_revision",
        "apple_health_daily_summary",
        ["user_id", "summary_kind", "stable_key", "revision_id"],
        unique=True,
    )
    op.create_index(
        "uq_apple_health_daily_summary_current",
        "apple_health_daily_summary",
        ["user_id", "summary_kind", "stable_key"],
        unique=True,
        postgresql_where=sa.text("is_current"),
    )
    op.create_index(
        "ix_apple_health_daily_summary_user_date_kind",
        "apple_health_daily_summary",
        ["user_id", "local_date", "summary_kind", "series_type"],
    )


def downgrade() -> None:
    bind = op.get_bind()
    populated = bind.execute(
        sa.text(
            """
            SELECT
              EXISTS (SELECT 1 FROM apple_health_daily_summary) OR
              EXISTS (
                SELECT 1 FROM sdk_batch_receipt
                WHERE daily_summaries_saved > 0
                   OR revision_set_digest IS NOT NULL
              )
            """
        )
    ).scalar_one()
    if populated:
        raise RuntimeError("f1a2b3c4d5e6 is forward-only after Apple Health daily summary state has been populated")

    op.drop_index(
        "ix_apple_health_daily_summary_user_date_kind",
        table_name="apple_health_daily_summary",
    )
    op.drop_index(
        "uq_apple_health_daily_summary_current",
        table_name="apple_health_daily_summary",
    )
    op.drop_index(
        "uq_apple_health_daily_summary_revision",
        table_name="apple_health_daily_summary",
    )
    op.drop_table("apple_health_daily_summary")
    op.drop_constraint(
        "ck_sdk_batch_receipt_revision_set_digest_state",
        "sdk_batch_receipt",
        type_="check",
    )
    op.drop_constraint(
        "ck_sdk_batch_receipt_revision_set_digest_format",
        "sdk_batch_receipt",
        type_="check",
    )
    op.drop_column("sdk_batch_receipt", "revision_set_digest")
    op.drop_constraint(
        "ck_sdk_batch_receipt_daily_summaries_saved",
        "sdk_batch_receipt",
        type_="check",
    )
    op.drop_column("sdk_batch_receipt", "daily_summaries_saved")
