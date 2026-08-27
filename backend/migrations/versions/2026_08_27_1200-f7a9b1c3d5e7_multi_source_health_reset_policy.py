"""multi-source health reset policy

Revision ID: f7a9b1c3d5e7
Revises: e6f8a0b2c4d5
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "f7a9b1c3d5e7"
down_revision: str | None = "e6f8a0b2c4d5"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "user",
        sa.Column("health_reset_resulting_source_policy", sa.String(length=32), nullable=True),
    )
    op.execute(
        """
        UPDATE "user"
        SET health_reset_resulting_source_policy = 'apple-mobile-v2-only'
        WHERE health_reset_operation_id IS NOT NULL
          AND health_reset_resulting_source_policy IS NULL
        """
    )
    op.drop_constraint("ck_user_health_source_policy", "user", type_="check")
    op.create_check_constraint(
        "ck_user_health_source_policy",
        "user",
        "health_source_policy IN ('legacy-mixed', 'apple-mobile-v2-only', 'multi-source')",
    )
    op.create_check_constraint(
        "ck_user_health_reset_resulting_source_policy",
        "user",
        "health_reset_resulting_source_policy IS NULL OR "
        "health_reset_resulting_source_policy IN ('apple-mobile-v2-only', 'multi-source')",
    )


def downgrade() -> None:
    bind = op.get_bind()
    multi_source_exists = bind.execute(
        sa.text(
            """
            SELECT EXISTS (
                SELECT 1
                FROM "user"
                WHERE health_source_policy = 'multi-source'
                   OR health_reset_resulting_source_policy = 'multi-source'
            )
            """
        )
    ).scalar_one()
    if multi_source_exists:
        raise RuntimeError("Cannot downgrade multi-source health reset policy while multi-source state exists")

    op.drop_constraint(
        "ck_user_health_reset_resulting_source_policy",
        "user",
        type_="check",
    )
    op.drop_column("user", "health_reset_resulting_source_policy")
    op.drop_constraint("ck_user_health_source_policy", "user", type_="check")
    op.create_check_constraint(
        "ck_user_health_source_policy",
        "user",
        "health_source_policy IN ('legacy-mixed', 'apple-mobile-v2-only')",
    )
