"""bind an optional activation policy to user invitation codes

Revision ID: a2c4e6f8b0d1
Revises: 8e3f1a9c2b47
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "a2c4e6f8b0d1"
down_revision: str | None = "8e3f1a9c2b47"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "user_invitation_code",
        sa.Column(
            "activation_policy",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=True,
        ),
    )


def downgrade() -> None:
    op.drop_column("user_invitation_code", "activation_policy")
