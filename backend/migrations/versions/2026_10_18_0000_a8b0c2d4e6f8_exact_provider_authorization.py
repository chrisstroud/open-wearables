"""Exact provider authorization and confirmed disconnect generation.

Revision ID: a8b0c2d4e6f8
Revises: f7a9b1c3d5e7
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a8b0c2d4e6f8"
down_revision: str | None = "f7a9b1c3d5e7"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Existing authorizations start at 1, without claiming an earlier disconnect
    # was verified. Never infer generations from timestamps or token contents.
    op.add_column(
        "user_connection", sa.Column("authorization_generation", sa.BigInteger(), nullable=False, server_default="1")
    )
    op.add_column("user_connection", sa.Column("disconnect_confirmed_generation", sa.BigInteger(), nullable=True))
    op.create_check_constraint(
        "ck_connection_authorization_generation", "user_connection", "authorization_generation > 0"
    )
    op.create_check_constraint(
        "ck_connection_disconnect_generation",
        "user_connection",
        "disconnect_confirmed_generation IS NULL OR "
        "(disconnect_confirmed_generation > 0 AND disconnect_confirmed_generation <= authorization_generation)",
    )


def downgrade() -> None:
    raise RuntimeError(
        "Preserve authorization generations and disconnect receipts; disable the exact endpoint before rollback"
    )
