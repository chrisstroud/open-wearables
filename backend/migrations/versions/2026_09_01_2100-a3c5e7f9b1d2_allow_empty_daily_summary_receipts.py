"""allow empty apple health daily summary receipts

Revision ID: a3c5e7f9b1d2
Revises: f1a2b3c4d5e6
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "a3c5e7f9b1d2"
down_revision: str | None = "f1a2b3c4d5e6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

EMPTY_REVISION_SET_DIGEST = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


def upgrade() -> None:
    op.drop_constraint(
        "ck_sdk_batch_receipt_revision_set_digest_state",
        "sdk_batch_receipt",
        type_="check",
    )
    op.create_check_constraint(
        "ck_sdk_batch_receipt_revision_set_digest_state",
        "sdk_batch_receipt",
        "(revision_set_digest IS NULL AND daily_summaries_saved = 0) OR "
        "(provider = 'apple' AND status = 'succeeded' AND "
        "((daily_summaries_saved = 0 AND revision_set_digest = "
        f"'{EMPTY_REVISION_SET_DIGEST}') OR "
        "(daily_summaries_saved > 0 AND revision_set_digest <> "
        f"'{EMPTY_REVISION_SET_DIGEST}')))",
    )


def downgrade() -> None:
    bind = op.get_bind()
    has_empty_revision_receipts = bind.execute(
        sa.text(
            """
            SELECT EXISTS (
                SELECT 1
                FROM sdk_batch_receipt
                WHERE daily_summaries_saved = 0
                  AND revision_set_digest IS NOT NULL
            )
            """
        )
    ).scalar_one()
    if has_empty_revision_receipts:
        raise RuntimeError("a3c5e7f9b1d2 cannot downgrade after an empty daily-summary receipt is accepted")

    op.drop_constraint(
        "ck_sdk_batch_receipt_revision_set_digest_state",
        "sdk_batch_receipt",
        type_="check",
    )
    op.create_check_constraint(
        "ck_sdk_batch_receipt_revision_set_digest_state",
        "sdk_batch_receipt",
        "(revision_set_digest IS NULL OR "
        "(provider = 'apple' AND status = 'succeeded' AND daily_summaries_saved > 0)) "
        "AND (daily_summaries_saved = 0 OR "
        "(provider = 'apple' AND status = 'succeeded' AND revision_set_digest IS NOT NULL))",
    )
