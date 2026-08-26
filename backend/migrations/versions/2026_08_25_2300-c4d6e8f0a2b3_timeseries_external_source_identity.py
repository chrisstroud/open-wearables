"""timeseries external source identity

Revision ID: c4d6e8f0a2b3
Revises: a2c4e6f8b0d1
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "c4d6e8f0a2b3"
down_revision: str | None = "a2c4e6f8b0d1"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    # Existing timestamp uniqueness collapses UUID-distinct HealthKit samples.
    # Refuse to guess if historical rows already contradict stable source UUID
    # identity; no accepted row is deleted or rewritten by this migration.
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM data_point_series
                WHERE external_id IS NOT NULL
                GROUP BY data_source_id, series_type_definition_id, external_id
                HAVING count(*) > 1
            ) THEN
                RAISE EXCEPTION
                    'cannot install external source identity: duplicate non-null external_id rows exist';
            END IF;
        END
        $$;
        """
    )
    op.drop_constraint(
        "uq_data_point_series_source_type_time",
        "data_point_series",
        type_="unique",
    )
    op.create_index(
        "uq_data_point_series_source_type_external_id",
        "data_point_series",
        ["data_source_id", "series_type_definition_id", "external_id"],
        unique=True,
        postgresql_where=sa.text("external_id IS NOT NULL"),
    )
    op.create_index(
        "uq_data_point_series_source_type_time_legacy",
        "data_point_series",
        ["data_source_id", "series_type_definition_id", "recorded_at"],
        unique=True,
        postgresql_where=sa.text("external_id IS NULL"),
    )


def downgrade() -> None:
    # Once UUID-distinct same-time samples exist, the old constraint cannot
    # represent them. Fail closed rather than discard one during rollback.
    op.execute(
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1
                FROM data_point_series
                GROUP BY data_source_id, series_type_definition_id, recorded_at
                HAVING count(*) > 1
            ) THEN
                RAISE EXCEPTION
                    'cannot restore timestamp-only identity: UUID-distinct same-time rows exist';
            END IF;
        END
        $$;
        """
    )
    op.drop_index(
        "uq_data_point_series_source_type_time_legacy",
        table_name="data_point_series",
    )
    op.drop_index(
        "uq_data_point_series_source_type_external_id",
        table_name="data_point_series",
    )
    op.create_unique_constraint(
        "uq_data_point_series_source_type_time",
        "data_point_series",
        ["data_source_id", "series_type_definition_id", "recorded_at"],
    )
