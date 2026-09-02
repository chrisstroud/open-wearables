from datetime import date, datetime
from uuid import UUID

from sqlalchemy import CheckConstraint, ForeignKey, Index, String, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.database import BaseDbModel
from app.mappings import FKUser, PrimaryKey, str_32, str_64, str_100


class AppleHealthDailySummary(BaseDbModel):
    """Append-only revision for one compact Apple Health summary item."""

    __tablename__ = "apple_health_daily_summary"
    __table_args__ = (
        Index(
            "uq_apple_health_daily_summary_revision",
            "user_id",
            "summary_kind",
            "stable_key",
            "revision_id",
            unique=True,
        ),
        Index(
            "uq_apple_health_daily_summary_current",
            "user_id",
            "summary_kind",
            "stable_key",
            unique=True,
            postgresql_where=text("is_current"),
        ),
        Index(
            "ix_apple_health_daily_summary_user_date_kind",
            "user_id",
            "local_date",
            "summary_kind",
            "series_type",
        ),
        CheckConstraint(
            "stable_key ~ '^[0-9a-f]{64}$' AND revision_id ~ '^[0-9a-f]{64}$' "
            "AND (supersedes_revision_id IS NULL OR supersedes_revision_id ~ '^[0-9a-f]{64}$') "
            "AND contributor_set_digest ~ '^[0-9a-f]{64}$' AND input_set_digest ~ '^[0-9a-f]{64}$'",
            name="ck_apple_health_daily_summary_digests",
        ),
        CheckConstraint(
            "(summary_kind = 'metric' AND schema_version = 'apple-health-daily-summary.v1' "
            "AND series_type IS NOT NULL) OR (summary_kind = 'sleep' "
            "AND schema_version = 'apple-health-sleep-summary.v1' AND series_type IS NULL) "
            "OR (summary_kind = 'workout' AND schema_version = 'apple-health-workout-summary.v1' "
            "AND series_type IS NOT NULL)",
            name="ck_apple_health_daily_summary_kind_schema",
        ),
        CheckConstraint(
            "installation_generation > 0 AND health_evidence_generation >= 0",
            name="ck_apple_health_daily_summary_generations",
        ),
        CheckConstraint(
            "jsonb_typeof(payload) = 'object'",
            name="ck_apple_health_daily_summary_payload_object",
        ),
    )

    id: Mapped[PrimaryKey[UUID]]
    user_id: Mapped[FKUser]
    installation_id: Mapped[UUID] = mapped_column(
        ForeignKey("sdk_client_installation.id", ondelete="RESTRICT"),
        nullable=False,
    )
    installation_generation: Mapped[int]
    health_evidence_generation: Mapped[int]
    batch_id: Mapped[UUID] = mapped_column(
        ForeignKey("sdk_batch_receipt.id", ondelete="RESTRICT"),
        nullable=False,
    )
    summary_kind: Mapped[str_32]
    stable_key: Mapped[str_64]
    schema_version: Mapped[str_64]
    revision_id: Mapped[str_64]
    supersedes_revision_id: Mapped[str_64 | None]
    local_date: Mapped[date]
    timezone: Mapped[str_100]
    timezone_boundary_version: Mapped[str_64]
    series_type: Mapped[str | None] = mapped_column(String(100), nullable=True)
    contributor_set_digest: Mapped[str_64]
    input_set_digest: Mapped[str_64]
    computed_at: Mapped[datetime]
    payload: Mapped[dict] = mapped_column(JSONB)
    is_current: Mapped[bool]
