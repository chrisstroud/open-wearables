from datetime import datetime
from uuid import UUID

from sqlalchemy import CheckConstraint, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.database import BaseDbModel
from app.mappings import PrimaryKey, Unique, email, str_100, str_255


class User(BaseDbModel):
    """Data owner model"""

    __table_args__ = (
        CheckConstraint(
            "health_write_state IN ('active', 'fenced', 'awaiting-v2-pairing', 'activating')",
            name="ck_user_health_write_state",
        ),
        CheckConstraint(
            "health_source_policy IN ('legacy-mixed', 'apple-mobile-v2-only')",
            name="ck_user_health_source_policy",
        ),
        CheckConstraint(
            "health_evidence_generation >= 0",
            name="ck_user_health_evidence_generation",
        ),
    )

    id: Mapped[PrimaryKey[UUID]]

    first_name: Mapped[str_100 | None]
    last_name: Mapped[str_100 | None]
    email: Mapped[email | None]

    external_user_id: Mapped[Unique[str_255] | None]
    health_evidence_generation: Mapped[int] = mapped_column(default=0, server_default="0")
    health_reset_operation_id: Mapped[UUID | None]
    health_reset_manifest_sha256: Mapped[str | None] = mapped_column(String(64), nullable=True)
    health_reset_manifest_counts: Mapped[dict[str, int] | None] = mapped_column(JSONB, nullable=True)
    health_reset_deleted_counts: Mapped[dict[str, int] | None] = mapped_column(JSONB, nullable=True)
    health_reset_applied_at: Mapped[datetime | None]
    health_write_state: Mapped[str] = mapped_column(String(32), default="active", server_default="active")
    health_source_policy: Mapped[str] = mapped_column(
        String(32),
        default="legacy-mixed",
        server_default="legacy-mixed",
    )

    personal_record: Mapped["PersonalRecord | None"] = relationship(
        back_populates="user",
        uselist=False,
        cascade="all, delete-orphan",
    )
