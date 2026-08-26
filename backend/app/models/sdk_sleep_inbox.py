from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import CheckConstraint, Index, UniqueConstraint
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID as PGUUID
from sqlalchemy.orm import Mapped, mapped_column

from app.database import BaseDbModel
from app.mappings import FKUser, PrimaryKey, str_32, str_64, str_100


class SDKSleepInbox(BaseDbModel):
    """Durable source payload awaiting projection into the sleep model."""

    __tablename__ = "sdk_sleep_inbox"
    __table_args__ = (
        UniqueConstraint(
            "user_id",
            "provider",
            "external_id",
            name="uq_sdk_sleep_inbox_identity",
        ),
        Index(
            "ix_sdk_sleep_inbox_due",
            "status",
            "next_attempt_at",
        ),
        Index(
            "ix_sdk_sleep_inbox_user_provider",
            "user_id",
            "provider",
        ),
        Index(
            "ix_sdk_sleep_inbox_batch_ids",
            "batch_ids",
            postgresql_using="gin",
        ),
        CheckConstraint(
            "status IN ('staged', 'projecting', 'projected', 'materialized')",
            name="ck_sdk_sleep_inbox_status",
        ),
        CheckConstraint("attempt_count >= 0", name="ck_sdk_sleep_inbox_attempt_count"),
        CheckConstraint("cardinality(batch_ids) > 0", name="ck_sdk_sleep_inbox_batch_ids"),
    )

    id: Mapped[PrimaryKey[UUID]]
    user_id: Mapped[FKUser]
    provider: Mapped[str_32]
    external_id: Mapped[str_100]
    batch_ids: Mapped[list[UUID]] = mapped_column(ARRAY(PGUUID(as_uuid=True)))
    payload_sha256: Mapped[str_64]
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    status: Mapped[str_32]
    attempt_count: Mapped[int]
    next_attempt_at: Mapped[datetime]
    last_attempt_at: Mapped[datetime | None]
    materialized_at: Mapped[datetime | None]
    last_error: Mapped[str_100 | None]
    updated_at: Mapped[datetime]
