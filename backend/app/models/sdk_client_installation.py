from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import CheckConstraint, Index, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.database import BaseDbModel
from app.mappings import FKUser, PrimaryKey, str_32, str_64, str_100


class SDKClientInstallation(BaseDbModel):
    """One independently revocable mobile SDK installation."""

    __tablename__ = "sdk_client_installation"
    __table_args__ = (
        Index(
            "uq_sdk_client_installation_active_user",
            "user_id",
            unique=True,
            postgresql_where=text("status = 'active'"),
        ),
        Index(
            "uq_sdk_client_installation_user_generation",
            "user_id",
            "generation",
            unique=True,
        ),
        Index("uq_sdk_client_installation_app_id", "app_id", unique=True),
        CheckConstraint(
            "status IN ('active', 'revoked')",
            name="ck_sdk_client_installation_status",
        ),
        CheckConstraint("generation > 0", name="ck_sdk_client_installation_generation"),
        CheckConstraint("protocol_version > 0", name="ck_sdk_client_installation_protocol_version"),
    )

    id: Mapped[PrimaryKey[UUID]]
    user_id: Mapped[FKUser]
    app_id: Mapped[str_64]
    bundle_id: Mapped[str_100]
    app_version: Mapped[str_32]
    build_number: Mapped[str_32]
    protocol_version: Mapped[int]
    activation_policy: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    health_evidence_generation: Mapped[int]
    generation: Mapped[int]
    status: Mapped[str_32]
    connected_at: Mapped[datetime]
    last_contact_at: Mapped[datetime]
    last_terminal_receipt_at: Mapped[datetime | None]
    revoked_at: Mapped[datetime | None]
