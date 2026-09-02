from datetime import datetime
from uuid import UUID

from sqlalchemy import CheckConstraint, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column

from app.database import BaseDbModel
from app.mappings import PrimaryKey, str_32


class WhoopAuthorizationLease(BaseDbModel):
    """Fail-closed mutex between WHOOP reauthorization and an exact pull."""

    __tablename__ = "whoop_authorization_lease"
    __table_args__ = (
        CheckConstraint(
            "lease_kind IN ('oauth_callback', 'full_history_sync', 'disconnect', 'token_refresh')",
            name="ck_whoop_authorization_lease_kind",
        ),
        CheckConstraint(
            "authorization_generation >= 0",
            name="ck_whoop_authorization_lease_generation",
        ),
        CheckConstraint(
            "lease_expires_at > acquired_at",
            name="ck_whoop_authorization_lease_expiry",
        ),
    )

    user_id: Mapped[PrimaryKey[UUID]] = mapped_column(
        ForeignKey("user.id", ondelete="CASCADE"),
    )
    connection_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("user_connection.id", ondelete="CASCADE"),
    )
    authorization_generation: Mapped[int]
    lease_token: Mapped[UUID]
    lease_kind: Mapped[str_32]
    acquired_at: Mapped[datetime]
    lease_expires_at: Mapped[datetime]
    updated_at: Mapped[datetime]
