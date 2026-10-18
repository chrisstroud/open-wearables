from datetime import datetime
from uuid import UUID

from sqlalchemy import BigInteger, CheckConstraint, Index
from sqlalchemy.orm import Mapped, mapped_column

from app.database import BaseDbModel
from app.mappings import FKUser, PrimaryKey, str_64
from app.schemas.auth import ConnectionStatus


class UserConnection(BaseDbModel):
    """OAuth connections to external cloud providers (Suunto, Garmin, Polar, Coros)"""

    __table_args__ = (
        CheckConstraint("authorization_generation > 0", name="ck_connection_authorization_generation"),
        CheckConstraint(
            "disconnect_confirmed_generation IS NULL OR "
            "(disconnect_confirmed_generation > 0 AND disconnect_confirmed_generation <= authorization_generation)",
            name="ck_connection_disconnect_generation",
        ),
        Index(
            "ix_user_connection_token_expiry",
            "token_expires_at",
            postgresql_where="status = 'active'",
        ),
        Index("ix_user_connection_user_provider", "user_id", "provider", unique=True),
        Index("ix_user_connection_status_user_id", "status", "user_id"),
        Index(
            "ix_user_connection_provider_external_id",
            "provider",
            "provider_user_id",
            postgresql_where="provider_user_id IS NOT NULL AND status = 'active'",
        ),
    )
    __tablename__ = "user_connection"

    id: Mapped[PrimaryKey[UUID]]
    user_id: Mapped[FKUser]
    provider: Mapped[str_64]  # 'suunto', 'garmin', 'polar', 'coros'
    # New consent increments this; refreshing the same authorization does not.
    authorization_generation: Mapped[int] = mapped_column(BigInteger, default=1, server_default="1")
    # Written only after an exact disconnect has verified provider deregistration.
    disconnect_confirmed_generation: Mapped[int | None] = mapped_column(BigInteger)

    # Provider user data
    provider_user_id: Mapped[str | None]
    provider_username: Mapped[str | None]

    # OAuth tokens (optional for SDK-based providers like Apple)
    access_token: Mapped[str | None]
    refresh_token: Mapped[str | None]
    token_expires_at: Mapped[datetime | None]
    scope: Mapped[str | None]

    # Metadata
    status: Mapped[ConnectionStatus]
    last_synced_at: Mapped[datetime | None]
    updated_at: Mapped[datetime]
