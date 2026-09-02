from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.database import BaseDbModel
from app.mappings import FKDeveloper, FKUser, Indexed, PrimaryKey, Unique, str_10


class UserInvitationCode(BaseDbModel):
    """Single-use invitation code for SDK user onboarding.

    A developer generates a code for a specific user_id. The mobile app user
    enters this code, which is exchanged for SDK access_token + refresh_token.
    """

    __tablename__ = "user_invitation_code"

    id: Mapped[PrimaryKey[UUID]]
    code: Mapped[Unique[str_10]]
    user_id: Mapped[Indexed[FKUser]]
    created_by_id: Mapped[FKDeveloper]
    expires_at: Mapped[datetime]
    redeemed_at: Mapped[datetime | None]
    revoked_at: Mapped[datetime | None]
    activation_policy: Mapped[dict[str, Any] | None] = mapped_column(JSONB, nullable=True)
    health_evidence_generation: Mapped[int] = mapped_column(default=0, server_default="0")
