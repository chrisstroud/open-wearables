from datetime import datetime, time
from typing import Any, Literal
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

from app.schemas.auth import TokenResponse


class UserInvitationActivationPolicy(BaseModel):
    """Immutable account-local activation window carried to the mobile SDK."""

    model_config = ConfigDict(extra="forbid")

    purpose: Literal["activation"]
    window_version: Literal[2]
    lower_bound_inclusive: AwareDatetime
    upper_bound_exclusive: AwareDatetime
    timezone: str = Field(..., min_length=1, max_length=64)
    completed_day_count: Literal[30]

    @model_validator(mode="after")
    def validate_completed_local_days(self) -> "UserInvitationActivationPolicy":
        try:
            local_zone = ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError as error:
            raise ValueError("timezone must be a valid IANA timezone") from error

        lower = self.lower_bound_inclusive.astimezone(local_zone)
        upper = self.upper_bound_exclusive.astimezone(local_zone)
        if lower.timetz().replace(tzinfo=None) != time.min or upper.timetz().replace(tzinfo=None) != time.min:
            raise ValueError("activation bounds must be account-local midnight instants")
        if self.lower_bound_inclusive >= self.upper_bound_exclusive:
            raise ValueError("activation lower bound must precede upper bound")
        if (upper.date() - lower.date()).days != self.completed_day_count:
            raise ValueError("activation policy must cover exactly 30 completed local days")
        return self

    def storage_value(self) -> dict[str, Any]:
        """Return a JSON-safe value without logging or widening the policy."""
        return self.model_dump(mode="json")


class UserInvitationCodeCreate(BaseModel):
    """Internal schema with all fields for repository creation."""

    id: UUID
    code: str
    user_id: UUID
    created_by_id: UUID
    expires_at: datetime
    redeemed_at: None = None
    revoked_at: None = None
    activation_policy: dict[str, Any] | None = None
    created_at: datetime


class UserInvitationCodeRead(BaseModel):
    """Response schema after generating an invitation code."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    code: str
    user_id: UUID
    expires_at: datetime
    activation_policy: UserInvitationActivationPolicy | None = None
    created_at: datetime


class UserInvitationCodeGenerate(BaseModel):
    """Optional policy-bound generation request; omitted for legacy clients."""

    model_config = ConfigDict(extra="forbid")

    activation_policy: UserInvitationActivationPolicy | None = None


class UserInvitationCodeRedeem(BaseModel):
    """API input for redeeming an invitation code."""

    code: str = Field(..., min_length=8, max_length=8, pattern=r"^[A-Z2-9]{8}$")


class InvitationCodeRedeemResponse(TokenResponse):
    """Redeem response with user_id included."""

    user_id: UUID
    activation_policy: UserInvitationActivationPolicy | None = None
