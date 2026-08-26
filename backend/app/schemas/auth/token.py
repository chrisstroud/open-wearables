from enum import StrEnum
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class TokenType(StrEnum):
    """Type of refresh token."""

    SDK = "sdk"
    DEVELOPER = "developer"


class TokenResponse(BaseModel):
    """Token response with optional refresh token."""

    access_token: str
    token_type: str = "bearer"
    refresh_token: str | None = None
    expires_in: int | None = None  # seconds


class SDKClientMetadataRefresh(BaseModel):
    """Release metadata an existing installation may refresh during rotation."""

    model_config = ConfigDict(extra="forbid")

    installation_id: UUID
    bundle_id: Literal["fitness.dashboard.app"]
    app_version: str = Field(..., min_length=1, max_length=32, pattern=r"^[0-9]+(?:\.[0-9]+){0,2}$")
    build_number: str = Field(..., min_length=1, max_length=32, pattern=r"^[0-9]+$")
    protocol_version: Literal[2]


class RefreshTokenRequest(BaseModel):
    """Request to exchange refresh token for new access token."""

    refresh_token: str
    client: SDKClientMetadataRefresh | None = None
