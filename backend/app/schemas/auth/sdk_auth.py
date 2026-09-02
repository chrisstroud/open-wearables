from typing import Literal
from uuid import UUID

from pydantic import BaseModel


class SDKTokenRequest(BaseModel):
    """Request schema for exchanging app credentials for user token.

    Both fields are optional - if not provided, admin authentication is required.
    """

    app_id: str | None = None
    app_secret: str | None = None


class SDKAuthContext(BaseModel):
    """Context returned by SDK authentication dependency."""

    auth_type: Literal["sdk_token", "api_key"]
    user_id: UUID | None = None  # From SDK token (sub claim)
    app_id: str | None = None  # From SDK token
    installation_id: UUID | None = None  # First-class mobile installation, when present
    installation_generation: int | None = None
    protocol_version: int | None = None
    health_evidence_generation: int | None = None
    api_key_id: str | None = None  # From API key
