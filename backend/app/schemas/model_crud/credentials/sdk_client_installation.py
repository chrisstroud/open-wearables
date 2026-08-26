from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class SDKClientRegistration(BaseModel):
    """Release identity supplied while redeeming a dashboard.fitness grant."""

    model_config = ConfigDict(extra="forbid")

    installation_id: UUID
    bundle_id: Literal["fitness.dashboard.app"]
    app_version: str = Field(..., min_length=1, max_length=32, pattern=r"^[0-9]+(?:\.[0-9]+){0,2}$")
    build_number: str = Field(..., min_length=1, max_length=32, pattern=r"^[0-9]+$")
    protocol_version: Literal[2]


class SDKClientInstallationRead(BaseModel):
    """Privacy-safe connected-device projection for the product website."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    user_id: UUID
    bundle_id: str
    app_version: str
    build_number: str
    protocol_version: int
    generation: int
    health_evidence_generation: int
    status: Literal["active", "revoked"]
    connected_at: datetime
    last_contact_at: datetime
    last_terminal_receipt_at: datetime | None
    recent_history_ready_at: datetime | None
    archive_earliest_confirmed_at: datetime | None
    revoked_at: datetime | None


class SDKClientInstallationRevokeRequest(BaseModel):
    """Optimistic authority required for dashboard-initiated revocation."""

    model_config = ConfigDict(extra="forbid")

    expected_generation: int = Field(..., gt=0)
    expected_health_evidence_generation: int = Field(..., ge=0)


class SDKHealthResetTransitionRequest(BaseModel):
    """Idempotency and generation authority for a protected reset transition."""

    model_config = ConfigDict(extra="forbid")

    operation_id: UUID
    expected_health_evidence_generation: int = Field(..., ge=0)
    expected_installation_generation: int | None = Field(None, gt=0)
    expected_inventory_digest_sha256: str | None = Field(None, pattern=r"^[0-9a-f]{64}$")


class SDKHealthResetStateRead(BaseModel):
    """Privacy-safe health-write fence state; it never contains health values."""

    user_id: UUID
    operation_id: UUID | None
    health_evidence_generation: int
    health_write_state: Literal["active", "fenced", "awaiting-v2-pairing", "activating"]
    health_source_policy: Literal["legacy-mixed", "apple-mobile-v2-only"]
    active_installation_id: UUID | None
    active_installation_generation: int | None
    queued_or_processing_upload_count: int
    pending_sleep_projection_count: int
    drained: bool
    operational_digest_sha256: str
    resource_counts: dict[str, int]
    inventory_digest_sha256: str
    blockers: list[str]
    verified_empty: bool
