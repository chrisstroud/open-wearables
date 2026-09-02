from datetime import datetime, timezone
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class WhoopSyncDispatchStatus(StrEnum):
    QUEUED = "queued"
    RUNNING = "running"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    SUPERSEDED = "superseded"


class WhoopFullHistorySyncCommand(BaseModel):
    """Idempotent command bound to one exact WHOOP authorization generation."""

    model_config = ConfigDict(frozen=True)

    idempotency_key: UUID
    authorization_generation: int = Field(ge=1)
    requested_start_at: datetime
    requested_end_at: datetime

    @model_validator(mode="after")
    def validate_and_normalize_bounds(self) -> "WhoopFullHistorySyncCommand":
        if self.requested_start_at.tzinfo is None or self.requested_end_at.tzinfo is None:
            raise ValueError("WHOOP full-history bounds must include a timezone")
        start = self.requested_start_at.astimezone(timezone.utc)
        end = self.requested_end_at.astimezone(timezone.utc)
        if start >= end:
            raise ValueError("requested_start_at must precede requested_end_at")
        object.__setattr__(self, "requested_start_at", start)
        object.__setattr__(self, "requested_end_at", end)
        return self


class WhoopSyncDispatchRead(BaseModel):
    """Durable acknowledgement for an exact WHOOP history command."""

    model_config = ConfigDict(from_attributes=True)

    dispatch_id: UUID = Field(validation_alias="id")
    task_id: UUID
    user_id: UUID
    connection_id: UUID
    authorization_generation: int
    requested_start_at: datetime
    requested_end_at: datetime
    status: WhoopSyncDispatchStatus
    enqueue_attempt_count: int
    execution_attempt_count: int
    completed_at: datetime | None
    error_code: str | None
    created_at: datetime
    updated_at: datetime

    @field_validator("requested_start_at", "requested_end_at", mode="after")
    @classmethod
    def normalize_bounds(cls, value: datetime) -> datetime:
        return value.astimezone(timezone.utc)


class WhoopFullHistorySyncResponse(WhoopSyncDispatchRead):
    model_config = ConfigDict(from_attributes=True, populate_by_name=True)

    success: bool = True
    async_: bool = Field(default=True, alias="async")
    provider: str = "whoop"
    scope: str = "full-available"
    method: str = "pull_api"
