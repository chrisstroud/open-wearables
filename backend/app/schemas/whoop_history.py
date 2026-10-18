from datetime import datetime, timedelta, timezone
from typing import Literal
from uuid import UUID

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

WHOOP_HISTORY_START = datetime(2000, 1, 1, tzinfo=timezone.utc)


class WhoopHistoryRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    idempotency_key: UUID
    authorization_generation: int = Field(strict=True, gt=0, le=9_007_199_254_740_991)
    requested_start_at: AwareDatetime
    requested_end_at: AwareDatetime

    @model_validator(mode="after")
    def validate_window(self) -> "WhoopHistoryRequest":
        if self.requested_start_at != WHOOP_HISTORY_START:
            raise ValueError("Full history starts at 2000-01-01T00:00:00Z")
        if not self.requested_start_at < self.requested_end_at <= datetime.now(timezone.utc) + timedelta(minutes=5):
            raise ValueError("History requires a finite past window")
        return self


class WhoopHistoryReceipt(BaseModel):
    success: Literal[True] = True
    asynchronous: Literal[True] = Field(default=True, serialization_alias="async")
    provider: Literal["whoop"] = "whoop"
    scope: Literal["full-available"] = "full-available"
    method: Literal["pull_api"] = "pull_api"
    dispatch_id: UUID
    user_id: UUID
    connection_id: UUID
    authorization_generation: int
    task_id: UUID
    status: Literal["queued", "running", "succeeded", "failed", "superseded"]
    requested_start_at: datetime
    requested_end_at: datetime
    completed_at: datetime | None
    error_code: str | None
