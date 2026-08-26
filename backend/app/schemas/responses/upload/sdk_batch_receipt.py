from datetime import datetime
from enum import StrEnum
from uuid import UUID

from pydantic import BaseModel, Field


class SDKBatchReceiptStatus(StrEnum):
    QUEUED = "queued"
    PROCESSING = "processing"
    SUCCEEDED = "succeeded"
    FAILED = "failed"


class SDKBatchReceiptResponse(BaseModel):
    """Durable processing state for one idempotent mobile SDK upload."""

    batch_id: UUID
    status: SDKBatchReceiptStatus
    terminal: bool
    accepted: bool
    retryable: bool = False
    dropped_count: int = Field(0, ge=0)
    records_saved: int = Field(0, ge=0)
    workouts_saved: int = Field(0, ge=0)
    sleep_saved: int = Field(0, ge=0)
    tombstones_received: int = Field(0, ge=0)
    tombstones_applied: int = Field(0, ge=0)
    tombstones_unresolved: int = Field(0, ge=0)
    tombstone_rows_deleted: int = Field(0, ge=0)
    error_code: str | None = None
    created_at: datetime
    updated_at: datetime
    completed_at: datetime | None = None
