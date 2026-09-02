# ruff: noqa: N815

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class SDKSyncWindowReceiptResponse(BaseModel):
    """Accepted bounded sync-window authority exposed to the web dashboard."""

    model_config = ConfigDict(from_attributes=True)

    windowId: UUID
    userId: UUID
    installationId: UUID | None = None
    installationGeneration: int | None = None
    healthEvidenceGeneration: int | None = None
    provider: str
    purpose: str
    windowVersion: int
    lowerBoundInclusive: datetime
    upperBoundExclusive: datetime
    batchIds: list[UUID]
    emptyOrNoAccessTypes: list[str]
    reconciliationStartInclusive: datetime | None = None
    reconciliationEndExclusive: datetime | None = None
    acceptedAt: datetime
