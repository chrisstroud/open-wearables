from datetime import datetime, timezone
from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

ApiKeyScope = Literal["source-reset"]


class ApiKeyRead(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: str
    name: str
    created_by: UUID | None
    scopes: list[ApiKeyScope]
    created_at: datetime


class ApiKeyCreate(BaseModel):
    id: str
    name: str
    created_by: UUID | None = None
    scopes: list[ApiKeyScope] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class ApiKeyUpdate(BaseModel):
    name: str | None = None
    scopes: list[ApiKeyScope] | None = None
