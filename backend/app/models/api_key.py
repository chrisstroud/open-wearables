from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.database import BaseDbModel
from app.mappings import FKDeveloper, PrimaryKey, str_64


class ApiKey(BaseDbModel):
    """Global API key for external service access."""

    __tablename__ = "api_key"

    id: Mapped[PrimaryKey[str_64]]  # The actual key value (sk-...)
    name: Mapped[str]
    created_by: Mapped[FKDeveloper | None]
    scopes: Mapped[list[str]] = mapped_column(JSONB, default=list)
