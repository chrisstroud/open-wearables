from datetime import datetime
from uuid import UUID

from sqlalchemy import CheckConstraint, ForeignKey, Index
from sqlalchemy.orm import Mapped, mapped_column

from app.database import BaseDbModel
from app.mappings import FKUser, str_32, str_64, str_100


class SDKUploadInbox(BaseDbModel):
    """Durable SDK payload retained until its terminal receipt succeeds."""

    __tablename__ = "sdk_upload_inbox"
    __table_args__ = (
        Index("ix_sdk_upload_inbox_expires_at", "expires_at"),
        CheckConstraint(
            "(installation_id IS NULL AND installation_generation IS NULL "
            "AND health_evidence_generation IS NULL) OR "
            "(installation_id IS NOT NULL AND installation_generation > 0 "
            "AND health_evidence_generation >= 0)",
            name="ck_sdk_upload_inbox_installation_scope",
        ),
        CheckConstraint("content_size_bytes > 0", name="ck_sdk_upload_inbox_content_size"),
    )

    id: Mapped[UUID] = mapped_column(
        ForeignKey("sdk_batch_receipt.id", ondelete="CASCADE"),
        primary_key=True,
    )
    user_id: Mapped[FKUser]
    installation_id: Mapped[UUID | None] = mapped_column(
        ForeignKey("sdk_client_installation.id", ondelete="RESTRICT"),
        nullable=True,
    )
    installation_generation: Mapped[int | None]
    health_evidence_generation: Mapped[int | None]
    provider: Mapped[str_32]
    payload_sha256: Mapped[str_64]
    content_type: Mapped[str_100]
    content_size_bytes: Mapped[int]
    expires_at: Mapped[datetime]
    content: Mapped[str]
