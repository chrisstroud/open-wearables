from uuid import UUID

from sqlalchemy import CheckConstraint, Index
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.database import BaseDbModel
from app.mappings import FKUser, PrimaryKey, str_64


class SDKSourceResetSeal(BaseDbModel):
    """Immutable exact post-fence inventory accepted for one reset operation."""

    __tablename__ = "sdk_source_reset_seal"
    __table_args__ = (
        Index(
            "uq_sdk_source_reset_seal_user_generation",
            "user_id",
            "health_evidence_generation",
            unique=True,
        ),
        CheckConstraint(
            "health_evidence_generation >= 0",
            name="ck_sdk_source_reset_seal_generation",
        ),
        CheckConstraint(
            "inventory_digest_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_sdk_source_reset_seal_digest",
        ),
        CheckConstraint(
            "configuration_digest_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_sdk_source_reset_seal_configuration_digest",
        ),
        CheckConstraint(
            "jsonb_typeof(resource_counts) = 'object'",
            name="ck_sdk_source_reset_seal_counts_object",
        ),
    )

    operation_id: Mapped[PrimaryKey[UUID]]
    user_id: Mapped[FKUser]
    health_evidence_generation: Mapped[int]
    inventory_digest_sha256: Mapped[str_64]
    configuration_digest_sha256: Mapped[str_64]
    resource_counts: Mapped[dict[str, int]] = mapped_column(JSONB)
