"""Minimal recovery evidence, intentionally not cascaded with the deleted user."""

from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import CheckConstraint, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import Mapped, mapped_column

from app.database import BaseDbModel
from app.mappings import PrimaryKey, str_64


class AccountErasureOperation(BaseDbModel):
    __tablename__ = "account_erasure_operation"
    __table_args__ = (
        CheckConstraint("state IN ('planned', 'fenced', 'deleted')", name="ck_account_erasure_state"),
        CheckConstraint("binding_digest ~ '^[0-9a-f]{64}$'", name="ck_account_erasure_binding"),
        CheckConstraint(
            "plan_fingerprint IS NULL OR plan_fingerprint ~ '^[0-9a-f]{64}$'", name="ck_account_erasure_plan"
        ),
        CheckConstraint(
            "(state = 'deleted' AND receipt_fingerprint IS NOT NULL AND recorded_at IS NOT NULL "
            "AND identity_proof IS NOT NULL AND configuration_digest IS NOT NULL AND plan_fingerprint IS NOT NULL) "
            "OR (state <> 'deleted' AND receipt_fingerprint IS NULL AND recorded_at IS NULL)",
            name="ck_account_erasure_terminal",
        ),
    )

    operation_id: Mapped[PrimaryKey[UUID]]
    open_wearables_user_id: Mapped[UUID] = mapped_column(index=True)
    binding_digest: Mapped[str_64]
    plan_fingerprint: Mapped[str | None] = mapped_column(String(64))
    inventory_digest: Mapped[str_64]
    worker_set_digest: Mapped[str_64]
    health_evidence_generation: Mapped[int]
    installation_generation: Mapped[int | None]
    state: Mapped[str] = mapped_column(String(16), default="planned")
    # HMAC-only provider/internal locator proof; never credentials or health values.
    identity_proof: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    configuration_digest: Mapped[str | None] = mapped_column(String(64))
    receipt_fingerprint: Mapped[str | None] = mapped_column(String(64))
    recorded_at: Mapped[datetime | None]
