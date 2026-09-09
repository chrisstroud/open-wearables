"""Exact operation bindings for Dashboard's verified account-erasure protocol."""

from datetime import datetime
from typing import Annotated, Literal, Self

from pydantic import BaseModel, ConfigDict, StringConstraints, field_validator, model_validator
from pydantic.alias_generators import to_camel

PROTOCOL = "dashboard-account-erasure-v1"
Fingerprint = Annotated[str, StringConstraints(pattern=r"^[0-9a-f]{64}$")]
Identity = Annotated[str, StringConstraints(min_length=1, max_length=255, pattern=r"^[^\x00-\x1f\x7f]+$")]
OperationId = Annotated[
    str,
    StringConstraints(pattern=r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$"),
]


class ErasureBindingModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        extra="forbid",
        frozen=True,
        strict=True,
    )


class ErasurePlanAuthorization(ErasureBindingModel):
    operation_id: OperationId
    actor_digest: Fingerprint


class ErasureOperationAuthorization(ErasurePlanAuthorization):
    plan_fingerprint: Fingerprint


class ErasureOwner(ErasureBindingModel):
    kind: Literal["owner"]
    user_id: Identity
    subject_id: Identity


class ErasureAccount(ErasureBindingModel):
    subject_id: Identity
    open_wearables_user_id: OperationId


class AccountErasurePlanRequest(ErasureBindingModel):
    protocol: Literal["dashboard-account-erasure-v1"]
    authorization: ErasurePlanAuthorization
    target: ErasureOwner
    account: ErasureAccount
    scope_fingerprint: Fingerprint

    @model_validator(mode="after")
    def same_subject(self) -> Self:
        if self.target.subject_id != self.account.subject_id:
            raise ValueError("account and target subject bindings must match")
        return self


class AccountErasureOperationRequest(AccountErasurePlanRequest):
    authorization: ErasureOperationAuthorization


PlanStatus = Literal["planned", "scope-rejected", "identity_mismatch", "unavailable", "upstream_unavailable"]
PendingStatus = Literal[
    "scope-rejected",
    "identity_mismatch",
    "unavailable",
    "upstream_unavailable",
    "stale-plan",
    "verification-failed",
    "outcome-unknown",
    "reconciliation_pending",
    "outcome_unknown",
    "cleanup_pending",
    "deregistration_unverified",
    "cleanup_verification_failed",
]


class AccountErasurePlanResponse(AccountErasurePlanRequest):
    status: PlanStatus


class AccountErasurePendingResponse(AccountErasureOperationRequest):
    status: PendingStatus


class AccountErasureTerminalResponse(AccountErasureOperationRequest):
    status: Literal["deleted", "already_deleted"]
    receipt_fingerprint: Fingerprint
    recorded_at: Annotated[
        str, StringConstraints(pattern=r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})$")
    ]

    @field_validator("recorded_at")
    @classmethod
    def valid_recorded_at(cls, value: str) -> str:
        parsed = datetime.fromisoformat(value)
        if parsed.tzinfo is None:
            raise ValueError("terminal receipt requires a timezone")
        return value


AccountErasureOperationResponse = AccountErasurePendingResponse | AccountErasureTerminalResponse
