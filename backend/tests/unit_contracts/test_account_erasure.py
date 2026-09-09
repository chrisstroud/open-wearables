"""Pure wire-contract tests; no app, database, provider or customer fixture."""

from copy import deepcopy

import pytest
from pydantic import ValidationError

from app.schemas.account_erasure import (
    AccountErasureOperationRequest,
    AccountErasurePlanRequest,
    AccountErasureTerminalResponse,
)


def plan_payload() -> dict:
    return {
        "protocol": "dashboard-account-erasure-v1",
        "authorization": {
            "operationId": "11111111-1111-4111-8111-111111111111",
            "actorDigest": "a" * 64,
        },
        "target": {"kind": "owner", "userId": "synthetic-user", "subjectId": "synthetic-subject"},
        "account": {
            "subjectId": "synthetic-subject",
            "openWearablesUserId": "22222222-2222-4222-8222-222222222222",
        },
        "scopeFingerprint": "c" * 64,
    }


def test_plan_round_trips_exact_dashboard_wire_names() -> None:
    payload = plan_payload()
    assert AccountErasurePlanRequest.model_validate(payload).model_dump(by_alias=True) == payload


def test_operation_requires_global_plan_binding() -> None:
    payload = plan_payload()
    with pytest.raises(ValidationError):
        AccountErasureOperationRequest.model_validate(payload)
    payload["authorization"]["planFingerprint"] = "b" * 64
    assert AccountErasureOperationRequest.model_validate(payload).model_dump(by_alias=True) == payload
    with pytest.raises(ValidationError):
        AccountErasurePlanRequest.model_validate(payload)


@pytest.mark.parametrize(
    ("part", "field", "value"),
    [
        ("target", "kind", "viewer"),
        ("target", "subjectId", "different-subject"),
        ("target", "userId", ""),
        ("target", "userId", "line\nbreak"),
        ("account", "openWearablesUserId", "not-a-uuid"),
        ("authorization", "operationId", "../another-operation"),
        ("authorization", "actorDigest", "not-a-digest"),
        ("authorization", "actorDigest", "A" * 64),
    ],
)
def test_rejects_invalid_or_mismatched_bindings(part: str, field: str, value: str) -> None:
    payload = plan_payload()
    payload[part][field] = value
    with pytest.raises(ValidationError):
        AccountErasurePlanRequest.model_validate(payload)


def test_rejects_unknown_protocol_and_injected_authority_fields() -> None:
    payload = plan_payload()
    payload["protocol"] = "legacy-delete"
    with pytest.raises(ValidationError):
        AccountErasurePlanRequest.model_validate(payload)
    payload = plan_payload()
    payload["skipVerification"] = True
    with pytest.raises(ValidationError):
        AccountErasurePlanRequest.model_validate(payload)


def test_bindings_are_immutable_snapshots() -> None:
    payload = plan_payload()
    expected = deepcopy(payload)
    bound = AccountErasurePlanRequest.model_validate(payload)
    payload["target"]["userId"] = "changed-after-validation"
    assert bound.model_dump(by_alias=True) == expected
    with pytest.raises(ValidationError):
        bound.target.user_id = "another-user"


@pytest.mark.parametrize(
    "timestamp",
    [
        "2026-02-31T00:00:00Z",
        "2026-09-09T24:00:00Z",
        "0000-01-01T00:00:00Z",
        "2026-09-09T00:00:00",
        "2026-09-09T00:00:00.1234567Z",
    ],
)
def test_terminal_receipt_rejects_impossible_or_ambiguous_timestamp(timestamp: str) -> None:
    payload = plan_payload()
    payload["authorization"]["planFingerprint"] = "b" * 64
    with pytest.raises(ValidationError):
        AccountErasureTerminalResponse.model_validate(
            {
                **payload,
                "status": "deleted",
                "receiptFingerprint": "d" * 64,
                "recordedAt": timestamp,
            }
        )
