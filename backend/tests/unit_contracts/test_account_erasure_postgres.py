"""Real PostgreSQL + HTTP saga tests; external providers/planes are explicit fakes.

Run with --confcutdir=tests/unit_contracts and an owned synthetic database URL.
These tests do not certify deployed provider, object-store, Redis or worker proof.
"""

import importlib
import os
from collections.abc import Iterator
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock
from uuid import UUID, uuid4

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from sqlalchemy import create_engine, func, select
from sqlalchemy.engine import make_url
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from app.api.routes.v1.account_erasure import router
from app.config import settings
from app.database import BaseDbModel, _get_db_dependency
from app.models import AccountErasureOperation, ApiKey, PersonalRecord, User, UserConnection, WhoopAuthorizationLease
from app.repositories.account_erasure_repository import account_erasure_repository
from app.schemas.auth import ConnectionStatus
from app.services.sdk_source_reset_external import (
    FIT_OBJECTS,
    QUEUED_TASKS,
    RAW_OBJECTS,
    REDIS_COORDINATION,
    RESULT_BACKEND,
    ExternalResetInventory,
)
from tests.unit_contracts.test_account_erasure import plan_payload


@pytest.fixture
def database() -> Iterator:
    explicit = os.environ.get("ACCOUNT_ERASURE_TEST_DATABASE_URL")
    if not explicit:
        pytest.skip("Requires an explicitly owned synthetic PostgreSQL database")
    url = make_url(explicit)
    assert url.host == "127.0.0.1"
    assert url.username == "erasure_test"
    assert url.database == "ow_account_erasure_test"
    assert url.port is not None
    engine = create_engine(url, connect_args={"connect_timeout": 5})
    BaseDbModel.metadata.create_all(engine)
    try:
        yield engine
    finally:
        BaseDbModel.metadata.drop_all(engine)
        engine.dispose()


@pytest.fixture
def harness(database: object, monkeypatch: pytest.MonkeyPatch) -> dict:
    reset_module = importlib.import_module("app.services.sdk_source_reset_service")
    erasure_module = importlib.import_module("app.services.account_erasure_service")
    monkeypatch.setattr(erasure_module, "require_account_erasure_workers", Mock(return_value=("synthetic-worker",)))
    monkeypatch.setattr(settings, "account_erasure_enabled", True)
    counts = {key: 0 for key in (RAW_OBJECTS, FIT_OBJECTS, QUEUED_TASKS, RESULT_BACKEND, REDIS_COORDINATION)}
    external = Mock()
    external.inventory.side_effect = lambda *_args, **_kwargs: ExternalResetInventory(
        counts=dict(counts),
        identity_tokens={key: () for key in counts},
        blockers=(),
        objects=(),
        redis_references=(),
        active_task_ids=(),
        configuration_digest_sha256="f" * 64,
    )
    monkeypatch.setattr(reset_module, "sdk_source_reset_external_planes", external)
    deregister = Mock()
    monkeypatch.setattr(reset_module.sdk_source_reset_provider_fence, "deregister", deregister)
    payload = plan_payload()
    user_id = UUID(payload["account"]["openWearablesUserId"])
    peer_id = uuid4()
    with Session(database) as db:
        db.add_all(
            [
                ApiKey(id="synthetic-erasure-key", name="synthetic", scopes=["account-erasure"]),
                ApiKey(id="synthetic-ordinary-key", name="synthetic", scopes=[]),
                ApiKey(id="synthetic-reset-key", name="synthetic", scopes=["source-reset"]),
                User(id=user_id, external_user_id=payload["account"]["subjectId"]),
                User(id=peer_id, external_user_id="synthetic-peer"),
            ]
        )
        db.commit()
        db.add_all(
            [
                PersonalRecord(id=uuid4(), user_id=user_id, gender="synthetic-target"),
                PersonalRecord(id=uuid4(), user_id=peer_id, gender="synthetic-peer"),
                UserConnection(
                    id=uuid4(),
                    user_id=user_id,
                    provider="whoop",
                    provider_user_id="synthetic-whoop-owner",
                    access_token="synthetic-access",
                    status=ConnectionStatus.ACTIVE,
                    updated_at=datetime.now(timezone.utc),
                ),
            ]
        )
        db.commit()
    api = FastAPI()
    api.include_router(router, prefix="/api/v1")

    def session() -> Iterator[Session]:
        with Session(database) as db:
            try:
                yield db
            except Exception:
                db.rollback()
                raise

    api.dependency_overrides[_get_db_dependency] = session
    return {
        "client": TestClient(api, raise_server_exceptions=False),
        "database": database,
        "payload": payload,
        "user_id": user_id,
        "peer_id": peer_id,
        "external": external,
        "deregister": deregister,
        "counts": counts,
    }


def post(harness: dict, phase: str, payload: dict | None = None, key: str = "synthetic-erasure-key") -> object:
    return harness["client"].post(
        f"/api/v1/users/{harness['user_id']}/account-erasure/{phase}",
        json=payload or harness["payload"],
        headers={"X-Open-Wearables-API-Key": key},
    )


def operation(harness: dict) -> dict:
    payload = deepcopy(harness["payload"])
    payload["authorization"]["planFingerprint"] = "b" * 64
    return payload


@pytest.mark.parametrize("key", ["synthetic-ordinary-key", "synthetic-reset-key", "invalid-key"])
def test_only_dedicated_account_erasure_key_is_admitted(harness: dict, key: str) -> None:
    response = post(harness, "plan", key=key)
    assert response.status_code in {401, 403}
    harness["external"].inventory.assert_not_called()


def test_disabled_endpoint_does_not_plan(harness: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(settings, "account_erasure_enabled", False)
    assert post(harness, "plan").status_code == 503
    harness["external"].inventory.assert_not_called()


def test_plan_is_exact_idempotent_and_stores_no_profile_or_request_payload(harness: dict) -> None:
    for _ in range(2):
        response = post(harness, "plan")
        assert response.status_code == 200, response.text
        assert response.json() == {**harness["payload"], "status": "planned"}
        assert response.headers["cache-control"] == "no-store"
    with Session(harness["database"]) as db:
        assert db.scalar(select(func.count()).select_from(AccountErasureOperation)) == 1
        row = db.scalar(select(AccountErasureOperation))
        assert row.state == "planned"
        assert row.plan_fingerprint is None
        assert "synthetic-subject" not in repr(row)
        assert "synthetic-access" not in repr(row)
        assert db.get(User, harness["user_id"]).health_write_state == "active"


def test_whole_account_delete_and_lost_response_replay_preserve_peer(harness: dict) -> None:
    planned = post(harness, "plan")
    assert planned.status_code == 200, planned.text
    payload = operation(harness)
    deleted = post(harness, "erase", payload)
    assert deleted.status_code == 200, deleted.text
    assert deleted.json()["status"] == "deleted"
    for phase in ("verify", "erase"):
        replay = post(harness, phase, payload)
        assert replay.status_code == 200, replay.text
        assert replay.json() == {**deleted.json(), "status": "already_deleted"}
    with Session(harness["database"]) as db:
        assert db.get(User, harness["user_id"]) is None
        assert db.scalar(select(PersonalRecord).where(PersonalRecord.user_id == harness["user_id"])) is None
        assert db.get(User, harness["peer_id"]).external_user_id == "synthetic-peer"
        peer_profile = db.scalar(select(PersonalRecord).where(PersonalRecord.user_id == harness["peer_id"]))
        assert peer_profile.gender == "synthetic-peer"
        assert db.scalar(select(AccountErasureOperation)).state == "deleted"
    assert harness["deregister"].call_count == 1


@pytest.mark.parametrize(
    ("part", "field", "value"),
    [
        ("authorization", "actorDigest", "e" * 64),
        ("target", "userId", "another-owner"),
        ("authorization", "operationId", "33333333-3333-4333-8333-333333333333"),
    ],
)
def test_changed_operation_binding_cannot_delete(harness: dict, part: str, field: str, value: str) -> None:
    assert post(harness, "plan").status_code == 200
    payload = operation(harness)
    payload[part][field] = value
    response = post(harness, "erase", payload)
    assert response.status_code == 409
    assert response.json()["status"] == "scope-rejected"
    harness["deregister"].assert_not_called()


def test_missing_user_without_exact_receipt_is_not_terminal(harness: dict) -> None:
    with Session(harness["database"]) as db:
        db.delete(db.get(User, harness["user_id"]))
        db.commit()
    response = post(harness, "verify", operation(harness))
    assert response.status_code == 409
    assert "receiptFingerprint" not in response.json()
    assert post(harness, "plan").json()["status"] == "identity_mismatch"


def test_provider_failure_leaves_durable_fence_and_retries_exact_operation(harness: dict) -> None:
    assert post(harness, "plan").status_code == 200
    harness["deregister"].side_effect = [RuntimeError("synthetic outage"), None]
    failed = post(harness, "erase", operation(harness))
    assert failed.status_code == 503
    assert "receiptFingerprint" not in failed.json()
    with Session(harness["database"]) as db:
        user = db.get(User, harness["user_id"])
        assert user.health_write_state == "fenced"
        assert user.account_erasure_operation_id is not None
        user.health_write_state = "active"
        with pytest.raises(IntegrityError):
            db.commit()
        db.rollback()
    retry = post(harness, "erase", operation(harness))
    assert retry.status_code == 200, retry.text


def test_external_cleanup_failure_after_database_half_recovers(harness: dict) -> None:
    assert post(harness, "plan").status_code == 200
    harness["external"].erase_objects.side_effect = [RuntimeError("synthetic outage"), None]
    failed = post(harness, "erase", operation(harness))
    assert failed.status_code == 503, failed.text
    with Session(harness["database"]) as db:
        user = db.get(User, harness["user_id"])
        assert user.health_write_state == "fenced"
        assert user.health_reset_applied_at is not None
        assert db.scalar(select(AccountErasureOperation)).receipt_fingerprint is None
    retry = post(harness, "erase", operation(harness))
    assert retry.status_code == 200, retry.text


def test_receipt_does_not_mask_later_external_verification_failure(harness: dict) -> None:
    assert post(harness, "plan").status_code == 200
    assert post(harness, "erase", operation(harness)).status_code == 200
    calls = harness["external"].erase_objects.call_count
    harness["counts"][RAW_OBJECTS] = 1
    result = post(harness, "verify", operation(harness))
    assert result.status_code == 202
    assert result.json()["status"] == "cleanup_verification_failed"
    assert "receiptFingerprint" not in result.json()
    assert harness["external"].erase_objects.call_count == calls


def test_no_false_receipt_when_final_database_transaction_fails(harness: dict, monkeypatch: pytest.MonkeyPatch) -> None:
    assert post(harness, "plan").status_code == 200
    original = account_erasure_repository.delete_user

    def interrupted(db: Session, user_id: UUID) -> None:
        original(db, user_id)
        raise RuntimeError("synthetic interruption before receipt commit")

    monkeypatch.setattr(account_erasure_repository, "delete_user", interrupted)
    assert post(harness, "erase", operation(harness)).status_code == 500
    with Session(harness["database"]) as db:
        assert db.get(User, harness["user_id"]) is not None
        assert db.scalar(select(AccountErasureOperation)).receipt_fingerprint is None
    monkeypatch.setattr(account_erasure_repository, "delete_user", original)
    response = post(harness, "erase", operation(harness))
    assert response.status_code == 200, response.text


def test_global_plan_cannot_change_after_first_erase(harness: dict) -> None:
    assert post(harness, "plan").status_code == 200
    harness["deregister"].side_effect = RuntimeError("synthetic outage")
    assert post(harness, "erase", operation(harness)).status_code == 503
    changed = operation(harness)
    changed["authorization"]["planFingerprint"] = "e" * 64
    response = post(harness, "erase", changed)
    assert response.status_code == 409
    assert response.json()["status"] == "stale-plan"
    assert harness["deregister"].call_count == 1


def test_tampered_terminal_record_is_not_replayed(harness: dict) -> None:
    assert post(harness, "plan").status_code == 200
    assert post(harness, "erase", operation(harness)).status_code == 200
    with Session(harness["database"]) as db:
        db.scalar(select(AccountErasureOperation)).receipt_fingerprint = "e" * 64
        db.commit()
    response = post(harness, "verify", operation(harness))
    assert response.status_code == 202
    assert response.json()["status"] == "cleanup_verification_failed"


def test_active_provider_authorization_prevents_cleanup(harness: dict) -> None:
    assert post(harness, "plan").status_code == 200
    now = datetime.now(timezone.utc)
    with Session(harness["database"]) as db:
        db.add(
            WhoopAuthorizationLease(
                user_id=harness["user_id"],
                authorization_generation=1,
                lease_token=uuid4(),
                lease_kind="token_refresh",
                acquired_at=now,
                lease_expires_at=now + timedelta(minutes=1),
                updated_at=now,
            )
        )
        db.commit()
    response = post(harness, "erase", operation(harness))
    assert response.status_code == 202
    harness["deregister"].assert_not_called()
    with Session(harness["database"]) as db:
        assert db.get(User, harness["user_id"]).health_write_state == "active"


def test_changed_inventory_before_fence_requires_fresh_plan(harness: dict) -> None:
    assert post(harness, "plan").status_code == 200
    with Session(harness["database"]) as db:
        db.scalar(select(UserConnection).where(UserConnection.user_id == harness["user_id"])).access_token = "changed"
        db.commit()
    response = post(harness, "erase", operation(harness))
    assert response.status_code == 409
    assert response.json()["status"] == "stale-plan"
    harness["deregister"].assert_not_called()
