from concurrent.futures import ThreadPoolExecutor
from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi.testclient import TestClient
from sqlalchemy import Engine, delete
from sqlalchemy.orm import Session, sessionmaker

from app.integrations.celery.tasks.whoop_history_task import recover_whoop_history, sync_whoop_history
from app.models import DataPointSeries, DataSource, EventRecord, HealthScore, User, UserConnection, WhoopHistoryDispatch
from app.repositories.data_source_repository import DataSourceRepository
from app.repositories.health_write_authority import (
    HealthWriteAuthorityError,
    WhoopHistoryAuthority,
)
from app.repositories.user_connection_repository import UserConnectionRepository
from app.repositories.whoop_history_repository import WhoopHistoryRepository
from app.schemas.auth import ConnectionStatus
from app.schemas.enums import ProviderName
from app.schemas.whoop_history import WHOOP_HISTORY_START
from tests.factories import ApiKeyFactory, UserConnectionFactory, UserFactory
from tests.utils import api_key_headers


def url(connection: UserConnection) -> str:
    return f"/api/v1/providers/whoop/users/{connection.user_id}/connections/{connection.id}/sync/full-history"


def body() -> dict[str, object]:
    return {
        "idempotency_key": str(uuid4()),
        "authorization_generation": 1,
        "requested_start_at": "2000-01-01T00:00:00.000Z",
        "requested_end_at": "2026-01-01T00:00:00.000Z",
    }


def admit(client: TestClient, connection: UserConnection) -> UUID:
    with patch("app.services.whoop_history_service.publish_history_dispatch"):
        response = client.post(url(connection), json=body(), headers=api_key_headers(ApiKeyFactory().id))
    assert response.status_code == 202, response.text
    return UUID(response.json()["dispatch_id"])


def test_durable_admission_replay_and_conflicting_body(client: TestClient, db: Session) -> None:
    connection = UserConnectionFactory(provider="whoop")
    headers, request = api_key_headers(ApiKeyFactory().id), body()
    with patch("app.services.whoop_history_service.publish_history_dispatch", side_effect=TimeoutError("private")):
        first = client.post(url(connection), json=request, headers=headers)
        second = client.post(url(connection), json=request, headers=headers)
    assert first.status_code == second.status_code == 202
    assert first.json() == second.json()
    receipt = first.json()
    assert receipt["async"] is True
    assert receipt["status"] == "queued"
    assert receipt["authorization_generation"] == 1
    assert db.query(WhoopHistoryDispatch).count() == 1
    changed = {**request, "requested_end_at": "2025-01-01T00:00:00Z"}
    assert client.post(url(connection), json=changed, headers=headers).status_code == 409
    assert client.post(url(connection), json=body(), headers=headers).json() == {"detail": "history_already_active"}
    read_url = f"/api/v1/providers/whoop/users/{connection.user_id}/sync/full-history/{receipt['dispatch_id']}"
    assert client.get(read_url, headers=headers).json() == receipt
    assert client.get(read_url.replace(str(connection.user_id), str(uuid4())), headers=headers).status_code == 404
    assert client.get(read_url).status_code in (401, 403)


@pytest.mark.parametrize(
    "change",
    [
        {"requested_start_at": "1999-01-01T00:00:00Z"},
        {"requested_end_at": "2099-01-01T00:00:00Z"},
        {"requested_end_at": "2000-01-01T00:00:00Z"},
        {"requested_end_at": "2026-01-01"},
        {"authorization_generation": 0},
        {"authorization_generation": True},
        {"extra": "rejected"},
    ],
)
def test_invalid_history_request(client: TestClient, change: dict[str, object]) -> None:
    connection = UserConnectionFactory(provider="whoop")
    result = client.post(url(connection), json={**body(), **change}, headers=api_key_headers(ApiKeyFactory().id))
    assert result.status_code == 400, result.text


@pytest.mark.parametrize("change", ["generation", "owner", "fenced", "revoked"])
def test_admission_requires_current_authority(client: TestClient, db: Session, change: str) -> None:
    connection = UserConnectionFactory(provider="whoop")
    target, request = url(connection), body()
    if change == "generation":
        request["authorization_generation"] = 2
    elif change == "owner":
        target = target.replace(str(connection.user_id), str(uuid4()))
    elif change == "fenced":
        user = db.get(User, connection.user_id)
        assert user is not None
        user.health_write_state = "fenced"
        db.commit()
    else:
        connection.status = "revoked"
        db.commit()
    assert client.post(target, json=request, headers=api_key_headers(ApiKeyFactory().id)).status_code == 409


def test_actual_adapters_use_fixed_window_once_and_do_not_advance_live_cursor(client: TestClient, db: Session) -> None:
    connection = UserConnectionFactory(
        provider="whoop", token_expires_at=datetime.now(timezone.utc) + timedelta(days=1)
    )
    dispatch_id = admit(client, connection)
    prior_cursor = connection.last_synced_at
    requests: list[tuple[str, dict[str, str]]] = []

    def provider_response(*args: object, **kwargs: object) -> httpx.Response:
        target = str(kwargs["url"])
        requests.append((target, dict(kwargs["params"])))  # type: ignore[arg-type]
        return httpx.Response(200, json={"records": []}, request=httpx.Request("GET", target))

    with (
        patch("app.integrations.celery.tasks.whoop_history_task.SessionLocal", return_value=nullcontext(db)),
        patch(
            "app.services.providers.api_client.httpx.Client.request",
            side_effect=provider_response,
        ),
    ):
        sync_whoop_history(str(dispatch_id))
        sync_whoop_history(str(dispatch_id))
    dispatch = db.get(WhoopHistoryDispatch, dispatch_id)
    assert dispatch is not None
    assert dispatch.status == "succeeded", dispatch.error_code
    assert dispatch.completed_at is not None
    assert len(requests) == 3
    assert {item[0].rsplit("/", 1)[-1] for item in requests} == {"workout", "sleep", "recovery"}
    for _, params in requests:
        assert datetime.fromisoformat(params["start"].replace("Z", "+00:00")) == dispatch.requested_start_at
        assert datetime.fromisoformat(params["end"].replace("Z", "+00:00")) == dispatch.requested_end_at
    db.refresh(connection)
    assert connection.last_synced_at == prior_cursor
    assert "whoop_history_authority" not in db.info


@pytest.mark.parametrize("change", ["consent", "reset", "revoked", "deleted"])
def test_queued_work_never_uses_replacement_authority(client: TestClient, db: Session, change: str) -> None:
    connection = UserConnectionFactory(provider="whoop")
    dispatch_id = admit(client, connection)
    if change == "consent":
        connection.authorization_generation += 1
    elif change == "reset":
        user = db.get(User, connection.user_id)
        assert user is not None
        user.health_evidence_generation += 1
    elif change == "revoked":
        connection.status = "revoked"
    else:
        db.delete(connection)
    db.commit()
    with (
        patch("app.integrations.celery.tasks.whoop_history_task.SessionLocal", return_value=nullcontext(db)),
        patch(
            "app.services.providers.api_client.httpx.Client.request",
        ) as remote,
    ):
        sync_whoop_history(str(dispatch_id))
        remote.assert_not_called()
    dispatch = db.get(WhoopHistoryDispatch, dispatch_id)
    assert dispatch is not None
    assert dispatch.status == "superseded"


@pytest.mark.parametrize("change", ["consent", "reset", "expired", "terminal"])
def test_writer_rechecks_captured_authority_after_commit(client: TestClient, db: Session, change: str) -> None:
    connection = UserConnectionFactory(provider="whoop")
    dispatch_id = admit(client, connection)
    dispatch = WhoopHistoryRepository().claim(db, dispatch_id)
    assert dispatch is not None
    authority = WhoopHistoryAuthority(connection.user_id, connection.id, 1, 0, dispatch_id)
    if change == "consent":
        connection.authorization_generation += 1
    elif change == "reset":
        user = db.get(User, connection.user_id)
        assert user is not None
        user.health_evidence_generation += 1
    elif change == "expired":
        dispatch.expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    else:
        dispatch.status = "failed"
        dispatch.completed_at = datetime.now(timezone.utc)
    db.commit()
    db.info["whoop_history_authority"] = authority
    try:
        with pytest.raises(HealthWriteAuthorityError):
            DataSourceRepository().ensure_data_source(
                db,
                user_id=connection.user_id,
                provider=ProviderName.WHOOP,
                user_connection_id=connection.id,
            )
        assert db.query(DataSource).count() == 0
    finally:
        db.info.pop("whoop_history_authority", None)


def test_recovery_publishes_queue_gap_and_finishes_lost_worker(client: TestClient, db: Session) -> None:
    queued = admit(client, UserConnectionFactory(provider="whoop"))
    lost = admit(client, UserConnectionFactory(provider="whoop"))
    dispatch = WhoopHistoryRepository().claim(db, lost)
    assert dispatch is not None
    dispatch.expires_at = datetime.now(timezone.utc) - timedelta(seconds=1)
    db.commit()
    with (
        patch("app.integrations.celery.tasks.whoop_history_task.SessionLocal", return_value=nullcontext(db)),
        patch(
            "app.services.whoop_history_service.publish_history_dispatch",
        ) as publish,
    ):
        recover_whoop_history()
    assert publish.call_args.args[0] == queued
    publish.assert_called_once()
    db.refresh(dispatch)
    assert dispatch.status == "failed"
    assert dispatch.error_code == "worker_timeout"


def test_provider_failure_is_sanitized_and_not_complete(client: TestClient, db: Session) -> None:
    dispatch_id = admit(client, UserConnectionFactory(provider="whoop"))
    with (
        patch("app.integrations.celery.tasks.whoop_history_task.SessionLocal", return_value=nullcontext(db)),
        patch(
            "app.services.providers.api_client.httpx.Client.request",
            side_effect=TimeoutError("private-provider-detail"),
        ),
    ):
        sync_whoop_history(str(dispatch_id))
    dispatch = db.get(WhoopHistoryDispatch, dispatch_id)
    assert dispatch is not None
    assert dispatch.status == "failed"
    assert dispatch.error_code == "provider_sync_failed"


def test_new_consent_supersedes_old_dispatch_without_rebinding_it(client: TestClient, db: Session) -> None:
    connection = UserConnectionFactory(provider="whoop")
    old_id = admit(client, connection)
    connection.authorization_generation = 2
    db.commit()
    request = {**body(), "authorization_generation": 2}
    with patch("app.services.whoop_history_service.publish_history_dispatch"):
        response = client.post(url(connection), json=request, headers=api_key_headers(ApiKeyFactory().id))
    assert response.status_code == 202
    old = db.get(WhoopHistoryDispatch, old_id)
    assert old is not None
    db.refresh(old)
    assert old.status == "superseded"
    assert old.authorization_generation == 1
    assert response.json()["authorization_generation"] == 2


@pytest.mark.parametrize("replacement", ["consent", "reset"])
def test_inflight_response_cannot_write_after_real_separate_transaction(
    engine: Engine,
    replacement: str,
) -> None:
    user_id, connection_id, dispatch_id = uuid4(), uuid4(), uuid4()
    sessions = sessionmaker(bind=engine)
    with sessions() as seed:
        seed.add(User(id=user_id))
        seed.flush()
        seed.add(
            UserConnection(
                id=connection_id,
                user_id=user_id,
                provider="whoop",
                status=ConnectionStatus.ACTIVE,
                access_token="original",
                refresh_token="original-refresh",
                token_expires_at=datetime.now(timezone.utc) + timedelta(days=1),
                updated_at=datetime.now(timezone.utc),
            )
        )
        seed.commit()
        WhoopHistoryRepository().admit(
            seed,
            user_id=user_id,
            connection_id=connection_id,
            generation=1,
            dispatch_id=dispatch_id,
            start=WHOOP_HISTORY_START,
            end=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

    def replace_authority(*args: object, **kwargs: object) -> httpx.Response:
        # This transaction must finish while the provider request is in flight:
        # no lock is held over paginated provider GETs.
        with sessions() as writer:
            if replacement == "consent":
                current = writer.get(UserConnection, connection_id)
                assert current is not None
                UserConnectionRepository().update_connection_info(writer, current, "new-grant", "new-refresh", 3600)
            else:
                user = writer.query(User).filter_by(id=user_id).with_for_update().one()
                user.health_evidence_generation += 1
                writer.commit()
        return httpx.Response(200, json={"records": []}, request=httpx.Request("GET", str(kwargs["url"])))

    try:
        with (
            patch("app.integrations.celery.tasks.whoop_history_task.SessionLocal", sessions),
            patch(
                "app.services.providers.api_client.httpx.Client.request",
                side_effect=replace_authority,
            ) as remote,
        ):
            sync_whoop_history(str(dispatch_id))
            remote.assert_called_once()
        with sessions() as check:
            dispatch = check.get(WhoopHistoryDispatch, dispatch_id)
            assert dispatch is not None
            assert dispatch.status == "superseded"
            assert check.query(DataSource).filter_by(user_id=user_id).count() == 0
            current = check.get(UserConnection, connection_id)
            assert current is not None
            assert current.access_token == ("new-grant" if replacement == "consent" else "original")
    finally:
        with sessions() as cleanup:
            cleanup.execute(delete(UserConnection).where(UserConnection.user_id == user_id))
            cleanup.execute(delete(User).where(User.id == user_id))
            cleanup.commit()


def test_concurrent_deliveries_claim_once_in_separate_transactions(engine: Engine) -> None:
    user_id, connection_id, dispatch_id = uuid4(), uuid4(), uuid4()
    with Session(engine) as seed:
        seed.add(User(id=user_id))
        seed.flush()
        seed.add(
            UserConnection(
                id=connection_id,
                user_id=user_id,
                provider="whoop",
                status=ConnectionStatus.ACTIVE,
                access_token="synthetic",
                updated_at=datetime.now(timezone.utc),
            )
        )
        seed.commit()
        WhoopHistoryRepository().admit(
            seed,
            user_id=user_id,
            connection_id=connection_id,
            generation=1,
            dispatch_id=dispatch_id,
            start=WHOOP_HISTORY_START,
            end=datetime(2026, 1, 1, tzinfo=timezone.utc),
        )

    def claim() -> bool:
        with Session(engine) as session:
            return WhoopHistoryRepository().claim(session, dispatch_id) is not None

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            results = list(pool.map(lambda _: claim(), range(2)))
        assert sorted(results) == [False, True]
    finally:
        with Session(engine) as cleanup:
            cleanup.execute(delete(UserConnection).where(UserConnection.user_id == user_id))
            cleanup.execute(delete(User).where(User.id == user_id))
            cleanup.commit()


def test_populated_multisource_pull_reuses_existing_persistence(client: TestClient, db: Session) -> None:
    user = UserFactory(health_source_policy="multi-source", health_evidence_generation=4)
    connection = UserConnectionFactory(
        user=user, provider="whoop", token_expires_at=datetime.now(timezone.utc) + timedelta(days=1)
    )
    records = {
        "workout": {
            "id": str(uuid4()),
            "user_id": 12345,
            "created_at": "2025-01-02T01:01:00Z",
            "updated_at": "2025-01-02T01:02:00Z",
            "start": "2025-01-02T00:00:00Z",
            "end": "2025-01-02T01:00:00Z",
            "timezone_offset": "+00:00",
            "sport_name": "running",
            "score_state": "SCORED",
            "score": {"strain": 8.2},
        },
        "sleep": {
            "id": str(uuid4()),
            "user_id": 12345,
            "created_at": "2025-01-01T08:01:00Z",
            "updated_at": "2025-01-01T08:02:00Z",
            "start": "2025-01-01T00:00:00Z",
            "end": "2025-01-01T08:00:00Z",
            "zone_offset": "+00:00",
            "nap": False,
            "score_state": "SCORED",
            "score": {
                "sleep_performance_percentage": 84,
                "stage_summary": {
                    "total_in_bed_time_milli": 28_800_000,
                    "total_light_sleep_time_milli": 13_200_000,
                    "total_slow_wave_sleep_time_milli": 6_600_000,
                    "total_rem_sleep_time_milli": 7_200_000,
                },
            },
        },
        "recovery": {
            "cycle_id": 100,
            "sleep_id": str(uuid4()),
            "created_at": "2025-01-01T08:00:00Z",
            "score_state": "SCORED",
            "score": {"recovery_score": 74, "resting_heart_rate": 52, "hrv_rmssd_milli": 61.2},
        },
    }

    def respond(*args: object, **kwargs: object) -> httpx.Response:
        target = str(kwargs["url"])
        record = records[target.rsplit("/", 1)[-1]]
        return httpx.Response(200, json={"records": [record]}, request=httpx.Request("GET", target))

    counts = []
    for _ in range(2):
        dispatch_id = admit(client, connection)
        with (
            patch("app.integrations.celery.tasks.whoop_history_task.SessionLocal", return_value=nullcontext(db)),
            patch(
                "app.services.providers.api_client.httpx.Client.request",
                side_effect=respond,
            ),
        ):
            sync_whoop_history(str(dispatch_id))
        dispatch = db.get(WhoopHistoryDispatch, dispatch_id)
        assert dispatch is not None
        assert dispatch.status == "succeeded", dispatch.error_code
        source = db.query(DataSource).filter_by(user_id=user.id, provider="whoop").one()
        assert source.user_connection_id == connection.id
        counts.append(
            (
                db.query(EventRecord).filter_by(data_source_id=source.id).count(),
                db.query(DataPointSeries).filter_by(data_source_id=source.id).count(),
                db.query(HealthScore).filter_by(user_id=user.id, provider="whoop").count(),
            )
        )
    # New sleep emits the existing asynchronous score hook; replay recomputes
    # its internal derived score inline. Provider observations/scores are the
    # imported evidence and must stay identical across repeated pulls.
    assert counts[0] == counts[1]
    assert counts[0][0] == 2
    assert counts[0][1] >= 2
    assert counts[0][2] >= 3
    assert db.query(HealthScore).filter_by(user_id=user.id, provider="internal", category="sleep").count() == 1
