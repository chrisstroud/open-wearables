"""Real HTTP/database coverage for the opt-in exact authorization endpoint."""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from threading import Event
from unittest.mock import Mock, patch
from uuid import uuid4

import httpx
import pytest
from fastapi import HTTPException
from fastapi.testclient import TestClient
from sqlalchemy import Engine, delete, text
from sqlalchemy.orm import Session

from app.models import User, UserConnection
from app.repositories.health_write_authority import HealthWriteAuthorityError
from app.repositories.user_connection_repository import UserConnectionRepository
from app.schemas.auth import ConnectionStatus
from app.services.providers.factory import ProviderFactory
from app.services.user_connection_service import user_connection_service
from tests.factories import ApiKeyFactory, DataPointSeriesFactory, DataSourceFactory, UserConnectionFactory, UserFactory
from tests.utils import api_key_headers


def endpoint(connection: UserConnection, generation: int | None = None) -> str:
    target = connection.authorization_generation if generation is None else generation
    return f"/api/v1/users/{connection.user_id}/connections/whoop/{connection.id}/authorizations/{target}"


def test_disconnect_acknowledges_once_and_retains_evidence(client: TestClient, db: Session) -> None:
    user = UserFactory()
    connection = UserConnectionFactory(user=user, provider="whoop")
    source = DataSourceFactory(user=user, provider="whoop", user_connection_id=connection.id)
    point = DataPointSeriesFactory(data_source=source)
    headers = api_key_headers(ApiKeyFactory().id)
    url = endpoint(connection)
    with patch("app.services.providers.whoop.oauth.WhoopOAuth.deregister_user") as deregister:
        first = client.delete(url, headers=headers)
        assert first.status_code == 204, first.text
        assert first.content == b""
        second = client.delete(url, headers=headers)
        assert second.status_code == 204
        deregister.assert_called_once()
    db.refresh(connection)
    assert connection.status == ConnectionStatus.REVOKED
    assert connection.access_token is None
    assert connection.refresh_token is None
    assert connection.disconnect_confirmed_generation == 1
    assert db.get(type(source), source.id) is not None
    assert db.get(type(point), point.id) is not None
    listed = client.get(f"/api/v1/users/{user.id}/connections", headers=headers)
    assert listed.json()[0]["authorization_generation"] == 1
    assert "disconnect_confirmed_generation" not in listed.json()[0]


def test_reconnect_changes_generation_and_old_request_cannot_revoke(client: TestClient, db: Session) -> None:
    connection = UserConnectionFactory(provider="whoop")
    headers = api_key_headers(ApiKeyFactory().id)
    old_url = endpoint(connection)
    repo = UserConnectionRepository()
    with patch("app.services.providers.whoop.oauth.WhoopOAuth.deregister_user") as deregister:
        assert client.delete(old_url, headers=headers).status_code == 204
        repo.update_connection_info(db, connection, "new-consent-token", "new-refresh", 3600)
        assert connection.authorization_generation == 2
        stale = client.delete(old_url, headers=headers)
        assert stale.status_code == 409
        assert stale.json() == {"detail": "authorization_changed"}
        assert connection.access_token == "new-consent-token"
        deregister.assert_called_once()
        assert client.delete(endpoint(connection), headers=headers).status_code == 204
        assert deregister.call_count == 2


def test_uncertain_provider_result_never_becomes_local_success(client: TestClient, db: Session) -> None:
    connection = UserConnectionFactory(provider="whoop")
    original_token = connection.access_token
    headers = api_key_headers(ApiKeyFactory().id)
    with patch(
        "app.services.providers.whoop.oauth.WhoopOAuth.deregister_user", side_effect=TimeoutError("private-value")
    ):
        result = client.delete(endpoint(connection), headers=headers)
    assert result.status_code == 503
    assert result.json() == {"detail": "deregistration_unverified"}
    db.refresh(connection)
    assert connection.access_token == original_token
    assert connection.status == ConnectionStatus.ACTIVE
    assert connection.disconnect_confirmed_generation is None
    with patch("app.services.providers.whoop.oauth.WhoopOAuth.deregister_user"):
        assert client.delete(endpoint(connection), headers=headers).status_code == 204


def test_legacy_revocation_is_not_a_verified_receipt(client: TestClient, db: Session) -> None:
    connection = UserConnectionFactory(
        provider="whoop", status=ConnectionStatus.REVOKED, access_token=None, refresh_token=None
    )
    with patch("app.services.providers.whoop.oauth.WhoopOAuth.deregister_user") as deregister:
        result = client.delete(endpoint(connection), headers=api_key_headers(ApiKeyFactory().id))
        assert result.status_code == 503
        deregister.assert_not_called()
    assert connection.disconnect_confirmed_generation is None


@pytest.mark.parametrize("mismatch", ["user", "connection", "generation", "provider", "zero"])
def test_wrong_binding_has_no_remote_effect(client: TestClient, db: Session, mismatch: str) -> None:
    connection = UserConnectionFactory(provider="whoop")
    url = endpoint(connection)
    expected = 404
    if mismatch == "user":
        url = url.replace(str(connection.user_id), str(UserFactory().id))
    elif mismatch == "connection":
        url = url.replace(str(connection.id), str(uuid4()))
    elif mismatch == "generation":
        url = endpoint(connection, 2)
        expected = 409
    elif mismatch == "provider":
        url = url.replace("/whoop/", "/garmin/")
    else:
        url = endpoint(connection, 0)
        expected = 400
    with patch("app.services.providers.whoop.oauth.WhoopOAuth.deregister_user") as deregister:
        response = client.delete(url, headers=api_key_headers(ApiKeyFactory().id))
        assert response.status_code == expected, response.text
        deregister.assert_not_called()
    db.refresh(connection)
    assert connection.status == ConnectionStatus.ACTIVE


def test_authentication_is_required(client: TestClient, db: Session) -> None:
    connection = UserConnectionFactory(provider="whoop")
    with patch("app.services.providers.whoop.oauth.WhoopOAuth.deregister_user") as deregister:
        assert client.delete(endpoint(connection)).status_code in {401, 403}
        deregister.assert_not_called()


def test_openapi_describes_empty_acknowledgement_and_normalized_validation(client: TestClient) -> None:
    path = "/api/v1/users/{user_id}/connections/{provider}/{connection_id}/authorizations/{authorization_generation}"
    operation = client.get("/openapi.json").json()["paths"][path]["delete"]
    assert "204" in operation["responses"]
    assert "content" not in operation["responses"]["204"]
    assert "200" not in operation["responses"]
    assert "422" not in operation["responses"]
    assert {"400", "401", "403", "404", "409", "503"}.issubset(operation["responses"])


def test_refresh_preserves_generation_and_cannot_restore_disconnected_credentials(
    client: TestClient, db: Session
) -> None:
    connection = UserConnectionFactory(provider="whoop")
    repo = UserConnectionRepository()
    repo.update_tokens(db, connection, "refreshed", "refreshed-refresh", 3600)
    assert connection.authorization_generation == 1
    with patch("app.services.providers.whoop.oauth.WhoopOAuth.deregister_user"):
        assert client.delete(endpoint(connection), headers=api_key_headers(ApiKeyFactory().id)).status_code == 204
    with pytest.raises(HealthWriteAuthorityError, match="authorization changed"):
        repo.update_tokens(db, connection, "late-refresh", "late-refresh-secret", 3600)
    db.refresh(connection)
    assert connection.access_token is None


def test_stale_refresh_and_revocation_cannot_change_new_authorization(db: Session) -> None:
    connection = UserConnectionFactory(provider="whoop")
    stale = UserConnection(id=connection.id, user_id=connection.user_id, provider="whoop", authorization_generation=1)
    repo = UserConnectionRepository()
    repo.update_connection_info(db, connection, "new-consent", "new-refresh", 3600)
    for method in [
        lambda: repo.update_tokens(db, stale, "stale", "stale", 3600),
        lambda: repo.mark_as_revoked(db, stale),
    ]:
        with pytest.raises(HealthWriteAuthorityError, match="authorization changed"):
            method()
    db.refresh(connection)
    assert connection.authorization_generation == 2
    assert connection.access_token == "new-consent"
    assert connection.status == ConnectionStatus.ACTIVE


@pytest.mark.parametrize("response_status", [200, 401])
def test_refresh_success_or_rejection_after_new_consent_keeps_new_grant(db: Session, response_status: int) -> None:
    connection = UserConnectionFactory(provider="whoop", refresh_token="original-refresh")
    repo = UserConnectionRepository()
    oauth = ProviderFactory().get_provider("whoop").oauth
    assert oauth is not None

    def remote_refresh(*args: object, **kwargs: object) -> httpx.Response:
        repo.update_connection_info(db, connection, "new-consent", "new-refresh", 3600)
        return httpx.Response(
            response_status,
            request=httpx.Request("POST", "https://provider.example.test/token"),
            json={
                "access_token": "late-refresh",
                "refresh_token": "late-refresh-secret",
                "expires_in": 3600,
                "token_type": "Bearer",
            },
        )

    with patch("httpx.post", side_effect=remote_refresh), pytest.raises(HTTPException) as caught:
        oauth.refresh_access_token(db, connection.user_id, "original-refresh")
    assert caught.value.status_code == 409
    db.refresh(connection)
    assert connection.authorization_generation == 2
    assert connection.access_token == "new-consent"
    assert connection.status == ConnectionStatus.ACTIVE


def test_fenced_owner_cannot_dispatch_disconnect(client: TestClient, db: Session) -> None:
    user = UserFactory(health_write_state="fenced")
    connection = UserConnectionFactory(user=user, provider="whoop")
    with patch("app.services.providers.whoop.oauth.WhoopOAuth.deregister_user") as deregister:
        response = client.delete(endpoint(connection), headers=api_key_headers(ApiKeyFactory().id))
        assert response.status_code == 409
        assert response.json() == {"detail": "source_write_unavailable"}
        deregister.assert_not_called()


def test_disconnect_serializes_with_new_consent_in_separate_transactions(engine: Engine) -> None:
    user_id, connection_id = uuid4(), uuid4()
    entered, release, writer_started = Event(), Event(), Event()
    with Session(engine) as seed:
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
                updated_at=datetime.now(timezone.utc),
            )
        )
        seed.commit()

    def provider_ack(*args: object, **kwargs: object) -> None:
        entered.set()
        assert release.wait(5), "test did not release provider acknowledgement"

    def disconnect() -> None:
        with Session(engine) as session:
            user_connection_service.disconnect_exact(
                session, user_id, "whoop", connection_id, 1, Mock(deregister_user=Mock(side_effect=provider_ack))
            )

    def consent() -> None:
        with Session(engine) as session:
            session.execute(text("SET application_name = 'exact_disconnect_consent_test'"))
            current = session.get(UserConnection, connection_id)
            assert current is not None
            writer_started.set()
            UserConnectionRepository().update_connection_info(session, current, "new-consent", "new-refresh", 3600)

    try:
        with ThreadPoolExecutor(max_workers=2) as pool:
            disconnection = pool.submit(disconnect)
            assert entered.wait(5)
            consent_write = pool.submit(consent)
            assert writer_started.wait(5)
            try:
                # Observe a real database lock wait, not merely an unfinished Future.
                with Session(engine) as observer:
                    for _ in range(100):
                        blocked = observer.execute(
                            text(
                                "SELECT 1 FROM pg_stat_activity WHERE "
                                "application_name = 'exact_disconnect_consent_test' AND wait_event_type = 'Lock'"
                            )
                        ).first()
                        observer.rollback()  # refresh pg_stat_activity's transaction snapshot
                        if blocked:
                            break
                        Event().wait(0.01)
                    assert blocked is not None
                assert not consent_write.done()
            finally:
                release.set()
            disconnection.result(timeout=5)
            consent_write.result(timeout=5)
        with Session(engine) as check:
            current = check.get(UserConnection, connection_id)
            assert current is not None
            assert current.authorization_generation == 2
            assert current.disconnect_confirmed_generation == 1
            assert current.status == ConnectionStatus.ACTIVE
            assert current.access_token == "new-consent"
            with pytest.raises(HTTPException) as caught:
                user_connection_service.disconnect_exact(check, user_id, "whoop", connection_id, 1, Mock())
            assert caught.value.status_code == 409
    finally:
        release.set()
        with Session(engine) as cleanup:
            cleanup.execute(delete(UserConnection).where(UserConnection.id == connection_id))
            cleanup.execute(delete(User).where(User.id == user_id))
            cleanup.commit()
