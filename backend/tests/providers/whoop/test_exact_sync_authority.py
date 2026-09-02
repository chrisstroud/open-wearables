from datetime import datetime, timedelta, timezone
from typing import cast
from unittest.mock import MagicMock, patch
from uuid import uuid4

import httpx
import pytest
from fastapi import HTTPException
from sqlalchemy.orm import Session

from app.models import User, UserConnection, WhoopAuthorizationLease, WhoopSyncDispatchReceipt
from app.repositories.health_write_authority import HealthWriteAuthorityError
from app.repositories.user_connection_repository import UserConnectionRepository
from app.repositories.user_repository import UserRepository
from app.repositories.whoop_sync_dispatch_repository import WhoopSyncDispatchRepository
from app.schemas.auth import ConnectionStatus
from app.schemas.model_crud.credentials import OAuthState, OAuthTokenResponse
from app.schemas.whoop_sync_dispatch import WhoopFullHistorySyncCommand
from app.services.providers.api_client import _get_valid_token
from app.services.providers.whoop.exact_sync_authority import (
    ExactWhoopSyncAuthority,
    scoped_exact_whoop_sync_authority,
)
from app.services.providers.whoop.oauth import WhoopOAuth
from app.services.user_connection_service import UserConnectionService
from tests.factories import UserConnectionFactory, UserFactory


def _running_authority(
    db: Session,
) -> tuple[User, UserConnection, WhoopSyncDispatchReceipt, ExactWhoopSyncAuthority]:
    user = cast(User, UserFactory())
    connection = cast(
        UserConnection,
        UserConnectionFactory(
            user=user,
            provider="whoop",
            authorization_generation=3,
            access_token="generation-a-token",
            token_expires_at=datetime.now(timezone.utc) + timedelta(hours=1),
        ),
    )
    repository = WhoopSyncDispatchRepository()
    dispatch_id = uuid4()
    receipt = repository.create_or_get(
        db,
        user_id=user.id,
        connection_id=connection.id,
        command=WhoopFullHistorySyncCommand(
            idempotency_key=dispatch_id,
            authorization_generation=3,
            requested_start_at=datetime(2000, 1, 1, tzinfo=timezone.utc),
            requested_end_at=datetime(2026, 9, 2, tzinfo=timezone.utc),
        ),
    )
    lease_token = uuid4()
    assert repository.try_acquire_authorization_lease(
        db,
        user_id=user.id,
        connection_id=connection.id,
        authorization_generation=3,
        lease_token=lease_token,
        lease_kind="full_history_sync",
    )
    assert repository.claim_execution(db, dispatch_id=dispatch_id, lease_token=lease_token)
    authority = ExactWhoopSyncAuthority(
        dispatch_id=dispatch_id,
        user_id=user.id,
        connection_id=connection.id,
        authorization_generation=3,
        lease_token=lease_token,
    )
    return user, connection, receipt, authority


class TestExactWhoopCredentialResolution:
    def test_exact_context_never_calls_account_level_lookup(self, db: Session) -> None:
        user, _connection, _receipt, authority = _running_authority(db)
        connection_repository = UserConnectionRepository()
        oauth = MagicMock()

        with (
            patch.object(
                connection_repository,
                "get_by_user_and_provider",
                side_effect=AssertionError("account-level fallback is forbidden"),
            ),
            scoped_exact_whoop_sync_authority(authority),
        ):
            token = _get_valid_token(db, user.id, "whoop", connection_repository, oauth)

        assert token == "generation-a-token"

    def test_generation_change_cannot_resolve_new_account_token(self, db: Session) -> None:
        user, connection, _receipt, authority = _running_authority(db)
        connection.access_token = "generation-b-token"
        connection.authorization_generation = 4
        db.commit()
        connection_repository = UserConnectionRepository()

        with scoped_exact_whoop_sync_authority(authority), pytest.raises(HTTPException) as error:
            _get_valid_token(db, user.id, "whoop", connection_repository, MagicMock())

        assert error.value.status_code == 401

    def test_exact_refresh_re_resolves_authority_before_returning_token(self, db: Session) -> None:
        user, connection, _receipt, authority = _running_authority(db)
        connection.token_expires_at = datetime.now(timezone.utc) - timedelta(minutes=1)
        connection.refresh_token = "refresh-a"
        db.commit()
        oauth = MagicMock()

        def advance_generation(*_args: object, **_kwargs: object) -> OAuthTokenResponse:
            connection.access_token = "generation-b-token"
            connection.authorization_generation = 4
            db.commit()
            return OAuthTokenResponse(
                access_token="generation-b-token",
                refresh_token="refresh-b",
                token_type="bearer",
                expires_in=3600,
            )

        oauth.refresh_access_token.side_effect = advance_generation
        with scoped_exact_whoop_sync_authority(authority), pytest.raises(HTTPException) as error:
            _get_valid_token(db, user.id, "whoop", UserConnectionRepository(), oauth)

        assert error.value.status_code == 401
        oauth.refresh_access_token.assert_called_once()

    def test_real_exact_refresh_never_uses_account_lookup_or_advances_generation(self, db: Session) -> None:
        user, connection, _receipt, authority = _running_authority(db)
        connection.token_expires_at = datetime.now(timezone.utc) - timedelta(minutes=1)
        connection.refresh_token = "refresh-a"
        db.commit()
        connection_repository = UserConnectionRepository()
        oauth = WhoopOAuth(
            UserRepository(User),
            connection_repository,
            "whoop",
            "https://api.prod.whoop.com/developer",
        )
        response = MagicMock()
        response.raise_for_status.return_value = None
        response.json.return_value = {
            "access_token": "refreshed-token",
            "refresh_token": "refreshed-refresh",
            "token_type": "bearer",
            "expires_in": 3600,
        }

        with (
            patch.object(
                connection_repository,
                "get_by_user_and_provider",
                side_effect=AssertionError("account-level fallback is forbidden"),
            ),
            patch("app.services.providers.templates.base_oauth.httpx.post", return_value=response),
            scoped_exact_whoop_sync_authority(authority),
        ):
            token = _get_valid_token(db, user.id, "whoop", connection_repository, oauth)

        db.refresh(connection)
        assert token == "refreshed-token"
        assert connection.access_token == "refreshed-token"
        assert connection.authorization_generation == 3

    def test_ordinary_refresh_cannot_cross_running_exact_sync_lease(self, db: Session) -> None:
        user, connection, _receipt, _authority = _running_authority(db)
        connection.refresh_token = "refresh-a"
        db.commit()
        oauth = WhoopOAuth(
            UserRepository(User),
            UserConnectionRepository(),
            "whoop",
            "https://api.prod.whoop.com/developer",
        )

        with (
            patch("app.services.providers.templates.base_oauth.httpx.post") as refresh_request,
            pytest.raises(HTTPException) as error,
        ):
            oauth.refresh_access_token(db, user.id, "refresh-a")

        assert error.value.status_code == 409
        refresh_request.assert_not_called()
        db.refresh(connection)
        assert connection.status == ConnectionStatus.ACTIVE

    def test_rejected_ordinary_refresh_revokes_only_under_token_refresh_lease(self, db: Session) -> None:
        user = cast(User, UserFactory())
        connection = cast(
            UserConnection,
            UserConnectionFactory(
                user=user,
                provider="whoop",
                authorization_generation=3,
                access_token="access-a",
                refresh_token="refresh-a",
            ),
        )
        oauth = WhoopOAuth(
            UserRepository(User),
            UserConnectionRepository(),
            "whoop",
            "https://api.prod.whoop.com/developer",
        )
        response = httpx.Response(
            401,
            request=httpx.Request("POST", "https://api.prod.whoop.com/oauth/oauth2/token"),
            text="invalid refresh token",
        )

        with (
            patch("app.services.providers.templates.base_oauth.httpx.post", return_value=response),
            pytest.raises(HTTPException) as error,
        ):
            oauth.refresh_access_token(db, user.id, "refresh-a")

        assert error.value.status_code == 401
        db.refresh(connection)
        assert connection.status == ConnectionStatus.REVOKED
        assert db.get(WhoopAuthorizationLease, user.id) is None

    def test_unleased_whoop_revocation_is_rejected(self, db: Session) -> None:
        connection = cast(UserConnection, UserConnectionFactory(provider="whoop"))

        with pytest.raises(HealthWriteAuthorityError, match="authorization lease"):
            UserConnectionRepository().mark_as_revoked(db, connection)

        db.refresh(connection)
        assert connection.status == ConnectionStatus.ACTIVE


class TestWhoopOAuthLease:
    def test_stale_generation_cannot_acquire_authorization_lease(self, db: Session) -> None:
        user = cast(User, UserFactory())
        connection = cast(
            UserConnection,
            UserConnectionFactory(user=user, provider="whoop", authorization_generation=4),
        )

        acquired = WhoopSyncDispatchRepository().try_acquire_authorization_lease(
            db,
            user_id=user.id,
            connection_id=connection.id,
            authorization_generation=3,
            lease_token=uuid4(),
            lease_kind="disconnect",
        )

        assert not acquired
        assert db.get(WhoopAuthorizationLease, user.id) is None

    def test_running_exact_sync_blocks_reauthorization_before_token_exchange(self, db: Session) -> None:
        user, _connection, _receipt, authority = _running_authority(db)
        oauth = WhoopOAuth(
            UserRepository(User),
            UserConnectionRepository(),
            "whoop",
            "https://api.prod.whoop.com/developer",
        )
        state = OAuthState(user_id=user.id, provider="whoop")

        with (
            patch.object(oauth, "_validate_state", return_value=(state, None)),
            patch.object(oauth, "_exchange_token") as exchange,
            pytest.raises(HTTPException) as error,
        ):
            oauth.handle_callback(db, "code", "state")

        assert error.value.status_code == 409
        exchange.assert_not_called()
        assert authority.authorization_generation == 3

    def test_running_exact_sync_blocks_disconnect_before_provider_deregister(self, db: Session) -> None:
        user, connection, _receipt, _authority = _running_authority(db)
        oauth = MagicMock()
        service = UserConnectionService(log=MagicMock())

        with pytest.raises(HTTPException) as error:
            service.disconnect(db, user.id, "whoop", oauth=oauth)

        assert error.value.status_code == 409
        oauth.deregister_user.assert_not_called()
        db.refresh(connection)
        assert connection.status == ConnectionStatus.ACTIVE

    def test_exact_disconnect_rejects_a_stale_review_before_provider_deregister(self, db: Session) -> None:
        user = cast(User, UserFactory())
        connection = cast(
            UserConnection,
            UserConnectionFactory(user=user, provider="whoop", authorization_generation=4),
        )
        oauth = MagicMock()
        service = UserConnectionService(log=MagicMock())

        with pytest.raises(HTTPException) as error:
            service.disconnect(
                db,
                user.id,
                "whoop",
                oauth=oauth,
                expected_connection_id=connection.id,
                expected_authorization_generation=3,
            )

        assert error.value.status_code == 409
        oauth.deregister_user.assert_not_called()
        db.refresh(connection)
        assert connection.status == ConnectionStatus.ACTIVE

    def test_disconnect_fails_closed_when_lease_expires_during_provider_io(self, db: Session) -> None:
        user = cast(User, UserFactory())
        connection = cast(
            UserConnection,
            UserConnectionFactory(user=user, provider="whoop", authorization_generation=3),
        )
        oauth = MagicMock()
        service = UserConnectionService(log=MagicMock())

        def expire_disconnect_lease(*_args: object, **_kwargs: object) -> None:
            lease = db.get(WhoopAuthorizationLease, user.id)
            assert lease is not None
            now = datetime.now(timezone.utc)
            lease.acquired_at = now - timedelta(minutes=10)
            lease.lease_expires_at = now - timedelta(seconds=1)
            db.commit()

        oauth.deregister_user.side_effect = expire_disconnect_lease
        with pytest.raises(HTTPException) as error:
            service.disconnect(db, user.id, "whoop", oauth=oauth)

        assert error.value.status_code == 409
        db.refresh(connection)
        assert connection.status == ConnectionStatus.ACTIVE

    def test_disconnect_stale_snapshot_cannot_revoke_newer_grant(self, db: Session) -> None:
        user = cast(User, UserFactory())
        connection = cast(
            UserConnection,
            UserConnectionFactory(
                user=user,
                provider="whoop",
                authorization_generation=3,
                access_token="generation-a-token",
            ),
        )
        oauth = MagicMock()
        service = UserConnectionService(log=MagicMock())

        def install_newer_grant(*_args: object, **_kwargs: object) -> None:
            connection.authorization_generation = 4
            connection.access_token = "generation-b-token"
            db.commit()

        oauth.deregister_user.side_effect = install_newer_grant
        with pytest.raises(HTTPException) as error:
            service.disconnect(db, user.id, "whoop", oauth=oauth)

        assert error.value.status_code == 409
        db.refresh(connection)
        assert connection.status == ConnectionStatus.ACTIVE
        assert connection.authorization_generation == 4
        assert connection.access_token == "generation-b-token"

    def test_successful_reauthorization_advances_generation_once(self, db: Session) -> None:
        user = cast(User, UserFactory())
        connection = cast(
            UserConnection,
            UserConnectionFactory(
                user=user,
                provider="whoop",
                authorization_generation=3,
                access_token="generation-a-token",
            ),
        )
        oauth = WhoopOAuth(
            UserRepository(User),
            UserConnectionRepository(),
            "whoop",
            "https://api.prod.whoop.com/developer",
        )
        state = OAuthState(user_id=user.id, provider="whoop")
        response = OAuthTokenResponse(
            access_token="generation-b-token",
            refresh_token="generation-b-refresh",
            token_type="bearer",
            expires_in=3600,
        )

        with (
            patch.object(oauth, "_validate_state", return_value=(state, None)),
            patch.object(oauth, "_exchange_token", return_value=response),
            patch.object(oauth, "_get_provider_user_info", return_value={"user_id": "whoop-user", "username": None}),
        ):
            oauth.handle_callback(db, "code", "state")

        db.refresh(connection)
        assert connection.authorization_generation == 4
        assert connection.access_token == "generation-b-token"

    def test_stale_callback_snapshot_cannot_overwrite_newer_grant(self, db: Session) -> None:
        user = cast(User, UserFactory())
        connection = cast(
            UserConnection,
            UserConnectionFactory(
                user=user,
                provider="whoop",
                authorization_generation=3,
                access_token="generation-a-token",
            ),
        )
        oauth = WhoopOAuth(
            UserRepository(User),
            UserConnectionRepository(),
            "whoop",
            "https://api.prod.whoop.com/developer",
        )
        state = OAuthState(user_id=user.id, provider="whoop")
        callback_response = OAuthTokenResponse(
            access_token="stale-callback-token",
            refresh_token="stale-callback-refresh",
            token_type="bearer",
            expires_in=3600,
        )

        def install_newer_grant(*_args: object, **_kwargs: object) -> OAuthTokenResponse:
            connection.authorization_generation = 4
            connection.access_token = "generation-b-token"
            db.commit()
            return callback_response

        with (
            patch.object(oauth, "_validate_state", return_value=(state, None)),
            patch.object(oauth, "_exchange_token", side_effect=install_newer_grant),
            patch.object(oauth, "_get_provider_user_info", return_value={"user_id": "whoop-user", "username": None}),
            pytest.raises(HTTPException) as error,
        ):
            oauth.handle_callback(db, "code", "state")

        assert error.value.status_code == 409
        db.refresh(connection)
        assert connection.authorization_generation == 4
        assert connection.access_token == "generation-b-token"
