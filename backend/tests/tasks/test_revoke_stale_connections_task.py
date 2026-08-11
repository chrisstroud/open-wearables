"""
Tests for revoke_stale_connections Celery task.

Covers the sweep that revokes connections which stopped delivering data. SDK
providers cannot report sign-out or permission revocation, so inactivity is the
only signal available.
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from sqlalchemy.orm import Session

from app.config import settings
from app.integrations.celery.tasks.revoke_stale_connections_task import revoke_stale_connections
from app.schemas.auth import ConnectionStatus
from tests.factories import UserConnectionFactory, UserFactory


def _ago(days: int) -> datetime:
    return datetime.now(timezone.utc) - timedelta(days=days)


class TestRevokeStaleConnectionsTask:
    """Test suite for revoke_stale_connections task."""

    @patch("app.integrations.celery.tasks.revoke_stale_connections_task.SessionLocal")
    def test_revokes_connection_past_threshold(
        self, mock_session_local: MagicMock, db: Session, mock_celery_app: MagicMock
    ) -> None:
        """A connection idle beyond the threshold is revoked and reported."""
        mock_session_local.return_value.__enter__.return_value = db
        user = UserFactory()
        connection = UserConnectionFactory(
            user=user,
            provider="apple",
            status=ConnectionStatus.ACTIVE,
            access_token=None,
            refresh_token=None,
            last_synced_at=_ago(settings.stale_connection_days + 5),
        )

        with patch("app.services.user_connection_service.on_connection_revoked") as emit:
            result = revoke_stale_connections()

        db.refresh(connection)
        assert connection.status == ConnectionStatus.REVOKED
        assert result["revoked_count"] == 1
        assert {"user_id": str(user.id), "provider": "apple"} in result["revoked"]
        # reason must distinguish the sweep from a real user disconnect
        assert emit.call_args.kwargs["reason"] == "stale"
        assert emit.call_args.kwargs["provider"] == "apple"

    @patch("app.integrations.celery.tasks.revoke_stale_connections_task.SessionLocal")
    def test_leaves_recent_connection_alone(
        self, mock_session_local: MagicMock, db: Session, mock_celery_app: MagicMock
    ) -> None:
        """A recently synced connection is untouched and emits nothing."""
        mock_session_local.return_value.__enter__.return_value = db
        connection = UserConnectionFactory(
            provider="apple",
            status=ConnectionStatus.ACTIVE,
            access_token=None,
            refresh_token=None,
            last_synced_at=_ago(1),
        )

        with patch("app.services.user_connection_service.on_connection_revoked") as emit:
            result = revoke_stale_connections()

        db.refresh(connection)
        assert connection.status == ConnectionStatus.ACTIVE
        assert result["revoked_count"] == 0
        emit.assert_not_called()

    @patch("app.integrations.celery.tasks.revoke_stale_connections_task.SessionLocal")
    def test_catches_connection_that_never_synced(
        self, mock_session_local: MagicMock, db: Session, mock_celery_app: MagicMock
    ) -> None:
        """last_synced_at NULL falls back to created_at, so it is still caught."""
        mock_session_local.return_value.__enter__.return_value = db
        connection = UserConnectionFactory(
            provider="apple",
            status=ConnectionStatus.ACTIVE,
            access_token=None,
            refresh_token=None,
            last_synced_at=None,
            created_at=_ago(settings.stale_connection_days + 1),
        )

        with patch("app.services.user_connection_service.on_connection_revoked"):
            result = revoke_stale_connections()

        db.refresh(connection)
        assert connection.status == ConnectionStatus.REVOKED
        assert result["revoked_count"] == 1

    @patch("app.integrations.celery.tasks.revoke_stale_connections_task.SessionLocal")
    def test_skips_already_revoked(
        self, mock_session_local: MagicMock, db: Session, mock_celery_app: MagicMock
    ) -> None:
        """Already-revoked connections must not re-emit on every sweep."""
        mock_session_local.return_value.__enter__.return_value = db
        UserConnectionFactory(
            provider="apple",
            status=ConnectionStatus.REVOKED,
            last_synced_at=_ago(settings.stale_connection_days + 30),
        )

        with patch("app.services.user_connection_service.on_connection_revoked") as emit:
            result = revoke_stale_connections()

        assert result["revoked_count"] == 0
        emit.assert_not_called()

    @patch("app.integrations.celery.tasks.revoke_stale_connections_task.SessionLocal")
    def test_skips_oauth_backed_connections(
        self, mock_session_local: MagicMock, db: Session, mock_celery_app: MagicMock
    ) -> None:
        """Webhook/REST providers resolve via active-only queries and never recover."""
        mock_session_local.return_value.__enter__.return_value = db
        stale = _ago(settings.stale_connection_days + 10)
        for provider in ("whoop", "oura", "garmin"):
            UserConnectionFactory(
                provider=provider,
                status=ConnectionStatus.ACTIVE,
                access_token="tok",
                refresh_token="ref",
                last_synced_at=stale,
            )

        with patch("app.services.user_connection_service.on_connection_revoked") as emit:
            result = revoke_stale_connections()

        assert result["revoked_count"] == 0
        emit.assert_not_called()

    @patch("app.integrations.celery.tasks.revoke_stale_connections_task.SessionLocal")
    def test_skips_sdk_provider_that_holds_oauth_tokens(
        self, mock_session_local: MagicMock, db: Session, mock_celery_app: MagicMock
    ) -> None:
        """Google carries both an SDK and an OAuth integration; tokens mark the OAuth one."""
        mock_session_local.return_value.__enter__.return_value = db
        connection = UserConnectionFactory(
            provider="google",
            status=ConnectionStatus.ACTIVE,
            access_token="tok",
            refresh_token="ref",
            last_synced_at=_ago(settings.stale_connection_days + 10),
        )

        with patch("app.services.user_connection_service.on_connection_revoked") as emit:
            result = revoke_stale_connections()

        db.refresh(connection)
        assert connection.status == ConnectionStatus.ACTIVE
        assert result["revoked_count"] == 0
        emit.assert_not_called()
