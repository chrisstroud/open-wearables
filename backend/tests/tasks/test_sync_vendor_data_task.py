"""
Tests for sync_vendor_data Celery task.

Tests synchronization of workout data from external providers (Garmin, Polar, Suunto).
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from freezegun import freeze_time
from sqlalchemy.orm import Session

from app.config import Settings, settings
from app.integrations.celery.tasks.sync_vendor_data_task import sync_vendor_data
from app.schemas.auth import ConnectionStatus
from app.utils.sync_params import build_sync_params
from tests.factories import UserConnectionFactory, UserFactory


class TestSyncVendorDataTask:
    """Test suite for sync_vendor_data task."""

    @patch("app.integrations.celery.tasks.sync_vendor_data_task.SessionLocal")
    @patch("app.services.providers.factory.ProviderFactory.get_provider")
    def test_sync_vendor_data_success(
        self,
        mock_get_provider: MagicMock,
        mock_session_local: MagicMock,
        db: Session,
        mock_celery_app: MagicMock,
    ) -> None:
        """Test successful sync of vendor data."""
        # Arrange
        user = UserFactory()
        connection = UserConnectionFactory(
            user=user,
            provider="garmin",
            status=ConnectionStatus.ACTIVE,
        )

        # Mock the database session
        mock_session_local.return_value.__enter__.return_value = db
        mock_session_local.return_value.__exit__.return_value = None

        # Mock the provider strategy
        mock_workouts = MagicMock()
        mock_workouts.load_data.return_value = True

        mock_strategy = MagicMock()
        mock_strategy.capabilities.rest_pull = True
        mock_strategy.capabilities.webhook_stream = False
        mock_strategy.workouts = mock_workouts
        mock_get_provider.return_value = mock_strategy

        # Act
        result = sync_vendor_data(str(user.id))

        # Assert
        assert str(result["user_id"]) == str(user.id)
        assert "garmin" in result["providers_synced"]
        assert result["providers_synced"]["garmin"]["success"] is True
        assert result["errors"] == {}
        mock_workouts.load_data.assert_called_once()

        # Verify connection was updated
        db.refresh(connection)
        assert connection.last_synced_at is not None

    @patch("app.integrations.celery.tasks.sync_vendor_data_task.SessionLocal")
    @patch("app.services.providers.factory.ProviderFactory.get_provider")
    def test_sync_vendor_data_with_date_range(
        self,
        mock_get_provider: MagicMock,
        mock_session_local: MagicMock,
        db: Session,
        mock_celery_app: MagicMock,
    ) -> None:
        """Test sync with specific date range."""
        # Arrange
        user = UserFactory()
        UserConnectionFactory(
            user=user,
            provider="polar",
            status=ConnectionStatus.ACTIVE,
        )

        mock_session_local.return_value.__enter__.return_value = db
        mock_session_local.return_value.__exit__.return_value = None

        mock_workouts = MagicMock()
        mock_workouts.load_data.return_value = True

        mock_strategy = MagicMock()
        mock_strategy.capabilities.rest_pull = True
        mock_strategy.capabilities.webhook_stream = False
        mock_strategy.workouts = mock_workouts
        mock_get_provider.return_value = mock_strategy

        start_date = "2025-01-01T00:00:00Z"
        end_date = "2025-12-31T23:59:59Z"

        # Act
        result = sync_vendor_data(str(user.id), start_date=start_date, end_date=end_date)

        # Assert
        assert str(result["user_id"]) == str(user.id)
        assert result["start_date"] == start_date
        assert result["end_date"] == end_date
        assert "polar" in result["providers_synced"]
        mock_workouts.load_data.assert_called_once()

    @patch("app.integrations.celery.tasks.sync_vendor_data_task.SessionLocal")
    @patch("app.services.providers.factory.ProviderFactory.get_provider")
    def test_whoop_sync_propagates_active_connection_identity_to_both_loaders(
        self,
        mock_get_provider: MagicMock,
        mock_session_local: MagicMock,
        db: Session,
        mock_celery_app: MagicMock,
    ) -> None:
        """WHOOP writes must retain the concrete authorization that produced them."""
        user = UserFactory()
        original_watermark = datetime(2026, 7, 31, tzinfo=timezone.utc)
        connection = UserConnectionFactory(
            user=user,
            provider="whoop",
            status=ConnectionStatus.ACTIVE,
            last_synced_at=original_watermark,
        )

        mock_session_local.return_value.__enter__.return_value = db
        mock_session_local.return_value.__exit__.return_value = None

        workouts = MagicMock()
        workouts.load_data.return_value = 1
        data_247 = MagicMock()
        data_247.load_and_save_all.return_value = {}
        strategy = MagicMock()
        strategy.capabilities.rest_pull = True
        strategy.capabilities.webhook_stream = True
        strategy.workouts = workouts
        strategy.data_247 = data_247
        mock_get_provider.return_value = strategy

        result = sync_vendor_data(
            str(user.id),
            start_date="2026-08-01T00:00:00Z",
            end_date="2026-08-02T00:00:00Z",
            providers=["whoop"],
            is_historical=True,
        )

        assert workouts.load_data.call_args.kwargs["user_connection_id"] == connection.id
        assert data_247.load_and_save_all.call_args.kwargs["user_connection_id"] == connection.id
        assert "user_connection_id" not in result["providers_synced"]["whoop"]["params"]["workouts"]
        assert "user_connection_id" not in result["providers_synced"]["whoop"]["params"]["data_247"]
        assert workouts.load_data.call_args.kwargs["start_date"] == "2026-08-01T00:00:00Z"
        assert workouts.load_data.call_args.kwargs["end_date"] == "2026-08-02T00:00:00Z"
        assert data_247.load_and_save_all.call_args.kwargs["start_time"] == datetime(2026, 8, 1, tzinfo=timezone.utc)
        assert data_247.load_and_save_all.call_args.kwargs["end_time"] == datetime(2026, 8, 2, tzinfo=timezone.utc)
        db.refresh(connection)
        assert connection.last_synced_at == original_watermark

    @patch("app.integrations.celery.tasks.sync_vendor_data_task.SessionLocal")
    @patch("app.services.providers.factory.ProviderFactory.get_provider")
    def test_live_sync_uses_default_lookback_and_fans_out_exact_captured_window(
        self,
        mock_get_provider: MagicMock,
        mock_session_local: MagicMock,
        db: Session,
        mock_celery_app: MagicMock,
    ) -> None:
        """A live pull has one bounded window shared by provider work, cursor, and fan-out."""
        user = UserFactory()
        linked_user = UserFactory()
        provider_user_id = "shared-whoop-account"
        original_watermark = datetime(2026, 8, 29, 9, tzinfo=timezone.utc)
        connection = UserConnectionFactory(
            user=user,
            provider="whoop",
            provider_user_id=provider_user_id,
            status=ConnectionStatus.ACTIVE,
            last_synced_at=original_watermark,
        )
        linked_connection = UserConnectionFactory(
            user=linked_user,
            provider="whoop",
            provider_user_id=provider_user_id,
            status=ConnectionStatus.ACTIVE,
            last_synced_at=original_watermark,
        )

        mock_session_local.return_value.__enter__.return_value = db
        mock_session_local.return_value.__exit__.return_value = None

        workouts = MagicMock()
        strategy = MagicMock()
        strategy.capabilities.rest_pull = True
        strategy.capabilities.max_historical_days = 30
        strategy.workouts = workouts
        strategy.data_247 = None
        mock_get_provider.return_value = strategy

        window_end = datetime(2026, 8, 30, 12, tzinfo=timezone.utc)
        expected_start = original_watermark - timedelta(days=2)
        assert Settings.model_fields["pull_sync_lookback"].default == timedelta(days=2)

        with (
            freeze_time(window_end) as frozen_time,
            patch.object(settings, "pull_sync_lookback", timedelta(days=2)),
            patch(
                "app.integrations.celery.tasks.sync_vendor_data_task.try_become_primary",
                return_value=(True, "primary-token", user.id),
            ),
            patch("app.integrations.celery.tasks.sync_vendor_data_task.release_primary"),
            patch.object(sync_vendor_data, "apply_async") as mock_fan_out,
        ):

            def finish_after_window_was_captured(*args: object, **kwargs: object) -> bool:
                frozen_time.tick(delta=timedelta(minutes=10))
                return True

            workouts.load_data.side_effect = finish_after_window_was_captured
            sync_vendor_data(str(user.id), providers=["whoop"])

        assert workouts.load_data.call_args.kwargs["start_date"] == expected_start.isoformat()
        assert workouts.load_data.call_args.kwargs["end_date"] == window_end.isoformat()
        db.refresh(connection)
        assert connection.last_synced_at == window_end
        db.refresh(linked_connection)
        assert linked_connection.last_synced_at == original_watermark
        mock_fan_out.assert_called_once()
        fan_out_kwargs = mock_fan_out.call_args.kwargs["kwargs"]
        assert fan_out_kwargs["start_date"] == expected_start.isoformat()
        assert fan_out_kwargs["end_date"] == window_end.isoformat()
        assert fan_out_kwargs["user_id"] == str(linked_user.id)

    @patch("app.integrations.celery.tasks.sync_vendor_data_task.SessionLocal")
    @patch("app.services.providers.factory.ProviderFactory.get_provider")
    def test_live_sync_lookback_is_capped_by_provider_history_limit(
        self,
        mock_get_provider: MagicMock,
        mock_session_local: MagicMock,
        db: Session,
        mock_celery_app: MagicMock,
    ) -> None:
        """Configured lookback never asks a provider for data older than its hard limit."""
        user = UserFactory()
        UserConnectionFactory(
            user=user,
            provider="oura",
            status=ConnectionStatus.ACTIVE,
            last_synced_at=datetime(2026, 7, 1, tzinfo=timezone.utc),
        )
        mock_session_local.return_value.__enter__.return_value = db
        mock_session_local.return_value.__exit__.return_value = None

        workouts = MagicMock()
        workouts.load_data.return_value = True
        strategy = MagicMock()
        strategy.capabilities.rest_pull = True
        strategy.capabilities.max_historical_days = 7
        strategy.workouts = workouts
        strategy.data_247 = None
        mock_get_provider.return_value = strategy

        window_end = datetime(2026, 8, 30, 12, tzinfo=timezone.utc)
        with (
            freeze_time(window_end),
            patch.object(settings, "pull_sync_lookback", timedelta(days=30)),
        ):
            sync_vendor_data(str(user.id), providers=["oura"])

        assert workouts.load_data.call_args.kwargs["start_date"] == (window_end - timedelta(days=7)).isoformat()

    @patch("app.integrations.celery.tasks.sync_vendor_data_task.SessionLocal")
    @patch("app.services.providers.factory.ProviderFactory.get_provider")
    def test_partial_sync_does_not_advance_cursor_or_fan_out(
        self,
        mock_get_provider: MagicMock,
        mock_session_local: MagicMock,
        db: Session,
        mock_celery_app: MagicMock,
    ) -> None:
        """Every required provider substream must succeed before any cursor advances."""
        user = UserFactory()
        linked_user = UserFactory()
        provider_user_id = "shared-oura-account"
        original_watermark = datetime(2026, 8, 29, tzinfo=timezone.utc)
        connection = UserConnectionFactory(
            user=user,
            provider="oura",
            provider_user_id=provider_user_id,
            status=ConnectionStatus.ACTIVE,
            last_synced_at=original_watermark,
        )
        UserConnectionFactory(
            user=linked_user,
            provider="oura",
            provider_user_id=provider_user_id,
            status=ConnectionStatus.ACTIVE,
            last_synced_at=original_watermark,
        )
        mock_session_local.return_value.__enter__.return_value = db
        mock_session_local.return_value.__exit__.return_value = None

        workouts = MagicMock()
        workouts.load_data.return_value = True
        data_247 = MagicMock()
        data_247.load_and_save_all.side_effect = RuntimeError("sleep stream unavailable")
        strategy = MagicMock()
        strategy.capabilities.rest_pull = True
        strategy.capabilities.max_historical_days = None
        strategy.workouts = workouts
        strategy.data_247 = data_247
        mock_get_provider.return_value = strategy

        with (
            patch(
                "app.integrations.celery.tasks.sync_vendor_data_task.try_become_primary",
                return_value=(True, "primary-token", user.id),
            ),
            patch("app.integrations.celery.tasks.sync_vendor_data_task.release_primary") as mock_release,
            patch.object(sync_vendor_data, "apply_async") as mock_fan_out,
        ):
            result = sync_vendor_data(str(user.id), providers=["oura"])

        assert result["providers_synced"]["oura"]["params"]["workouts"]["success"] is True
        assert result["providers_synced"]["oura"]["params"]["data_247"]["success"] is False
        db.refresh(connection)
        assert connection.last_synced_at == original_watermark
        mock_release.assert_called_once()
        mock_fan_out.assert_not_called()

    @patch("app.integrations.celery.tasks.sync_vendor_data_task.SessionLocal")
    @patch("app.services.providers.factory.ProviderFactory.get_provider")
    def test_exception_after_lock_acquisition_releases_token_without_advancing_cursor(
        self,
        mock_get_provider: MagicMock,
        mock_session_local: MagicMock,
        db: Session,
        mock_celery_app: MagicMock,
    ) -> None:
        """A post-provider fan-out error releases the lock and preserves the cursor."""
        user = UserFactory()
        linked_user = UserFactory()
        provider_user_id = "shared-oura-fan-out-error"
        original_watermark = datetime(2026, 8, 29, tzinfo=timezone.utc)
        connection = UserConnectionFactory(
            user=user,
            provider="oura",
            provider_user_id=provider_user_id,
            status=ConnectionStatus.ACTIVE,
            last_synced_at=original_watermark,
        )
        UserConnectionFactory(
            user=linked_user,
            provider="oura",
            provider_user_id=provider_user_id,
            status=ConnectionStatus.ACTIVE,
            last_synced_at=original_watermark,
        )
        mock_session_local.return_value.__enter__.return_value = db
        mock_session_local.return_value.__exit__.return_value = None

        workouts = MagicMock()
        workouts.load_data.return_value = True
        strategy = MagicMock()
        strategy.capabilities.rest_pull = True
        strategy.capabilities.max_historical_days = None
        strategy.workouts = workouts
        strategy.data_247 = None
        mock_get_provider.return_value = strategy

        with (
            patch(
                "app.integrations.celery.tasks.sync_vendor_data_task.try_become_primary",
                return_value=(True, "primary-token", user.id),
            ),
            patch("app.integrations.celery.tasks.sync_vendor_data_task.release_primary") as mock_release,
            patch.object(
                sync_vendor_data,
                "apply_async",
                side_effect=RuntimeError("fan-out enqueue failed"),
            ),
        ):
            result = sync_vendor_data(str(user.id), providers=["oura"])

        assert result["errors"]["oura"] == "fan-out enqueue failed"
        mock_release.assert_called_once_with(
            "oura",
            connection.provider_user_id,
            user.id,
            "primary-token",
            scope="pull",
        )
        db.refresh(connection)
        assert connection.last_synced_at == original_watermark

    @patch("app.integrations.celery.tasks.sync_vendor_data_task.SessionLocal")
    @patch("app.services.providers.factory.ProviderFactory.get_provider")
    def test_losing_linked_lock_does_not_advance_secondary_cursor(
        self,
        mock_get_provider: MagicMock,
        mock_session_local: MagicMock,
        db: Session,
        mock_celery_app: MagicMock,
    ) -> None:
        """A lock loss is pending fan-out, not evidence that this profile synced."""
        primary_user = UserFactory()
        secondary_user = UserFactory()
        provider_user_id = "shared-polar-account"
        UserConnectionFactory(
            user=primary_user,
            provider="polar",
            provider_user_id=provider_user_id,
            status=ConnectionStatus.ACTIVE,
        )
        original_watermark = datetime(2026, 8, 29, tzinfo=timezone.utc)
        secondary_connection = UserConnectionFactory(
            user=secondary_user,
            provider="polar",
            provider_user_id=provider_user_id,
            status=ConnectionStatus.ACTIVE,
            last_synced_at=original_watermark,
        )
        mock_session_local.return_value.__enter__.return_value = db
        mock_session_local.return_value.__exit__.return_value = None

        workouts = MagicMock()
        strategy = MagicMock()
        strategy.capabilities.rest_pull = True
        strategy.workouts = workouts
        strategy.data_247 = None
        mock_get_provider.return_value = strategy

        with patch(
            "app.integrations.celery.tasks.sync_vendor_data_task.try_become_primary",
            return_value=(False, "", primary_user.id),
        ):
            result = sync_vendor_data(str(secondary_user.id), providers=["polar"])

        assert result["providers_synced"]["polar"]["params"] == {"linked_account": True}
        workouts.load_data.assert_not_called()
        db.refresh(secondary_connection)
        assert secondary_connection.last_synced_at == original_watermark

    @patch("app.integrations.celery.tasks.sync_vendor_data_task.SessionLocal")
    @patch("app.services.providers.factory.ProviderFactory.get_provider")
    def test_linked_fan_out_advances_only_successful_secondary_to_primary_window_end(
        self,
        mock_get_provider: MagicMock,
        mock_session_local: MagicMock,
        db: Session,
        mock_celery_app: MagicMock,
    ) -> None:
        """Fan-out secondaries own their success decision but reuse the primary window."""
        primary_user = UserFactory()
        successful_user = UserFactory()
        failed_user = UserFactory()
        original_watermark = datetime(2026, 8, 28, tzinfo=timezone.utc)
        successful_connection = UserConnectionFactory(
            user=successful_user,
            provider="polar",
            status=ConnectionStatus.ACTIVE,
            last_synced_at=original_watermark,
        )
        failed_connection = UserConnectionFactory(
            user=failed_user,
            provider="polar",
            status=ConnectionStatus.ACTIVE,
            last_synced_at=original_watermark,
        )
        mock_session_local.return_value.__enter__.return_value = db
        mock_session_local.return_value.__exit__.return_value = None

        workouts = MagicMock()
        workouts.load_data.side_effect = [True, False]
        strategy = MagicMock()
        strategy.capabilities.rest_pull = True
        strategy.workouts = workouts
        strategy.data_247 = None
        mock_get_provider.return_value = strategy
        window_start = "2026-08-29T00:00:00+00:00"
        window_end = "2026-08-30T12:00:00+00:00"

        for linked_user in (successful_user, failed_user):
            sync_vendor_data(
                str(linked_user.id),
                start_date=window_start,
                end_date=window_end,
                providers=["polar"],
                _skip_linked_fan_out=True,
                _linked_primary_user_id=str(primary_user.id),
            )

        for call in workouts.load_data.call_args_list:
            assert call.kwargs["start_date"] == window_start
            assert call.kwargs["end_date"] == window_end
        db.refresh(successful_connection)
        assert successful_connection.last_synced_at == datetime.fromisoformat(window_end)
        db.refresh(failed_connection)
        assert failed_connection.last_synced_at == original_watermark

    @patch("app.integrations.celery.tasks.sync_vendor_data_task.SessionLocal")
    @patch("app.services.providers.factory.ProviderFactory.get_provider")
    def test_sync_vendor_data_multiple_providers(
        self,
        mock_get_provider: MagicMock,
        mock_session_local: MagicMock,
        db: Session,
        mock_celery_app: MagicMock,
    ) -> None:
        """Test sync with multiple provider connections."""
        # Arrange
        user = UserFactory()
        UserConnectionFactory(user=user, provider="garmin", status=ConnectionStatus.ACTIVE)
        UserConnectionFactory(user=user, provider="polar", status=ConnectionStatus.ACTIVE)
        UserConnectionFactory(user=user, provider="suunto", status=ConnectionStatus.ACTIVE)

        mock_session_local.return_value.__enter__.return_value = db
        mock_session_local.return_value.__exit__.return_value = None

        mock_workouts = MagicMock()
        mock_workouts.load_data.return_value = True

        mock_strategy = MagicMock()
        mock_strategy.capabilities.rest_pull = True
        mock_strategy.capabilities.webhook_stream = False
        mock_strategy.workouts = mock_workouts
        mock_get_provider.return_value = mock_strategy

        # Act
        result = sync_vendor_data(str(user.id))

        # Assert
        assert len(result["providers_synced"]) == 3
        assert "garmin" in result["providers_synced"]
        assert "polar" in result["providers_synced"]
        assert "suunto" in result["providers_synced"]
        assert mock_workouts.load_data.call_count == 3

    @patch("app.integrations.celery.tasks.sync_vendor_data_task.SessionLocal")
    @patch("app.services.providers.factory.ProviderFactory.get_provider")
    def test_sync_vendor_data_specific_providers_only(
        self,
        mock_get_provider: MagicMock,
        mock_session_local: MagicMock,
        db: Session,
        mock_celery_app: MagicMock,
    ) -> None:
        """Test sync with specific provider filter."""
        # Arrange
        user = UserFactory()
        UserConnectionFactory(user=user, provider="garmin", status=ConnectionStatus.ACTIVE)
        UserConnectionFactory(user=user, provider="polar", status=ConnectionStatus.ACTIVE)

        mock_session_local.return_value.__enter__.return_value = db
        mock_session_local.return_value.__exit__.return_value = None

        mock_workouts = MagicMock()
        mock_workouts.load_data.return_value = True

        mock_strategy = MagicMock()
        mock_strategy.capabilities.rest_pull = True
        mock_strategy.capabilities.webhook_stream = False
        mock_strategy.workouts = mock_workouts
        mock_get_provider.return_value = mock_strategy

        # Act - sync only Garmin
        result = sync_vendor_data(str(user.id), providers=["garmin"])

        # Assert
        assert len(result["providers_synced"]) == 1
        assert "garmin" in result["providers_synced"]
        assert "polar" not in result["providers_synced"]
        mock_workouts.load_data.assert_called_once()

    @patch("app.integrations.celery.tasks.sync_vendor_data_task.SessionLocal")
    def test_sync_vendor_data_no_active_connections(
        self,
        mock_session_local: MagicMock,
        db: Session,
        mock_celery_app: MagicMock,
    ) -> None:
        """Test sync when user has no active connections."""
        # Arrange
        user = UserFactory()
        # Create a disconnected connection
        UserConnectionFactory(
            user=user,
            provider="garmin",
            status=ConnectionStatus.REVOKED,
        )

        mock_session_local.return_value.__enter__.return_value = db
        mock_session_local.return_value.__exit__.return_value = None

        # Act
        result = sync_vendor_data(str(user.id))

        # Assert
        assert str(result["user_id"]) == str(user.id)
        assert result["providers_synced"] == {}
        assert result["message"] == "No active provider connections found"

    @patch("app.integrations.celery.tasks.sync_vendor_data_task.SessionLocal")
    @patch("app.services.providers.factory.ProviderFactory.get_provider")
    def test_sync_vendor_data_provider_error(
        self,
        mock_get_provider: MagicMock,
        mock_session_local: MagicMock,
        db: Session,
        mock_celery_app: MagicMock,
    ) -> None:
        """Test handling of provider API errors."""
        # Arrange
        user = UserFactory()
        UserConnectionFactory(user=user, provider="garmin", status=ConnectionStatus.ACTIVE)

        mock_session_local.return_value.__enter__.return_value = db
        mock_session_local.return_value.__exit__.return_value = None

        # Mock provider that fails during sync
        mock_workouts = MagicMock()
        mock_workouts.load_data.side_effect = Exception("Provider API unavailable")

        mock_strategy = MagicMock()
        mock_strategy.capabilities.rest_pull = True
        mock_strategy.capabilities.webhook_stream = False
        mock_strategy.workouts = mock_workouts
        mock_get_provider.return_value = mock_strategy

        # Act
        result = sync_vendor_data(str(user.id))

        # Assert
        assert str(result["user_id"]) == str(user.id)
        assert "garmin" in result["providers_synced"]
        assert result["providers_synced"]["garmin"]["params"]["workouts"]["success"] is False
        assert "Provider API unavailable" in result["providers_synced"]["garmin"]["params"]["workouts"]["error"]

    @patch("app.integrations.celery.tasks.sync_vendor_data_task.SessionLocal")
    @patch("app.services.providers.factory.ProviderFactory.get_provider")
    def test_sync_vendor_data_sync_returns_false(
        self,
        mock_get_provider: MagicMock,
        mock_session_local: MagicMock,
        db: Session,
        mock_celery_app: MagicMock,
    ) -> None:
        """Test handling when provider sync returns False."""
        # Arrange
        user = UserFactory()
        UserConnectionFactory(user=user, provider="polar", status=ConnectionStatus.ACTIVE)

        mock_session_local.return_value.__enter__.return_value = db
        mock_session_local.return_value.__exit__.return_value = None

        mock_workouts = MagicMock()
        mock_workouts.load_data.return_value = False

        mock_strategy = MagicMock()
        mock_strategy.capabilities.rest_pull = True
        mock_strategy.capabilities.webhook_stream = False
        mock_strategy.workouts = mock_workouts
        mock_get_provider.return_value = mock_strategy

        # Act
        result = sync_vendor_data(str(user.id))

        # Assert - provider is added to providers_synced with workouts success=False
        assert "polar" in result["providers_synced"]
        assert result["providers_synced"]["polar"]["params"]["workouts"]["success"] is False
        assert result["errors"] == {}

    @patch("app.integrations.celery.tasks.sync_vendor_data_task.SessionLocal")
    @patch("app.services.providers.factory.ProviderFactory.get_provider")
    def test_sync_vendor_data_workouts_not_supported(
        self,
        mock_get_provider: MagicMock,
        mock_session_local: MagicMock,
        db: Session,
        mock_celery_app: MagicMock,
    ) -> None:
        """Test handling when provider doesn't support workouts."""
        # Arrange
        user = UserFactory()
        UserConnectionFactory(user=user, provider="garmin", status=ConnectionStatus.ACTIVE)

        mock_session_local.return_value.__enter__.return_value = db
        mock_session_local.return_value.__exit__.return_value = None

        # Mock provider without workout support
        mock_strategy = MagicMock()
        mock_strategy.capabilities.rest_pull = True
        mock_strategy.capabilities.webhook_stream = False
        mock_strategy.workouts = None
        # Also ensure data_247 is not set so the strategy is still processed
        del mock_strategy.data_247
        mock_get_provider.return_value = mock_strategy

        # Act
        result = sync_vendor_data(str(user.id))

        # Assert - provider is added to providers_synced without workout params
        assert "garmin" in result["providers_synced"]
        assert "workouts" not in result["providers_synced"]["garmin"]["params"]
        assert result["errors"] == {}

    @patch("app.integrations.celery.tasks.sync_vendor_data_task.SessionLocal")
    @patch("app.services.providers.factory.ProviderFactory.get_provider")
    def test_sync_vendor_data_skips_push_based_provider(
        self,
        mock_get_provider: MagicMock,
        mock_session_local: MagicMock,
        db: Session,
        mock_celery_app: MagicMock,
    ) -> None:
        """Test that push-based providers (no cloud API) are filtered out entirely."""
        # Arrange
        user = UserFactory()
        UserConnectionFactory(user=user, provider="apple", status=ConnectionStatus.ACTIVE)

        mock_session_local.return_value.__enter__.return_value = db
        mock_session_local.return_value.__exit__.return_value = None

        mock_strategy = MagicMock()
        mock_strategy.capabilities.rest_pull = False
        mock_get_provider.return_value = mock_strategy

        # Act
        result = sync_vendor_data(str(user.id))

        # Assert - SDK provider is filtered out, never enters sync loop
        assert "apple" not in result["providers_synced"]
        assert result["errors"] == {}
        assert result["message"] == "No active provider connections found"

    def test_sync_vendor_data_invalid_user_id(self, mock_celery_app: MagicMock) -> None:
        """Test handling of invalid user ID format."""
        # Act
        result = sync_vendor_data("not-a-valid-uuid")

        # Assert
        assert result["user_id"] == "not-a-valid-uuid"
        assert "user_id" in result["errors"]
        assert "Invalid UUID format" in result["errors"]["user_id"]


class TestBuildSyncParams:
    """Test suite for build_sync_params helper function."""

    def test_build_sync_params_with_dates(self) -> None:
        """Both dates are passed through under the canonical keys."""
        start_date = "2025-01-01T00:00:00Z"
        end_date = "2025-12-31T23:59:59Z"

        params = build_sync_params(start_date, end_date)

        assert params == {"start_date": start_date, "end_date": end_date}

    def test_build_sync_params_no_dates(self) -> None:
        """None in, None out - no provider-specific keys are invented."""
        params = build_sync_params(None, None)

        assert params == {"start_date": None, "end_date": None}

    def test_build_sync_params_invalid_date_format(self) -> None:
        """An unparseable date is passed through as-is, not dropped or raised."""
        params = build_sync_params("invalid-date", "2025-12-31T23:59:59Z")

        assert params == {"start_date": "invalid-date", "end_date": "2025-12-31T23:59:59Z"}


def test_pull_sync_lookback_can_be_disabled_explicitly() -> None:
    """The safe two-day default remains operator-configurable."""
    configured = Settings(
        _env_file=None,
        secret_key="test-secret-key",
        pull_sync_lookback="0m",
    )

    assert configured.pull_sync_lookback == timedelta(0)
