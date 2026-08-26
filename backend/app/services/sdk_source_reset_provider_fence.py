from collections.abc import Iterable

import httpx

from app.models import UserConnection
from app.services.providers.factory import ProviderFactory

_USER_DEREGISTRATION_PROVIDERS = frozenset(
    {
        "fitbit",
        "garmin",
        "oura",
        "polar",
        "strava",
        "ultrahuman",
        "whoop",
    }
)


class SDKSourceResetProviderFence:
    """Fail-closed per-user provider deregistration for source reset."""

    def deregister(self, connections: Iterable[UserConnection]) -> None:
        provider_factory = ProviderFactory()
        for connection in sorted(connections, key=lambda row: (row.provider, str(row.id))):
            if connection.provider not in _USER_DEREGISTRATION_PROVIDERS or not connection.access_token:
                continue
            strategy = provider_factory.get_provider(connection.provider)
            if strategy.oauth is None:
                raise RuntimeError(f"Provider deregistration is unavailable for {connection.provider}")
            try:
                strategy.oauth.deregister_user(
                    connection.access_token,
                    provider_user_id=connection.provider_user_id,
                )
            except httpx.HTTPStatusError as exc:
                # A retry after the provider accepted deregistration commonly
                # sees an invalid token or missing registration. Both prove the
                # old credential can no longer authorize future callbacks.
                if exc.response.status_code in {401, 404}:
                    continue
                raise RuntimeError(f"Provider deregistration failed for {connection.provider}") from exc
            except Exception as exc:
                raise RuntimeError(f"Provider deregistration failed for {connection.provider}") from exc


sdk_source_reset_provider_fence = SDKSourceResetProviderFence()
