from functools import cache
from logging import getLogger

from celery import shared_task

from app.config import settings
from app.database import SessionLocal
from app.schemas.enums import ProviderName
from app.services.providers.factory import ProviderFactory
from app.services.user_connection_service import user_connection_service
from app.utils.structured_logging import log_structured

logger = getLogger(__name__)


@cache
def _client_sdk_providers() -> list[str]:
    """Providers that expose an SDK upload path.

    Capabilities live in Python, so this narrows the UPDATE to candidate providers.
    Whether a given connection is actually SDK-fed is decided per row inside the
    query, by its OAuth tokens being NULL.
    """
    providers = []
    for provider in ProviderName:
        try:
            caps = ProviderFactory().get_provider(provider.value).capabilities
        except ValueError:
            continue
        if caps.client_sdk:
            providers.append(provider.value)
    return providers


@shared_task
def revoke_stale_connections() -> dict:
    """Revoke SDK connections that have stopped delivering data.

    SDK providers cannot report sign-out, HealthKit permission revocation or app
    deletion, so inactivity is the only available signal. Without this an abandoned
    connection stays ``active`` with a plausible-looking ``last_synced_at`` forever
    and is indistinguishable from a healthy one.
    """
    threshold_days = settings.stale_connection_days

    with SessionLocal() as db:
        revoked = user_connection_service.revoke_stale_sdk_connections(db, _client_sdk_providers(), threshold_days)
        result = [{"user_id": str(c.user_id), "provider": c.provider} for c in revoked]

    log_structured(
        logger,
        "info",
        f"Stale connection sweep revoked {len(result)} connection(s)",
        action="stale_connection_sweep_complete",
        threshold_days=threshold_days,
        revoked_count=len(result),
    )

    return {"threshold_days": threshold_days, "revoked_count": len(result), "revoked": result}
