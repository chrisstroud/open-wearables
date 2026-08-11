from logging import getLogger

from celery import shared_task

from app.config import settings
from app.database import SessionLocal
from app.models import UserConnection
from app.repositories.user_connection_repository import UserConnectionRepository
from app.services.providers.factory import ProviderFactory
from app.services.user_connection_service import user_connection_service
from app.utils.sentry_helpers import log_and_capture_error
from app.utils.structured_logging import log_structured

logger = getLogger(__name__)


def _is_sdk_fed(connection: UserConnection) -> bool:
    """True when the SDK upload endpoint is this connection's only way in.

    Only then is revoking safe: the next upload reactivates it. Decided per connection,
    not per provider - Google has both an SDK and an OAuth integration under one slug,
    and no tokens is what marks the SDK-provisioned rows.
    """
    try:
        caps = ProviderFactory().get_provider(connection.provider).capabilities
    except Exception:
        return False

    return caps.client_sdk and connection.access_token is None and connection.refresh_token is None


@shared_task
def revoke_stale_connections() -> dict:
    """Revoke SDK connections that have stopped delivering data.

    SDK providers cannot report sign-out, HealthKit permission revocation or app
    deletion, so inactivity is the only available signal. Without this an abandoned
    connection stays ``active`` with a plausible-looking ``last_synced_at`` forever
    and is indistinguishable from a healthy one.
    """
    threshold_days = settings.stale_connection_days
    revoked: list[dict[str, str]] = []

    with SessionLocal() as db:
        stale = UserConnectionRepository().get_stale_active(db, threshold_days)
        candidates: list[UserConnection] = [c for c in stale if _is_sdk_fed(c)]

        log_structured(
            logger,
            "info",
            f"Found {len(candidates)} stale SDK connection(s)",
            action="stale_connection_sweep_start",
            threshold_days=threshold_days,
            stale_total=len(stale),
            stale_sdk=len(candidates),
            skipped_non_sdk=len(stale) - len(candidates),
        )

        for connection in candidates:
            try:
                if user_connection_service.revoke_as_stale(db, connection):
                    revoked.append({"user_id": str(connection.user_id), "provider": connection.provider})
            except Exception as e:
                log_and_capture_error(
                    e,
                    logger,
                    f"Failed to revoke stale connection for user {connection.user_id}: {e}",
                    extra={"user_id": str(connection.user_id), "provider": connection.provider},
                )

    log_structured(
        logger,
        "info",
        f"Revoked {len(revoked)} stale connection(s)",
        action="stale_connection_sweep_complete",
        threshold_days=threshold_days,
        revoked_count=len(revoked),
        revoked=revoked,
    )

    return {"threshold_days": threshold_days, "revoked_count": len(revoked), "revoked": revoked}
