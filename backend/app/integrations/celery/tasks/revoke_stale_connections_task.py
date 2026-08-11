from logging import getLogger

from celery import shared_task

from app.config import settings
from app.database import SessionLocal
from app.repositories.user_connection_repository import UserConnectionRepository
from app.services.user_connection_service import user_connection_service
from app.utils.sentry_helpers import log_and_capture_error
from app.utils.structured_logging import log_structured

logger = getLogger(__name__)

STALE_REASON = "stale"


@shared_task
def revoke_stale_connections() -> dict:
    """Revoke active connections that have stopped delivering data.

    SDK providers cannot report sign-out, HealthKit permission revocation or app
    deletion, so inactivity is the only available signal. Without this an abandoned
    connection stays ``active`` with a plausible-looking ``last_synced_at`` forever
    and is indistinguishable from a healthy one.
    """
    threshold_days = settings.stale_connection_days
    revoked: list[dict[str, str]] = []

    with SessionLocal() as db:
        stale = UserConnectionRepository().get_stale_active(db, threshold_days)

        log_structured(
            logger,
            "info",
            f"Found {len(stale)} stale connection(s)",
            action="stale_connection_sweep_start",
            threshold_days=threshold_days,
            stale_count=len(stale),
        )

        for connection in stale:
            try:
                # No oauth: deregistering at the provider is not our call here, the user
                # never asked to disconnect. This only records that we stopped receiving.
                user_connection_service.disconnect(
                    db,
                    connection.user_id,
                    connection.provider,
                    reason=STALE_REASON,
                )
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
