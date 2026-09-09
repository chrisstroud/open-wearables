"""Fail closed unless every release-owned worker is observable.

The release operator must reconcile this set with the deployment inventory.
One responding Celery worker is not proof that another worker is not writing.
"""

from celery import current_app as current_celery_app

from app.config import settings


def require_account_erasure_workers() -> tuple[str, ...]:
    names = tuple(sorted(settings.account_erasure_worker_names))
    if not names or len(names) != len(set(names)):
        raise RuntimeError("Verified account-erasure worker inventory is unavailable")
    expected = set(names)
    inspector = current_celery_app.control.inspect(timeout=2)
    ping = inspector.ping()
    if not isinstance(ping, dict) or set(ping) != expected or any(value != {"ok": "pong"} for value in ping.values()):
        raise RuntimeError("Verified account-erasure worker set is incomplete")
    for collection in ("active", "reserved", "scheduled"):
        result = getattr(inspector, collection)()
        if (
            not isinstance(result, dict)
            or set(result) != expected
            or any(not isinstance(tasks, list) for tasks in result.values())
        ):
            raise RuntimeError("Verified account-erasure worker inspection is incomplete")
    return names
