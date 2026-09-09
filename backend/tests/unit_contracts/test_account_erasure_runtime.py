from unittest.mock import Mock

import pytest

from app.config import settings
from app.services.account_erasure_runtime import require_account_erasure_workers


@pytest.fixture
def inspector(monkeypatch: pytest.MonkeyPatch) -> Mock:
    monkeypatch.setattr(settings, "account_erasure_worker_names", ["worker-a", "worker-b"])
    inspector = Mock()
    inspector.ping.return_value = {name: {"ok": "pong"} for name in settings.account_erasure_worker_names}
    for name in ("active", "reserved", "scheduled"):
        getattr(inspector, name).return_value = {worker: [] for worker in settings.account_erasure_worker_names}
    monkeypatch.setattr(
        "app.services.account_erasure_runtime.current_celery_app.control.inspect", Mock(return_value=inspector)
    )
    return inspector


def test_requires_complete_configured_worker_set(inspector: Mock) -> None:
    assert require_account_erasure_workers() == ("worker-a", "worker-b")


@pytest.mark.parametrize("names", [[], ["worker-a", "worker-a"]])
def test_missing_or_duplicate_deployment_inventory_fails(
    inspector: Mock, monkeypatch: pytest.MonkeyPatch, names: list
) -> None:
    monkeypatch.setattr(settings, "account_erasure_worker_names", names)
    with pytest.raises(RuntimeError):
        require_account_erasure_workers()
    inspector.ping.assert_not_called()


@pytest.mark.parametrize("collection", ["ping", "active", "reserved", "scheduled"])
def test_missing_worker_response_is_not_proof(inspector: Mock, collection: str) -> None:
    getattr(inspector, collection).return_value.pop("worker-b")
    with pytest.raises(RuntimeError):
        require_account_erasure_workers()


def test_unreviewed_extra_worker_fails(inspector: Mock) -> None:
    inspector.ping.return_value["worker-c"] = {"ok": "pong"}
    with pytest.raises(RuntimeError):
        require_account_erasure_workers()
