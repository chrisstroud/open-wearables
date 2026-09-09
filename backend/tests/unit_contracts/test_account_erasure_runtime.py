from unittest.mock import Mock
from uuid import UUID

import pytest

from app.config import settings
from app.services.account_erasure_runtime import require_account_erasure_workers
from app.services.sdk_source_reset_external import SDKSourceResetExternalPlanes


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


TARGET = UUID("22222222-2222-4222-8222-222222222222")


@pytest.mark.parametrize("collection", ["active", "reserved", "scheduled"])
def test_partial_task_scan_cannot_be_certified_by_a_later_complete_probe(
    inspector: Mock, monkeypatch: pytest.MonkeyPatch, collection: str
) -> None:
    partial = Mock()
    partial.ping.return_value = inspector.ping.return_value
    for name in ("active", "reserved", "scheduled"):
        getattr(partial, name).return_value = {"worker-a": [], "worker-b": []}
    getattr(partial, collection).return_value.pop("worker-b")
    getattr(inspector, collection).return_value["worker-b"] = [
        {"id": "synthetic-target-task", "kwargs": {"user_id": str(TARGET)}}
    ]
    monkeypatch.setattr(
        "app.services.sdk_source_reset_external.current_celery_app.control.inspect",
        Mock(side_effect=[partial, inspector]),
    )
    tasks, blockers = SDKSourceResetExternalPlanes._task_inventory(TARGET)
    assert tasks == ()
    assert blockers == ("open-wearables.queued-tasks.worker-inspection-incomplete",)
    assert require_account_erasure_workers() == ("worker-a", "worker-b")


@pytest.mark.parametrize("collection", ["active", "reserved", "scheduled"])
def test_complete_task_scan_retains_target_work_from_every_worker(inspector: Mock, collection: str) -> None:
    getattr(inspector, collection).return_value["worker-b"] = [
        {"id": "synthetic-target-task", "kwargs": {"user_id": str(TARGET)}}
    ]
    assert SDKSourceResetExternalPlanes._task_inventory(TARGET) == (("synthetic-target-task",), ())


@pytest.mark.parametrize("fault", ["missing-ping-worker", "extra-ping-worker", "duplicate-config", "missing-config"])
def test_task_scan_requires_the_deployment_worker_inventory(
    inspector: Mock, monkeypatch: pytest.MonkeyPatch, fault: str
) -> None:
    monkeypatch.setattr(settings, "account_erasure_enabled", True)
    if fault == "missing-ping-worker":
        inspector.ping.return_value.pop("worker-b")
    elif fault == "extra-ping-worker":
        inspector.ping.return_value["worker-c"] = {"ok": "pong"}
    else:
        monkeypatch.setattr(
            settings, "account_erasure_worker_names", [] if fault == "missing-config" else ["worker-a"] * 2
        )
    assert SDKSourceResetExternalPlanes._task_inventory(TARGET) == (
        (),
        ("open-wearables.queued-tasks.worker-inspection-incomplete",),
    )


@pytest.mark.parametrize("collection", ["active", "reserved", "scheduled"])
@pytest.mark.parametrize("value", [None, {}, "not-a-list"])
def test_task_scan_rejects_malformed_worker_task_collections(inspector: Mock, collection: str, value: object) -> None:
    getattr(inspector, collection).return_value["worker-b"] = value
    assert SDKSourceResetExternalPlanes._task_inventory(TARGET) == (
        (),
        ("open-wearables.queued-tasks.worker-inspection-incomplete",),
    )
