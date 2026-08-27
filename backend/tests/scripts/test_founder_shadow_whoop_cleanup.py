"""Operator-surface tests for the founder shadow WHOOP cleanup script."""

import importlib.util
import json
from contextlib import nullcontext
from pathlib import Path
from types import ModuleType, SimpleNamespace
from uuid import UUID

import pytest

_SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "founder_shadow_whoop_cleanup.py"
_TARGET = UUID("11111111-1111-4111-8111-111111111111")
_KEEPER = UUID("22222222-2222-4222-8222-222222222222")


def _load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("founder_shadow_whoop_cleanup_script", _SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Service:
    def __init__(self) -> None:
        self.calls: list[tuple[str, UUID, UUID, str | None]] = []

    def plan(self, _db: object, *, target_user_id: UUID, keeper_user_id: UUID) -> SimpleNamespace:
        self.calls.append(("plan", target_user_id, keeper_user_id, None))
        return SimpleNamespace(
            executable=True,
            public_dict=lambda: {
                "phase": "planned",
                "plan_digest_sha256": "a" * 64,
                "counts": {"whoop_data_sources": 2},
                "blockers": [],
                "executable": True,
            },
        )

    def execute(
        self,
        _db: object,
        *,
        target_user_id: UUID,
        keeper_user_id: UUID,
        expected_plan_sha256: str,
    ) -> SimpleNamespace:
        self.calls.append(("execute", target_user_id, keeper_user_id, expected_plan_sha256))
        return SimpleNamespace(verified=True, public_dict=lambda: {"verified": True, "blockers": []})

    def verify(self, _db: object, *, target_user_id: UUID, keeper_user_id: UUID) -> SimpleNamespace:
        self.calls.append(("verify", target_user_id, keeper_user_id, None))
        return SimpleNamespace(verified=True, public_dict=lambda: {"verified": True, "blockers": []})


def _configure(module: ModuleType, monkeypatch: pytest.MonkeyPatch, service: _Service) -> None:
    monkeypatch.setenv(module.TARGET_ENV, str(_TARGET))
    monkeypatch.setenv(module.KEEPER_ENV, str(_KEEPER))
    monkeypatch.setattr(module, "SessionLocal", lambda: nullcontext(object()))
    monkeypatch.setattr(module, "founder_shadow_whoop_cleanup_service", service)


def test_plan_output_is_value_minimized(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _load_module()
    service = _Service()
    _configure(module, monkeypatch, service)

    assert module.main(["plan"]) == 0

    output = capsys.readouterr().out
    payload = json.loads(output)
    assert payload["phase"] == "planned"
    assert str(_TARGET) not in output
    assert str(_KEEPER) not in output
    assert service.calls == [("plan", _TARGET, _KEEPER, None)]


def test_execute_requires_exact_confirmation_and_plan_digest(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _load_module()
    service = _Service()
    _configure(module, monkeypatch, service)

    assert module.main(["execute"]) == 2
    denied = capsys.readouterr().out
    assert "founder-shadow.explicit-confirmation-required" in denied
    assert service.calls == []

    monkeypatch.setenv(module.CONFIRM_ENV, module.CONFIRM_VALUE)
    monkeypatch.setenv(module.PLAN_ENV, "a" * 64)
    assert module.main(["execute"]) == 0

    output = capsys.readouterr().out
    assert json.loads(output)["verified"] is True
    assert str(_TARGET) not in output
    assert str(_KEEPER) not in output
    assert service.calls == [("execute", _TARGET, _KEEPER, "a" * 64)]


def test_unexpected_failure_does_not_echo_sensitive_values(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _load_module()
    service = _Service()
    _configure(module, monkeypatch, service)

    def fail(*_args: object, **_kwargs: object) -> None:
        raise RuntimeError(f"database failure for {_TARGET} with secret-token")

    monkeypatch.setattr(service, "plan", fail)

    assert module.main(["plan"]) == 2

    output = capsys.readouterr().out
    assert json.loads(output) == {
        "blockers": ["founder-shadow.unexpected-operator-failure"],
        "verified": False,
    }
    assert str(_TARGET) not in output
    assert "secret-token" not in output
