"""Regression tests for the Apple walking-metric normalization script."""

import importlib.util
from contextlib import nullcontext
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any
from uuid import UUID

import pytest
from sqlalchemy.sql.elements import TextClause

_SCRIPT_PATH = (
    Path(__file__).resolve().parents[2] / "scripts" / "data_migrations" / "normalize_apple_walking_metrics.py"
)
_RECORD_ID = UUID("8ba15473-1e13-4a97-afae-1aa254b9ee9e")
_RECORDED_AT = datetime(2026, 8, 24, 13, 47, 19, tzinfo=timezone.utc)


def _load_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location("normalize_apple_walking_metrics", _SCRIPT_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _Result:
    def __init__(self, *, scalar_value: int | None = None, rowcount: int = 0) -> None:
        self.scalar_value = scalar_value
        self.rowcount = rowcount

    def scalar(self) -> int | None:
        return self.scalar_value

    def fetchall(self) -> list[SimpleNamespace]:
        return [
            SimpleNamespace(
                id=_RECORD_ID,
                provider="apple",
                value=Decimal("0.314159265"),
                corrected=Decimal("31.4159265"),
                recorded_at=_RECORDED_AT,
            )
        ]


class _Session:
    def __init__(self, counts: dict[str, int]) -> None:
        self.counts = counts
        self.executed_sql: list[str] = []
        self.commit_count = 0

    def execute(self, statement: TextClause, params: dict[str, Any]) -> _Result:
        sql = str(statement)
        self.executed_sql.append(sql)
        code = params["code"]
        if "SELECT COUNT(*)" in sql:
            return _Result(scalar_value=self.counts.get(code, 0))
        if "UPDATE data_point_series" in sql:
            rowcount = self.counts.get(code, 0)
            self.counts[code] = 0
            return _Result(rowcount=rowcount)
        return _Result()

    def commit(self) -> None:
        self.commit_count += 1


def _run_with_session(module: ModuleType, session: _Session, monkeypatch: pytest.MonkeyPatch, *, dry_run: bool) -> None:
    monkeypatch.setattr(module, "SessionLocal", lambda: nullcontext(session))
    module.main(dry_run=dry_run)


def test_dry_run_logs_counts_without_record_data(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _load_module()
    session = _Session({"walking_double_support_percentage": 1})

    _run_with_session(module, session, monkeypatch, dry_run=True)

    output = capsys.readouterr().out
    assert "walking_double_support_percentage" in output
    assert "1 row(s) to fix" in output
    assert "Dry run — no changes made." in output
    assert str(_RECORD_ID) not in output
    assert "apple" not in output.lower()
    assert "0.314159265" not in output
    assert "31.4159265" not in output
    assert _RECORDED_AT.isoformat() not in output
    assert all("SELECT COUNT(*)" in sql for sql in session.executed_sql)
    assert session.commit_count == 0


def test_normalization_queries_remain_scoped_and_idempotent(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    module = _load_module()
    session = _Session(
        {
            "walking_asymmetry_percentage": 1,
            "walking_step_length": 1,
        }
    )

    _run_with_session(module, session, monkeypatch, dry_run=False)
    first_output = capsys.readouterr().out

    update_sql = [sql for sql in session.executed_sql if "UPDATE data_point_series" in sql]
    assert len(update_sql) == 2
    assert all("SET value = dps.value * 100" in sql for sql in update_sql)
    assert all("ds.provider = 'apple'" in sql for sql in update_sql)
    assert any("dps.value < 1" in sql for sql in update_sql)
    assert any("dps.value < 10" in sql for sql in update_sql)
    assert first_output.count("Updated 1 row(s).") == 2
    assert session.commit_count == 1

    session.executed_sql.clear()
    _run_with_session(module, session, monkeypatch, dry_run=False)
    second_output = capsys.readouterr().out

    assert "No affected rows across all metrics — nothing to do." in second_output
    assert "Updated" not in second_output
    assert all("SELECT COUNT(*)" in sql for sql in session.executed_sql)
    assert session.commit_count == 1
