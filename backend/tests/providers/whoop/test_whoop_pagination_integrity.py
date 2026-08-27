"""WHOOP collection reads fail closed when pagination is interrupted."""

from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4

import pytest

from app.config import settings
from app.services.providers.whoop.data_247 import Whoop247Data
from app.services.providers.whoop.workouts import WhoopWorkouts

START = datetime(2020, 1, 1, tzinfo=timezone.utc)
END = datetime(2026, 1, 1, tzinfo=timezone.utc)


def _workout_record() -> dict[str, object]:
    return {
        "id": "22222222-2222-4222-8222-222222222222",
        "user_id": 12345,
        "created_at": "2025-01-01T01:01:00Z",
        "updated_at": "2025-01-01T01:02:00Z",
        "start": "2025-01-01T00:00:00Z",
        "end": "2025-01-01T01:00:00Z",
        "timezone_offset": "+00:00",
        "sport_name": "running",
        "score_state": "SCORED",
        "score": {"strain": 8.2},
    }


def _data_247() -> Whoop247Data:
    return Whoop247Data(
        provider_name="whoop",
        api_base_url="https://example.test",
        oauth=MagicMock(),
    )


def _workouts() -> WhoopWorkouts:
    return WhoopWorkouts(
        workout_repo=MagicMock(),
        connection_repo=MagicMock(),
        provider_name="whoop",
        api_base_url="https://example.test",
        oauth=MagicMock(),
    )


def test_default_oauth_scope_covers_every_supported_whoop_read() -> None:
    assert set(settings.whoop_default_scope.split()) >= {
        "offline",
        "read:profile",
        "read:body_measurement",
        "read:cycles",
        "read:sleep",
        "read:recovery",
        "read:workout",
    }


@pytest.mark.parametrize("method_name", ["get_sleep_data", "get_recovery_data"])
def test_data_247_pagination_error_never_returns_partial_collection(
    monkeypatch: pytest.MonkeyPatch,
    method_name: str,
) -> None:
    handler = _data_247()
    request = MagicMock(
        side_effect=[
            {"records": [{"id": "first-page"}], "next_token": "page-two"},
            RuntimeError("second page unavailable"),
        ]
    )
    monkeypatch.setattr(handler, "_make_api_request", request)

    with pytest.raises(RuntimeError, match="second page unavailable"):
        getattr(handler, method_name)(MagicMock(), uuid4(), START, END)

    assert request.call_count == 2
    assert request.call_args_list[1].kwargs["params"]["nextToken"] == "page-two"


def test_get_workouts_pagination_error_never_returns_partial_collection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = _workouts()
    request = MagicMock(
        side_effect=[
            {"records": [_workout_record()], "next_token": "page-two"},
            RuntimeError("second page unavailable"),
        ]
    )
    monkeypatch.setattr(handler, "_make_api_request", request)

    with pytest.raises(RuntimeError, match="second page unavailable"):
        handler.get_workouts(MagicMock(), uuid4(), START, END)

    assert request.call_count == 2
    assert request.call_args_list[1].kwargs["params"]["nextToken"] == "page-two"


def test_load_data_pagination_error_never_persists_partial_collection(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = _workouts()
    request = MagicMock(
        side_effect=[
            {"records": [_workout_record()], "next_token": "page-two"},
            RuntimeError("second page unavailable"),
        ]
    )
    monkeypatch.setattr(handler, "get_workouts_from_api", request)

    with pytest.raises(RuntimeError, match="second page unavailable"):
        handler.load_data(MagicMock(), uuid4(), start=START, end=END)

    assert request.call_count == 2
    assert request.call_args_list[1].kwargs["nextToken"] == "page-two"


def test_all_247_sync_propagates_a_substream_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handler = _data_247()
    sleep = MagicMock(side_effect=RuntimeError("sleep history incomplete"))
    recovery = MagicMock()
    body = MagicMock()
    monkeypatch.setattr(handler, "load_and_save_sleep", sleep)
    monkeypatch.setattr(handler, "load_and_save_recovery", recovery)
    monkeypatch.setattr(handler, "load_and_save_body_measurement", body)

    with pytest.raises(RuntimeError, match="sleep history incomplete"):
        handler.load_and_save_all(MagicMock(), uuid4(), start_time=START, end_time=END)

    recovery.assert_not_called()
    body.assert_not_called()
