"""WHOOP persistence retains the active Open Wearables connection identity."""

from datetime import datetime, timezone
from logging import getLogger
from unittest.mock import MagicMock

import pytest
from sqlalchemy.orm import Session

from app.models import DataPointSeries, DataSource, EventRecord, HealthScore
from app.schemas.auth import ConnectionStatus
from app.schemas.enums import HealthScoreCategory, ProviderName
from app.services.priority_service import PriorityService
from app.services.providers.whoop.data_247 import Whoop247Data
from app.services.providers.whoop.webhook_handler import WhoopWebhookHandler
from app.services.providers.whoop.workouts import WhoopWorkouts
from tests.factories import DataSourceFactory, HealthScoreFactory, UserConnectionFactory, UserFactory

START = datetime(2026, 8, 20, tzinfo=timezone.utc)
END = datetime(2026, 8, 21, tzinfo=timezone.utc)


def _workouts() -> WhoopWorkouts:
    return WhoopWorkouts(
        workout_repo=MagicMock(),
        connection_repo=MagicMock(),
        provider_name="whoop",
        api_base_url="https://example.test",
        oauth=MagicMock(),
    )


def _data_247() -> Whoop247Data:
    return Whoop247Data(provider_name="whoop", api_base_url="https://example.test", oauth=MagicMock())


def _workout_record() -> dict[str, object]:
    return {
        "id": "22222222-2222-4222-8222-222222222222",
        "user_id": 12345,
        "created_at": "2026-08-20T01:01:00Z",
        "updated_at": "2026-08-20T01:02:00Z",
        "start": "2026-08-20T00:00:00Z",
        "end": "2026-08-20T01:00:00Z",
        "timezone_offset": "+00:00",
        "sport_name": "running",
        "score_state": "SCORED",
        "score": {"strain": 8.2},
    }


def _recovery_record() -> dict[str, object]:
    return {
        "cycle_id": 100,
        "sleep_id": "33333333-3333-4333-8333-333333333333",
        "created_at": "2026-08-20T08:00:00Z",
        "score_state": "SCORED",
        "score": {
            "recovery_score": 74,
            "resting_heart_rate": 52,
            "hrv_rmssd_milli": 61.2,
        },
    }


def test_workout_loader_adopts_legacy_source_and_persists_connection(
    db: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user = UserFactory()
    connection = UserConnectionFactory(user=user, provider="whoop", status=ConnectionStatus.ACTIVE)
    legacy = DataSourceFactory(
        user=user,
        provider=ProviderName.WHOOP,
        source="whoop",
        device_model=None,
        user_connection_id=None,
    )
    handler = _workouts()
    monkeypatch.setattr(handler, "get_workouts_from_api", MagicMock(return_value={"records": [_workout_record()]}))

    assert (
        handler.load_data(
            db,
            user.id,
            start=START,
            end=END,
            user_connection_id=connection.id,
        )
        == 1
    )

    sources = db.query(DataSource).filter(DataSource.user_id == user.id, DataSource.provider == "whoop").all()
    assert len(sources) == 1
    assert sources[0].id == legacy.id
    assert sources[0].user_connection_id == connection.id
    event = db.query(EventRecord).filter(EventRecord.data_source_id == legacy.id).one()
    assert event.external_id == _workout_record()["id"]
    score = db.query(HealthScore).filter(HealthScore.user_id == user.id).one()
    assert score.data_source_id == legacy.id


def test_247_loader_adopts_legacy_source_and_emits_connection_in_source_api(
    db: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user = UserFactory()
    connection = UserConnectionFactory(user=user, provider="whoop", status=ConnectionStatus.ACTIVE)
    legacy = DataSourceFactory(
        user=user,
        provider=ProviderName.WHOOP,
        source="whoop",
        device_model=None,
        user_connection_id=None,
    )
    handler = _data_247()
    legacy_score = HealthScoreFactory(
        user_id=user.id,
        data_source_id=None,
        provider=ProviderName.WHOOP,
        category=HealthScoreCategory.RECOVERY,
        recorded_at=datetime(2026, 8, 20, 8, tzinfo=timezone.utc),
    )
    monkeypatch.setattr(handler, "get_sleep_data", MagicMock(return_value=[]))
    monkeypatch.setattr(handler, "get_recovery_data", MagicMock(return_value=[_recovery_record()]))
    monkeypatch.setattr(handler, "get_body_measurement", MagicMock(return_value={}))

    result = handler.load_and_save_all(
        db,
        user.id,
        start_time=START,
        end_time=END,
        user_connection_id=connection.id,
    )

    assert result["recovery_samples_synced"] == 2
    sources = db.query(DataSource).filter(DataSource.user_id == user.id, DataSource.provider == "whoop").all()
    assert len(sources) == 1
    assert sources[0].id == legacy.id
    assert sources[0].user_connection_id == connection.id
    samples = db.query(DataPointSeries).filter(DataPointSeries.data_source_id == legacy.id).all()
    assert len(samples) == 2
    score = db.query(HealthScore).filter(HealthScore.user_id == user.id).one()
    assert score.id == legacy_score.id
    assert score.data_source_id == legacy.id

    source_response = PriorityService(log=getLogger(__name__)).get_user_data_sources(db, user.id)
    assert source_response.total == 1
    assert source_response.items[0].user_connection_id == connection.id


def test_247_historical_sync_repairs_bounded_legacy_scores_without_provider_rows(
    db: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user = UserFactory()
    connection = UserConnectionFactory(user=user, provider="whoop", status=ConnectionStatus.ACTIVE)
    legacy_scores = [
        HealthScoreFactory(
            user_id=user.id,
            data_source_id=None,
            provider=ProviderName.WHOOP,
            category=category,
            recorded_at=datetime(2026, 8, 20, 8, tzinfo=timezone.utc),
        )
        for category in (HealthScoreCategory.SLEEP, HealthScoreCategory.RECOVERY)
    ]
    handler = _data_247()
    monkeypatch.setattr(handler, "get_sleep_data", MagicMock(return_value=[]))
    monkeypatch.setattr(handler, "get_recovery_data", MagicMock(return_value=[]))
    monkeypatch.setattr(handler, "get_body_measurement", MagicMock(return_value={}))

    result = handler.load_and_save_all(
        db,
        user.id,
        start_time=START,
        end_time=END,
        user_connection_id=connection.id,
    )

    assert result["health_scores_attributed"] == 2
    source = db.query(DataSource).filter(DataSource.user_id == user.id, DataSource.provider == "whoop").one()
    assert source.user_connection_id == connection.id
    assert {score.data_source_id for score in legacy_scores} == {source.id}


def test_webhook_update_propagates_exact_active_connection_identity(
    db: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user = UserFactory()
    connection = UserConnectionFactory(
        user=user,
        provider="whoop",
        provider_user_id="12345",
        status=ConnectionStatus.ACTIVE,
    )
    workouts = _workouts()
    legacy_score = HealthScoreFactory(
        user_id=user.id,
        data_source_id=None,
        provider=ProviderName.WHOOP,
        category=HealthScoreCategory.STRAIN,
        recorded_at=START,
    )
    monkeypatch.setattr(
        workouts,
        "get_workout_detail_from_api",
        MagicMock(return_value=_workout_record()),
    )
    data_247 = MagicMock()
    handler = WhoopWebhookHandler(data_247=data_247, workouts=workouts)

    result = handler.process_payload(
        db,
        {
            "user_id": 12345,
            "id": "22222222-2222-4222-8222-222222222222",
            "type": "workout.updated",
        },
        "trace-test",
    )

    assert result["status"] == "processed"
    source = db.query(DataSource).filter(DataSource.user_id == user.id).one()
    assert source.user_connection_id == connection.id
    event = db.query(EventRecord).filter(EventRecord.data_source_id == source.id).one()
    assert event.external_id == "22222222-2222-4222-8222-222222222222"
    score = db.query(HealthScore).filter(HealthScore.user_id == user.id).one()
    assert score.id == legacy_score.id
    assert score.data_source_id == source.id
