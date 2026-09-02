"""WHOOP sleep identities are isolated by OW user and safe to replay."""

from datetime import datetime
from unittest.mock import MagicMock
from uuid import UUID, uuid5

from sqlalchemy.orm import Session

from app.models import DataSource, EventRecord
from app.schemas.enums import ProviderName
from app.services.providers.whoop.data_247 import Whoop247Data
from tests.factories import DataSourceFactory, EventRecordFactory, SleepDetailsFactory, UserFactory

PROVIDER_SLEEP_ID = "22222222-2222-4222-8222-222222222222"
START = datetime.fromisoformat("2026-08-20T23:00:00+00:00")
END = datetime.fromisoformat("2026-08-21T07:00:00+00:00")


def _whoop_sleep() -> dict[str, object]:
    return {
        "id": PROVIDER_SLEEP_ID,
        "user_id": 12345,
        "created_at": "2026-08-21T07:01:00.000Z",
        "updated_at": "2026-08-21T07:05:00.000Z",
        "start": START.isoformat(),
        "end": END.isoformat(),
        "zone_offset": "+00:00",
        "nap": False,
        "score_state": "SCORED",
        "score": {
            "sleep_performance_percentage": 84,
            "sleep_consistency_percentage": 79,
            "sleep_efficiency_percentage": 91,
            "respiratory_rate": 14.2,
            "stage_summary": {
                "total_in_bed_time_milli": 28_800_000,
                "total_awake_time_milli": 1_800_000,
                "total_light_sleep_time_milli": 13_200_000,
                "total_slow_wave_sleep_time_milli": 6_600_000,
                "total_rem_sleep_time_milli": 7_200_000,
            },
        },
    }


def _handler() -> Whoop247Data:
    return Whoop247Data(provider_name="whoop", api_base_url="https://example.test", oauth=MagicMock())


def _whoop_sleep_rows(db: Session) -> list[tuple[EventRecord, DataSource]]:
    return (
        db.query(EventRecord, DataSource)
        .join(DataSource, EventRecord.data_source_id == DataSource.id)
        .filter(EventRecord.external_id == PROVIDER_SLEEP_ID, DataSource.source == "whoop")
        .all()
    )


def test_same_provider_sleep_persists_for_two_users_and_replays_idempotently(db: Session) -> None:
    handler = _handler()
    users = [UserFactory(), UserFactory()]

    for user in users:
        normalized, _ = handler.normalize_sleep(_whoop_sleep(), user.id)
        handler.save_sleep_data(db, user.id, normalized)

    rows = _whoop_sleep_rows(db)
    expected_ids = {uuid5(user.id, f"whoop:sleep:{PROVIDER_SLEEP_ID}") for user in users}

    assert len(rows) == 2
    assert {source.user_id for _, source in rows} == {user.id for user in users}
    assert {record.id for record, _ in rows} == expected_ids
    assert {record.external_id for record, _ in rows} == {PROVIDER_SLEEP_ID}

    normalized, _ = handler.normalize_sleep(_whoop_sleep(), users[0].id)
    handler.save_sleep_data(db, users[0].id, normalized)

    replayed_rows = _whoop_sleep_rows(db)
    assert len(replayed_rows) == 2
    assert {record.id for record, _ in replayed_rows} == expected_ids


def test_legacy_primary_account_sleep_id_is_retained_on_replay(db: Session) -> None:
    user = UserFactory()
    source = DataSourceFactory(
        user=user,
        provider=ProviderName.WHOOP,
        source="whoop",
        device_model=None,
    )
    legacy_id = UUID(PROVIDER_SLEEP_ID)
    legacy_record = EventRecordFactory(
        data_source=source,
        id=legacy_id,
        external_id=PROVIDER_SLEEP_ID,
        category="sleep",
        type="sleep_session",
        source_name="Whoop",
        start_datetime=START,
        end_datetime=END,
        duration_seconds=int((END - START).total_seconds()),
        zone_offset="+00:00",
    )
    SleepDetailsFactory(event_record=legacy_record)

    handler = _handler()
    normalized, _ = handler.normalize_sleep(_whoop_sleep(), user.id)
    assert normalized["id"] != legacy_id

    handler.save_sleep_data(db, user.id, normalized)

    rows = _whoop_sleep_rows(db)
    assert len(rows) == 1
    assert rows[0][0].id == legacy_id
    assert rows[0][0].external_id == PROVIDER_SLEEP_ID
