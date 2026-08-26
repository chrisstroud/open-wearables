import json
import logging
from typing import Any
from unittest.mock import patch
from uuid import UUID

import pytest
from sqlalchemy.orm import Session

from app.models import DataPointSeries, EventRecord, SDKSleepInbox
from app.services.apple.healthkit.import_service import ImportService
from app.services.sdk_sleep_inbox_service import sdk_sleep_inbox_service
from tests.factories import UserFactory

ENVELOPE: dict[str, Any] = {
    "provider": "apple",
    "sdkVersion": "1.0.0",
    "syncTimestamp": "2026-08-25T12:00:00Z",
}
STEP_TYPE = "HKQuantityTypeIdentifierStepCount"


@pytest.fixture
def import_service() -> ImportService:
    return ImportService(log=logging.getLogger("test.sdk-receipt-policy"))


def metric_record(external_id: str) -> dict[str, Any]:
    return {
        "id": external_id,
        "type": STEP_TYPE,
        "unit": "count",
        "value": 42,
        "startDate": "2026-08-24T10:00:00Z",
        "endDate": "2026-08-24T10:01:00Z",
        "source": {"name": "iPhone", "bundleIdentifier": "com.apple.health"},
    }


def test_envelope_validation_never_logs_or_captures_health_input_value(
    db: Session,
    import_service: ImportService,
    caplog: pytest.LogCaptureFixture,
) -> None:
    user = UserFactory()
    sentinel = "PRIVATE_GLUCOSE_VALUE_987654321"

    with (
        patch("app.utils.sentry_helpers.sentry_sdk.capture_exception") as capture_exception,
        patch("app.utils.sentry_helpers.sentry_sdk.push_scope") as push_scope,
        caplog.at_level(logging.WARNING, logger="test.sdk-receipt-policy"),
    ):
        response = import_service.import_data_from_request(
            db,
            json.dumps({**ENVELOPE, "data": sentinel}),
            "application/json",
            str(user.id),
            batch_id="12121212-1212-1212-1212-121212121212",
            require_terminal_receipt=True,
        )

    assert response.status_code == 400
    capture_exception.assert_called_once()
    captured = capture_exception.call_args.args[0]
    assert isinstance(captured, ValueError)
    assert sentinel not in str(captured)
    assert sentinel not in repr(push_scope.mock_calls)
    assert sentinel not in repr([(record.getMessage(), record.__dict__) for record in caplog.records])
    assert all(not record.exc_info for record in caplog.records)


def workout_record(external_id: str | None) -> dict[str, Any]:
    return {
        "id": external_id,
        "type": "running",
        "startDate": "2026-08-24T10:00:00Z",
        "endDate": "2026-08-24T11:00:00Z",
        "values": [{"type": "duration", "value": 3600, "unit": "s"}],
    }


@pytest.mark.parametrize(
    "object_type",
    [
        STEP_TYPE,
        "HKWorkoutTypeIdentifier",
        "HKCategoryTypeIdentifierSleepAnalysis",
    ],
)
def test_every_deletion_family_is_typed_terminal_quarantine(
    db: Session,
    import_service: ImportService,
    object_type: str,
) -> None:
    user = UserFactory()
    response = import_service.import_data_from_request(
        db,
        json.dumps(
            {
                **ENVELOPE,
                "data": {
                    "deletions": [
                        {
                            "id": "11111111-1111-1111-1111-111111111111",
                            "type": object_type,
                        }
                    ]
                },
            }
        ),
        "application/json",
        str(user.id),
        batch_id="22222222-2222-2222-2222-222222222222",
        require_terminal_receipt=True,
    )

    assert response.status_code == 409
    assert response.tombstones_received == 1
    assert response.tombstones_applied == 0
    assert response.tombstones_unresolved == 1
    assert response.tombstone_error_code == "deletion_projection_unsupported"


def test_deletion_prevents_supported_addition_from_partially_committing(
    db: Session,
    import_service: ImportService,
) -> None:
    user = UserFactory()
    metric_id = "33333333-3333-3333-3333-333333333333"
    response = import_service.import_data_from_request(
        db,
        json.dumps(
            {
                **ENVELOPE,
                "data": {
                    "records": [metric_record(metric_id)],
                    "deletions": [
                        {
                            "id": "44444444-4444-4444-4444-444444444444",
                            "type": STEP_TYPE,
                        }
                    ],
                },
            }
        ),
        "application/json",
        str(user.id),
        batch_id="55555555-5555-5555-5555-555555555555",
        require_terminal_receipt=True,
    )

    assert response.status_code == 409
    assert response.tombstone_error_code == "deletion_projection_unsupported"
    assert db.query(DataPointSeries).filter(DataPointSeries.external_id == metric_id).count() == 0


def test_invalid_deletion_is_typed_quarantine_before_addition(
    db: Session,
    import_service: ImportService,
) -> None:
    user = UserFactory()
    metric_id = "66666666-6666-6666-6666-666666666666"
    response = import_service.import_data_from_request(
        db,
        json.dumps(
            {
                **ENVELOPE,
                "data": {
                    "records": [metric_record(metric_id)],
                    "deletions": [{"id": "", "type": STEP_TYPE}],
                },
            }
        ),
        "application/json",
        str(user.id),
        batch_id="77777777-7777-7777-7777-777777777777",
        require_terminal_receipt=True,
    )

    assert response.status_code == 409
    assert response.tombstone_error_code == "invalid_tombstone"
    assert db.query(DataPointSeries).filter(DataPointSeries.external_id == metric_id).count() == 0


def test_terminal_receipt_durably_stages_supported_sleep(
    db: Session,
    import_service: ImportService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user = UserFactory()
    sleep_id = "88888888-8888-8888-8888-888888888888"
    scheduled: list[dict[str, Any]] = []
    monkeypatch.setattr(
        sdk_sleep_inbox_service,
        "schedule_projection",
        lambda **kwargs: scheduled.append(kwargs),
    )
    response = import_service.import_data_from_request(
        db,
        json.dumps(
            {
                **ENVELOPE,
                "data": {
                    "sleep": [
                        {
                            "id": sleep_id,
                            "stage": "light",
                            "startDate": "2026-08-24T22:00:00Z",
                            "endDate": "2026-08-24T23:00:00Z",
                        }
                    ]
                },
            }
        ),
        "application/json",
        str(user.id),
        batch_id="99999999-9999-9999-9999-999999999999",
        require_terminal_receipt=True,
    )

    assert response.status_code == 200
    assert response.dropped_count == 0
    assert response.sleep_saved == 1
    inbox = db.query(SDKSleepInbox).filter(SDKSleepInbox.external_id == sleep_id).one()
    assert inbox.status == "staged"
    assert inbox.batch_ids == [UUID("99999999-9999-9999-9999-999999999999")]
    assert inbox.payload["stage"] == "light"
    assert db.query(EventRecord).filter(EventRecord.external_id == sleep_id).count() == 0
    assert scheduled == [{"user_id": user.id, "provider": "apple"}]


@pytest.mark.parametrize(
    ("record_patch", "error_code"),
    [
        ({"stage": "futureSleepStage"}, "sleep_stage_unsupported"),
        ({"id": None}, "sleep_source_id_required"),
    ],
)
def test_sleep_without_durable_projection_identity_is_quarantined(
    db: Session,
    import_service: ImportService,
    record_patch: dict[str, Any],
    error_code: str,
) -> None:
    user = UserFactory()
    sleep = {
        "id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
        "stage": "light",
        "startDate": "2026-08-24T22:00:00Z",
        "endDate": "2026-08-24T23:00:00Z",
        **record_patch,
    }
    response = import_service.import_data_from_request(
        db,
        json.dumps({**ENVELOPE, "data": {"sleep": [sleep]}}),
        "application/json",
        str(user.id),
        batch_id="bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
        require_terminal_receipt=True,
    )

    assert response.status_code == 409
    assert response.processing_error_code == error_code
    assert db.query(SDKSleepInbox).filter(SDKSleepInbox.user_id == user.id).count() == 0


def test_terminal_receipt_quarantines_unmapped_metric_without_write(
    db: Session,
    import_service: ImportService,
) -> None:
    user = UserFactory()
    external_id = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    unsupported = metric_record(external_id)
    unsupported["type"] = "HKQuantityTypeIdentifierFutureMetric"
    response = import_service.import_data_from_request(
        db,
        json.dumps({**ENVELOPE, "data": {"records": [unsupported]}}),
        "application/json",
        str(user.id),
        batch_id="dddddddd-dddd-dddd-dddd-dddddddddddd",
        require_terminal_receipt=True,
    )

    assert response.status_code == 409
    assert response.processing_error_code == "unsupported_metric_type"
    assert response.dropped_count == 1
    assert db.query(DataPointSeries).filter(DataPointSeries.external_id == external_id).count() == 0


def test_uuid_distinct_same_time_metrics_are_both_durably_queryable(
    db: Session,
    import_service: ImportService,
) -> None:
    user = UserFactory()
    first_id = "13131313-1313-1313-1313-131313131313"
    second_id = "14141414-1414-1414-1414-141414141414"

    response = import_service.import_data_from_request(
        db,
        json.dumps(
            {
                **ENVELOPE,
                "data": {"records": [metric_record(first_id), metric_record(second_id)]},
            }
        ),
        "application/json",
        str(user.id),
        batch_id="15151515-1515-1515-1515-151515151515",
        require_terminal_receipt=True,
    )

    assert response.status_code == 200
    assert response.dropped_count == 0
    assert response.records_saved == 2
    stored = db.query(DataPointSeries).filter(DataPointSeries.external_id.in_([first_id, second_id])).all()
    assert {sample.external_id for sample in stored} == {first_id, second_id}


def test_exact_uuid_replay_and_later_correction_update_one_retained_sample(
    db: Session,
    import_service: ImportService,
) -> None:
    user = UserFactory()
    external_id = "16161616-1616-1616-1616-161616161616"
    original = metric_record(external_id)

    first = import_service.import_data_from_request(
        db,
        json.dumps({**ENVELOPE, "data": {"records": [original]}}),
        "application/json",
        str(user.id),
        batch_id="17171717-1717-1717-1717-171717171717",
        require_terminal_receipt=True,
    )
    stored_first = db.query(DataPointSeries).filter(DataPointSeries.external_id == external_id).one()
    stored_id = stored_first.id

    replay = import_service.import_data_from_request(
        db,
        json.dumps({**ENVELOPE, "data": {"records": [original]}}),
        "application/json",
        str(user.id),
        batch_id="18181818-1818-1818-1818-181818181818",
        require_terminal_receipt=True,
    )
    correction = {
        **original,
        "value": 84,
        "startDate": "2026-08-24T10:02:00Z",
        "endDate": "2026-08-24T10:03:00Z",
    }
    corrected = import_service.import_data_from_request(
        db,
        json.dumps({**ENVELOPE, "data": {"records": [correction]}}),
        "application/json",
        str(user.id),
        batch_id="19191919-1919-1919-1919-191919191919",
        require_terminal_receipt=True,
    )

    db.expire_all()
    retained = db.query(DataPointSeries).filter(DataPointSeries.external_id == external_id).one()
    assert first.status_code == replay.status_code == corrected.status_code == 200
    assert replay.records_saved == corrected.records_saved == 1
    assert retained.id == stored_id
    assert retained.value == 84
    assert retained.recorded_at.isoformat().startswith("2026-08-24T10:02:00")


def test_contradictory_same_uuid_payloads_in_one_batch_fail_closed(
    db: Session,
    import_service: ImportService,
) -> None:
    user = UserFactory()
    external_id = "20202020-2020-2020-2020-202020202020"
    first = metric_record(external_id)
    contradictory = {**first, "value": 43}

    response = import_service.import_data_from_request(
        db,
        json.dumps({**ENVELOPE, "data": {"records": [first, contradictory]}}),
        "application/json",
        str(user.id),
        batch_id="21212121-2121-2121-2121-212121212121",
        require_terminal_receipt=True,
    )

    assert response.status_code == 409
    assert response.processing_error_code == "metric_source_payload_conflict"
    assert response.dropped_count == 2
    assert db.query(DataPointSeries).filter(DataPointSeries.external_id == external_id).count() == 0


@pytest.mark.parametrize(
    ("data", "error_code"),
    [
        ({"records": [{**metric_record("temporary"), "id": None}]}, "metric_source_id_required"),
        ({"workouts": [workout_record(None)]}, "workout_source_id_required"),
        (
            {
                "workouts": [
                    {
                        **workout_record("abababab-abab-abab-abab-abababababab"),
                        "values": [{"type": "lapLength", "value": 400, "unit": "m"}],
                    }
                ]
            },
            "unsupported_workout_statistic_type",
        ),
    ],
)
def test_terminal_receipt_requires_replay_safe_source_identity_and_supported_fields(
    db: Session,
    import_service: ImportService,
    data: dict[str, Any],
    error_code: str,
) -> None:
    user = UserFactory()
    response = import_service.import_data_from_request(
        db,
        json.dumps({**ENVELOPE, "data": data}),
        "application/json",
        str(user.id),
        batch_id="acacacac-acac-acac-acac-acacacacacac",
        require_terminal_receipt=True,
    )

    assert response.status_code == 409
    assert response.processing_error_code == error_code
    assert response.dropped_count == 1
    assert db.query(DataPointSeries).count() == 0
    assert db.query(EventRecord).count() == 0


def test_failed_import_rolls_back_pending_addition_before_receipt_failure(
    db: Session,
    import_service: ImportService,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    user = UserFactory()
    external_id = "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
    original_bulk_create = import_service.timeseries_service.bulk_create_samples

    def fail_after_addition(*args: Any, **kwargs: Any) -> None:
        original_bulk_create(*args, **kwargs)
        raise RuntimeError("forced post-addition failure")

    monkeypatch.setattr(import_service.timeseries_service, "bulk_create_samples", fail_after_addition)
    response = import_service.import_data_from_request(
        db,
        json.dumps({**ENVELOPE, "data": {"records": [metric_record(external_id)]}}),
        "application/json",
        str(user.id),
        batch_id="ffffffff-ffff-ffff-ffff-ffffffffffff",
        require_terminal_receipt=True,
    )

    assert response.status_code == 400
    db.expire_all()
    assert db.query(DataPointSeries).filter(DataPointSeries.external_id == external_id).count() == 0
