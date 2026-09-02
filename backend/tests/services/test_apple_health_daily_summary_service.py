import json
import logging
from copy import deepcopy
from datetime import date
from decimal import Decimal
from hashlib import sha256
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from sqlalchemy.orm import Session

from app.models import AppleHealthDailySummary, SDKClientInstallation, User
from app.repositories.apple_health_daily_summary_repository import (
    DailySummaryConflictError,
    apple_health_daily_summary_repository,
)
from app.schemas.model_crud.credentials.sdk_client_installation import SDKClientRegistration
from app.schemas.providers.mobile_sdk import (
    AppleHealthSleepSummary,
    AppleHealthWorkoutSummary,
    DailySummary,
    SyncRequest,
    SyncRequestData,
)
from app.schemas.providers.mobile_sdk.sync_request import calculate_revision_set_digest
from app.services.apple.healthkit.import_service import ImportService
from app.services.apple_health_daily_summary_service import apple_health_daily_summary_service
from app.services.sdk_batch_receipt_service import sdk_batch_receipt_service
from app.services.sdk_client_installation_service import sdk_client_installation_service
from tests.factories import UserFactory


def digest(label: str) -> str:
    return sha256(label.encode()).hexdigest()


def contributors() -> list[dict]:
    return [
        {
            "provider_id": "apple",
            "source_bundle_identifier": "com.apple.Health",
            "source_name": "Apple Health",
            "source_version": "18.6",
            "product_type": "iPhone17,1",
            "device_manufacturer": "Apple Inc.",
            "device_model": "iPhone",
        }
    ]


def summary_payload(
    *,
    revision: str = "revision-1",
    supersedes: str | None = None,
    value: float | Decimal = 51.0,
) -> dict:
    return {
        "schema_version": "apple-health-daily-summary.v1",
        "summary_key": digest("rhr-2026-08-31"),
        "revision_id": digest(revision),
        "supersedes_revision_id": digest(supersedes) if supersedes else None,
        "registry_version": "dashboard-fitness-ios.v1",
        "aggregation_version": "daily-summary.v1",
        "healthkit_type": "HKQuantityTypeIdentifierRestingHeartRate",
        "series_type": "resting_heart_rate",
        "local_date": "2026-08-31",
        "timezone": "America/Toronto",
        "timezone_boundary_version": "calendar-day.v1",
        "day_start_inclusive": "2026-08-31T00:00:00-04:00",
        "day_end_exclusive": "2026-09-01T00:00:00-04:00",
        "assignment_policy": "calendar-day",
        "source_scope": "healthkit-merged",
        "contributors": contributors(),
        "contributor_set_digest": digest("contributors"),
        "canonical_unit": "bpm",
        "statistics": [
            {"name": "mean", "value": value, "unit": "bpm", "observed_at": None},
            {"name": "minimum", "value": value - 1, "unit": "bpm", "observed_at": None},
            {"name": "maximum", "value": value + 1, "unit": "bpm", "observed_at": None},
        ],
        "primary_statistic": "mean",
        "sample_count": 4,
        "input_set_digest": digest(f"inputs-{revision}"),
        "coverage_status": "observed",
        "computed_at": "2026-09-01T00:05:00-04:00",
    }


def sleep_payload() -> dict:
    return {
        "schema_version": "apple-health-sleep-summary.v1",
        "summary_key": digest("sleep-2026-08-31"),
        "revision_id": digest("sleep-revision-1"),
        "supersedes_revision_id": None,
        "registry_version": "dashboard-fitness-ios.v1",
        "aggregation_version": "daily-summary.v1",
        "local_date": "2026-08-31",
        "timezone": "America/Toronto",
        "timezone_boundary_version": "wake-day.v1",
        "day_start_inclusive": "2026-08-31T00:00:00-04:00",
        "day_end_exclusive": "2026-09-01T00:00:00-04:00",
        "assignment_policy": "wake-day",
        "source_scope": "healthkit-source",
        "contributors": contributors(),
        "contributor_set_digest": digest("sleep-contributors"),
        "episode_count": 1,
        "nap_count": 0,
        "earliest_onset": "2026-08-30T23:11:00-04:00",
        "latest_wake": "2026-08-31T07:02:00-04:00",
        "durations": [
            {"name": "asleep", "seconds": 26760},
            {"name": "deep", "seconds": 5220},
            {"name": "rem", "seconds": 6120},
        ],
        "sample_count": 9,
        "input_set_digest": digest("sleep-inputs"),
        "coverage_status": "observed",
        "computed_at": "2026-09-01T00:06:00-04:00",
    }


def workout_payload() -> dict:
    return {
        "schema_version": "apple-health-workout-summary.v1",
        "event_key": digest("workout-2026-08-31-tennis"),
        "revision_id": digest("workout-revision-1"),
        "supersedes_revision_id": None,
        "registry_version": "dashboard-fitness-ios.v1",
        "aggregation_version": "daily-summary.v1",
        "local_date": "2026-08-31",
        "timezone": "America/Toronto",
        "timezone_boundary_version": "session-start-day.v1",
        "assignment_policy": "session-start-day",
        "source_scope": "healthkit-source",
        "activity_type": "tennis",
        "start": "2026-08-31T07:49:00-04:00",
        "end": "2026-08-31T08:17:00-04:00",
        "crosses_local_midnight": False,
        "duration_seconds": 1680,
        "total_energy_kcal": 241,
        "total_distance_meters": None,
        "average_heart_rate_bpm": 126,
        "maximum_heart_rate_bpm": 158,
        "contributors": contributors(),
        "contributor_set_digest": digest("workout-contributors"),
        "input_set_digest": digest("workout-inputs"),
        "coverage_status": "observed",
        "computed_at": "2026-09-01T00:07:00-04:00",
    }


def client_revision_set_digest(data: dict) -> str:
    """Independent test implementation of the iOS revision-set wire contract."""
    identities = [
        *(("daily_summary", item["summary_key"], item["revision_id"]) for item in data.get("daily_summaries", [])),
        *(
            ("sleep", item["summary_key"], item["revision_id"])
            for item in data.get("sleep", [])
            if item.get("schema_version") == "apple-health-sleep-summary.v1"
        ),
        *(
            ("workout", item["event_key"], item["revision_id"])
            for item in data.get("workouts", [])
            if item.get("schema_version") == "apple-health-workout-summary.v1"
        ),
    ]
    hasher = sha256()
    for identity in sorted(identities):
        for value in identity:
            encoded = value.encode("utf-8")
            hasher.update(len(encoded).to_bytes(8, byteorder="big", signed=False))
            hasher.update(encoded)
    return hasher.hexdigest()


def daily_summary_envelope(
    *,
    daily_summaries: list[dict] | None = None,
    sleep: list[dict] | None = None,
    workouts: list[dict] | None = None,
) -> dict:
    data = {
        "daily_summaries": daily_summaries if daily_summaries is not None else [summary_payload()],
        "sleep": sleep if sleep is not None else [sleep_payload()],
        "workouts": workouts if workouts is not None else [workout_payload()],
    }
    return {
        "schema_version": "apple-health-daily-summary.v1",
        "revision_set_digest": client_revision_set_digest(data),
        "provider": "apple",
        "sdk_version": "1.0.0",
        "sync_timestamp": "2026-09-01T04:07:00Z",
        "data": data,
    }


def authority(db: Session) -> tuple[User, SDKClientInstallation, UUID]:
    user = UserFactory()
    installation = sdk_client_installation_service.activate(
        db,
        user_id=user.id,
        registration=SDKClientRegistration(
            installation_id=uuid4(),
            bundle_id="fitness.dashboard.app",
            app_version="1.0.0",
            build_number="1",
            protocol_version=3,
        ),
    )
    batch_id = uuid4()
    sdk_batch_receipt_service.prepare_submission(
        db,
        batch_id=batch_id,
        user_id=user.id,
        installation_id=installation.id,
        installation_generation=installation.generation,
        health_evidence_generation=user.health_evidence_generation,
        provider="apple",
        payload_sha256=digest(str(batch_id)),
    )
    return user, installation, batch_id


def prepare_batch(db: Session, user: User, installation: SDKClientInstallation) -> UUID:
    batch_id = uuid4()
    sdk_batch_receipt_service.prepare_submission(
        db,
        batch_id=batch_id,
        user_id=user.id,
        installation_id=installation.id,
        installation_generation=installation.generation,
        health_evidence_generation=user.health_evidence_generation,
        provider="apple",
        payload_sha256=digest(str(batch_id)),
    )
    return batch_id


def accept(
    db: Session,
    user: User,
    installation: SDKClientInstallation,
    batch_id: UUID,
    summaries: list[DailySummary | AppleHealthSleepSummary | AppleHealthWorkoutSummary],
) -> int:
    return apple_health_daily_summary_repository.accept_batch(
        db,
        user_id=user.id,
        installation_id=installation.id,
        installation_generation=installation.generation,
        health_evidence_generation=user.health_evidence_generation,
        batch_id=batch_id,
        summaries=summaries,
    )


class TestAppleHealthDailySummaryService:
    def test_empty_revision_set_uses_sha256_of_empty_bytes(self) -> None:
        assert calculate_revision_set_digest(SyncRequestData()) == sha256(b"").hexdigest()

    def test_legacy_payload_cannot_declare_the_empty_revision_set_digest(self) -> None:
        with pytest.raises(ValueError, match="requires the daily-summary envelope schema"):
            SyncRequest.model_validate(
                {
                    "revision_set_digest": sha256(b"").hexdigest(),
                    "provider": "apple",
                    "sdkVersion": "1.0.0",
                    "syncTimestamp": "2026-09-01T04:07:00Z",
                    "data": {},
                }
            )

    def test_schema_requires_exact_dst_aware_local_midnight_geometry(self) -> None:
        wrong_day = summary_payload()
        wrong_day["day_end_exclusive"] = "2026-09-02T00:00:00-04:00"
        with pytest.raises(ValueError, match="exact next local midnight"):
            DailySummary.model_validate(wrong_day)

        shifted_day = summary_payload()
        shifted_day["day_start_inclusive"] = "2026-08-31T12:00:00-04:00"
        shifted_day["day_end_exclusive"] = "2026-09-01T12:00:00-04:00"
        with pytest.raises(ValueError, match="exact local midnight"):
            DailySummary.model_validate(shifted_day)

        dst_day = summary_payload()
        dst_day.update(
            {
                "local_date": "2026-03-08",
                "day_start_inclusive": "2026-03-08T00:00:00-05:00",
                "day_end_exclusive": "2026-03-09T00:00:00-04:00",
            }
        )
        assert DailySummary.model_validate(dst_day).local_date == date(2026, 3, 8)

        fall_back_day = summary_payload()
        fall_back_day.update(
            {
                "local_date": "2026-11-01",
                "day_start_inclusive": "2026-11-01T00:00:00-04:00",
                "day_end_exclusive": "2026-11-02T00:00:00-05:00",
            }
        )
        assert DailySummary.model_validate(fall_back_day).local_date == date(2026, 11, 1)

    def test_schema_requires_observed_provenance_and_canonical_units(self) -> None:
        no_metric_provenance = summary_payload()
        no_metric_provenance["contributors"] = []
        with pytest.raises(ValueError, match="at least one contributor"):
            DailySummary.model_validate(no_metric_provenance)

        no_sleep_provenance = sleep_payload()
        no_sleep_provenance["contributors"] = []
        with pytest.raises(ValueError, match="at least one contributor"):
            AppleHealthSleepSummary.model_validate(no_sleep_provenance)

        wrong_unit = summary_payload()
        wrong_unit["statistics"][0]["unit"] = "count"
        with pytest.raises(ValueError, match="must match canonical_unit"):
            DailySummary.model_validate(wrong_unit)

        duplicate = summary_payload()
        duplicate["statistics"].append(duplicate["statistics"][0])
        with pytest.raises(ValueError, match="must be unique"):
            DailySummary.model_validate(duplicate)

    def test_precise_decimal_is_retained_durably_and_serialized_as_json_number(self, db: Session) -> None:
        user, installation, batch_id = authority(db)
        precise = Decimal("51.1234567890123456789012345678901234")
        summary = DailySummary.model_validate(summary_payload(value=precise))

        assert accept(db, user, installation, batch_id, [summary]) == 1
        row = db.query(AppleHealthDailySummary).one()
        assert row.payload["statistics"][0]["value"] == str(precise)

        page = apple_health_daily_summary_service.list_metrics(
            db,
            user_id=user.id,
            start_date=date(2026, 8, 1),
            end_date=date(2026, 9, 2),
            series_types=["resting_heart_rate"],
            cursor=None,
            limit=100,
        )
        assert page.data[0].statistics[0].value == precise
        wire_value = json.loads(page.model_dump_json())["data"][0]["statistics"][0]["value"]
        assert isinstance(wire_value, int | float)
        assert wire_value == float(precise)

        with pytest.raises(ValueError, match="finite JSON number"):
            DailySummary.model_validate(summary_payload(value=Decimal("1e10000")))

    def test_exact_replay_is_idempotent_and_next_revision_supersedes_current(self, db: Session) -> None:
        user, installation, first_batch = authority(db)
        first = DailySummary.model_validate(summary_payload())
        assert accept(db, user, installation, first_batch, [first]) == 1
        assert accept(db, user, installation, first_batch, [first]) == 1
        assert db.query(AppleHealthDailySummary).count() == 1

        next_batch = uuid4()
        sdk_batch_receipt_service.prepare_submission(
            db,
            batch_id=next_batch,
            user_id=user.id,
            installation_id=installation.id,
            installation_generation=installation.generation,
            health_evidence_generation=user.health_evidence_generation,
            provider="apple",
            payload_sha256=digest(str(next_batch)),
        )
        second = DailySummary.model_validate(
            summary_payload(revision="revision-2", supersedes="revision-1", value=49),
        )
        assert accept(db, user, installation, next_batch, [second]) == 1
        rows = db.query(AppleHealthDailySummary).order_by(AppleHealthDailySummary.created_at).all()
        assert len(rows) == 2
        assert sum(row.is_current for row in rows) == 1
        assert next(row for row in rows if row.is_current).revision_id == digest("revision-2")

        page = apple_health_daily_summary_service.list_metrics(
            db,
            user_id=user.id,
            start_date=date(2026, 8, 1),
            end_date=date(2026, 9, 2),
            series_types=["resting_heart_rate"],
            cursor=None,
            limit=100,
        )
        assert page.data[0].statistics[0].value == 49

    def test_reauthorized_same_installation_generation_replays_exact_revision_only(self, db: Session) -> None:
        user, installation, first_batch = authority(db)
        first = DailySummary.model_validate(summary_payload())
        original_generation = installation.generation
        assert accept(db, user, installation, first_batch, [first]) == 1

        registration = SDKClientRegistration(
            installation_id=installation.id,
            bundle_id="fitness.dashboard.app",
            app_version="1.0.0",
            build_number="1",
            protocol_version=3,
        )
        repaired = sdk_client_installation_service.activate(db, user_id=user.id, registration=registration)
        assert repaired.generation > original_generation
        repaired_batch = prepare_batch(db, user, repaired)

        assert accept(db, user, repaired, repaired_batch, [first]) == 1
        assert db.query(AppleHealthDailySummary).count() == 1

        replacement_registration = registration.model_copy(update={"installation_id": uuid4()})
        replacement = sdk_client_installation_service.activate(
            db,
            user_id=user.id,
            registration=replacement_registration,
        )
        replacement_batch = prepare_batch(db, user, replacement)
        with pytest.raises(DailySummaryConflictError, match="revision_authority_conflict"):
            accept(db, user, replacement, replacement_batch, [first])
        assert db.query(AppleHealthDailySummary).count() == 1

        reset_user, reset_installation, reset_first_batch = authority(db)
        assert accept(db, reset_user, reset_installation, reset_first_batch, [first]) == 1
        reset_user.health_evidence_generation += 1
        db.flush()
        reset_registration = registration.model_copy(update={"installation_id": reset_installation.id})
        reset_generation = sdk_client_installation_service.activate(
            db,
            user_id=reset_user.id,
            registration=reset_registration,
        )
        reset_generation_batch = prepare_batch(db, reset_user, reset_generation)
        with pytest.raises(DailySummaryConflictError, match="revision_authority_conflict"):
            accept(db, reset_user, reset_generation, reset_generation_batch, [first])
        assert db.query(AppleHealthDailySummary).filter_by(user_id=reset_user.id).count() == 1

    def test_sleep_workout_and_numeric_revisions_share_one_atomic_protocol(self, db: Session) -> None:
        user, installation, batch_id = authority(db)
        items = [
            DailySummary.model_validate(summary_payload()),
            AppleHealthSleepSummary.model_validate(sleep_payload()),
            AppleHealthWorkoutSummary.model_validate(workout_payload()),
        ]
        assert accept(db, user, installation, batch_id, items) == 3
        assert db.query(AppleHealthDailySummary).count() == 3

        sleep_page = apple_health_daily_summary_service.list_sleep(
            db,
            user_id=user.id,
            start_date=date(2026, 8, 1),
            end_date=date(2026, 9, 2),
            cursor=None,
            limit=100,
        )
        workout_page = apple_health_daily_summary_service.list_workouts(
            db,
            user_id=user.id,
            start_date=date(2026, 8, 1),
            end_date=date(2026, 9, 2),
            activity_types=["tennis"],
            cursor=None,
            limit=100,
        )
        assert sleep_page.data[0].durations[0].seconds == 26760
        assert workout_page.data[0].activity_type == "tennis"

    def test_workout_retraction_exactly_supersedes_without_raw_observations(self, db: Session) -> None:
        user, installation, first_batch = authority(db)
        observed = AppleHealthWorkoutSummary.model_validate(workout_payload())
        assert accept(db, user, installation, first_batch, [observed]) == 1

        next_batch = uuid4()
        sdk_batch_receipt_service.prepare_submission(
            db,
            batch_id=next_batch,
            user_id=user.id,
            installation_id=installation.id,
            installation_generation=installation.generation,
            health_evidence_generation=user.health_evidence_generation,
            provider="apple",
            payload_sha256=digest(str(next_batch)),
        )
        retraction_payload = workout_payload()
        retraction_payload.update(
            {
                "revision_id": digest("workout-retraction-1"),
                "supersedes_revision_id": observed.revision_id,
                "duration_seconds": 0,
                "total_energy_kcal": None,
                "average_heart_rate_bpm": None,
                "maximum_heart_rate_bpm": None,
                "contributors": [],
                "contributor_set_digest": observed.contributor_set_digest,
                "input_set_digest": observed.input_set_digest,
                "coverage_status": "retracted",
            }
        )
        retraction = AppleHealthWorkoutSummary.model_validate(retraction_payload)
        assert accept(db, user, installation, next_batch, [retraction]) == 1

        page = apple_health_daily_summary_service.list_workouts(
            db,
            user_id=user.id,
            start_date=date(2026, 8, 1),
            end_date=date(2026, 9, 2),
            activity_types=["tennis"],
            cursor=None,
            limit=100,
        )
        assert page.data[0].coverage_status == "retracted"
        assert page.data[0].contributors == []

        invalid = workout_payload()
        invalid.update(
            {
                "revision_id": digest("workout-retraction-missing-lineage"),
                "duration_seconds": 0,
                "total_energy_kcal": None,
                "average_heart_rate_bpm": None,
                "maximum_heart_rate_bpm": None,
                "contributors": [],
                "contributor_set_digest": digest("workout-contributors"),
                "input_set_digest": digest("workout-inputs"),
                "coverage_status": "retracted",
            }
        )
        with pytest.raises(ValueError, match="must supersede an exact revision"):
            AppleHealthWorkoutSummary.model_validate(invalid)

    def test_forked_or_identity_drifting_revision_fails_closed(self, db: Session) -> None:
        user, installation, batch_id = authority(db)
        first = DailySummary.model_validate(summary_payload())
        accept(db, user, installation, batch_id, [first])

        fork = DailySummary.model_validate(summary_payload(revision="revision-2", supersedes="not-current"))
        with pytest.raises(DailySummaryConflictError, match="supersedes_mismatch"):
            accept(db, user, installation, batch_id, [fork])

        drift_payload = summary_payload(revision="revision-2", supersedes="revision-1")
        drift_payload["series_type"] = "heart_rate"
        drift = DailySummary.model_validate(drift_payload)
        with pytest.raises(DailySummaryConflictError, match="identity_drift"):
            accept(db, user, installation, batch_id, [drift])

    def test_exact_ios_envelope_accepts_all_three_families_under_one_receipt(self, db: Session) -> None:
        user, installation, batch_id = authority(db)
        db.info["health_write_authority"] = (
            user.id,
            user.health_evidence_generation,
            installation.id,
            installation.generation,
        )
        payload = daily_summary_envelope()

        with patch("app.services.apple.healthkit.import_service.handle_sleep_data") as raw_sleep_projection:
            response = ImportService(logging.getLogger("test.daily-summary")).import_data_from_request(
                db,
                json.dumps(payload),
                "application/json",
                str(user.id),
                batch_id=str(batch_id),
                require_terminal_receipt=True,
            )

        assert response.status_code == 200
        assert response.daily_summaries_saved == 3
        assert response.revision_set_digest == payload["revision_set_digest"]
        assert response.records_saved == response.sleep_saved == response.workouts_saved == 0
        assert db.query(AppleHealthDailySummary).count() == 3
        raw_sleep_projection.assert_not_called()

    def test_revision_set_digest_is_order_independent_and_matches_frozen_vector(self) -> None:
        second_summary = summary_payload(revision="revision-2")
        second_summary["summary_key"] = digest("rhr-2026-08-30")
        payload = daily_summary_envelope(daily_summaries=[summary_payload(), second_summary])
        reordered = deepcopy(payload)
        reordered["data"]["daily_summaries"].reverse()

        original = SyncRequest.model_validate(payload)
        reordered_request = SyncRequest.model_validate(reordered)

        assert payload["revision_set_digest"] == "b1bda60d06e2f0cc3bd5abb05de65f3d457ba41ec5aabd82f89650b6388aaff1"
        assert original.revision_set_digest == reordered_request.revision_set_digest

    def test_count_equal_revision_change_rejects_declared_digest(self) -> None:
        payload = daily_summary_envelope()
        changed = deepcopy(payload)
        changed["data"]["daily_summaries"][0]["revision_id"] = digest("different-revision")

        with pytest.raises(ValueError, match="must match the exact parsed summary revision set"):
            SyncRequest.model_validate(changed)

    @pytest.mark.parametrize("declared", [None, "malformed"])
    def test_daily_summary_envelope_requires_well_formed_revision_set_digest(
        self,
        declared: str | None,
    ) -> None:
        payload = daily_summary_envelope()
        if declared is None:
            payload.pop("revision_set_digest")
        else:
            payload["revision_set_digest"] = declared

        with pytest.raises(ValueError, match="revision_set_digest"):
            SyncRequest.model_validate(payload)

    def test_invalid_summary_sibling_rejects_the_whole_envelope_before_mutation(self, db: Session) -> None:
        user, installation, batch_id = authority(db)
        db.info["health_write_authority"] = (
            user.id,
            user.health_evidence_generation,
            installation.id,
            installation.generation,
        )
        invalid = summary_payload()
        invalid["statistics"].append(invalid["statistics"][0])
        payload = daily_summary_envelope(daily_summaries=[invalid])

        response = ImportService(logging.getLogger("test.daily-summary")).import_data_from_request(
            db,
            json.dumps(payload),
            "application/json",
            str(user.id),
            batch_id=str(batch_id),
            require_terminal_receipt=True,
        )

        assert response.status_code == 400
        assert db.query(AppleHealthDailySummary).count() == 0

    def test_shifted_day_is_rejected_before_any_summary_mutation(self, db: Session) -> None:
        user, installation, batch_id = authority(db)
        db.info["health_write_authority"] = (
            user.id,
            user.health_evidence_generation,
            installation.id,
            installation.generation,
        )
        shifted = summary_payload()
        shifted["day_start_inclusive"] = "2026-08-31T12:00:00-04:00"
        shifted["day_end_exclusive"] = "2026-09-01T12:00:00-04:00"
        payload = daily_summary_envelope(daily_summaries=[shifted], sleep=[], workouts=[])

        response = ImportService(logging.getLogger("test.daily-summary")).import_data_from_request(
            db,
            json.dumps(payload),
            "application/json",
            str(user.id),
            batch_id=str(batch_id),
            require_terminal_receipt=True,
        )

        assert response.status_code == 400
        assert response.daily_summaries_saved == 0
        assert db.query(AppleHealthDailySummary).count() == 0
