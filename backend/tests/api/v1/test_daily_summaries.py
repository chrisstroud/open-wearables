from datetime import date
from uuid import uuid4

from sqlalchemy.orm import Session
from starlette.testclient import TestClient

from app.schemas.model_crud.credentials.sdk_client_installation import SDKClientRegistration
from app.schemas.providers.mobile_sdk import AppleHealthSleepSummary, AppleHealthWorkoutSummary, DailySummary
from app.services.sdk_batch_receipt_service import sdk_batch_receipt_service
from app.services.sdk_client_installation_service import sdk_client_installation_service
from tests.factories import ApiKeyFactory, UserFactory
from tests.services.test_apple_health_daily_summary_service import (
    accept,
    digest,
    sleep_payload,
    summary_payload,
    workout_payload,
)
from tests.utils import api_key_headers


def test_daily_summary_endpoints_return_exact_dashboard_contracts(
    client: TestClient,
    db: Session,
    api_v1_prefix: str,
) -> None:
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
    accept(
        db,
        user,
        installation,
        batch_id,
        [
            DailySummary.model_validate(summary_payload()),
            AppleHealthSleepSummary.model_validate(sleep_payload()),
            AppleHealthWorkoutSummary.model_validate(workout_payload()),
        ],
    )
    api_key = ApiKeyFactory()
    headers = api_key_headers(api_key.id)
    params = {
        "start_date": date(2026, 8, 1).isoformat(),
        "end_date": date(2026, 9, 2).isoformat(),
    }

    metric_response = client.get(
        f"{api_v1_prefix}/users/{user.id}/daily-summaries",
        params={**params, "types": "resting_heart_rate"},
        headers=headers,
    )
    sleep_response = client.get(
        f"{api_v1_prefix}/users/{user.id}/daily-summaries/sleep",
        params=params,
        headers=headers,
    )
    workout_response = client.get(
        f"{api_v1_prefix}/users/{user.id}/daily-summaries/workouts",
        params={**params, "activity_types": "tennis"},
        headers=headers,
    )

    assert metric_response.status_code == 200
    metric = metric_response.json()
    assert metric["pagination"]["total_count"] == 1
    assert metric["data"][0]["schema_version"] == "apple-health-daily-summary.v1"
    assert metric["data"][0]["series_type"] == "resting_heart_rate"
    assert metric["data"][0]["statistics"][0]["value"] == 51.0
    assert metric["data"][0]["contributors"][0]["provider_id"] == "apple"

    assert sleep_response.status_code == 200
    assert sleep_response.json()["data"][0]["schema_version"] == "apple-health-sleep-summary.v1"
    assert sleep_response.json()["data"][0]["durations"][0]["seconds"] == 26760

    assert workout_response.status_code == 200
    assert workout_response.json()["data"][0]["schema_version"] == "apple-health-workout-summary.v1"
    assert workout_response.json()["data"][0]["activity_type"] == "tennis"
