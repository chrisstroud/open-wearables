from uuid import uuid4

import pytest
from pydantic import ValidationError

from app.schemas.providers.mobile_sdk import DeletedObject, SleepRecord, SourceInfo, SyncWindowManifest
from app.schemas.providers.mobile_sdk.sync_request import MetricRecord, Workout


@pytest.mark.parametrize(
    ("field_name", "limit"),
    [
        ("appId", 100),
        ("name", 100),
        ("bundleIdentifier", 100),
        ("version", 50),
        ("productType", 100),
        ("deviceId", 100),
        ("deviceName", 100),
        ("deviceManufacturer", 100),
        ("deviceType", 32),
        ("deviceModel", 100),
        ("deviceHardwareVersion", 50),
        ("deviceSoftwareVersion", 50),
        ("recordingMethod", 32),
    ],
)
def test_source_descriptor_lengths_are_bounded(field_name: str, limit: int) -> None:
    accepted = SourceInfo.model_validate({field_name: "x" * limit})
    assert accepted.model_dump(by_alias=True)[field_name] == "x" * limit

    bounded = SourceInfo.model_validate({field_name: "x" * (limit + 1)})
    assert bounded.model_dump(by_alias=True)[field_name] == "x" * limit


def test_native_apple_provenance_shape_is_accepted() -> None:
    source = SourceInfo.model_validate(
        {
            "appId": "com.dexcom.stelo",
            "name": "Stelo",
            "bundleIdentifier": "com.dexcom.stelo",
            "deviceName": "Stelo",
            "deviceManufacturer": "Dexcom",
            "deviceModel": "Stelo",
            "deviceType": "cgm",
            "deviceHardwareVersion": "1",
            "deviceSoftwareVersion": "2.3.4",
            "operatingSystemVersion": {
                "majorVersion": 26,
                "minorVersion": 5,
                "patchVersion": 2,
            },
        }
    )

    assert source.bundle_identifier == "com.dexcom.stelo"
    assert source.device_model == "Stelo"
    assert source.device_type == "cgm"
    assert source.operating_system_version is not None
    assert source.operating_system_version.major_version == 26


@pytest.mark.parametrize("component", ["majorVersion", "minorVersion", "patchVersion"])
def test_operating_system_version_components_are_nonnegative_and_bounded(component: str) -> None:
    version = {"majorVersion": 1, "minorVersion": 2, "patchVersion": 3}
    version[component] = -1
    with pytest.raises(ValidationError):
        SourceInfo.model_validate({"operatingSystemVersion": version})

    version[component] = 65536
    with pytest.raises(ValidationError):
        SourceInfo.model_validate({"operatingSystemVersion": version})


@pytest.mark.parametrize(
    "record",
    [
        {
            "id": "x" * 101,
            "type": "HKQuantityTypeIdentifierStepCount",
            "startDate": "2026-08-25T00:00:00Z",
            "endDate": "2026-08-25T01:00:00Z",
            "value": 1,
            "unit": "count",
        },
        {
            "id": "x" * 101,
            "stage": "light",
            "startDate": "2026-08-25T00:00:00Z",
            "endDate": "2026-08-25T01:00:00Z",
        },
        {
            "id": "x" * 101,
            "type": "running",
            "startDate": "2026-08-25T00:00:00Z",
            "endDate": "2026-08-25T01:00:00Z",
        },
        {
            "id": "x" * 101,
            "type": "HKQuantityTypeIdentifierStepCount",
        },
    ],
)
def test_persisted_event_source_ids_are_bounded_at_ingress(record: dict[str, object]) -> None:
    if "stage" in record:
        validate = SleepRecord.model_validate
    elif "value" in record:
        validate = MetricRecord.model_validate
    elif "startDate" in record:
        validate = Workout.model_validate
    else:
        validate = DeletedObject.model_validate
    with pytest.raises(ValidationError):
        validate(record)


def test_sync_window_authority_lists_are_bounded_before_service_queries() -> None:
    base = {
        "windowId": str(uuid4()),
        "purpose": "activation",
        "windowVersion": 2,
        "lowerBoundInclusive": "2026-07-26T00:00:00Z",
        "upperBoundExclusive": "2026-08-25T00:00:00Z",
    }
    with pytest.raises(ValidationError):
        SyncWindowManifest.model_validate({**base, "batchIds": [str(uuid4())] * 4097})
    with pytest.raises(ValidationError):
        SyncWindowManifest.model_validate(
            {**base, "emptyOrNoAccessTypes": [f"HKQuantityTypeIdentifierSynthetic{index}" for index in range(257)]}
        )
