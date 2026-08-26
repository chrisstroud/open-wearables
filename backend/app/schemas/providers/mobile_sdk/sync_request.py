# ruff: noqa: N815

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from enum import StrEnum
from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from app.constants.series_types.sdk import SDKMetricType, SleepPhase, WorkoutStatisticType
from app.constants.workout_types import SDKWorkoutType

SourceText32 = Annotated[str, Field(max_length=32)]
SourceText50 = Annotated[str, Field(max_length=50)]
SourceText100 = Annotated[str, Field(max_length=100)]


class DeviceType(StrEnum):
    """Device type for HealthKit records."""

    PHONE = "phone"
    WATCH = "watch"
    SCALE = "scale"
    RING = "ring"
    FITNESS_BAND = "fitness_band"
    CHEST_STRAP = "chest_strap"
    HEAD_MOUNTED = "head_mounted"
    SMART_DISPLAY = "smart_display"
    UNKNOWN = "unknown"


class RecordingMethod(StrEnum):
    """Recording method for HealthKit records."""

    ACTIVE = "active"
    AUTOMATIC = "automatic"
    MANUAL = "manual"
    UNKNOWN = "unknown"


class OSVersion(BaseModel):
    """Operating system version info from HealthKit source."""

    model_config = ConfigDict(populate_by_name=True)

    major_version: int = Field(alias="majorVersion", ge=0, le=65535)
    minor_version: int = Field(alias="minorVersion", ge=0, le=65535)
    patch_version: int = Field(alias="patchVersion", ge=0, le=65535)


class SourceInfo(BaseModel):
    """Source/device information for HealthKit records."""

    model_config = ConfigDict(populate_by_name=True)

    app_id: SourceText100 | None = Field(default=None, alias="appId")
    name: SourceText100 | None = None
    bundle_identifier: SourceText100 | None = Field(default=None, alias="bundleIdentifier")
    version: SourceText50 | None = None
    product_type: SourceText100 | None = Field(default=None, alias="productType")
    operating_system_version: OSVersion | None = Field(default=None, alias="operatingSystemVersion")
    device_id: SourceText100 | None = Field(default=None, alias="deviceId")
    device_name: SourceText100 | None = Field(default=None, alias="deviceName")
    device_manufacturer: SourceText100 | None = Field(default=None, alias="deviceManufacturer")
    device_type: DeviceType | SourceText32 | None = Field(default=None, alias="deviceType")
    device_model: SourceText100 | None = Field(default=None, alias="deviceModel")
    device_hardware_version: SourceText50 | None = Field(default=None, alias="deviceHardwareVersion")
    device_software_version: SourceText50 | None = Field(default=None, alias="deviceSoftwareVersion")
    recording_method: RecordingMethod | SourceText32 | None = Field(default=None, alias="recordingMethod")

    @field_validator(
        "app_id",
        "name",
        "bundle_identifier",
        "product_type",
        "device_id",
        "device_name",
        "device_manufacturer",
        "device_model",
        mode="before",
    )
    @classmethod
    def bound_source_text_100(cls, value: Any) -> Any:
        # Source labels are non-essential metadata. Bound them instead of
        # rejecting an otherwise durable health batch from a noisy writer.
        return value[:100] if isinstance(value, str) else value

    @field_validator(
        "version",
        "device_hardware_version",
        "device_software_version",
        mode="before",
    )
    @classmethod
    def bound_source_text_50(cls, value: Any) -> Any:
        return value[:50] if isinstance(value, str) else value

    @field_validator("device_type", "recording_method", mode="before")
    @classmethod
    def bound_source_text_32(cls, value: Any) -> Any:
        return value[:32] if isinstance(value, str) else value


class MetricRecord(BaseModel):
    """Health metric record from HealthKit (heart rate, steps, distance, etc.)."""

    id: str | None = Field(default=None, max_length=100)
    parentId: str | None = None
    type: SDKMetricType | str | None = None
    startDate: datetime
    endDate: datetime
    zoneOffset: str | None = None
    source: SourceInfo | None = None
    value: Decimal
    unit: str | None
    metadata: list[dict[str, Any]] | dict[str, Any] | None = None


class SleepRecord(BaseModel):
    """Sleep analysis record from HealthKit."""

    id: str | None = Field(default=None, max_length=100)
    parentId: str | None = None
    stage: SleepPhase | str
    startDate: datetime
    endDate: datetime
    zoneOffset: str | None = None
    source: SourceInfo | None = None
    values: list[dict[str, Any]] | None = None
    metadata: list[dict[str, Any]] | dict[str, Any] | None = None


class WorkoutStatistic(BaseModel):
    """Schema for workout statistic (distance, heart rate, calories, etc.)."""

    type: WorkoutStatisticType | str
    unit: str
    value: float | int


class Workout(BaseModel):
    """Schema for workout/exercise session from HealthKit."""

    id: str | None = Field(default=None, max_length=100)
    parentId: str | None = None
    type: SDKWorkoutType | str | None = None
    startDate: datetime
    endDate: datetime
    zoneOffset: str | None = None
    source: SourceInfo | None = None
    title: str | None = None
    notes: str | None = None
    values: list[WorkoutStatistic] | None = None

    # everything below is unused for now
    segments: list[dict[str, Any]] | None = None
    laps: list[dict[str, Any]] | None = None
    route: list[dict[str, Any]] | None = None
    samples: list[dict[str, Any]] | None = None
    metadata: list[dict[str, Any]] | dict[str, Any] | None = None


class DeletedObject(BaseModel):
    """A HealthKit object deletion emitted by an anchored query.

    ``HKDeletedObject`` exposes only the stable object UUID. The query's sample
    type is carried alongside it so the server can select the correct storage
    family without guessing from dates or source metadata.
    """

    id: str = Field(min_length=1, max_length=100)
    type: str = Field(min_length=1, max_length=255)


class SyncWindowManifest(BaseModel):
    """Manifest proving one bounded mobile export window is durably complete."""

    model_config = ConfigDict(extra="forbid")

    windowId: UUID
    purpose: Literal["activation", "archive", "incremental"]
    windowVersion: Literal[2]
    lowerBoundInclusive: datetime
    upperBoundExclusive: datetime
    batchIds: list[UUID] = Field(default_factory=list, max_length=4096)
    emptyOrNoAccessTypes: list[Annotated[str, Field(min_length=1, max_length=255)]] = Field(
        default_factory=list,
        max_length=256,
    )
    reconciliationStartInclusive: datetime | None = None
    reconciliationEndExclusive: datetime | None = None

    @model_validator(mode="after")
    def validate_bounds(self) -> SyncWindowManifest:
        if self.lowerBoundInclusive.tzinfo is None or self.upperBoundExclusive.tzinfo is None:
            raise ValueError("syncWindow bounds must include timezone offsets")
        if self.lowerBoundInclusive >= self.upperBoundExclusive:
            raise ValueError("syncWindow lowerBoundInclusive must precede upperBoundExclusive")
        if not self.batchIds and not self.emptyOrNoAccessTypes:
            raise ValueError("syncWindow must reference accepted batches or terminal empty/no-access types")
        reconciliation_values = (
            self.reconciliationStartInclusive,
            self.reconciliationEndExclusive,
        )
        if (reconciliation_values[0] is None) != (reconciliation_values[1] is None):
            raise ValueError("syncWindow reconciliation bounds must be supplied together")
        if self.purpose == "incremental" and reconciliation_values[0] is None:
            raise ValueError("incremental syncWindow requires reconciliation bounds")
        if reconciliation_values[0] is not None and reconciliation_values[1] is not None:
            if reconciliation_values[0].tzinfo is None or reconciliation_values[1].tzinfo is None:
                raise ValueError("syncWindow reconciliation bounds must include timezone offsets")
            if reconciliation_values[0] >= reconciliation_values[1]:
                raise ValueError("syncWindow reconciliation start must precede its end")
        return self


class SyncRequestData(BaseModel):
    """Inner data structure for Apple HealthKit sync request.

    Contains the actual health data arrays.
    """

    records: list[MetricRecord] = Field(
        default_factory=list,
        description="Time-series health measurements (heart rate, steps, distance, etc.)",
    )
    sleep: list[SleepRecord] = Field(
        default_factory=list,
        description="Sleep phase records (in bed, awake, light, deep, REM).",
    )
    workouts: list[Workout] = Field(
        default_factory=list,
        description="Exercise/workout sessions with optional statistics (distance, heart rate, calories, etc.)",
    )
    deletions: list[DeletedObject] = Field(
        default_factory=list,
        description="HealthKit tombstones identified by source object UUID and HealthKit sample type.",
    )


class SyncRequest(BaseModel):
    """Schema for Apple HealthKit data import via SDK.

    This schema represents the structure of health data exported from Apple HealthKit
    and sent to the SDK sync endpoint. The data is processed asynchronously via Celery.

    Structure:
    - `data.records`: Time-series measurements (heart rate, steps, distance, etc.)
    - `data.sleep`: Sleep phase records (in bed, awake, light, deep, REM)
    - `data.workouts`: Exercise/workout sessions with statistics

    All fields within `data` are optional - you can send any combination of records, sleep, and workouts.
    """

    provider: str
    sdkVersion: str
    syncTimestamp: datetime
    data: SyncRequestData = Field(
        default_factory=SyncRequestData,
        description="Container for health data arrays (records, sleep, workouts)",
    )
    syncWindow: SyncWindowManifest | None = None

    class Config:
        json_schema_extra = {
            "example": {
                "provider": "samsung",
                "sdkVersion": "0.1.0",
                "syncTimestamp": "2026-02-24T10:00:00Z",
                "data": {
                    "records": [
                        {
                            "id": "abc-xyz-123-sys",
                            "type": "BLOOD_PRESSURE_SYSTOLIC",
                            "startDate": "2026-02-24T08:30:00Z",
                            "endDate": "2026-02-24T08:30:00Z",
                            "zoneOffset": "+01:00",
                            "source": {
                                "appId": "com.sec.android.app.shealth",
                                "deviceId": "R9ZW30ABC12",
                                "deviceName": "Galaxy Watch7",
                                "deviceManufacturer": "Samsung",
                                "deviceModel": "SM-R960",
                                "deviceType": "watch",
                                "recordingMethod": None,  # null
                            },
                            "value": 122.0,
                            "unit": "mmHg",
                            "parentId": "abc-xyz-123",
                            "metadata": None,  # null
                        }
                    ],
                    "sleep": [
                        {
                            "id": "slp-001-s0-0",
                            "parentId": "slp-001",
                            "stage": "light",
                            "startDate": "2026-02-23T23:10:00Z",
                            "endDate": "2026-02-24T00:00:00Z",
                            "zoneOffset": "+01:00",
                            "source": {
                                "appId": "com.sec.android.app.shealth",
                                "deviceId": "R9ZW30ABC12",
                                "deviceName": "Galaxy Watch7",
                                "deviceManufacturer": "Samsung",
                                "deviceModel": "SM-R960",
                                "deviceType": "watch",
                                "recordingMethod": None,  # null
                            },
                            "values": [{"type": "sleepScore", "value": 82, "unit": "score"}],
                            "metadata": None,  # null
                        }
                    ],
                    "workouts": [
                        {
                            "id": "wrk-001-s0",
                            "parentId": "wrk-001",
                            "type": "RUNNING",
                            "startDate": "2026-02-24T06:00:00Z",
                            "endDate": "2026-02-24T06:45:00Z",
                            "zoneOffset": "+01:00",
                            "source": {
                                "appId": "com.sec.android.app.shealth",
                                "deviceId": "R9ZW30ABC12",
                                "deviceName": "Galaxy Watch7",
                                "deviceManufacturer": "Samsung",
                                "deviceModel": "SM-R960",
                                "deviceType": "watch",
                                "recordingMethod": None,  # null
                            },
                            "title": None,  # null
                            "notes": "Morning run in the park",
                            "values": [
                                {"type": "duration", "value": 2700000, "unit": "ms"},
                                {"type": "calories", "value": 345.5, "unit": "kcal"},
                                {"type": "distance", "value": 5234.0, "unit": "m"},
                                {"type": "meanHeartRate", "value": 142.3, "unit": "bpm"},
                                {"type": "maxHeartRate", "value": 178.0, "unit": "bpm"},
                                {"type": "minHeartRate", "value": 95.0, "unit": "bpm"},
                                {"type": "meanSpeed", "value": 1.50, "unit": "m/s"},
                                {"type": "maxSpeed", "value": 3.20, "unit": "m/s"},
                                {"type": "meanCadence", "value": 165.0, "unit": "spm"},
                                {"type": "maxCadence", "value": 182.0, "unit": "spm"},
                                {"type": "altitudeGain", "value": 45.0, "unit": "m"},
                                {"type": "altitudeLoss", "value": 42.0, "unit": "m"},
                                {"type": "maxAltitude", "value": 185.0, "unit": "m"},
                                {"type": "minAltitude", "value": 140.0, "unit": "m"},
                                {"type": "vo2Max", "value": 42.5, "unit": "mL/kg/min"},
                            ],
                            "segments": None,  # null
                            "laps": None,  # null
                            "route": [
                                {
                                    "timestamp": "2026-02-24T06:01:00Z",
                                    "latitude": 52.229676,
                                    "longitude": 21.012229,
                                    "altitudeM": 142.0,
                                    "horizontalAccuracyM": 3.5,
                                    "verticalAccuracyM": None,  # null
                                }
                            ],
                            "samples": [
                                {
                                    "timestamp": "2026-02-24T06:01:00Z",
                                    "type": "heartRate",
                                    "value": 110.0,
                                    "unit": "bpm",
                                },
                                {"timestamp": "2026-02-24T06:01:00Z", "type": "cadence", "value": 155.0, "unit": "spm"},
                                {"timestamp": "2026-02-24T06:01:00Z", "type": "speed", "value": 1.8, "unit": "m/s"},
                            ],
                            "metadata": None,  # null
                        }
                    ],
                },
            }
        }
