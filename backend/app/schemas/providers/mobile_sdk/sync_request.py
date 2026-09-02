# ruff: noqa: N815

from __future__ import annotations

from datetime import date, datetime, time, timedelta
from decimal import Decimal
from enum import StrEnum
from hashlib import sha256
from hmac import compare_digest
from math import isfinite
from typing import Annotated, Any, Literal
from uuid import UUID
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

from pydantic import AliasChoices, BaseModel, ConfigDict, Field, field_serializer, field_validator, model_validator

from app.constants.series_types.sdk import SDKMetricType, SleepPhase, WorkoutStatisticType
from app.constants.workout_types import SDKWorkoutType

SourceText32 = Annotated[str, Field(max_length=32)]
SourceText50 = Annotated[str, Field(max_length=50)]
SourceText100 = Annotated[str, Field(max_length=100)]
SHA256Digest = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


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


class DailySummaryContributor(BaseModel):
    """Identifier-safe provenance for one locally aggregated HealthKit day."""

    model_config = ConfigDict(extra="forbid")

    provider_id: Annotated[str, Field(min_length=1, max_length=32)]
    source_bundle_identifier: Annotated[str, Field(min_length=1, max_length=160)]
    source_name: Annotated[str, Field(min_length=1, max_length=100)] | None = None
    source_version: Annotated[str, Field(min_length=1, max_length=50)] | None = None
    product_type: Annotated[str, Field(min_length=1, max_length=100)] | None = None
    device_manufacturer: Annotated[str, Field(min_length=1, max_length=100)] | None = None
    device_model: Annotated[str, Field(min_length=1, max_length=100)] | None = None


class DailySummaryStatistic(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: Literal["sum", "mean", "p50", "minimum", "maximum", "latest", "basal", "bolus", "unclassified", "total"]
    value: Decimal
    unit: Annotated[str, Field(min_length=1, max_length=50)]
    observed_at: datetime | None = None

    @field_validator("value")
    @classmethod
    def validate_finite_wire_number(cls, value: Decimal) -> Decimal:
        # The durable JSONB payload retains the exact decimal text, while the
        # public dashboard contract is a finite ECMAScript number. Reject a
        # finite Decimal that would overflow that wire representation.
        if not value.is_finite() or not isfinite(float(value)):
            raise ValueError("daily summary statistic value must be a finite JSON number")
        return value

    @field_serializer("value", when_used="json")
    def serialize_wire_number(self, value: Decimal) -> float:
        return float(value)


class DailySummary(BaseModel):
    """Versioned daily aggregate computed on-device from HealthKit samples.

    The payload deliberately contains no raw HealthKit sample identifier,
    sample timestamp, device descriptor, or per-sample value.
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["apple-health-daily-summary.v1"]
    summary_key: SHA256Digest
    revision_id: SHA256Digest
    supersedes_revision_id: SHA256Digest | None = None
    registry_version: Annotated[str, Field(min_length=1, max_length=64)]
    aggregation_version: Annotated[str, Field(min_length=1, max_length=64)]
    healthkit_type: Annotated[str, Field(min_length=1, max_length=255)]
    series_type: Annotated[str, Field(min_length=1, max_length=100)]
    local_date: date
    timezone: Annotated[str, Field(min_length=1, max_length=100)]
    timezone_boundary_version: Annotated[str, Field(min_length=1, max_length=64)]
    day_start_inclusive: datetime
    day_end_exclusive: datetime
    assignment_policy: Literal["calendar-day", "wake-day", "session-start-day"]
    source_scope: Literal["healthkit-merged", "healthkit-source"]
    contributors: list[DailySummaryContributor] = Field(max_length=32)
    contributor_set_digest: SHA256Digest
    canonical_unit: Annotated[str, Field(min_length=1, max_length=50)]
    statistics: list[DailySummaryStatistic] = Field(max_length=16)
    primary_statistic: Literal[
        "sum",
        "mean",
        "p50",
        "minimum",
        "maximum",
        "latest",
        "basal",
        "bolus",
        "unclassified",
        "total",
    ]
    sample_count: int = Field(ge=0)
    input_set_digest: SHA256Digest
    coverage_status: Literal[
        "observed",
        "provisional",
        "empty-or-no-access",
        "unavailable",
        "retracted",
    ]
    computed_at: datetime

    @model_validator(mode="after")
    def validate_daily_geometry_and_value(self) -> DailySummary:
        _validate_day_geometry(
            self.local_date,
            self.timezone,
            self.day_start_inclusive,
            self.day_end_exclusive,
            self.computed_at,
        )
        names = [statistic.name for statistic in self.statistics]
        if len(names) != len(set(names)):
            raise ValueError("daily summary statistic names must be unique")
        if any(statistic.unit != self.canonical_unit for statistic in self.statistics):
            raise ValueError("daily summary statistic units must match canonical_unit")
        if self.coverage_status in ("observed", "provisional"):
            if self.primary_statistic not in names:
                raise ValueError("observed daily summary must supply its primary statistic")
            if self.sample_count <= 0:
                raise ValueError("observed daily summary must include at least one input sample")
            if not self.contributors:
                raise ValueError("observed daily summary must include at least one contributor")
        elif self.statistics:
            raise ValueError("unobserved daily summary statistics must be empty")
        return self


class SleepSummaryDuration(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: Literal["in-bed", "asleep", "awake", "core-light", "deep", "rem", "unspecified", "nap"]
    seconds: float = Field(ge=0)


class AppleHealthSleepSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["apple-health-sleep-summary.v1"]
    summary_key: SHA256Digest
    revision_id: SHA256Digest
    supersedes_revision_id: SHA256Digest | None = None
    registry_version: Annotated[str, Field(min_length=1, max_length=64)]
    aggregation_version: Annotated[str, Field(min_length=1, max_length=64)]
    local_date: date
    timezone: Annotated[str, Field(min_length=1, max_length=100)]
    timezone_boundary_version: Annotated[str, Field(min_length=1, max_length=64)]
    day_start_inclusive: datetime
    day_end_exclusive: datetime
    assignment_policy: Literal["wake-day"]
    source_scope: Literal["healthkit-merged", "healthkit-source"]
    contributors: list[DailySummaryContributor] = Field(max_length=32)
    contributor_set_digest: SHA256Digest
    episode_count: int = Field(ge=0)
    nap_count: int = Field(ge=0)
    earliest_onset: datetime | None = None
    latest_wake: datetime | None = None
    durations: list[SleepSummaryDuration] = Field(max_length=8)
    sample_count: int = Field(ge=0)
    input_set_digest: SHA256Digest
    coverage_status: Literal[
        "observed",
        "provisional",
        "empty-or-no-access",
        "unavailable",
        "retracted",
    ]
    computed_at: datetime

    @model_validator(mode="after")
    def validate_sleep_summary(self) -> AppleHealthSleepSummary:
        _validate_day_geometry(
            self.local_date,
            self.timezone,
            self.day_start_inclusive,
            self.day_end_exclusive,
            self.computed_at,
        )
        names = [duration.name for duration in self.durations]
        if len(names) != len(set(names)):
            raise ValueError("sleep duration names must be unique")
        if self.coverage_status in ("observed", "provisional") and self.sample_count <= 0:
            raise ValueError("observed sleep summary must include at least one input sample")
        if self.coverage_status in ("observed", "provisional") and not self.contributors:
            raise ValueError("observed sleep summary must include at least one contributor")
        if self.coverage_status not in ("observed", "provisional") and (
            self.durations or self.episode_count or self.nap_count or self.earliest_onset or self.latest_wake
        ):
            raise ValueError("unobserved sleep summary must not carry observations")
        return self


class AppleHealthWorkoutSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["apple-health-workout-summary.v1"]
    event_key: SHA256Digest
    revision_id: SHA256Digest
    supersedes_revision_id: SHA256Digest | None = None
    registry_version: Annotated[str, Field(min_length=1, max_length=64)]
    aggregation_version: Annotated[str, Field(min_length=1, max_length=64)]
    local_date: date
    timezone: Annotated[str, Field(min_length=1, max_length=100)]
    timezone_boundary_version: Annotated[str, Field(min_length=1, max_length=64)]
    assignment_policy: Literal["session-start-day"]
    source_scope: Literal["healthkit-source"]
    activity_type: Annotated[str, Field(min_length=1, max_length=100)]
    start: datetime
    end: datetime
    crosses_local_midnight: bool
    duration_seconds: float = Field(ge=0)
    total_energy_kcal: float | None = Field(default=None, ge=0)
    total_distance_meters: float | None = Field(default=None, ge=0)
    average_heart_rate_bpm: float | None = Field(default=None, ge=0)
    maximum_heart_rate_bpm: float | None = Field(default=None, ge=0)
    contributors: list[DailySummaryContributor] = Field(max_length=32)
    contributor_set_digest: SHA256Digest
    input_set_digest: SHA256Digest
    coverage_status: Literal["observed", "retracted"]
    computed_at: datetime

    @model_validator(mode="after")
    def validate_workout_summary(self) -> AppleHealthWorkoutSummary:
        if self.start.tzinfo is None or self.end.tzinfo is None or self.computed_at.tzinfo is None:
            raise ValueError("workout summary timestamps must include timezone offsets")
        if self.start >= self.end:
            raise ValueError("workout summary start must precede end")
        try:
            zone = ZoneInfo(self.timezone)
        except ZoneInfoNotFoundError as exc:
            raise ValueError("workout summary timezone must be a valid IANA timezone") from exc
        if self.start.astimezone(zone).date() != self.local_date:
            raise ValueError("workout summary start must resolve to local_date")
        crosses = self.start.astimezone(zone).date() != self.end.astimezone(zone).date()
        if crosses != self.crosses_local_midnight:
            raise ValueError("workout summary cross-midnight flag must match exact geometry")
        if self.coverage_status == "observed":
            if not self.contributors:
                raise ValueError("observed workout summary must include at least one contributor")
            if self.duration_seconds <= 0:
                raise ValueError("observed workout summary must include positive duration")
        else:
            if self.supersedes_revision_id is None:
                raise ValueError("retracted workout summary must supersede an exact revision")
            if (
                self.contributors
                or self.duration_seconds != 0
                or self.total_energy_kcal is not None
                or self.total_distance_meters is not None
                or self.average_heart_rate_bpm is not None
                or self.maximum_heart_rate_bpm is not None
            ):
                raise ValueError("retracted workout summary must not carry observations")
        return self


def _validate_day_geometry(
    local_date: date,
    timezone: str,
    day_start_inclusive: datetime,
    day_end_exclusive: datetime,
    computed_at: datetime,
) -> None:
    if day_start_inclusive.tzinfo is None or day_end_exclusive.tzinfo is None or computed_at.tzinfo is None:
        raise ValueError("summary timestamps must include timezone offsets")
    try:
        zone = ZoneInfo(timezone)
    except ZoneInfoNotFoundError as exc:
        raise ValueError("summary timezone must be a valid IANA timezone") from exc
    local_start = day_start_inclusive.astimezone(zone)
    local_end = day_end_exclusive.astimezone(zone)
    if local_start.date() != local_date or local_start.time().replace(tzinfo=None) != time.min:
        raise ValueError("summary start must be exact local midnight for local_date")
    if local_end.date() != local_date + timedelta(days=1) or local_end.time().replace(tzinfo=None) != time.min:
        raise ValueError("summary end must be exact next local midnight")
    duration = day_end_exclusive - day_start_inclusive
    if duration <= timedelta(0) or duration > timedelta(hours=26):
        raise ValueError("summary bounds must cover one DST-aware local day")


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

    model_config = ConfigDict(extra="forbid")

    records: list[MetricRecord] = Field(
        default_factory=list,
        description="Time-series health measurements (heart rate, steps, distance, etc.)",
    )
    sleep: list[SleepRecord | AppleHealthSleepSummary] = Field(
        default_factory=list,
        description="Sleep phase records (in bed, awake, light, deep, REM).",
    )
    workouts: list[Workout | AppleHealthWorkoutSummary] = Field(
        default_factory=list,
        description="Exercise/workout sessions with optional statistics (distance, heart rate, calories, etc.)",
    )
    deletions: list[DeletedObject] = Field(
        default_factory=list,
        description="HealthKit tombstones identified by source object UUID and HealthKit sample type.",
    )
    daily_summaries: list[DailySummary] = Field(
        default_factory=list,
        max_length=4096,
        description="Revisioned, on-device daily HealthKit aggregates; never raw samples.",
    )


def calculate_revision_set_digest(data: SyncRequestData) -> str:
    """Hash the exact compact-summary identities independently of array order."""
    identities = [
        *(("daily_summary", item.summary_key, item.revision_id) for item in data.daily_summaries),
        *(
            ("sleep", item.summary_key, item.revision_id)
            for item in data.sleep
            if isinstance(item, AppleHealthSleepSummary)
        ),
        *(
            ("workout", item.event_key, item.revision_id)
            for item in data.workouts
            if isinstance(item, AppleHealthWorkoutSummary)
        ),
    ]
    digest = sha256()
    for identity in sorted(identities):
        for value in identity:
            encoded = value.encode("utf-8")
            digest.update(len(encoded).to_bytes(8, byteorder="big", signed=False))
            digest.update(encoded)
    return digest.hexdigest()


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

    model_config = ConfigDict(populate_by_name=True, extra="forbid")

    schema_version: Literal["apple-health-daily-summary.v1"] | None = None
    revision_set_digest: SHA256Digest | None = None
    provider: str
    sdkVersion: str = Field(validation_alias=AliasChoices("sdkVersion", "sdk_version"))
    syncTimestamp: datetime = Field(validation_alias=AliasChoices("syncTimestamp", "sync_timestamp"))
    data: SyncRequestData = Field(
        default_factory=SyncRequestData,
        description="Container for health data arrays (records, sleep, workouts)",
    )
    syncWindow: SyncWindowManifest | None = Field(
        default=None,
        validation_alias=AliasChoices("syncWindow", "sync_window"),
    )

    @model_validator(mode="after")
    def validate_payload_generation(self) -> SyncRequest:
        compact_sleep = [item for item in self.data.sleep if isinstance(item, AppleHealthSleepSummary)]
        raw_sleep = [item for item in self.data.sleep if isinstance(item, SleepRecord)]
        compact_workouts = [item for item in self.data.workouts if isinstance(item, AppleHealthWorkoutSummary)]
        raw_workouts = [item for item in self.data.workouts if isinstance(item, Workout)]
        parsed_revision_set_digest = calculate_revision_set_digest(self.data)

        if self.revision_set_digest is not None and not compare_digest(
            self.revision_set_digest,
            parsed_revision_set_digest,
        ):
            raise ValueError("revision_set_digest must match the exact parsed summary revision set")

        if self.schema_version == "apple-health-daily-summary.v1":
            if self.revision_set_digest is None:
                raise ValueError("daily-summary envelope requires revision_set_digest")
            if self.provider != "apple":
                raise ValueError("daily-summary envelope requires provider apple")
            if self.syncWindow is not None or self.data.records or self.data.deletions:
                raise ValueError("daily-summary envelope cannot contain raw records, deletions, or a sync window")
            if raw_sleep or raw_workouts:
                raise ValueError("daily-summary envelope cannot contain raw sleep or workout records")
            if not (self.data.daily_summaries or compact_sleep or compact_workouts):
                required_empty_collections = {"daily_summaries", "sleep", "workouts"}
                if "data" not in self.model_fields_set or not required_empty_collections.issubset(
                    self.data.model_fields_set
                ):
                    raise ValueError(
                        "zero-revision daily-summary envelope must explicitly declare empty "
                        "daily_summaries, sleep, and workouts collections"
                    )
        elif self.revision_set_digest is not None:
            raise ValueError("revision_set_digest requires the daily-summary envelope schema")
        elif self.data.daily_summaries or compact_sleep or compact_workouts:
            raise ValueError("summary revisions require the daily-summary envelope schema")
        return self
