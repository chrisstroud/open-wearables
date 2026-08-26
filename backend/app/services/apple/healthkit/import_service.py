import json
import time
from datetime import datetime
from decimal import Decimal
from logging import Logger, getLogger
from typing import Iterable, TypedDict
from uuid import UUID, uuid4

import sentry_sdk
from pydantic import ValidationError

from app.constants.series_types.sdk import (
    WorkoutStatisticType,
    get_detail_field_from_workout_statistic_type,
    get_series_type_from_metric_type,
    get_series_type_from_workout_statistic_type,
)
from app.constants.workout_types import get_unified_apple_workout_type_sdk
from app.database import DbSession
from app.repositories.data_point_series_repository import DataPointSourcePayloadConflictError
from app.repositories.data_source_repository import DataSourceProvenanceConflictError
from app.repositories.user_connection_repository import UserConnectionRepository
from app.schemas.enums import SeriesType, daily_total_flag
from app.schemas.model_crud.activities import (
    EventRecordCreate,
    EventRecordDetailCreate,
    EventRecordMetrics,
    HeartRateSampleCreate,
    StepSampleCreate,
    TimeSeriesSampleCreate,
)
from app.schemas.providers.mobile_sdk import (
    SyncRequest as SDKSyncRequest,
)
from app.schemas.providers.mobile_sdk import (
    WorkoutStatistic,
)
from app.schemas.providers.mobile_sdk.sync_request import (
    DeletedObject,
    MetricRecord,
    SleepRecord,
    SyncRequestData,
    Workout,
)
from app.schemas.responses.upload import UploadDataResponse
from app.services.event_record_service import event_record_service
from app.services.sdk_sleep_inbox_service import sdk_sleep_inbox_service
from app.services.sdk_sync_window_receipt_service import sdk_sync_window_receipt_service
from app.services.timeseries_service import timeseries_service
from app.utils.sentry_helpers import log_and_capture_error
from app.utils.structured_logging import log_structured

from .device_resolution import extract_device_info, extract_source_identifier
from .sleep_service import handle_sleep_data

# Health Connect's own mg/dL converter uses exactly 18.0, so values written to HC
# in mg/dL round-trip with a ~0.1% offset under this factor.
MMOL_L_TO_MG_DL = Decimal("18.0182")

_SDK_ITEM_MODELS = (
    ("records", MetricRecord),
    ("sleep", SleepRecord),
    ("workouts", Workout),
    ("deletions", DeletedObject),
)

_SUPPORTED_SCALAR_WORKOUT_STATISTICS = {
    WorkoutStatisticType.DURATION.value,
    WorkoutStatisticType.TOTAL_DURATION.value,
    WorkoutStatisticType.ACTIVE_ENERGY_BURNED.value,
    WorkoutStatisticType.BASAL_ENERGY_BURNED.value,
    WorkoutStatisticType.CALORIES.value,
    WorkoutStatisticType.TOTAL_CALORIES.value,
}


class InvalidRecord(TypedDict):
    """A single record dropped by per-record validation (PII-free — only the location, no values)."""

    collection: str  # "records" | "sleep" | "workouts" | "deletions"
    index: int
    loc: str
    msg: str | None
    type: str | None


class LoadDataResult(TypedDict):
    workouts_saved: int
    records_saved: int
    types: list[str]  # series types written
    sleep_saved: int
    dropped: list[InvalidRecord]
    validation_ms: float
    tombstones_received: int
    tombstones_applied: int
    tombstones_unresolved: int
    tombstone_rows_deleted: int
    tombstone_error_code: str | None
    unprocessed_count: int
    processing_error_code: str | None


def _content_coverage(request: SDKSyncRequest) -> tuple[list[str], str | None, str | None]:
    """Derive bounded metadata from the validated payload without retaining values."""
    type_identifiers = {str(record.type) for record in request.data.records if record.type is not None}
    if request.data.sleep:
        type_identifiers.add("HKCategoryTypeIdentifierSleepAnalysis")
    if request.data.workouts:
        type_identifiers.add("HKWorkoutType")

    records = [*request.data.records, *request.data.sleep, *request.data.workouts]
    if not records:
        return sorted(type_identifiers), None, None
    lower = min(record.startDate for record in records)
    upper = max(record.endDate for record in records)
    return sorted(type_identifiers), lower.isoformat(), upper.isoformat()


def validated_content_coverage(request_content: str) -> dict[str, list[str] | str | None]:
    """Return privacy-safe bounds/types from a terminal-success JSON payload."""
    raw = json.loads(request_content)
    request = SDKSyncRequest.model_validate(raw)
    covered_types, content_lower, content_upper = _content_coverage(request)
    return {
        "covered_type_identifiers": covered_types,
        "content_lower_bound_inclusive": content_lower,
        "content_upper_bound_exclusive": content_upper,
    }


def _parse_sync_request(raw: dict) -> tuple[SDKSyncRequest, list[InvalidRecord]]:
    """Parse a raw payload into a SyncRequest, salvaging valid records when items fail.

    Fast path validates in one shot. On failure, valid records are kept and the per-item
    failures returned (PII-free: loc/msg/type). Raises ValidationError when the envelope or
    a container shape is invalid — nothing salvageable, so the batch is rejected (400).
    """
    try:
        return SDKSyncRequest(**raw), []
    except ValidationError:
        pass  # fall through to per-item validation

    # Only individual records are salvageable. A malformed container shape (data not a dict,
    # or a collection not a list) is not — re-validate to re-raise the real ValidationError.
    data = raw.get("data")
    if not isinstance(data, dict) or any(not isinstance(data.get(key, []), list) for key, _ in _SDK_ITEM_MODELS):
        return SDKSyncRequest(**raw), []

    kept: dict[str, list] = {}
    dropped: list[InvalidRecord] = []
    for key, model in _SDK_ITEM_MODELS:
        good: list = []
        for idx, item in enumerate(data.get(key, [])):
            try:
                good.append(model.model_validate(item))
            except ValidationError as exc:
                err = (exc.errors() or [{}])[0]
                dropped.append(
                    {
                        "collection": key,
                        "index": idx,
                        "loc": ".".join(str(x) for x in err.get("loc", [])),
                        "msg": err.get("msg"),
                        "type": err.get("type"),
                    }
                )
        kept[key] = good

    # Reconstruct via model_validate so Pydantic still rejects a bad envelope; `data` now
    # carries only the kept records.
    request = SDKSyncRequest.model_validate(
        {
            **raw,
            "data": SyncRequestData(
                records=kept["records"],
                sleep=kept["sleep"],
                workouts=kept["workouts"],
                deletions=kept["deletions"],
            ),
        }
    )
    return request, dropped


def _is_supported_workout_statistic(statistic: WorkoutStatistic) -> bool:
    statistic_type = str(statistic.type)
    return (
        statistic_type in _SUPPORTED_SCALAR_WORKOUT_STATISTICS
        or get_series_type_from_workout_statistic_type(statistic_type) is not None
        or get_detail_field_from_workout_statistic_type(statistic_type) is not None
    )


class ImportService:
    def __init__(
        self,
        log: Logger,
    ):
        self.log = log
        self.event_record_service = event_record_service
        self.timeseries_service = timeseries_service
        self.user_connection_repo = UserConnectionRepository()

    def _dec(self, value: float | int | Decimal | None) -> Decimal | None:
        return None if value is None else Decimal(str(value))

    def _build_workout_bundles(
        self,
        request: SDKSyncRequest,
        user_id: str,
    ) -> Iterable[tuple[EventRecordCreate, EventRecordDetailCreate, list[TimeSeriesSampleCreate]]]:
        """
        Given the parsed SDKSyncRequest, yield tuples of
        (EventRecordCreate, EventRecordDetailCreate) ready to insert into your ORM session.
        """
        user_uuid = UUID(user_id)
        provider = request.provider

        for wjson in request.data.workouts:
            workout_id = uuid4()
            external_id = wjson.id if wjson.id else None

            device_model, software_version, original_source_name = extract_device_info(wjson.source)

            metrics, time_series_samples, duration = self._extract_metrics_from_workout_stats(
                wjson.values,
                user_uuid,
                device_model,
                software_version,
                wjson.endDate,
                wjson.zoneOffset,
                provider,
                original_source_name,
            )

            if duration is None:
                duration = int((wjson.endDate - wjson.startDate).total_seconds())

            workout_type = wjson.type.lower() if wjson.type else None
            type = get_unified_apple_workout_type_sdk(workout_type).value if workout_type else None

            record = EventRecordCreate(
                category="workout",
                type=type,
                source_name=original_source_name or "unknown",
                device_model=device_model,
                duration_seconds=int(duration),
                start_datetime=wjson.startDate,
                end_datetime=wjson.endDate,
                zone_offset=wjson.zoneOffset,
                id=workout_id,
                external_id=external_id,
                source=original_source_name,
                software_version=software_version,
                provider=provider,
                user_id=user_uuid,
            )

            detail = EventRecordDetailCreate(
                record_id=workout_id,
                **metrics,
            )

            yield record, detail, time_series_samples

    def _normalize_unit(self, series_type: SeriesType, value: Decimal, provider: str | None = None) -> Decimal:
        match series_type:
            # meters → cm
            case SeriesType.height | SeriesType.walking_step_length:
                return value * 100
            # 0-1 fraction → percent (body_fat only for Apple; Health Connect already reports percent)
            case SeriesType.body_fat_percentage if provider == "apple":
                return value * 100
            # HealthKit walking metrics returned as 0-1 fraction, DB stores percent
            # apple-only metrics, so no need to check if provider is apple
            case (
                SeriesType.walking_double_support_percentage
                | SeriesType.walking_asymmetry_percentage
                | SeriesType.walking_steadiness
            ):
                return value * 100
            case _:
                return value

    def _build_statistic_bundles(
        self,
        request: SDKSyncRequest,
        user_id: str,
    ) -> list[HeartRateSampleCreate | StepSampleCreate | TimeSeriesSampleCreate]:
        time_series_samples: list[HeartRateSampleCreate | StepSampleCreate | TimeSeriesSampleCreate] = []
        user_uuid = UUID(user_id)
        provider = request.provider

        for rjson in request.data.records:
            value = Decimal(str(rjson.value))

            record_type = rjson.type or ""
            series_type = get_series_type_from_metric_type(record_type)

            if not series_type:
                continue
            value = self._normalize_unit(series_type, value, provider)

            # Health Connect reports blood glucose in mmol/L; the series unit is mg/dL.
            if series_type == SeriesType.blood_glucose and (rjson.unit or "").lower().startswith("mmol"):
                value = value * MMOL_L_TO_MG_DL

            # Extract device info
            device_model, software_version, original_source_name = extract_device_info(rjson.source)
            source_identifier = extract_source_identifier(rjson.source)

            sample = TimeSeriesSampleCreate(
                id=uuid4(),
                external_id=rjson.id,
                user_id=user_uuid,
                source=source_identifier,
                original_source_name=original_source_name,
                device_model=device_model,
                software_version=software_version,
                provider=provider,
                recorded_at=rjson.startDate,
                zone_offset=rjson.zoneOffset,
                value=value,
                series_type=series_type,
                is_daily_total=daily_total_flag(series_type, is_daily=False),
            )

            match series_type:
                case SeriesType.heart_rate:
                    time_series_samples.append(HeartRateSampleCreate(**sample.model_dump()))
                case SeriesType.steps:
                    time_series_samples.append(StepSampleCreate(**sample.model_dump()))
                case _:
                    time_series_samples.append(sample)

        return time_series_samples

    def _compute_aggregates(self, values: list[Decimal]) -> tuple[Decimal | None, Decimal | None, Decimal | None]:
        if not values:
            return None, None, None
        min_v = min(values)
        max_v = max(values)
        avg_v = sum(values, Decimal("0")) / Decimal(len(values))
        return min_v, max_v, avg_v

    def _extract_metrics_from_workout_stats(
        self,
        stats: list[WorkoutStatistic] | None,
        user_uuid: UUID,
        device_model: str | None,
        software_version: str | None,
        end_date: datetime,
        zone_offset: str | None,
        provider: str,
        source_name: str | None,
    ) -> tuple[EventRecordMetrics, list[TimeSeriesSampleCreate], int | float | None]:
        """
        Returns a tuple with the metrics, time series samples, and duration.
        """
        if stats is None:
            return EventRecordMetrics(), [], None

        stats_dict: dict[str, Decimal | int] = {}
        stats_dict["energy_burned"] = Decimal("0")
        time_series_samples: list[TimeSeriesSampleCreate] = []
        duration: float | None = None

        for stat in stats:
            value = self._dec(stat.value)
            if value is None or stat.type is None:
                continue

            series_type = get_series_type_from_workout_statistic_type(stat.type)
            if series_type:
                time_series_samples.append(
                    TimeSeriesSampleCreate(
                        id=uuid4(),
                        external_id=None,
                        user_id=user_uuid,
                        source=source_name,
                        device_model=device_model,
                        software_version=software_version,
                        provider=provider,
                        recorded_at=end_date,
                        zone_offset=zone_offset,
                        value=value,
                        series_type=series_type,
                        is_daily_total=daily_total_flag(series_type, is_daily=False),
                    )
                )
                continue

            match stat.type:
                # duration is sent as a workout statistic but stored separately
                case WorkoutStatisticType.DURATION | WorkoutStatisticType.TOTAL_DURATION:
                    duration = float(value) / 1000 if stat.unit == "ms" else float(value)
                case (
                    WorkoutStatisticType.ACTIVE_ENERGY_BURNED
                    | WorkoutStatisticType.BASAL_ENERGY_BURNED
                    | WorkoutStatisticType.CALORIES
                    | WorkoutStatisticType.TOTAL_CALORIES
                ):
                    stats_dict["energy_burned"] += value
                case _:
                    detail_field = get_detail_field_from_workout_statistic_type(stat.type)
                    if detail_field:
                        # Apple SDK may send fractional Decimals for integer fields (e.g. stepCount)
                        if detail_field in ("steps_count", "moving_time_seconds"):
                            value = int(value)
                        stats_dict[detail_field] = value

        return EventRecordMetrics(**stats_dict), time_series_samples, duration

    def load_data(
        self,
        db_session: DbSession,
        raw: dict,
        user_id: str,
        batch_id: str | None = None,
        require_terminal_receipt: bool = False,
    ) -> LoadDataResult:
        """
        Load data into database and return counts of saved items plus per-record failures.
        """
        # Per-record validation: keep valid records, drop+report the invalid ones instead
        # of failing the whole batch. Raises ValidationError only if the envelope is invalid.
        # validation_ms measures the parse/per-record pass so we can watch the cost of a
        # malformed payload in the logs.
        started = time.perf_counter()
        request, dropped = _parse_sync_request(raw)
        validation_ms = round((time.perf_counter() - started) * 1000, 1)
        workouts_saved = 0
        records_saved = 0
        sleep_saved = 0
        types: set[str] = set()

        if request.syncWindow is not None:
            if not require_terminal_receipt or batch_id is None:
                return {
                    "workouts_saved": 0,
                    "records_saved": 0,
                    "types": [],
                    "sleep_saved": 0,
                    "dropped": dropped,
                    "validation_ms": validation_ms,
                    "tombstones_received": 0,
                    "tombstones_applied": 0,
                    "tombstones_unresolved": 0,
                    "tombstone_rows_deleted": 0,
                    "tombstone_error_code": None,
                    "unprocessed_count": 1,
                    "processing_error_code": "sync_window_receipt_required",
                }
            if any(
                (
                    request.data.records,
                    request.data.sleep,
                    request.data.workouts,
                    request.data.deletions,
                )
            ):
                return {
                    "workouts_saved": 0,
                    "records_saved": 0,
                    "types": [],
                    "sleep_saved": 0,
                    "dropped": dropped,
                    "validation_ms": validation_ms,
                    "tombstones_received": len(request.data.deletions),
                    "tombstones_applied": 0,
                    "tombstones_unresolved": len(request.data.deletions),
                    "tombstone_rows_deleted": 0,
                    "tombstone_error_code": None,
                    "unprocessed_count": 1,
                    "processing_error_code": "window_payload_not_metadata_only",
                }
            user_uuid = UUID(user_id)
            try:
                terminal_batch_id = UUID(batch_id)
            except ValueError:
                return {
                    "workouts_saved": 0,
                    "records_saved": 0,
                    "types": [],
                    "sleep_saved": 0,
                    "dropped": dropped,
                    "validation_ms": validation_ms,
                    "tombstones_received": 0,
                    "tombstones_applied": 0,
                    "tombstones_unresolved": 0,
                    "tombstone_rows_deleted": 0,
                    "tombstone_error_code": None,
                    "unprocessed_count": 1,
                    "processing_error_code": "window_id_invalid",
                }
            acceptance = sdk_sync_window_receipt_service.accept(
                db_session,
                user_id=user_uuid,
                provider=request.provider,
                terminal_batch_id=terminal_batch_id,
                manifest=request.syncWindow,
            )
            if not acceptance.accepted:
                return {
                    "workouts_saved": 0,
                    "records_saved": 0,
                    "types": [],
                    "sleep_saved": 0,
                    "dropped": dropped,
                    "validation_ms": validation_ms,
                    "tombstones_received": 0,
                    "tombstones_applied": 0,
                    "tombstones_unresolved": 0,
                    "tombstone_rows_deleted": 0,
                    "tombstone_error_code": None,
                    "unprocessed_count": 1,
                    "processing_error_code": acceptance.error_code or "window_not_accepted",
                }
            if require_terminal_receipt:
                db_session.flush()
            else:
                db_session.commit()
            return {
                "workouts_saved": 0,
                "records_saved": 0,
                "types": [],
                "sleep_saved": 0,
                "dropped": [],
                "validation_ms": validation_ms,
                "tombstones_received": 0,
                "tombstones_applied": 0,
                "tombstones_unresolved": 0,
                "tombstone_rows_deleted": 0,
                "tombstone_error_code": None,
                "unprocessed_count": 0,
                "processing_error_code": None,
            }

        invalid_tombstones = [item for item in dropped if item["collection"] == "deletions"]
        if invalid_tombstones:
            # A malformed deletion cannot be silently salvaged around additions:
            # the phone must retain the whole page at its last acknowledged anchor.
            return {
                "workouts_saved": 0,
                "records_saved": 0,
                "types": [],
                "sleep_saved": 0,
                "dropped": dropped,
                "validation_ms": validation_ms,
                "tombstones_received": len(request.data.deletions) + len(invalid_tombstones),
                "tombstones_applied": 0,
                "tombstones_unresolved": len(invalid_tombstones),
                "tombstone_rows_deleted": 0,
                "tombstone_error_code": "invalid_tombstone",
                "unprocessed_count": 0,
                "processing_error_code": None,
            }

        if request.data.deletions:
            # The downstream dashboard currently has no stable-ID retraction
            # contract for canonical metric assertions or workouts. Accepting a
            # server-side delete would therefore leave stale canonical facts.
            # Keep every deletion-bearing batch at the phone checkpoint until an
            # end-to-end immutable tombstone feed exists.
            return {
                "workouts_saved": 0,
                "records_saved": 0,
                "types": [],
                "sleep_saved": 0,
                "dropped": dropped,
                "validation_ms": validation_ms,
                "tombstones_received": len(request.data.deletions),
                "tombstones_applied": 0,
                "tombstones_unresolved": len(request.data.deletions),
                "tombstone_rows_deleted": 0,
                "tombstone_error_code": "deletion_projection_unsupported",
                "unprocessed_count": 0,
                "processing_error_code": None,
            }

        if require_terminal_receipt and dropped:
            # Receipt-mode batches are atomic: salvage remains available to
            # legacy tasks, but a terminal receipt can never follow a partial
            # write whose invalid siblings remain on the phone.
            return {
                "workouts_saved": 0,
                "records_saved": 0,
                "types": [],
                "sleep_saved": 0,
                "dropped": dropped,
                "validation_ms": validation_ms,
                "tombstones_received": 0,
                "tombstones_applied": 0,
                "tombstones_unresolved": 0,
                "tombstone_rows_deleted": 0,
                "tombstone_error_code": None,
                "unprocessed_count": 0,
                "processing_error_code": "validation_failed",
            }

        records_without_source_id = [record for record in request.data.records if not record.id]
        if require_terminal_receipt and records_without_source_id:
            return {
                "workouts_saved": 0,
                "records_saved": 0,
                "types": [],
                "sleep_saved": 0,
                "dropped": dropped,
                "validation_ms": validation_ms,
                "tombstones_received": 0,
                "tombstones_applied": 0,
                "tombstones_unresolved": 0,
                "tombstone_rows_deleted": 0,
                "tombstone_error_code": None,
                "unprocessed_count": len(records_without_source_id),
                "processing_error_code": "metric_source_id_required",
            }

        workouts_without_source_id = [workout for workout in request.data.workouts if not workout.id]
        if require_terminal_receipt and workouts_without_source_id:
            return {
                "workouts_saved": 0,
                "records_saved": 0,
                "types": [],
                "sleep_saved": 0,
                "dropped": dropped,
                "validation_ms": validation_ms,
                "tombstones_received": 0,
                "tombstones_applied": 0,
                "tombstones_unresolved": 0,
                "tombstone_rows_deleted": 0,
                "tombstone_error_code": None,
                "unprocessed_count": len(workouts_without_source_id),
                "processing_error_code": "workout_source_id_required",
            }

        unsupported_metrics = [
            record
            for record in request.data.records
            if record.type is None or get_series_type_from_metric_type(record.type) is None
        ]
        if require_terminal_receipt and unsupported_metrics:
            # A syntactically valid but unmapped metric used to be silently
            # skipped and then marked lineage-complete. Keep the entire batch
            # unacknowledged so the phone's checkpoint cannot pass it.
            return {
                "workouts_saved": 0,
                "records_saved": 0,
                "types": [],
                "sleep_saved": 0,
                "dropped": dropped,
                "validation_ms": validation_ms,
                "tombstones_received": 0,
                "tombstones_applied": 0,
                "tombstones_unresolved": 0,
                "tombstone_rows_deleted": 0,
                "tombstone_error_code": None,
                "unprocessed_count": len(unsupported_metrics),
                "processing_error_code": "unsupported_metric_type",
            }

        unsupported_workout_statistics = [
            statistic
            for workout in request.data.workouts
            for statistic in workout.values or []
            if not _is_supported_workout_statistic(statistic)
        ]
        if require_terminal_receipt and unsupported_workout_statistics:
            return {
                "workouts_saved": 0,
                "records_saved": 0,
                "types": [],
                "sleep_saved": 0,
                "dropped": dropped,
                "validation_ms": validation_ms,
                "tombstones_received": 0,
                "tombstones_applied": 0,
                "tombstones_unresolved": 0,
                "tombstone_rows_deleted": 0,
                "tombstone_error_code": None,
                "unprocessed_count": len(unsupported_workout_statistics),
                "processing_error_code": "unsupported_workout_statistic_type",
            }

        user_uuid = UUID(user_id)
        if require_terminal_receipt and request.data.sleep:
            if batch_id is None:
                return {
                    "workouts_saved": 0,
                    "records_saved": 0,
                    "types": [],
                    "sleep_saved": 0,
                    "dropped": dropped,
                    "validation_ms": validation_ms,
                    "tombstones_received": 0,
                    "tombstones_applied": 0,
                    "tombstones_unresolved": 0,
                    "tombstone_rows_deleted": 0,
                    "tombstone_error_code": None,
                    "unprocessed_count": len(request.data.sleep),
                    "processing_error_code": "batch_id_invalid",
                }
            try:
                sleep_batch_id = UUID(batch_id)
            except ValueError:
                return {
                    "workouts_saved": 0,
                    "records_saved": 0,
                    "types": [],
                    "sleep_saved": 0,
                    "dropped": dropped,
                    "validation_ms": validation_ms,
                    "tombstones_received": 0,
                    "tombstones_applied": 0,
                    "tombstones_unresolved": 0,
                    "tombstone_rows_deleted": 0,
                    "tombstone_error_code": None,
                    "unprocessed_count": len(request.data.sleep),
                    "processing_error_code": "batch_id_invalid",
                }
            sleep_stage = sdk_sleep_inbox_service.stage(
                db_session,
                user_id=user_uuid,
                provider=request.provider,
                batch_id=sleep_batch_id,
                records=request.data.sleep,
            )
            if sleep_stage.error_code:
                return {
                    "workouts_saved": 0,
                    "records_saved": 0,
                    "types": [],
                    "sleep_saved": 0,
                    "dropped": dropped,
                    "validation_ms": validation_ms,
                    "tombstones_received": 0,
                    "tombstones_applied": 0,
                    "tombstones_unresolved": 0,
                    "tombstone_rows_deleted": 0,
                    "tombstone_error_code": None,
                    "unprocessed_count": len(request.data.sleep),
                    "processing_error_code": sleep_stage.error_code,
                }
            sleep_saved = sleep_stage.staged_count

        # Process workouts in batch
        workout_bundles = list(self._build_workout_bundles(request, user_id))
        if workout_bundles:
            records = [record for record, _, _ in workout_bundles]
            details_by_id = {detail.record_id: detail for _, detail, _ in workout_bundles}
            # Flatten all time series samples from all workouts into a single list
            time_series_samples = [sample for _, _, samples in workout_bundles for sample in samples]

            # Bulk create records - returns only IDs that were actually inserted
            inserted_ids = self.event_record_service.bulk_create(db_session, records)
            db_session.flush()

            # Filter details to only those records that were actually inserted (avoid FK violation)
            details_to_insert = [details_by_id[rid] for rid in inserted_ids if rid in details_by_id]

            # Bulk create details (requires event_record to exist due to FK)
            if details_to_insert:
                self.event_record_service.bulk_create_details(db_session, details_to_insert, detail_type="workout")
            workouts_saved = len(inserted_ids)

            # Bulk create time series samples
            if time_series_samples:
                self.timeseries_service.bulk_create_samples(db_session, time_series_samples)
                records_saved += len(time_series_samples)
                types.update(sample.series_type.value for sample in time_series_samples)

        # Process time series samples (records)
        samples = self._build_statistic_bundles(request, user_id)
        if samples:
            try:
                sample_writes = self.timeseries_service.bulk_create_samples(db_session, samples)
            except (DataPointSourcePayloadConflictError, DataSourceProvenanceConflictError) as exc:
                error_code = (
                    "metric_source_payload_conflict"
                    if isinstance(exc, DataPointSourcePayloadConflictError)
                    else "source_identity_conflict"
                )
                return {
                    "workouts_saved": 0,
                    "records_saved": 0,
                    "types": [],
                    "sleep_saved": 0,
                    "dropped": dropped,
                    "validation_ms": validation_ms,
                    "tombstones_received": 0,
                    "tombstones_applied": 0,
                    "tombstones_unresolved": 0,
                    "tombstone_rows_deleted": 0,
                    "tombstone_error_code": None,
                    "unprocessed_count": len(samples),
                    "processing_error_code": error_code,
                }
            records_saved += int(sample_writes)
            types.update(sample.series_type.value for sample in samples)

        # Commit all workout and timeseries changes in one transaction
        if require_terminal_receipt:
            db_session.flush()
        else:
            db_session.commit()

        # Process sleep (count sleep segments from input)
        if request.data.sleep and not require_terminal_receipt:
            handle_sleep_data(db_session, request, user_id)
            sleep_saved = len(request.data.sleep)
            db_session.commit()
        # Receipt-backed sleep projection is scheduled by the worker only
        # after its atomic write + terminal-receipt transaction commits.

        return {
            "workouts_saved": workouts_saved,
            "records_saved": records_saved,
            "types": sorted(types),
            "sleep_saved": sleep_saved,
            "dropped": dropped,
            "validation_ms": validation_ms,
            "tombstones_received": 0,
            "tombstones_applied": 0,
            "tombstones_unresolved": 0,
            "tombstone_rows_deleted": 0,
            "tombstone_error_code": None,
            "unprocessed_count": 0,
            "processing_error_code": None,
        }

    def import_data_from_request(
        self,
        db_session: DbSession,
        request_content: str,
        content_type: str,
        user_id: str,
        batch_id: str | None = None,
        require_terminal_receipt: bool = False,
    ) -> UploadDataResponse:
        provider = "unknown"
        try:
            # Parse content based on type
            if "multipart/form-data" in content_type:
                data = self._parse_multipart_content(request_content)
            else:
                data = self._parse_json_content(request_content)

            if not data:
                log_structured(
                    self.log,
                    "warning",
                    "No valid data found in request",
                    action="sdk_validate_data",
                    batch_id=batch_id,
                    user_id=user_id,
                )
                return UploadDataResponse(status_code=400, response="No valid data found", user_id=user_id)

            # Extract incoming counts (best-effort; invalid types must reach validation
            # below rather than raising TypeError here)
            provider = data.get("provider", "unknown")
            inner_data = data.get("data")
            if not isinstance(inner_data, dict):
                inner_data = {}
            records = inner_data.get("records")
            workouts = inner_data.get("workouts")
            sleep = inner_data.get("sleep")
            deletions = inner_data.get("deletions")
            incoming_records = len(records) if isinstance(records, list) else 0
            incoming_workouts = len(workouts) if isinstance(workouts, list) else 0
            incoming_sleep = len(sleep) if isinstance(sleep, list) else 0
            incoming_deletions = len(deletions) if isinstance(deletions, list) else 0

            # Load data and get saved counts
            saved_counts = self.load_data(
                db_session,
                data,
                user_id=user_id,
                batch_id=batch_id,
                require_terminal_receipt=require_terminal_receipt,
            )

            if saved_counts["processing_error_code"]:
                db_session.rollback()
                return UploadDataResponse(
                    status_code=409,
                    response="SDK item quarantined because durable processing is unavailable",
                    user_id=user_id,
                    dropped_count=len(saved_counts.get("dropped") or []) + saved_counts["unprocessed_count"],
                    processing_error_code=saved_counts["processing_error_code"],
                )

            if saved_counts["tombstones_unresolved"]:
                db_session.rollback()
                return UploadDataResponse(
                    status_code=409,
                    response="HealthKit deletion quarantined until end-to-end deletion projection is supported",
                    user_id=user_id,
                    dropped_count=len(saved_counts.get("dropped") or []),
                    tombstones_received=saved_counts["tombstones_received"],
                    tombstones_applied=saved_counts["tombstones_applied"],
                    tombstones_unresolved=saved_counts["tombstones_unresolved"],
                    tombstone_rows_deleted=saved_counts["tombstone_rows_deleted"],
                    tombstone_error_code=saved_counts["tombstone_error_code"],
                    processing_error_code=saved_counts["processing_error_code"],
                )

            connection = self.user_connection_repo.get_by_user_and_provider(db_session, UUID(user_id), provider)
            if connection:
                self.user_connection_repo.update_last_synced_at(
                    db_session,
                    connection,
                    commit=not require_terminal_receipt,
                )

            # Log detailed processing results
            log_structured(
                self.log,
                "info",
                f"{provider.capitalize()} data import completed",
                provider=f"{provider}",
                action=f"{provider}_sdk_import_complete",
                batch_id=batch_id,
                user_id=user_id,
                incoming_records=incoming_records,
                incoming_workouts=incoming_workouts,
                incoming_sleep=incoming_sleep,
                incoming_deletions=incoming_deletions,
                records_saved=saved_counts["records_saved"],
                workouts_saved=saved_counts["workouts_saved"],
                sleep_saved=saved_counts["sleep_saved"],
                tombstones_applied=saved_counts["tombstones_applied"],
                tombstone_rows_deleted=saved_counts["tombstone_rows_deleted"],
                validation_ms=saved_counts["validation_ms"],
            )

            dropped = saved_counts.get("dropped") or []
            if dropped:
                # Partial success: some records failed per-record validation. The good
                # ones are already saved above; report the exact field errors to Sentry
                # (PII-free: loc/msg/type) so we keep full visibility into what was lost.
                with sentry_sdk.push_scope() as scope:
                    scope.set_level("warning")
                    scope.set_context(
                        "dropped_records",
                        {
                            "batch_id": batch_id,
                            "user_id": user_id,
                            "provider": provider,
                            "dropped_count": len(dropped),
                            "errors": dropped[:20],
                        },
                    )
                    sentry_sdk.capture_message(f"{provider} SDK payload: dropped invalid records, kept the rest")
                # Compose the full location (collection[index].field) so the log one-liner
                # says which record failed, not just the field — the per-record `loc` is
                # relative to a single record. The full breakdown is in the Sentry context.
                first = dropped[0]
                first_loc = f"{first['collection']}[{first['index']}]"
                if first.get("loc"):
                    first_loc += f".{first['loc']}"
                log_structured(
                    self.log,
                    "warning",
                    f"{provider.capitalize()} SDK dropped invalid records",
                    provider=f"{provider}",
                    action=f"{provider}_sdk_records_dropped",
                    batch_id=batch_id,
                    user_id=user_id,
                    dropped_count=len(dropped),
                    first_error_loc=first_loc,
                    first_error_msg=first["msg"],
                )

            return UploadDataResponse(
                status_code=200,
                response="Import successful",
                user_id=user_id,
                dropped_count=len(dropped) + saved_counts["unprocessed_count"],
                records_saved=saved_counts["records_saved"],
                types=saved_counts["types"],
                workouts_saved=saved_counts["workouts_saved"],
                sleep_saved=saved_counts["sleep_saved"],
                tombstones_received=saved_counts["tombstones_received"],
                tombstones_applied=saved_counts["tombstones_applied"],
                tombstones_unresolved=saved_counts["tombstones_unresolved"],
                tombstone_rows_deleted=saved_counts["tombstone_rows_deleted"],
                tombstone_error_code=saved_counts["tombstone_error_code"],
                processing_error_code=saved_counts["processing_error_code"],
            )

        except ValidationError as e:
            db_session.rollback()
            # Reached ONLY when the envelope itself is invalid (provider/sdkVersion/
            # syncTimestamp/data shape) — _parse_sync_request re-raised because nothing was
            # salvageable. Per-record failures never reach here: they are collected inside
            # load_data and handled above as a partial success. Report + 400 the whole batch.
            errors = e.errors()
            first = errors[0] if errors else {}
            # Keep only loc/msg/type. Pydantic's raw exception string, `input`,
            # `ctx`, and traceback can all include submitted health values.
            safe_errors = [{key: err[key] for key in ("loc", "msg", "type") if key in err} for err in errors[:20]]
            sanitized_error = ValueError(
                f"SDK payload validation failed: {json.dumps(safe_errors, default=str, separators=(',', ':'))}"
            )
            log_and_capture_error(
                sanitized_error,
                self.log,
                f"{provider} SDK payload failed validation for user {user_id}",
                extra={
                    "user_id": user_id,
                    "batch_id": batch_id,
                    "provider": provider,
                    "error_count": len(errors),
                    "errors": safe_errors,
                },
                include_exc_info=False,
            )
            log_structured(
                self.log,
                "warning",
                f"{provider.capitalize()} SDK payload validation failed",
                provider=f"{provider}",
                action=f"{provider}_sdk_validation_failed",
                batch_id=batch_id,
                user_id=user_id,
                error_count=len(errors),
                first_error_loc=".".join(str(x) for x in first.get("loc", [])),
                first_error_msg=first.get("msg"),
            )
            return UploadDataResponse(
                status_code=400,
                response=f"Validation failed: {first.get('msg', 'invalid payload')}",
                user_id=user_id,
            )

        except Exception as e:
            db_session.rollback()
            sanitized_error = RuntimeError(f"SDK import failed ({type(e).__name__})")
            log_and_capture_error(
                sanitized_error,
                self.log,
                f"Import failed for user {user_id}",
                extra={
                    "user_id": user_id,
                    "batch_id": batch_id,
                    "provider": provider,
                    "error_type": type(e).__name__,
                },
                include_exc_info=False,
            )
            log_structured(
                self.log,
                "error",
                f"Import failed for user {user_id}",
                provider=f"{provider}",
                action=f"{provider}_sdk_import_failed",
                batch_id=batch_id,
                user_id=user_id,
                error_type=type(e).__name__,
            )
            return UploadDataResponse(
                status_code=400,
                response="Import failed",
                user_id=user_id,
            )

    def _parse_multipart_content(self, content: str) -> dict | None:
        """Parse multipart form data to extract JSON."""
        # Try to find JSON start with various field patterns
        json_start = content.find('{\n  "data"')
        if json_start == -1:
            json_start = content.find('{"data"')
        if json_start == -1:
            return None

        brace_count = 0
        json_end = json_start
        for i, char in enumerate(content[json_start:], json_start):
            if char == "{":
                brace_count += 1
            elif char == "}":
                brace_count -= 1
                if brace_count == 0:
                    json_end = i
                    break

        if brace_count != 0:
            return None

        json_str = content[json_start : json_end + 1]
        return json.loads(json_str)

    def _parse_json_content(self, content: str) -> dict | None:
        """Parse JSON content directly."""
        try:
            return json.loads(content)
        except json.JSONDecodeError:
            return None


import_service = ImportService(log=getLogger(__name__))
