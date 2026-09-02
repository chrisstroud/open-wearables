import base64
import json
from datetime import date, datetime
from decimal import Decimal
from typing import Literal, TypeAlias
from uuid import UUID, uuid4

from sqlalchemy import and_, func, or_, text

from app.database import DbSession
from app.models import AppleHealthDailySummary, SDKClientInstallation
from app.schemas.providers.mobile_sdk import (
    AppleHealthSleepSummary,
    AppleHealthWorkoutSummary,
    DailySummary,
)

SummaryKind: TypeAlias = Literal["metric", "sleep", "workout"]
SummaryItem: TypeAlias = DailySummary | AppleHealthSleepSummary | AppleHealthWorkoutSummary


class DailySummaryConflictError(ValueError):
    """A summary revision cannot be joined to the exact stored lineage."""

    def __init__(self, error_code: str):
        super().__init__(error_code)
        self.error_code = error_code


def _json_value(value: object) -> object:
    if isinstance(value, Decimal):
        # JSONB's Python adapter does not encode Decimal natively. Retain its
        # exact canonical text internally; the response schema serializes it as
        # a finite JSON number for the dashboard boundary.
        return str(value)
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    if isinstance(value, list):
        return [_json_value(item) for item in value]
    if isinstance(value, dict):
        return {key: _json_value(item) for key, item in value.items()}
    return value


def _summary_kind(summary: SummaryItem) -> SummaryKind:
    if isinstance(summary, DailySummary):
        return "metric"
    if isinstance(summary, AppleHealthSleepSummary):
        return "sleep"
    return "workout"


def _stable_key(summary: SummaryItem) -> str:
    return summary.event_key if isinstance(summary, AppleHealthWorkoutSummary) else summary.summary_key


def _series_type(summary: SummaryItem) -> str | None:
    if isinstance(summary, DailySummary):
        return summary.series_type
    if isinstance(summary, AppleHealthWorkoutSummary):
        return summary.activity_type
    return None


def _payload(summary: SummaryItem) -> dict:
    payload = _json_value(summary.model_dump(mode="python"))
    if not isinstance(payload, dict):
        raise DailySummaryConflictError("daily_summary_payload_invalid")
    return payload


def _stable_identity(summary: SummaryItem) -> tuple[object, ...]:
    if isinstance(summary, DailySummary):
        return (
            "metric",
            summary.healthkit_type,
            summary.series_type,
            summary.local_date,
            summary.source_scope,
            summary.canonical_unit,
        )
    if isinstance(summary, AppleHealthSleepSummary):
        return ("sleep", summary.local_date, summary.source_scope)
    return ("workout", summary.local_date, summary.source_scope, summary.activity_type)


def _row_identity(row: AppleHealthDailySummary) -> tuple[object, ...]:
    payload = row.payload
    if row.summary_kind == "metric":
        return (
            "metric",
            payload.get("healthkit_type"),
            payload.get("series_type"),
            row.local_date,
            payload.get("source_scope"),
            payload.get("canonical_unit"),
        )
    if row.summary_kind == "sleep":
        return ("sleep", row.local_date, payload.get("source_scope"))
    return ("workout", row.local_date, payload.get("source_scope"), payload.get("activity_type"))


def encode_daily_summary_cursor(local_date: date, summary_kind: str, stable_key: str) -> str:
    payload = json.dumps(
        {"date": local_date.isoformat(), "kind": summary_kind, "key": stable_key},
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    return base64.urlsafe_b64encode(payload).decode().rstrip("=")


def decode_daily_summary_cursor(cursor: str) -> tuple[date, str, str]:
    try:
        padding = "=" * (-len(cursor) % 4)
        payload = json.loads(base64.urlsafe_b64decode(cursor + padding))
        cursor_date = date.fromisoformat(payload["date"])
        summary_kind = str(payload["kind"])
        stable_key = str(payload["key"])
    except (ValueError, TypeError, KeyError, json.JSONDecodeError) as exc:
        raise DailySummaryConflictError("daily_summary_cursor_invalid") from exc
    if summary_kind not in {"metric", "sleep", "workout"}:
        raise DailySummaryConflictError("daily_summary_cursor_invalid")
    if len(stable_key) != 64 or any(character not in "0123456789abcdef" for character in stable_key):
        raise DailySummaryConflictError("daily_summary_cursor_invalid")
    return cursor_date, summary_kind, stable_key


class AppleHealthDailySummaryRepository:
    """Owns exact revision acceptance and current-head reads."""

    def accept_batch(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        installation_id: UUID,
        installation_generation: int,
        health_evidence_generation: int,
        batch_id: UUID,
        summaries: list[SummaryItem],
    ) -> int:
        installation = (
            db_session.query(SDKClientInstallation)
            .filter(
                SDKClientInstallation.id == installation_id,
                SDKClientInstallation.user_id == user_id,
                SDKClientInstallation.status == "active",
                SDKClientInstallation.generation == installation_generation,
                SDKClientInstallation.health_evidence_generation == health_evidence_generation,
            )
            .with_for_update()
            .one_or_none()
        )
        if installation is None:
            raise DailySummaryConflictError("daily_summary_revision_authority_conflict")

        identities = [(_summary_kind(summary), _stable_key(summary)) for summary in summaries]
        if len(identities) != len(set(identities)):
            raise DailySummaryConflictError("daily_summary_key_duplicated")

        for summary in sorted(summaries, key=lambda item: (_summary_kind(item), _stable_key(item))):
            kind = _summary_kind(summary)
            stable_key = _stable_key(summary)
            db_session.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:identity, 0))"),
                {"identity": f"apple-health-summary:{user_id}:{kind}:{stable_key}"},
            )
            current = (
                db_session.query(AppleHealthDailySummary)
                .filter(
                    AppleHealthDailySummary.user_id == user_id,
                    AppleHealthDailySummary.summary_kind == kind,
                    AppleHealthDailySummary.stable_key == stable_key,
                    AppleHealthDailySummary.is_current.is_(True),
                )
                .with_for_update()
                .one_or_none()
            )
            payload = _payload(summary)
            if current is not None and current.revision_id == summary.revision_id:
                if current.payload != payload:
                    raise DailySummaryConflictError("daily_summary_revision_payload_conflict")
                if (
                    current.installation_id != installation_id
                    or current.health_evidence_generation != health_evidence_generation
                    or current.installation_generation > installation_generation
                ):
                    raise DailySummaryConflictError("daily_summary_revision_authority_conflict")
                continue

            if current is None:
                if summary.supersedes_revision_id is not None:
                    raise DailySummaryConflictError("daily_summary_supersedes_missing")
            else:
                if (
                    current.installation_id != installation_id
                    or current.health_evidence_generation != health_evidence_generation
                    or current.installation_generation > installation_generation
                ):
                    raise DailySummaryConflictError("daily_summary_revision_authority_conflict")
                if summary.supersedes_revision_id != current.revision_id:
                    raise DailySummaryConflictError("daily_summary_supersedes_mismatch")
                if _row_identity(current) != _stable_identity(summary):
                    raise DailySummaryConflictError("daily_summary_identity_drift")
                current.is_current = False

            db_session.add(
                AppleHealthDailySummary(
                    id=uuid4(),
                    user_id=user_id,
                    installation_id=installation_id,
                    installation_generation=installation_generation,
                    health_evidence_generation=health_evidence_generation,
                    batch_id=batch_id,
                    summary_kind=kind,
                    stable_key=stable_key,
                    schema_version=summary.schema_version,
                    revision_id=summary.revision_id,
                    supersedes_revision_id=summary.supersedes_revision_id,
                    local_date=summary.local_date,
                    timezone=summary.timezone,
                    timezone_boundary_version=summary.timezone_boundary_version,
                    series_type=_series_type(summary),
                    contributor_set_digest=summary.contributor_set_digest,
                    input_set_digest=summary.input_set_digest,
                    computed_at=summary.computed_at,
                    payload=payload,
                    is_current=True,
                )
            )
            db_session.flush()
        return len(summaries)

    def list_current(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        start_date: date,
        end_date: date,
        summary_kind: SummaryKind,
        series_types: list[str],
        cursor: str | None,
        limit: int,
    ) -> tuple[list[AppleHealthDailySummary], bool, int]:
        filters = [
            AppleHealthDailySummary.user_id == user_id,
            AppleHealthDailySummary.summary_kind == summary_kind,
            AppleHealthDailySummary.is_current.is_(True),
            AppleHealthDailySummary.local_date >= start_date,
            AppleHealthDailySummary.local_date < end_date,
        ]
        if series_types:
            filters.append(AppleHealthDailySummary.series_type.in_(sorted(set(series_types))))
        total_count = db_session.query(func.count(AppleHealthDailySummary.id)).filter(*filters).scalar() or 0
        query = db_session.query(AppleHealthDailySummary).filter(*filters)
        if cursor is not None:
            cursor_date, cursor_kind, stable_key = decode_daily_summary_cursor(cursor)
            if cursor_kind != summary_kind:
                raise DailySummaryConflictError("daily_summary_cursor_invalid")
            query = query.filter(
                or_(
                    AppleHealthDailySummary.local_date > cursor_date,
                    and_(
                        AppleHealthDailySummary.local_date == cursor_date,
                        AppleHealthDailySummary.stable_key > stable_key,
                    ),
                )
            )
        rows = (
            query.order_by(
                AppleHealthDailySummary.local_date.asc(),
                AppleHealthDailySummary.stable_key.asc(),
            )
            .limit(limit + 1)
            .all()
        )
        return rows[:limit], len(rows) > limit, int(total_count)


apple_health_daily_summary_repository = AppleHealthDailySummaryRepository()
