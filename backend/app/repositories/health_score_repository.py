from datetime import date, datetime, timezone
from typing import Any, cast
from uuid import UUID

from sqlalchemy import and_, asc, desc, tuple_
from sqlalchemy.dialects.postgresql import insert

from app.database import DbSession
from app.models import DataSource, EventRecord, HealthScore
from app.repositories.health_write_authority import (
    HealthWriteAuthorityError,
    require_health_write_authorities,
    require_health_write_authority,
)
from app.repositories.repositories import CrudRepository
from app.schemas.enums import HealthScoreCategory
from app.schemas.model_crud.activities import HealthScoreCreate, HealthScoreQueryParams, HealthScoreUpdate
from app.utils.pagination import decode_cursor


class HealthScoreRepository(CrudRepository[HealthScore, HealthScoreCreate, HealthScoreUpdate]):
    @staticmethod
    def _require_creation_authorities(db_session: DbSession, creators: list[HealthScoreCreate]) -> None:
        data_source_ids = {creator.data_source_id for creator in creators if creator.data_source_id is not None}
        data_sources = {
            row.id: row
            for row in db_session.query(DataSource).filter(DataSource.id.in_(data_source_ids)).populate_existing().all()
        }
        if set(data_sources) != data_source_ids:
            raise HealthWriteAuthorityError("Health score data source does not exist")

        sleep_record_ids = {creator.sleep_record_id for creator in creators if creator.sleep_record_id is not None}
        sleep_owners = {
            record_id: user_id
            for record_id, user_id in db_session.query(EventRecord.id, DataSource.user_id)
            .join(DataSource, EventRecord.data_source_id == DataSource.id)
            .filter(EventRecord.id.in_(sleep_record_ids))
            .all()
        }
        if set(sleep_owners) != sleep_record_ids:
            raise HealthWriteAuthorityError("Health score sleep record does not exist")

        for creator in creators:
            if creator.data_source_id is not None:
                data_source = data_sources[creator.data_source_id]
                if data_source.user_id != creator.user_id:
                    raise HealthWriteAuthorityError("Health score data source belongs to another user")
            if creator.sleep_record_id is not None and sleep_owners[creator.sleep_record_id] != creator.user_id:
                raise HealthWriteAuthorityError("Health score sleep record belongs to another user")

        require_health_write_authorities(
            db_session,
            ((creator.user_id, creator.provider) for creator in creators),
            allow_internal_maintenance=True,
        )

    def create(self, db_session: DbSession, creator: HealthScoreCreate) -> HealthScore:
        self._require_creation_authorities(db_session, [creator])
        created = super().create(db_session, creator)
        assert created is not None
        return created

    def update(self, db_session: DbSession, originator: HealthScore, updater: HealthScoreUpdate) -> HealthScore:
        require_health_write_authority(
            db_session,
            user_id=originator.user_id,
            provider=originator.provider,
            allow_internal_maintenance=True,
        )
        return super().update(db_session, originator, updater)

    def delete(self, db_session: DbSession, originator: HealthScore) -> HealthScore:
        require_health_write_authority(
            db_session,
            user_id=originator.user_id,
            provider=originator.provider,
            allow_internal_maintenance=True,
        )
        return super().delete(db_session, originator)

    def delete_flush(self, db_session: DbSession, originator: HealthScore) -> None:
        require_health_write_authority(
            db_session,
            user_id=originator.user_id,
            provider=originator.provider,
            allow_internal_maintenance=True,
        )
        super().delete_flush(db_session, originator)

    def get_by_all_components(self, db_session: DbSession, components: list[str]) -> list[HealthScore]:
        """Return health scores whose components JSONB contains all specified keys (?& operator)."""
        return db_session.query(HealthScore).filter(HealthScore.components.has_all(components)).all()

    def get_by_any_component(self, db_session: DbSession, components: list[str]) -> list[HealthScore]:
        """Return health scores whose components JSONB contains any of the specified keys (?| operator)."""
        return db_session.query(HealthScore).filter(HealthScore.components.has_any(components)).all()

    def get_with_filters(
        self,
        db_session: DbSession,
        user_id: UUID,
        params: HealthScoreQueryParams,
    ) -> tuple[list[HealthScore], int]:
        filters = [HealthScore.user_id == user_id]

        if params.category:
            filters.append(HealthScore.category == params.category)
        if params.provider:
            filters.append(HealthScore.provider == params.provider)
        if params.data_source_id:
            filters.append(HealthScore.data_source_id == params.data_source_id)
        if params.start_datetime:
            filters.append(HealthScore.recorded_at >= params.start_datetime)
        if params.end_datetime:
            filters.append(HealthScore.recorded_at < params.end_datetime)

        query = db_session.query(HealthScore).filter(and_(*filters))

        total_count = query.count()
        results = query.order_by(desc(HealthScore.recorded_at)).offset(params.offset).limit(params.limit).all()
        return results, total_count

    def bulk_create(self, db_session: DbSession, creators: list[HealthScoreCreate]) -> None:
        """Bulk insert health scores, doing nothing on conflict with the unique constraint."""
        if not creators:
            return

        self._require_creation_authorities(db_session, creators)

        values = [c.model_dump() for c in creators]

        stmt = insert(HealthScore).values(values).on_conflict_do_nothing()
        db_session.execute(stmt)
        # Caller is responsible for commit — allows batching with other operations

    def get_latest_by_category(
        self,
        db_session: DbSession,
        user_id: UUID,
        category: HealthScoreCategory,
    ) -> HealthScore | None:
        """Return the most recent health score for a given category and user."""
        return (
            db_session.query(HealthScore)
            .filter(HealthScore.user_id == user_id, HealthScore.category == category)
            .order_by(desc(HealthScore.recorded_at))
            .first()
        )

    def delete_for_user_date(
        self,
        db_session: DbSession,
        user_id: UUID,
        score_date: date,
        category: HealthScoreCategory,
        provider: str = "internal",
    ) -> int:
        """Delete health scores matching user/category/provider/date without loading objects.

        Caller is responsible for commit. Returns deleted row count.
        Sleep scores are stored with recorded_at = midnight UTC of the local sleep date.
        """
        midnight = datetime(score_date.year, score_date.month, score_date.day, tzinfo=timezone.utc)
        require_health_write_authority(
            db_session,
            user_id=user_id,
            provider=provider,
            allow_internal_maintenance=True,
        )
        return (
            db_session.query(HealthScore)
            .filter(
                HealthScore.user_id == user_id,
                HealthScore.provider == provider,
                HealthScore.category == category,
                HealthScore.recorded_at == midnight,
            )
            .delete(synchronize_session=False)
        )

    def get_recovery_summaries(
        self,
        db_session: DbSession,
        user_id: UUID,
        start_date: datetime,
        end_date: datetime,
        cursor: str | None,
        limit: int,
    ) -> list[dict[str, Any]]:
        """Get recovery health scores for a date range with cursor-based pagination.

        Returns list of dicts with keys: recovery_date, provider, source, device_model,
        device_type, record_id, recorded_at, recovery_score, resting_heart_rate,
        hrv_rmssd_milli, spo2_percentage.
        Fetches limit+1 rows so callers can detect has_more without a separate COUNT query.
        Ordering matches get_sleep_summaries: ASC by default, DESC when paginating backward.
        """
        # Outer join so scores without a data_source (older rows) still come back.
        query = (
            db_session.query(HealthScore, DataSource)
            .outerjoin(DataSource, HealthScore.data_source_id == DataSource.id)
            .filter(
                HealthScore.user_id == user_id,
                HealthScore.category == HealthScoreCategory.RECOVERY,
                HealthScore.recorded_at >= start_date,
                HealthScore.recorded_at < end_date,
            )
        )

        if cursor:
            cursor_ts, cursor_id, direction = decode_cursor(cursor)
            if direction == "prev":
                query = query.filter(tuple_(HealthScore.recorded_at, HealthScore.id) < (cursor_ts, cursor_id)).order_by(
                    desc(HealthScore.recorded_at), desc(HealthScore.id)
                )
            else:
                query = query.filter(tuple_(HealthScore.recorded_at, HealthScore.id) > (cursor_ts, cursor_id)).order_by(
                    asc(HealthScore.recorded_at), asc(HealthScore.id)
                )
        else:
            query = query.order_by(asc(HealthScore.recorded_at), asc(HealthScore.id))

        rows = query.limit(limit + 1).all()

        return [
            {
                "recovery_date": row.recorded_at.date(),
                "provider": row.provider,
                "source": data_source.source if data_source else None,
                "device_model": data_source.device_model if data_source else None,
                "device_type": data_source.device_type if data_source else None,
                "record_id": row.id,
                "recorded_at": row.recorded_at,
                "recovery_score": int(row.value) if row.value is not None else None,
                "resting_heart_rate": cast(dict, row.components or {}).get("resting_heart_rate", {}).get("value"),
                "hrv_rmssd_milli": cast(dict, row.components or {}).get("hrv_rmssd_milli", {}).get("value"),
                "spo2_percentage": cast(dict, row.components or {}).get("spo2_percentage", {}).get("value"),
            }
            for row, data_source in rows
        ]

    def get_latest_per_category(
        self,
        db_session: DbSession,
        user_id: UUID,
    ) -> list[HealthScore]:
        """Return the most recent score for each category for a given user.

        Uses PostgreSQL DISTINCT ON (category) for efficiency.
        """
        return (
            db_session.query(HealthScore)
            .filter(HealthScore.user_id == user_id)
            .distinct(HealthScore.category)
            .order_by(HealthScore.category, desc(HealthScore.recorded_at))
            .all()
        )
