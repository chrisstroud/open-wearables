from datetime import date
from typing import TypeVar
from uuid import UUID

from pydantic import BaseModel

from app.database import DbSession
from app.repositories.apple_health_daily_summary_repository import (
    SummaryKind,
    apple_health_daily_summary_repository,
    encode_daily_summary_cursor,
)
from app.schemas.providers.mobile_sdk import (
    AppleHealthSleepSummary,
    AppleHealthWorkoutSummary,
    DailySummary,
)
from app.schemas.responses.daily_summary import (
    DailySummaryPage,
    DailySummaryPagination,
    SleepSummaryPage,
    WorkoutSummaryPage,
)

SummaryModel = TypeVar("SummaryModel", bound=BaseModel)


class AppleHealthDailySummaryService:
    """Projects immutable summary heads for dashboard reads."""

    def _list_current(
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
        model: type[SummaryModel],
    ) -> tuple[list[SummaryModel], DailySummaryPagination]:
        rows, has_more, total_count = apple_health_daily_summary_repository.list_current(
            db_session,
            user_id=user_id,
            start_date=start_date,
            end_date=end_date,
            summary_kind=summary_kind,
            series_types=series_types,
            cursor=cursor,
            limit=limit,
        )
        data = [model.model_validate(row.payload) for row in rows]
        next_cursor = (
            encode_daily_summary_cursor(rows[-1].local_date, rows[-1].summary_kind, rows[-1].stable_key)
            if has_more and rows
            else None
        )
        return data, DailySummaryPagination(
            next_cursor=next_cursor,
            has_more=has_more,
            total_count=total_count,
        )

    def list_metrics(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        start_date: date,
        end_date: date,
        series_types: list[str],
        cursor: str | None,
        limit: int,
    ) -> DailySummaryPage:
        data, pagination = self._list_current(
            db_session,
            user_id=user_id,
            start_date=start_date,
            end_date=end_date,
            summary_kind="metric",
            series_types=series_types,
            cursor=cursor,
            limit=limit,
            model=DailySummary,
        )
        return DailySummaryPage(data=data, pagination=pagination)

    def list_sleep(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        start_date: date,
        end_date: date,
        cursor: str | None,
        limit: int,
    ) -> SleepSummaryPage:
        data, pagination = self._list_current(
            db_session,
            user_id=user_id,
            start_date=start_date,
            end_date=end_date,
            summary_kind="sleep",
            series_types=[],
            cursor=cursor,
            limit=limit,
            model=AppleHealthSleepSummary,
        )
        return SleepSummaryPage(data=data, pagination=pagination)

    def list_workouts(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        start_date: date,
        end_date: date,
        activity_types: list[str],
        cursor: str | None,
        limit: int,
    ) -> WorkoutSummaryPage:
        data, pagination = self._list_current(
            db_session,
            user_id=user_id,
            start_date=start_date,
            end_date=end_date,
            summary_kind="workout",
            series_types=activity_types,
            cursor=cursor,
            limit=limit,
            model=AppleHealthWorkoutSummary,
        )
        return WorkoutSummaryPage(data=data, pagination=pagination)


apple_health_daily_summary_service = AppleHealthDailySummaryService()
