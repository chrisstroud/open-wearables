from datetime import date
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, status

from app.database import DbSession
from app.repositories.apple_health_daily_summary_repository import DailySummaryConflictError
from app.schemas.responses.daily_summary import DailySummaryPage, SleepSummaryPage, WorkoutSummaryPage
from app.services import ApiKeyDep
from app.services.apple_health_daily_summary_service import apple_health_daily_summary_service

router = APIRouter()


@router.get(
    "/users/{user_id}/daily-summaries",
    summary="Get current Apple Health daily summary revisions",
)
def get_daily_summaries(
    user_id: UUID,
    start_date: date,
    end_date: date,
    db: DbSession,
    _api_key: ApiKeyDep,
    types: Annotated[list[str], Query()] = [],
    cursor: str | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
) -> DailySummaryPage:
    """Return current daily-summary heads in a half-open local-date range."""
    if start_date >= end_date:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail="start_date must precede end_date",
        )
    try:
        return apple_health_daily_summary_service.list_metrics(
            db,
            user_id=user_id,
            start_date=start_date,
            end_date=end_date,
            series_types=types,
            cursor=cursor,
            limit=limit,
        )
    except DailySummaryConflictError as exc:
        raise HTTPException(
            status_code=status.HTTP_422_UNPROCESSABLE_ENTITY,
            detail=exc.error_code,
        ) from exc


@router.get(
    "/users/{user_id}/daily-summaries/sleep",
    summary="Get current Apple Health sleep-summary revisions",
)
def get_sleep_summaries(
    user_id: UUID,
    start_date: date,
    end_date: date,
    db: DbSession,
    _api_key: ApiKeyDep,
    cursor: str | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
) -> SleepSummaryPage:
    if start_date >= end_date:
        raise HTTPException(status_code=422, detail="start_date must precede end_date")
    try:
        return apple_health_daily_summary_service.list_sleep(
            db,
            user_id=user_id,
            start_date=start_date,
            end_date=end_date,
            cursor=cursor,
            limit=limit,
        )
    except DailySummaryConflictError as exc:
        raise HTTPException(status_code=422, detail=exc.error_code) from exc


@router.get(
    "/users/{user_id}/daily-summaries/workouts",
    summary="Get current Apple Health workout-summary revisions",
)
def get_workout_summaries(
    user_id: UUID,
    start_date: date,
    end_date: date,
    db: DbSession,
    _api_key: ApiKeyDep,
    activity_types: Annotated[list[str], Query()] = [],
    cursor: str | None = None,
    limit: Annotated[int, Query(ge=1, le=100)] = 50,
) -> WorkoutSummaryPage:
    if start_date >= end_date:
        raise HTTPException(status_code=422, detail="start_date must precede end_date")
    try:
        return apple_health_daily_summary_service.list_workouts(
            db,
            user_id=user_id,
            start_date=start_date,
            end_date=end_date,
            activity_types=activity_types,
            cursor=cursor,
            limit=limit,
        )
    except DailySummaryConflictError as exc:
        raise HTTPException(status_code=422, detail=exc.error_code) from exc
