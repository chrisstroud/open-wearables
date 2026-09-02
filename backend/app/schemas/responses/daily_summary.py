from pydantic import BaseModel

from app.schemas.providers.mobile_sdk import (
    AppleHealthSleepSummary,
    AppleHealthWorkoutSummary,
    DailySummary,
)


class DailySummaryPagination(BaseModel):
    next_cursor: str | None
    previous_cursor: str | None = None
    has_more: bool
    total_count: int


class DailySummaryPage(BaseModel):
    data: list[DailySummary]
    pagination: DailySummaryPagination


class SleepSummaryPage(BaseModel):
    data: list[AppleHealthSleepSummary]
    pagination: DailySummaryPagination


class WorkoutSummaryPage(BaseModel):
    data: list[AppleHealthWorkoutSummary]
    pagination: DailySummaryPagination
