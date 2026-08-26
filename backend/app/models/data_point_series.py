from uuid import UUID
from datetime import datetime

from sqlalchemy import Index, text
from sqlalchemy.orm import Mapped

from app.database import BaseDbModel
from app.mappings import (
    FKDataSource,
    FKSeriesTypeDefinition,
    PrimaryKey,
    numeric_10_3,
    str_10,
    str_100,
)


class DataPointSeries(BaseDbModel):
    """Unified time-series data points for device metrics (heart rate, steps, energy, etc.)."""

    __tablename__ = "data_point_series"
    __table_args__ = (
        Index(
            "uq_data_point_series_source_type_external_id",
            "data_source_id",
            "series_type_definition_id",
            "external_id",
            unique=True,
            postgresql_where=text("external_id IS NOT NULL"),
        ),
        Index(
            "uq_data_point_series_source_type_time_legacy",
            "data_source_id",
            "series_type_definition_id",
            "recorded_at",
            unique=True,
            postgresql_where=text("external_id IS NULL"),
        ),
    )

    id: Mapped[PrimaryKey[UUID]]
    external_id: Mapped[str_100 | None]
    data_source_id: Mapped[FKDataSource]
    recorded_at: Mapped[datetime]
    zone_offset: Mapped[str_10 | None]
    value: Mapped[numeric_10_3]
    series_type_definition_id: Mapped[FKSeriesTypeDefinition]
    is_daily_total: Mapped[bool | None] # True = pre-aggregated daily total; False = granular intraday samples
