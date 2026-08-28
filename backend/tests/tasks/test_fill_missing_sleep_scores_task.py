"""Regression coverage for internal sleep scores retaining source lineage."""

from contextlib import nullcontext
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

from sqlalchemy.orm import Session

from app.integrations.celery.tasks.fill_missing_sleep_scores_task import fill_missing_sleep_scores
from app.models import HealthScore
from app.schemas.enums import HealthScoreCategory, ProviderName
from tests.factories import DataSourceFactory, EventRecordFactory, SleepDetailsFactory, UserFactory


def test_fill_missing_sleep_scores_retains_external_sleep_source(db: Session) -> None:
    user = UserFactory()
    data_source = DataSourceFactory(user=user, provider=ProviderName.APPLE)
    end = datetime.now(timezone.utc) - timedelta(hours=2)
    sleep_record = EventRecordFactory(
        data_source=data_source,
        category="sleep",
        type="sleep",
        start_datetime=end - timedelta(hours=8),
        end_datetime=end,
        zone_offset="+00:00",
    )
    SleepDetailsFactory(event_record=sleep_record, is_nap=False)

    with patch(
        "app.integrations.celery.tasks.fill_missing_sleep_scores_task.SessionLocal",
        return_value=nullcontext(db),
    ):
        result = fill_missing_sleep_scores()

    assert result == {"saved": 1, "skipped": 0}
    score = (
        db.query(HealthScore)
        .filter(
            HealthScore.user_id == user.id,
            HealthScore.category == HealthScoreCategory.SLEEP,
            HealthScore.provider == ProviderName.INTERNAL,
        )
        .one()
    )
    assert score.sleep_record_id == sleep_record.id
    assert score.data_source_id == data_source.id
