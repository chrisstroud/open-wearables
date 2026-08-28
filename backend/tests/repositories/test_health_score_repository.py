"""
Tests for HealthScoreRepository.

Tests cover:
- Basic CRUD via CrudRepository base
- get_with_filters: category, provider, date range, user scoping
- get_latest_by_category
- get_latest_per_category
- bulk_create with on_conflict_do_nothing
"""

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from threading import Barrier
from uuid import UUID, uuid4

import pytest
from sqlalchemy.orm import Session, sessionmaker

from app.models import DataSource, HealthScore, User
from app.repositories.health_score_repository import HealthScoreProvenanceConflictError, HealthScoreRepository
from app.repositories.health_write_authority import HealthWriteAuthorityError
from app.schemas.auth import ConnectionStatus
from app.schemas.enums import HealthScoreCategory, ProviderName
from app.schemas.model_crud.activities import HealthScoreCreate, HealthScoreQueryParams
from tests.factories import (
    DataSourceFactory,
    EventRecordFactory,
    HealthScoreFactory,
    UserConnectionFactory,
    UserFactory,
)


@pytest.fixture
def repo() -> HealthScoreRepository:
    return HealthScoreRepository(HealthScore)


class TestHealthScoreRepositoryCreate:
    def test_create(self, db: Session, repo: HealthScoreRepository) -> None:
        data_source = DataSourceFactory(provider=ProviderName.GARMIN)
        score = HealthScoreCreate(
            id=uuid4(),
            user_id=data_source.user_id,
            data_source_id=data_source.id,
            provider=ProviderName.GARMIN,
            category=HealthScoreCategory.SLEEP,
            value=Decimal("82.00"),
            qualifier="GOOD",
            recorded_at=datetime.now(timezone.utc),
        )

        result = repo.create(db, score)

        assert result.id == score.id
        assert result.category == HealthScoreCategory.SLEEP
        assert result.value == Decimal("82.00")
        assert result.qualifier == "GOOD"

    def test_create_duplicate_returns_existing(self, db: Session, repo: HealthScoreRepository) -> None:
        recorded_at = datetime.now(timezone.utc)
        data_source = DataSourceFactory(provider=ProviderName.GARMIN)
        score = HealthScoreCreate(
            id=uuid4(),
            user_id=data_source.user_id,
            data_source_id=data_source.id,
            provider=ProviderName.GARMIN,
            category=HealthScoreCategory.SLEEP,
            value=Decimal("82.00"),
            recorded_at=recorded_at,
        )
        repo.create(db, score)

        duplicate = HealthScoreCreate(
            id=uuid4(),
            user_id=data_source.user_id,
            data_source_id=data_source.id,
            provider=ProviderName.GARMIN,
            category=HealthScoreCategory.SLEEP,
            value=Decimal("90.00"),
            recorded_at=recorded_at,
        )
        result = repo.create(db, duplicate)

        assert result.id == score.id
        assert result.value == Decimal("82.00")

    def test_create_rejects_data_source_from_different_provider(
        self,
        db: Session,
        repo: HealthScoreRepository,
    ) -> None:
        user = UserFactory()
        data_source = DataSourceFactory(user=user, provider=ProviderName.GARMIN)
        score = HealthScoreCreate(
            id=uuid4(),
            user_id=user.id,
            data_source_id=data_source.id,
            provider=ProviderName.WHOOP,
            category=HealthScoreCategory.RECOVERY,
            value=Decimal("82.00"),
            recorded_at=datetime.now(timezone.utc),
        )

        with pytest.raises(HealthWriteAuthorityError, match="another provider"):
            repo.create(db, score)

    def test_create_allows_internal_sleep_score_to_retain_external_source_lineage(
        self,
        db: Session,
        repo: HealthScoreRepository,
    ) -> None:
        user = UserFactory()
        data_source = DataSourceFactory(user=user, provider=ProviderName.APPLE)
        sleep_record = EventRecordFactory(data_source=data_source, category="sleep")
        score = HealthScoreCreate(
            id=uuid4(),
            user_id=user.id,
            data_source_id=data_source.id,
            provider=ProviderName.INTERNAL,
            category=HealthScoreCategory.SLEEP,
            value=Decimal("82.00"),
            sleep_record_id=sleep_record.id,
            recorded_at=datetime.now(timezone.utc),
        )

        created = repo.create(db, score)

        assert created.provider == ProviderName.INTERNAL
        assert created.data_source_id == data_source.id
        assert created.sleep_record_id == sleep_record.id


class TestHealthScoreRepositoryGetWithFilters:
    def test_filter_by_category(self, db: Session, repo: HealthScoreRepository) -> None:
        user = UserFactory()
        data_source = DataSourceFactory(user=user)
        HealthScoreFactory(data_source=data_source, category=HealthScoreCategory.SLEEP)
        HealthScoreFactory(data_source=data_source, category=HealthScoreCategory.SLEEP)
        HealthScoreFactory(data_source=data_source, category=HealthScoreCategory.RECOVERY)

        results, total = repo.get_with_filters(db, user.id, HealthScoreQueryParams(category=HealthScoreCategory.SLEEP))

        assert total == 2
        assert all(s.category == HealthScoreCategory.SLEEP for s in results)

    def test_filter_by_provider(self, db: Session, repo: HealthScoreRepository) -> None:
        user = UserFactory()
        data_source = DataSourceFactory(user=user)
        HealthScoreFactory(data_source=data_source, provider=ProviderName.GARMIN)
        HealthScoreFactory(data_source=data_source, provider=ProviderName.OURA)

        results, total = repo.get_with_filters(db, user.id, HealthScoreQueryParams(provider=ProviderName.GARMIN))

        assert total == 1
        assert results[0].provider == ProviderName.GARMIN

    def test_filter_by_date_range(self, db: Session, repo: HealthScoreRepository) -> None:
        user = UserFactory()
        data_source = DataSourceFactory(user=user)
        now = datetime.now(timezone.utc)
        HealthScoreFactory(data_source=data_source, recorded_at=now - timedelta(days=1))
        HealthScoreFactory(data_source=data_source, recorded_at=now - timedelta(days=5))
        HealthScoreFactory(data_source=data_source, recorded_at=now - timedelta(days=10))

        results, total = repo.get_with_filters(
            db,
            user.id,
            HealthScoreQueryParams(
                start_datetime=now - timedelta(days=6),
                end_datetime=now,
            ),
        )

        assert total == 2

    def test_scoped_to_user(self, db: Session, repo: HealthScoreRepository) -> None:
        user_a = UserFactory()
        user_b = UserFactory()
        HealthScoreFactory(data_source=DataSourceFactory(user=user_a))
        HealthScoreFactory(data_source=DataSourceFactory(user=user_b))

        results, total = repo.get_with_filters(db, user_a.id, HealthScoreQueryParams())

        assert total == 1


class TestHealthScoreRepositoryLatest:
    def test_get_latest_by_category(self, db: Session, repo: HealthScoreRepository) -> None:
        user = UserFactory()
        data_source = DataSourceFactory(user=user)
        now = datetime.now(timezone.utc)
        HealthScoreFactory(
            data_source=data_source,
            category=HealthScoreCategory.SLEEP,
            recorded_at=now - timedelta(days=2),
            value=Decimal("70.00"),
        )
        latest = HealthScoreFactory(
            data_source=data_source, category=HealthScoreCategory.SLEEP, recorded_at=now, value=Decimal("85.00")
        )

        result = repo.get_latest_by_category(db, user.id, HealthScoreCategory.SLEEP)

        assert result is not None
        assert result.id == latest.id
        assert result.value == Decimal("85.00")

    def test_get_latest_by_category_returns_none_when_missing(self, db: Session, repo: HealthScoreRepository) -> None:
        user = UserFactory()

        result = repo.get_latest_by_category(db, user.id, HealthScoreCategory.RECOVERY)

        assert result is None

    def test_get_latest_per_category(self, db: Session, repo: HealthScoreRepository) -> None:
        user = UserFactory()
        data_source = DataSourceFactory(user=user)
        now = datetime.now(timezone.utc)
        HealthScoreFactory(
            data_source=data_source, category=HealthScoreCategory.SLEEP, recorded_at=now - timedelta(days=1)
        )
        HealthScoreFactory(data_source=data_source, category=HealthScoreCategory.SLEEP, recorded_at=now)
        HealthScoreFactory(data_source=data_source, category=HealthScoreCategory.RECOVERY, recorded_at=now)

        results = repo.get_latest_per_category(db, user.id)

        categories = {s.category for s in results}
        assert categories == {HealthScoreCategory.SLEEP, HealthScoreCategory.RECOVERY}
        assert len(results) == 2


class TestHealthScoreRepositoryBulkCreate:
    def test_bulk_create(self, db: Session, repo: HealthScoreRepository) -> None:
        data_source = DataSourceFactory(provider=ProviderName.GARMIN)
        now = datetime.now(timezone.utc)
        scores = [
            HealthScoreCreate(
                id=uuid4(),
                user_id=data_source.user_id,
                data_source_id=data_source.id,
                provider=ProviderName.GARMIN,
                category=HealthScoreCategory.SLEEP,
                value=Decimal("80.00"),
                recorded_at=now - timedelta(days=i),
            )
            for i in range(3)
        ]

        repo.bulk_create(db, scores)
        db.commit()

        results = db.query(HealthScore).filter(HealthScore.data_source_id == data_source.id).all()
        assert len(results) == 3

    def test_bulk_create_ignores_duplicates(self, db: Session, repo: HealthScoreRepository) -> None:
        data_source = DataSourceFactory(provider=ProviderName.GARMIN)
        recorded_at = datetime.now(timezone.utc)
        original = HealthScoreCreate(
            id=uuid4(),
            user_id=data_source.user_id,
            data_source_id=data_source.id,
            provider=ProviderName.GARMIN,
            category=HealthScoreCategory.SLEEP,
            value=Decimal("80.00"),
            recorded_at=recorded_at,
        )
        repo.bulk_create(db, [original])
        db.commit()

        duplicate = HealthScoreCreate(
            id=uuid4(),
            user_id=data_source.user_id,
            data_source_id=data_source.id,
            provider=ProviderName.GARMIN,
            category=HealthScoreCategory.SLEEP,
            value=Decimal("99.00"),
            recorded_at=recorded_at,
        )
        repo.bulk_create(db, [duplicate])
        db.commit()

        results = db.query(HealthScore).filter(HealthScore.data_source_id == data_source.id).all()
        assert len(results) == 1
        assert results[0].value == Decimal("80.00")


class TestHealthScoreRepositoryMissingSourceAdoption:
    def test_adopts_only_null_scores_inside_bounded_window(
        self,
        db: Session,
        repo: HealthScoreRepository,
    ) -> None:
        user = UserFactory()
        connection = UserConnectionFactory(user=user, provider="whoop")
        source = DataSourceFactory(
            user=user,
            provider=ProviderName.WHOOP,
            source="whoop",
            user_connection_id=connection.id,
        )
        inside = HealthScoreFactory(
            user_id=user.id,
            data_source_id=None,
            provider=ProviderName.WHOOP,
            category=HealthScoreCategory.RECOVERY,
            recorded_at=datetime(2026, 8, 20, 8, tzinfo=timezone.utc),
        )
        outside = HealthScoreFactory(
            user_id=user.id,
            data_source_id=None,
            provider=ProviderName.WHOOP,
            category=HealthScoreCategory.RECOVERY,
            recorded_at=datetime(2026, 8, 19, 8, tzinfo=timezone.utc),
        )

        adopted = repo.adopt_missing_data_sources(
            db,
            user_id=user.id,
            provider=ProviderName.WHOOP,
            data_source_id=source.id,
            start_datetime=datetime(2026, 8, 20, tzinfo=timezone.utc),
            end_datetime=datetime(2026, 8, 21, tzinfo=timezone.utc),
        )

        assert adopted == 1
        assert inside.data_source_id == source.id
        assert outside.data_source_id is None

    def test_matching_attribution_is_idempotent(
        self,
        db: Session,
        repo: HealthScoreRepository,
    ) -> None:
        user = UserFactory()
        connection = UserConnectionFactory(user=user, provider="whoop")
        source = DataSourceFactory(
            user=user,
            provider=ProviderName.WHOOP,
            source="whoop",
            user_connection_id=connection.id,
        )
        score = HealthScoreFactory(
            user_id=user.id,
            data_source_id=source.id,
            provider=ProviderName.WHOOP,
            category=HealthScoreCategory.SLEEP,
            recorded_at=datetime(2026, 8, 20, 8, tzinfo=timezone.utc),
        )

        adopted = repo.adopt_missing_data_sources(
            db,
            user_id=user.id,
            provider=ProviderName.WHOOP,
            data_source_id=source.id,
            start_datetime=datetime(2026, 8, 20, tzinfo=timezone.utc),
            end_datetime=datetime(2026, 8, 21, tzinfo=timezone.utc),
        )

        assert adopted == 0
        assert score.data_source_id == source.id

    def test_rejects_ambiguous_connection_backed_sources(
        self,
        db: Session,
        repo: HealthScoreRepository,
    ) -> None:
        user = UserFactory()
        connection = UserConnectionFactory(user=user, provider="whoop")
        source = DataSourceFactory(
            user=user,
            provider=ProviderName.WHOOP,
            source="whoop",
            device_model=None,
            user_connection_id=connection.id,
        )
        DataSourceFactory(
            user=user,
            provider=ProviderName.WHOOP,
            source="whoop",
            device_model="legacy-device",
            user_connection_id=connection.id,
        )
        HealthScoreFactory(
            user_id=user.id,
            data_source_id=None,
            provider=ProviderName.WHOOP,
            category=HealthScoreCategory.RECOVERY,
            recorded_at=datetime(2026, 8, 20, 8, tzinfo=timezone.utc),
        )

        with pytest.raises(HealthScoreProvenanceConflictError, match="unambiguous"):
            repo.adopt_missing_data_sources(
                db,
                user_id=user.id,
                provider=ProviderName.WHOOP,
                data_source_id=source.id,
                start_datetime=datetime(2026, 8, 20, tzinfo=timezone.utc),
                end_datetime=datetime(2026, 8, 21, tzinfo=timezone.utc),
            )

    def test_rejects_window_already_attributed_to_different_source(
        self,
        db: Session,
        repo: HealthScoreRepository,
    ) -> None:
        user = UserFactory()
        connection = UserConnectionFactory(user=user, provider="whoop")
        canonical_source = DataSourceFactory(
            user=user,
            provider=ProviderName.WHOOP,
            source="whoop",
            device_model=None,
            user_connection_id=connection.id,
        )
        legacy_source = DataSourceFactory(
            user=user,
            provider=ProviderName.GARMIN,
            source="garmin",
            device_model="legacy-device",
            user_connection_id=None,
        )
        HealthScoreFactory(
            user_id=user.id,
            data_source_id=legacy_source.id,
            provider=ProviderName.WHOOP,
            category=HealthScoreCategory.RECOVERY,
            recorded_at=datetime(2026, 8, 20, 8, tzinfo=timezone.utc),
        )

        with pytest.raises(HealthScoreProvenanceConflictError, match="different data source"):
            repo.adopt_missing_data_sources(
                db,
                user_id=user.id,
                provider=ProviderName.WHOOP,
                data_source_id=canonical_source.id,
                start_datetime=datetime(2026, 8, 20, tzinfo=timezone.utc),
                end_datetime=datetime(2026, 8, 21, tzinfo=timezone.utc),
            )

    def test_rejects_inactive_connection(self, db: Session, repo: HealthScoreRepository) -> None:
        user = UserFactory()
        connection = UserConnectionFactory(
            user=user,
            provider="whoop",
            status=ConnectionStatus.REVOKED,
        )
        source = DataSourceFactory(
            user=user,
            provider=ProviderName.WHOOP,
            source="whoop",
            user_connection_id=connection.id,
        )
        HealthScoreFactory(
            user_id=user.id,
            data_source_id=None,
            provider=ProviderName.WHOOP,
            category=HealthScoreCategory.RECOVERY,
            recorded_at=datetime(2026, 8, 20, 8, tzinfo=timezone.utc),
        )

        with pytest.raises(HealthWriteAuthorityError, match="active user connection"):
            repo.adopt_missing_data_sources(
                db,
                user_id=user.id,
                provider=ProviderName.WHOOP,
                data_source_id=source.id,
                start_datetime=datetime(2026, 8, 20, tzinfo=timezone.utc),
                end_datetime=datetime(2026, 8, 21, tzinfo=timezone.utc),
            )


def test_concurrent_bulk_create_serializes_absent_identity_and_rejects_conflicting_source(
    session_factory: sessionmaker[Session],
) -> None:
    """The owner-row authority lock closes the absent-score adoption race."""
    user_id = uuid4()
    source_ids = (uuid4(), uuid4())
    recorded_at = datetime.now(timezone.utc)
    with session_factory() as setup:
        setup.add(
            User(
                id=user_id,
                external_user_id=f"health-score-race-{user_id}",
                health_evidence_generation=0,
                health_write_state="active",
                health_source_policy="legacy-mixed",
            )
        )
        setup.flush()
        setup.add_all(
            [
                DataSource(
                    id=source_id,
                    user_id=user_id,
                    provider=ProviderName.WHOOP,
                    source=f"whoop-{index}",
                )
                for index, source_id in enumerate(source_ids)
            ]
        )
        setup.commit()

    ready = Barrier(2)

    def write(source_id: UUID) -> str:
        with session_factory() as write_session:
            ready.wait(timeout=5)
            creator = HealthScoreCreate(
                id=uuid4(),
                user_id=user_id,
                data_source_id=source_id,
                provider=ProviderName.WHOOP,
                category=HealthScoreCategory.RECOVERY,
                value=Decimal("82.00"),
                recorded_at=recorded_at,
            )
            try:
                HealthScoreRepository(HealthScore).bulk_create(write_session, [creator])
                write_session.commit()
                return "created"
            except HealthScoreProvenanceConflictError:
                write_session.rollback()
                return "conflict"

    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            results = list(executor.map(write, source_ids))

        assert sorted(results) == ["conflict", "created"]
        with session_factory() as verify:
            stored = verify.query(HealthScore).filter(HealthScore.user_id == user_id).one()
            assert stored.data_source_id in source_ids
    finally:
        with session_factory() as cleanup:
            cleanup.query(HealthScore).filter(HealthScore.user_id == user_id).delete()
            cleanup.query(DataSource).filter(DataSource.user_id == user_id).delete()
            cleanup.query(User).filter(User.id == user_id).delete()
            cleanup.commit()
