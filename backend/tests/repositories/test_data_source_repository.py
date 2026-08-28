"""
Tests for DataSourceRepository.

Regression coverage for the ``data_source.source`` column width: Apple
HealthKit tags on-device data with a source bundle identifier of the form
``com.apple.health.<UUID>`` (53 chars). This overflowed the previous
``VARCHAR(50)`` and aborted the entire SDK import batch with
``StringDataRightTruncation``, so no Apple sleep/records were ever saved.
The column is now ``VARCHAR(100)``.
"""

import pytest
from sqlalchemy.orm import Session

from app.models import DataSource
from app.repositories.data_source_repository import DataSourceProvenanceConflictError, DataSourceRepository
from app.schemas.enums import ProviderName
from tests.factories import DataSourceFactory, UserConnectionFactory, UserFactory

# Realistic Apple HealthKit source bundle id: "com.apple.health." + a UUID.
APPLE_HEALTH_SOURCE = "com.apple.health.ED447642-08FD-4E45-AF20-633C02C83170"


class TestDataSourceRepository:
    """Test suite for DataSourceRepository."""

    def test_apple_health_source_bundle_id_persists(self, db: Session) -> None:
        """A 53-char Apple source bundle id imports without truncation.

        Exercises ``ensure_data_source`` (the SDK import path that failed) and
        asserts the full identifier round-trips — this would raise
        ``StringDataRightTruncation`` against the old ``VARCHAR(50)`` column.
        """
        # Arrange: a source longer than the old 50-char limit.
        assert len(APPLE_HEALTH_SOURCE) > 50
        user = UserFactory()
        repo = DataSourceRepository(DataSource)

        # Act
        created = repo.ensure_data_source(
            db,
            user_id=user.id,
            provider=ProviderName.APPLE,
            device_model="Watch7,5",
            source=APPLE_HEALTH_SOURCE,
        )
        db.commit()
        db.expire_all()

        # Assert: stored intact, retrievable by its full identity.
        assert created.source == APPLE_HEALTH_SOURCE
        stored = repo.get_by_identity(
            db,
            user_id=user.id,
            provider=ProviderName.APPLE,
            device_model="Watch7,5",
            source=APPLE_HEALTH_SOURCE,
        )
        assert stored is not None
        assert stored.source == APPLE_HEALTH_SOURCE
        assert len(stored.source) == len(APPLE_HEALTH_SOURCE)

    def test_batch_ensure_adopts_null_connection_without_duplicate_source(self, db: Session) -> None:
        """A replay links its legacy source row instead of inserting a second identity."""
        user = UserFactory()
        connection = UserConnectionFactory(user=user, provider="whoop")
        legacy = DataSourceFactory(
            user=user,
            provider=ProviderName.WHOOP,
            source="whoop",
            device_model=None,
            user_connection_id=None,
        )
        repo = DataSourceRepository(DataSource)
        identity = (user.id, None, "whoop")

        resolved = repo.batch_ensure_data_sources(
            db,
            ProviderName.WHOOP,
            connection.id,
            {identity},
        )
        db.commit()
        db.expire_all()

        assert resolved == {identity: legacy.id}
        assert db.query(DataSource).filter(DataSource.user_id == user.id).count() == 1
        adopted = db.get(DataSource, legacy.id)
        assert adopted is not None
        assert adopted.user_connection_id == connection.id

    def test_batch_ensure_rejects_source_owned_by_different_connection(self, db: Session) -> None:
        """A stable source identity cannot be silently reassigned to a new authorization."""
        user = UserFactory()
        original_connection = UserConnectionFactory(user=user, provider="garmin")
        next_connection = UserConnectionFactory(user=user, provider="whoop")
        DataSourceFactory(
            user=user,
            provider=ProviderName.WHOOP,
            source="whoop",
            device_model=None,
            user_connection_id=original_connection.id,
        )
        repo = DataSourceRepository(DataSource)

        with pytest.raises(
            DataSourceProvenanceConflictError,
            match="already attributed to a different connection",
        ):
            repo.batch_ensure_data_sources(
                db,
                ProviderName.WHOOP,
                next_connection.id,
                {(user.id, None, "whoop")},
            )
