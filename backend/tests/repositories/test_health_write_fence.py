from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from datetime import datetime, timezone
from threading import Event
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session, sessionmaker

from app.models import EventRecord, EventRecordDetail, HealthScore, User, UserConnection
from app.repositories.data_source_repository import DataSourceRepository
from app.repositories.event_record_detail_repository import EventRecordDetailRepository
from app.repositories.event_record_repository import EventRecordRepository
from app.repositories.health_score_repository import HealthScoreRepository
from app.repositories.health_write_authority import (
    HealthWriteAuthorityError,
    acquire_health_maintenance_authority,
)
from app.repositories.user_connection_repository import UserConnectionRepository
from app.schemas.auth import ConnectionStatus
from app.schemas.enums import HealthScoreCategory, ProviderName
from app.schemas.model_crud.activities import EventRecordCreate, EventRecordDetailCreate, HealthScoreCreate
from app.schemas.model_crud.user_management import UserConnectionCreate, UserConnectionUpdate
from app.services.provider_identity_authority import (
    ProviderIdentityFingerprint,
    acquire_provider_identity_locks,
    acquire_provider_identity_value_locks,
    provider_identity_fingerprints,
)
from tests.factories import DataSourceFactory, EventRecordFactory, UserConnectionFactory, UserFactory


@pytest.mark.parametrize("writer", ["connection", "data-source"])
def test_synchronous_writer_waits_for_account_fence_and_revalidates(
    session_factory: sessionmaker[Session],
    writer: str,
) -> None:
    user_id = uuid4()
    with session_factory() as setup:
        setup.add(
            User(
                id=user_id,
                first_name=None,
                last_name=None,
                email=None,
                external_user_id=f"health-writer-fence-{user_id}",
                health_evidence_generation=0,
                health_write_state="active",
                health_source_policy="legacy-mixed",
            )
        )
        setup.commit()

    writer_started = Event()

    def attempt_write() -> str:
        with session_factory() as write_session:
            writer_started.set()
            try:
                if writer == "connection":
                    UserConnectionRepository._require_write_authority(
                        write_session,
                        user_id=user_id,
                        provider="apple",
                    )
                else:
                    DataSourceRepository._require_health_write_authority(
                        write_session,
                        user_id=user_id,
                        provider=ProviderName.APPLE,
                    )
            except RuntimeError as exc:
                write_session.rollback()
                return str(exc)
            raise AssertionError("writer crossed a committed account fence")

    try:
        with session_factory() as fence_session:
            fenced_user = fence_session.query(User).filter(User.id == user_id).with_for_update().one()
            fenced_user.health_write_state = "fenced"
            fence_session.flush()

            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(attempt_write)
                assert writer_started.wait(timeout=5)
                with pytest.raises(TimeoutError):
                    future.result(timeout=0.25)
                fence_session.commit()
                assert "fenced" in future.result(timeout=5).lower()
    finally:
        with session_factory() as cleanup:
            cleanup.query(User).filter(User.id == user_id).delete()
            cleanup.commit()


@pytest.mark.parametrize("writer", ["create", "bulk-create"])
def test_health_score_writer_waits_for_account_fence_and_revalidates(
    session_factory: sessionmaker[Session],
    writer: str,
) -> None:
    user_id = uuid4()
    with session_factory() as setup:
        setup.add(
            User(
                id=user_id,
                first_name=None,
                last_name=None,
                email=None,
                external_user_id=f"health-score-fence-{user_id}",
                health_evidence_generation=0,
                health_write_state="active",
                health_source_policy="legacy-mixed",
            )
        )
        setup.commit()

    creator = HealthScoreCreate(
        id=uuid4(),
        user_id=user_id,
        provider=ProviderName.INTERNAL,
        category=HealthScoreCategory.SLEEP,
        recorded_at=datetime.now(timezone.utc),
    )
    writer_started = Event()

    def attempt_write() -> str:
        with session_factory() as write_session:
            writer_started.set()
            try:
                repo = HealthScoreRepository(HealthScore)
                if writer == "create":
                    repo.create(write_session, creator)
                else:
                    repo.bulk_create(write_session, [creator])
                    write_session.commit()
            except HealthWriteAuthorityError as exc:
                write_session.rollback()
                return str(exc)
            raise AssertionError("health score crossed a committed account fence")

    try:
        with session_factory() as fence_session:
            fenced_user = fence_session.query(User).filter(User.id == user_id).with_for_update().one()
            fenced_user.health_write_state = "fenced"
            fence_session.flush()
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(attempt_write)
                assert writer_started.wait(timeout=5)
                with pytest.raises(TimeoutError):
                    future.result(timeout=0.25)
                fence_session.commit()
                assert "fenced" in future.result(timeout=5).lower()
        with session_factory() as verify:
            assert verify.query(HealthScore).filter(HealthScore.user_id == user_id).count() == 0
    finally:
        with session_factory() as cleanup:
            cleanup.query(User).filter(User.id == user_id).delete()
            cleanup.commit()


def test_internal_score_requires_generation_bound_maintenance_authority(db: Session) -> None:
    user = UserFactory(health_source_policy="apple-mobile-v2-only", health_write_state="active")
    db.commit()
    creator = HealthScoreCreate(
        id=uuid4(),
        user_id=user.id,
        provider=ProviderName.INTERNAL,
        category=HealthScoreCategory.RESILIENCE,
        recorded_at=datetime.now(timezone.utc),
    )
    repo = HealthScoreRepository(HealthScore)

    with pytest.raises(HealthWriteAuthorityError, match="Current v2"):
        repo.bulk_create(db, [creator])
    db.rollback()

    acquire_health_maintenance_authority(db, user_id=user.id)
    repo.bulk_create(db, [creator])
    db.commit()
    assert db.get(HealthScore, creator.id) is not None


def test_event_record_direct_source_cannot_cross_account(db: Session) -> None:
    owner = UserFactory()
    attacker = UserFactory()
    source = DataSourceFactory(user=owner)
    creator = EventRecordCreate(
        id=uuid4(),
        user_id=attacker.id,
        data_source_id=source.id,
        category="workout",
        source_name="forged",
        start_datetime=datetime.now(timezone.utc),
        end_datetime=datetime.now(timezone.utc),
    )

    with pytest.raises(HealthWriteAuthorityError, match="another user"):
        EventRecordRepository(EventRecord).create(db, creator)


def test_detail_mutation_revalidates_fenced_owner(db: Session) -> None:
    user = UserFactory()
    record = EventRecordFactory(data_source=DataSourceFactory(user=user))
    user.health_write_state = "fenced"
    db.commit()

    with pytest.raises(HealthWriteAuthorityError, match="fenced"):
        EventRecordDetailRepository(EventRecordDetail).create(
            db,
            EventRecordDetailCreate(record_id=record.id, steps_count=1),
            detail_type="workout",
        )


@pytest.mark.parametrize("mutator", ["scope", "last-synced", "revoke"])
def test_connection_metadata_mutators_revalidate_fenced_owner(db: Session, mutator: str) -> None:
    user = UserFactory()
    connection = UserConnectionFactory(user=user)
    user.health_write_state = "fenced"
    db.commit()
    repo = UserConnectionRepository()
    mutate = {
        "scope": lambda: repo.update_scope(db, connection, "read_sleep"),
        "last-synced": lambda: repo.update_last_synced_at(db, connection),
        "revoke": lambda: repo.mark_as_revoked(db, connection),
    }[mutator]

    with pytest.raises(HealthWriteAuthorityError, match="fenced"):
        mutate()


@pytest.mark.parametrize(
    "writer",
    ["create", "update-old", "update-new", "suunto-update-old", "suunto-update-new"],
)
def test_connection_identity_writes_wait_for_sorted_advisory_authority(
    session_factory: sessionmaker[Session],
    writer: str,
) -> None:
    user_id = uuid4()
    connection_id = uuid4()
    old_identity = f"old-{uuid4()}"
    new_identity = f"new-{uuid4()}"
    provider = "suunto" if writer.startswith("suunto-") else "whoop"
    now = datetime.now(timezone.utc)
    with session_factory() as setup:
        setup.add(
            User(
                id=user_id,
                first_name=None,
                last_name=None,
                email=None,
                external_user_id=f"identity-writer-{user_id}",
                health_evidence_generation=0,
                health_write_state="active",
                health_source_policy="legacy-mixed",
            )
        )
        if writer != "create":
            setup.add(
                UserConnection(
                    id=connection_id,
                    user_id=user_id,
                    provider=provider,
                    provider_user_id=old_identity if provider == "whoop" else None,
                    provider_username=old_identity if provider == "suunto" else None,
                    access_token="token",
                    refresh_token="refresh",
                    token_expires_at=None,
                    scope=None,
                    status=ConnectionStatus.ACTIVE,
                    last_synced_at=None,
                    created_at=now,
                    updated_at=now,
                )
            )
        setup.commit()

    started = Event()

    def write_identity() -> str:
        with session_factory() as write_session:
            started.set()
            repo = UserConnectionRepository()
            if writer == "create":
                created = repo.create(
                    write_session,
                    UserConnectionCreate(
                        user_id=user_id,
                        provider=provider,
                        provider_user_id=new_identity,
                    ),
                )
                return str(created.id)
            connection = write_session.get(UserConnection, connection_id)
            assert connection is not None
            updater = (
                UserConnectionUpdate(provider_username=new_identity)
                if provider == "suunto"
                else UserConnectionUpdate(provider_user_id=new_identity)
            )
            updated = repo.update(
                write_session,
                connection,
                updater,
            )
            return str(updated.id)

    locked_identity = old_identity if writer.endswith("update-old") else new_identity
    try:
        with session_factory() as lock_session:
            acquire_provider_identity_value_locks(
                lock_session,
                ((provider, locked_identity),),
            )
            with ThreadPoolExecutor(max_workers=1) as executor:
                future = executor.submit(write_identity)
                assert started.wait(timeout=5)
                with pytest.raises(TimeoutError):
                    future.result(timeout=0.25)
                lock_session.commit()
                assert future.result(timeout=5)
    finally:
        with session_factory() as cleanup:
            cleanup.query(User).filter(User.id == user_id).delete()
            cleanup.commit()


def test_identity_update_restarts_with_fresh_old_identity_after_interleaving_writer(
    session_factory: sessionmaker[Session],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """An A->B writer must lock C->B after another writer commits A->C."""
    user_id = uuid4()
    connection_id = uuid4()
    identity_a = f"identity-a-{uuid4()}"
    identity_b = f"identity-b-{uuid4()}"
    identity_c = f"identity-c-{uuid4()}"
    now = datetime.now(timezone.utc)
    with session_factory() as setup:
        setup.add(
            User(
                id=user_id,
                first_name=None,
                last_name=None,
                email=None,
                external_user_id=f"identity-interleave-{user_id}",
                health_evidence_generation=0,
                health_write_state="active",
                health_source_policy="legacy-mixed",
            )
        )
        setup.add(
            UserConnection(
                id=connection_id,
                user_id=user_id,
                provider="whoop",
                provider_user_id=identity_a,
                provider_username=None,
                access_token="token",
                refresh_token="refresh",
                token_expires_at=None,
                scope=None,
                status=ConnectionStatus.ACTIVE,
                last_synced_at=None,
                created_at=now,
                updated_at=now,
            )
        )
        setup.commit()

    real_acquire = acquire_provider_identity_locks
    first_lock_plan = Event()
    permit_first_lock = Event()
    restarted_lock_plan = Event()
    writer_lock_sets: list[set[ProviderIdentityFingerprint]] = []

    def observe_writer_lock_plans(
        db_session: Session,
        identities: Iterable[ProviderIdentityFingerprint],
    ) -> tuple[ProviderIdentityFingerprint, ...]:
        captured = tuple(identities)
        if db_session.info.get("identity_interleave_writer"):
            writer_lock_sets.append(set(captured))
            if len(writer_lock_sets) == 1:
                first_lock_plan.set()
                if not permit_first_lock.wait(timeout=5):
                    raise RuntimeError("Timed out awaiting the injected identity writer")
            elif len(writer_lock_sets) == 2:
                restarted_lock_plan.set()
        return real_acquire(db_session, captured)

    monkeypatch.setattr(
        "app.repositories.user_connection_repository.acquire_provider_identity_locks",
        observe_writer_lock_plans,
    )

    def update_a_to_b() -> str | None:
        with session_factory() as writer_session:
            writer_session.info["identity_interleave_writer"] = True
            connection = writer_session.get(UserConnection, connection_id)
            assert connection is not None
            updated = UserConnectionRepository().update(
                writer_session,
                connection,
                UserConnectionUpdate(provider_user_id=identity_b),
            )
            return updated.provider_user_id

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(update_a_to_b)
            assert first_lock_plan.wait(timeout=5)

            # Commit A->C while the first writer has observed A but has not yet
            # acquired its planned A/B lock set.
            with session_factory() as interleaver:
                connection = interleaver.get(UserConnection, connection_id)
                assert connection is not None
                UserConnectionRepository().update(
                    interleaver,
                    connection,
                    UserConnectionUpdate(provider_user_id=identity_c),
                )

            # Model reset holding C authority. The restarted A->B writer must
            # now include C in its old/new set and wait until this transaction
            # releases it.
            with session_factory() as reset_lock:
                acquire_provider_identity_value_locks(reset_lock, (("whoop", identity_c),))
                permit_first_lock.set()
                assert restarted_lock_plan.wait(timeout=5)
                with pytest.raises(TimeoutError):
                    future.result(timeout=0.25)
                reset_lock.commit()
                assert future.result(timeout=5) == identity_b

        assert writer_lock_sets == [
            set(
                provider_identity_fingerprints(
                    "whoop",
                    provider_user_id=identity_a,
                    provider_username=None,
                )
                + provider_identity_fingerprints(
                    "whoop",
                    provider_user_id=identity_b,
                    provider_username=None,
                )
            ),
            set(
                provider_identity_fingerprints(
                    "whoop",
                    provider_user_id=identity_c,
                    provider_username=None,
                )
                + provider_identity_fingerprints(
                    "whoop",
                    provider_user_id=identity_b,
                    provider_username=None,
                )
            ),
        ]
    finally:
        permit_first_lock.set()
        with session_factory() as cleanup:
            cleanup.query(User).filter(User.id == user_id).delete()
            cleanup.commit()
