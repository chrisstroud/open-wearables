"""Safety tests for the one-off founder shadow WHOOP cleanup."""

from datetime import datetime, timezone
from decimal import Decimal
from typing import cast
from uuid import UUID, uuid4

import pytest
from sqlalchemy.orm import Session

from app.models import DataSource, HealthScore, SDKSourceResetSeal, User, UserConnection
from app.schemas.auth import ConnectionStatus
from app.schemas.enums import HealthScoreCategory, ProviderName
from app.services.founder_shadow_whoop_cleanup_service import (
    FounderShadowWhoopCleanupError,
    founder_shadow_whoop_cleanup_service,
)
from app.services.sdk_source_reset_external import (
    ExternalResetInventory,
    ObjectReference,
    RedisReference,
)
from tests.factories import DataSourceFactory, HealthScoreFactory, UserConnectionFactory, UserFactory


class _FakeExternalPlanes:
    def __init__(
        self,
        target_user_id: UUID,
        *,
        non_whoop_object: bool = False,
        fail_objects_once: bool = False,
    ) -> None:
        provider = "apple" if non_whoop_object else "whoop"
        self.object = ObjectReference(
            "open-wearables.raw-payload-objects",
            "private-bucket",
            f"raw-payloads/{provider}/api/2026-08-27/{target_user_id}/payload.json",
            None,
        )
        self.redis = RedisReference(
            "open-wearables.redis-coordination",
            f"sync_history:{target_user_id}:whoop",
            "key",
            None,
            None,
            reviewed_key_type="string",
            reviewed_state_digest_sha256="a" * 64,
        )
        self.deleted = False
        self.fail_objects_once = fail_objects_once
        self.erase_object_calls = 0
        self.erase_redis_calls = 0

    def inventory(self, _user_id: UUID, *, identity_scope: object) -> ExternalResetInventory:
        del identity_scope
        objects = () if self.deleted else (self.object,)
        redis = () if self.deleted else (self.redis,)
        return ExternalResetInventory(
            counts={
                "open-wearables.raw-payload-objects": len(objects),
                "open-wearables.fit-objects": 0,
                "open-wearables.queued-tasks": 0,
                "open-wearables.result-backend": 0,
                "open-wearables.redis-coordination": len(redis),
            },
            identity_tokens={
                "open-wearables.raw-payload-objects": tuple("object-token" for _row in objects),
                "open-wearables.fit-objects": (),
                "open-wearables.queued-tasks": (),
                "open-wearables.result-backend": (),
                "open-wearables.redis-coordination": tuple("redis-token" for _row in redis),
            },
            blockers=(),
            objects=objects,
            redis_references=redis,
            active_task_ids=(),
            configuration_digest_sha256="c" * 64,
        )

    def erase_objects(self, objects: tuple[ObjectReference, ...]) -> None:
        assert objects == (self.object,)
        self.erase_object_calls += 1
        if self.fail_objects_once:
            self.fail_objects_once = False
            raise RuntimeError("synthetic external deletion failure")

    def erase_redis(self, references: tuple[RedisReference, ...], *, include_results: bool) -> None:
        assert references == (self.redis,)
        assert include_results is True
        self.erase_redis_calls += 1
        self.deleted = True


def _seed_shared_whoop_pair(db: Session) -> tuple[User, User, UserConnection, UserConnection]:
    target = UserFactory(
        external_user_id=f"shadow-{uuid4()}",
        health_write_state="active",
        health_source_policy="legacy-mixed",
    )
    keeper = UserFactory(
        external_user_id=f"keeper-{uuid4()}",
        health_write_state="active",
        health_source_policy="multi-source",
    )
    shared_identity = f"whoop-{uuid4()}"
    target_connection = UserConnectionFactory(
        user=target,
        provider="whoop",
        provider_user_id=shared_identity,
        provider_username=None,
        access_token="shadow-access-token",
        refresh_token="shadow-refresh-token",
        status=ConnectionStatus.ACTIVE,
    )
    keeper_connection = UserConnectionFactory(
        user=keeper,
        provider="whoop",
        provider_user_id=shared_identity,
        provider_username=None,
        access_token="keeper-access-token",
        refresh_token="keeper-refresh-token",
        status=ConnectionStatus.ACTIVE,
    )
    db.flush()
    return (
        cast(User, target),
        cast(User, keeper),
        cast(UserConnection, target_connection),
        cast(UserConnection, keeper_connection),
    )


def _seed_target_whoop_evidence(
    db: Session,
    *,
    target: User,
    connection: UserConnection,
) -> DataSource:
    source = DataSourceFactory(
        user=target,
        provider=ProviderName.WHOOP,
        user_connection_id=connection.id,
        source="whoop_api",
        device_model="WHOOP 5.0",
    )
    HealthScoreFactory(
        data_source=source,
        user_id=target.id,
        provider=ProviderName.WHOOP,
        category=HealthScoreCategory.RECOVERY,
        value=Decimal("72"),
    )
    HealthScoreFactory(
        data_source_id=None,
        user_id=target.id,
        provider=ProviderName.WHOOP,
        category=HealthScoreCategory.SLEEP,
        value=Decimal("81"),
        recorded_at=datetime(2026, 8, 26, 12, tzinfo=timezone.utc),
    )
    db.flush()
    return cast(DataSource, source)


def test_execute_deletes_only_shadow_whoop_state_and_preserves_keeper(
    db: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target, keeper, target_connection, keeper_connection = _seed_shared_whoop_pair(db)
    _seed_target_whoop_evidence(db, target=target, connection=target_connection)
    keeper_source = DataSourceFactory(
        user=keeper,
        provider=ProviderName.WHOOP,
        user_connection_id=keeper_connection.id,
        source="whoop_api",
        device_model="WHOOP Keeper",
    )
    keeper_other_connection = UserConnectionFactory(
        user=keeper,
        provider="oura",
        provider_user_id=f"oura-{uuid4()}",
        access_token="keeper-oura-access-token",
        refresh_token="keeper-oura-refresh-token",
        status=ConnectionStatus.ACTIVE,
    )
    db.flush()
    keeper_token = keeper_connection.access_token
    keeper_other_token = keeper_other_connection.access_token
    target_external_id = target.external_user_id
    fake_external = _FakeExternalPlanes(target.id)
    monkeypatch.setattr(
        "app.services.founder_shadow_whoop_cleanup_service.sdk_source_reset_external_planes",
        fake_external,
    )

    plan = founder_shadow_whoop_cleanup_service.plan(
        db,
        target_user_id=target.id,
        keeper_user_id=keeper.id,
    )

    assert plan.executable is True
    assert plan.blockers == ()
    assert str(target.id) not in str(plan.public_dict())
    assert "shadow-access-token" not in str(plan.public_dict())

    result = founder_shadow_whoop_cleanup_service.execute(
        db,
        target_user_id=target.id,
        keeper_user_id=keeper.id,
        expected_plan_sha256=plan.plan_digest_sha256,
    )

    assert result.verified is True
    persisted_target = db.get(User, target.id)
    assert persisted_target is not None
    assert persisted_target.external_user_id == target_external_id
    assert persisted_target.health_write_state == "active"
    assert db.query(UserConnection).filter(UserConnection.user_id == target.id).count() == 0
    assert db.query(DataSource).filter(DataSource.user_id == target.id).count() == 0
    assert db.query(HealthScore).filter(HealthScore.user_id == target.id).count() == 0
    persisted_keeper = db.get(UserConnection, keeper_connection.id)
    assert persisted_keeper is not None
    assert persisted_keeper.status == ConnectionStatus.ACTIVE
    assert persisted_keeper.access_token == keeper_token
    persisted_keeper_other = db.get(UserConnection, keeper_other_connection.id)
    assert persisted_keeper_other is not None
    assert persisted_keeper_other.status == ConnectionStatus.ACTIVE
    assert persisted_keeper_other.access_token == keeper_other_token
    assert db.get(DataSource, keeper_source.id) is not None
    assert fake_external.erase_object_calls == 1
    assert fake_external.erase_redis_calls == 1


def test_plan_rejects_other_provider_and_cross_owner_state(
    db: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target, keeper, target_connection, keeper_connection = _seed_shared_whoop_pair(db)
    DataSourceFactory(
        user=target,
        provider=ProviderName.WHOOP,
        user_connection_id=keeper_connection.id,
        source="cross-owner",
        device_model="corrupt",
    )
    DataSourceFactory(
        user=target,
        provider=ProviderName.APPLE,
        user_connection_id=None,
        source="apple_health",
        device_model="iPhone",
    )
    db.flush()
    fake_external = _FakeExternalPlanes(target.id)
    monkeypatch.setattr(
        "app.services.founder_shadow_whoop_cleanup_service.sdk_source_reset_external_planes",
        fake_external,
    )

    plan = founder_shadow_whoop_cleanup_service.plan(
        db,
        target_user_id=target.id,
        keeper_user_id=keeper.id,
    )

    assert plan.executable is False
    assert "founder-shadow.other-provider-data-source-present" in plan.blockers
    assert "founder-shadow.target-source-connection-mismatch" in plan.blockers
    assert fake_external.erase_object_calls == 0
    assert db.get(UserConnection, target_connection.id) is not None


def test_plan_rejects_sdk_reset_state_and_third_identity_owner(
    db: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target, keeper, target_connection, _keeper_connection = _seed_shared_whoop_pair(db)
    third = UserFactory()
    UserConnectionFactory(
        user=third,
        provider="whoop",
        provider_user_id=target_connection.provider_user_id,
        status=ConnectionStatus.REVOKED,
    )
    db.add(
        SDKSourceResetSeal(
            operation_id=uuid4(),
            user_id=target.id,
            health_evidence_generation=0,
            inventory_digest_sha256="a" * 64,
            configuration_digest_sha256="b" * 64,
            resource_counts={},
        )
    )
    db.flush()
    fake_external = _FakeExternalPlanes(target.id)
    monkeypatch.setattr(
        "app.services.founder_shadow_whoop_cleanup_service.sdk_source_reset_external_planes",
        fake_external,
    )

    plan = founder_shadow_whoop_cleanup_service.plan(
        db,
        target_user_id=target.id,
        keeper_user_id=keeper.id,
    )

    assert "founder-shadow.provider-identity-owner-set-invalid" in plan.blockers
    assert "founder-shadow.sdk-or-reset-state-present" in plan.blockers


def test_execute_rejects_plan_drift_before_fencing(
    db: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target, keeper, target_connection, _keeper_connection = _seed_shared_whoop_pair(db)
    fake_external = _FakeExternalPlanes(target.id)
    monkeypatch.setattr(
        "app.services.founder_shadow_whoop_cleanup_service.sdk_source_reset_external_planes",
        fake_external,
    )
    plan = founder_shadow_whoop_cleanup_service.plan(
        db,
        target_user_id=target.id,
        keeper_user_id=keeper.id,
    )
    DataSourceFactory(
        user=target,
        provider=ProviderName.WHOOP,
        user_connection_id=target_connection.id,
        source="late-whoop-source",
        device_model="late",
    )
    db.flush()

    with pytest.raises(FounderShadowWhoopCleanupError) as exc_info:
        founder_shadow_whoop_cleanup_service.execute(
            db,
            target_user_id=target.id,
            keeper_user_id=keeper.id,
            expected_plan_sha256=plan.plan_digest_sha256,
        )

    assert exc_info.value.blockers == ("founder-shadow.plan-digest-mismatch",)
    db.refresh(target)
    db.refresh(target_connection)
    assert target.health_write_state == "active"
    assert target_connection.status == ConnectionStatus.ACTIVE
    assert fake_external.erase_object_calls == 0


def test_external_failure_leaves_prepared_state_that_can_be_replanned(
    db: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target, keeper, target_connection, _keeper_connection = _seed_shared_whoop_pair(db)
    fake_external = _FakeExternalPlanes(target.id, fail_objects_once=True)
    monkeypatch.setattr(
        "app.services.founder_shadow_whoop_cleanup_service.sdk_source_reset_external_planes",
        fake_external,
    )
    plan = founder_shadow_whoop_cleanup_service.plan(
        db,
        target_user_id=target.id,
        keeper_user_id=keeper.id,
    )

    with pytest.raises(FounderShadowWhoopCleanupError) as exc_info:
        founder_shadow_whoop_cleanup_service.execute(
            db,
            target_user_id=target.id,
            keeper_user_id=keeper.id,
            expected_plan_sha256=plan.plan_digest_sha256,
        )

    assert exc_info.value.blockers == ("founder-shadow.external-deletion-unavailable",)
    db.refresh(target)
    db.refresh(target_connection)
    assert target.health_write_state == "fenced"
    assert target_connection.status == ConnectionStatus.REVOKED
    assert target_connection.access_token is None
    assert target_connection.refresh_token is None

    prepared = founder_shadow_whoop_cleanup_service.plan(
        db,
        target_user_id=target.id,
        keeper_user_id=keeper.id,
    )
    assert prepared.phase == "prepared"
    assert prepared.executable is True

    result = founder_shadow_whoop_cleanup_service.execute(
        db,
        target_user_id=target.id,
        keeper_user_id=keeper.id,
        expected_plan_sha256=prepared.plan_digest_sha256,
    )

    assert result.verified is True


def test_plan_rejects_uuid_scoped_non_whoop_external_object(
    db: Session,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    target, keeper, _target_connection, _keeper_connection = _seed_shared_whoop_pair(db)
    fake_external = _FakeExternalPlanes(target.id, non_whoop_object=True)
    monkeypatch.setattr(
        "app.services.founder_shadow_whoop_cleanup_service.sdk_source_reset_external_planes",
        fake_external,
    )

    plan = founder_shadow_whoop_cleanup_service.plan(
        db,
        target_user_id=target.id,
        keeper_user_id=keeper.id,
    )

    assert plan.executable is False
    assert "founder-shadow.external-object-is-not-exact-whoop" in plan.blockers
    assert fake_external.erase_object_calls == 0


def test_redis_reference_requires_bounded_uuid_and_whoop_tokens() -> None:
    target_user_id = uuid4()

    def reference(key: str) -> RedisReference:
        return RedisReference(
            "open-wearables.redis-coordination",
            key,
            "key",
            None,
            None,
            reviewed_key_type="string",
            reviewed_state_digest_sha256="a" * 64,
        )

    assert founder_shadow_whoop_cleanup_service._redis_reference_is_exact_whoop(
        reference(f"sync_history:{target_user_id}:whoop"),
        target_user_id=target_user_id,
    )
    assert not founder_shadow_whoop_cleanup_service._redis_reference_is_exact_whoop(
        reference(f"sync_history:{target_user_id}a:whoop"),
        target_user_id=target_user_id,
    )
    assert not founder_shadow_whoop_cleanup_service._redis_reference_is_exact_whoop(
        reference(f"sync_history:{target_user_id}:notwhoop"),
        target_user_id=target_user_id,
    )
