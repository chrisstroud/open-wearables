from collections.abc import Iterable
from concurrent.futures import ThreadPoolExecutor, TimeoutError
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from threading import Event
from typing import Literal, cast
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy.orm import Session, sessionmaker
from starlette.testclient import TestClient

from app.models import (
    AppleHealthDailySummary,
    DataPointSeries,
    DataSource,
    EventRecord,
    HealthScore,
    PersonalRecord,
    RefreshToken,
    SDKBatchReceipt,
    SDKClientInstallation,
    SDKSleepInbox,
    SDKSyncWindowReceipt,
    SDKUploadInbox,
    User,
    UserConnection,
    UserInvitationCode,
)
from app.repositories.health_write_authority import HealthWriteAuthorityError
from app.repositories.user_connection_repository import UserConnectionRepository
from app.schemas.auth import ConnectionStatus, TokenType
from app.schemas.model_crud.credentials import SDKClientRegistration, SDKHealthResetTransitionRequest
from app.schemas.model_crud.user_management import UserConnectionCreate, UserConnectionUpdate
from app.services.provider_identity_authority import (
    ProviderIdentityFingerprint,
    acquire_provider_identity_locks,
)
from app.services.sdk_batch_receipt_service import SDKBatchReceiptService
from app.services.sdk_client_installation_service import sdk_client_installation_service
from app.services.sdk_sleep_inbox_service import sdk_sleep_inbox_service
from app.services.sdk_source_reset_external import (
    FIT_OBJECTS,
    INTERNAL_LOCATOR_PROVIDER,
    QUEUED_TASKS,
    RAW_OBJECTS,
    REDIS_COORDINATION,
    RESULT_BACKEND,
    ExternalResetInventory,
    ObjectReference,
    ProviderIdentityScope,
    RedisReference,
)
from app.services.sdk_source_reset_service import RESOURCE_KEYS, sdk_source_reset_service
from app.services.sdk_upload_inbox_service import sdk_upload_inbox_service
from app.services.user_invitation_code_service import user_invitation_code_service
from tests.factories import (
    ApiKeyFactory,
    DataPointSeriesFactory,
    DataSourceFactory,
    DeveloperFactory,
    EventRecordFactory,
    HealthScoreFactory,
    PersonalRecordFactory,
    UserConnectionFactory,
    UserFactory,
)
from tests.utils import api_key_headers


class FakeExternalPlanes:
    def __init__(self) -> None:
        self.counts = {
            RAW_OBJECTS: 1,
            FIT_OBJECTS: 1,
            QUEUED_TASKS: 1,
            RESULT_BACKEND: 1,
            REDIS_COORDINATION: 1,
        }
        self.identity_scopes: list[ProviderIdentityScope] = []
        self.configuration_digest_sha256 = "c" * 64
        self.fail_object_erase_once = False
        self.fail_redis_erase_once = False
        self.erase_object_calls = 0
        self.erase_redis_calls = 0

    def inventory(
        self,
        _user_id: UUID,
        *,
        identity_scope: ProviderIdentityScope | None = None,
    ) -> ExternalResetInventory:
        self.identity_scopes.append(identity_scope or ProviderIdentityScope())
        objects = tuple(
            row
            for row in (
                ObjectReference(RAW_OBJECTS, "private", "raw/user.json", None),
                ObjectReference(FIT_OBJECTS, "private", "fit/user.fit", None),
            )
            if self.counts[row.resource_key]
        )
        redis_references = tuple(
            row
            for row in (
                RedisReference(QUEUED_TASKS, "sdk_sync", "list", None, "opaque-task"),
                RedisReference(RESULT_BACKEND, "celery-task-meta-proof", "key", None, None),
                RedisReference(REDIS_COORDINATION, "sync:user", "key", None, None),
            )
            if self.counts[row.resource_key]
        )
        return ExternalResetInventory(
            counts=dict(self.counts),
            identity_tokens={
                key: tuple(f"{key}:{index}" for index in range(count)) for key, count in self.counts.items()
            },
            blockers=(),
            objects=objects,
            redis_references=redis_references,
            active_task_ids=(),
            configuration_digest_sha256=self.configuration_digest_sha256,
        )

    def erase_objects(self, _objects: tuple[ObjectReference, ...]) -> None:
        self.erase_object_calls += 1
        if self.fail_object_erase_once:
            self.fail_object_erase_once = False
            raise RuntimeError("injected object deletion outage")
        self.counts[RAW_OBJECTS] = 0
        self.counts[FIT_OBJECTS] = 0

    def revoke_tasks(self, _task_ids: tuple[str, ...]) -> None:
        pass

    def erase_redis(
        self,
        _references: tuple[RedisReference, ...],
        *,
        include_results: bool,
    ) -> None:
        self.erase_redis_calls += 1
        if include_results and self.fail_redis_erase_once:
            self.fail_redis_erase_once = False
            raise RuntimeError("injected Redis deletion outage")
        self.counts[QUEUED_TASKS] = 0
        self.counts[REDIS_COORDINATION] = 0
        if include_results:
            self.counts[RESULT_BACKEND] = 0


class FakeProviderFence:
    def __init__(self) -> None:
        self.providers: list[str] = []
        self.fail_once = False

    def deregister(self, connections: list[UserConnection]) -> None:
        self.providers.extend(connection.provider for connection in connections if connection.access_token)
        if self.fail_once:
            self.fail_once = False
            raise RuntimeError("injected provider deregistration outage")


def _transition(
    operation_id: UUID,
    *,
    generation: int = 0,
    installation_generation: int | None = None,
    digest: str | None = None,
    resulting_policy: Literal["apple-mobile-v2-only", "multi-source"] = "apple-mobile-v2-only",
) -> SDKHealthResetTransitionRequest:
    return SDKHealthResetTransitionRequest(
        operation_id=operation_id,
        expected_health_evidence_generation=generation,
        expected_installation_generation=installation_generation,
        expected_inventory_digest_sha256=digest,
        resulting_health_source_policy=resulting_policy,
    )


def _seed_committed_identity_users(
    session_factory: sessionmaker[Session],
) -> tuple[UUID, UUID, str]:
    target_user_id = uuid4()
    other_user_id = uuid4()
    identity = f"whoop-race-{uuid4()}"
    now = datetime.now(timezone.utc)
    with session_factory() as setup:
        for user_id, label in ((target_user_id, "target"), (other_user_id, "other")):
            setup.add(
                User(
                    id=user_id,
                    first_name=None,
                    last_name=None,
                    email=None,
                    external_user_id=f"reset-race-{label}-{user_id}",
                    health_evidence_generation=0,
                    health_write_state="active",
                    health_source_policy="legacy-mixed",
                )
            )
        setup.add(
            UserConnection(
                id=uuid4(),
                user_id=target_user_id,
                provider="whoop",
                provider_user_id=identity,
                provider_username=None,
                access_token="target-access-token",
                refresh_token="target-refresh-token",
                token_expires_at=None,
                scope="read_all",
                status=ConnectionStatus.ACTIVE,
                last_synced_at=None,
                created_at=now,
                updated_at=now,
            )
        )
        setup.commit()
    return target_user_id, other_user_id, identity


def _cleanup_committed_users(
    session_factory: sessionmaker[Session],
    *user_ids: UUID,
) -> None:
    with session_factory() as cleanup:
        cleanup.query(User).filter(User.id.in_(user_ids)).delete(synchronize_session=False)
        cleanup.commit()


def _attach_shared_identity(
    session_factory: sessionmaker[Session],
    *,
    user_id: UUID,
    identity: str,
    status: ConnectionStatus = ConnectionStatus.ACTIVE,
) -> None:
    with session_factory() as attach_session:
        UserConnectionRepository().create(
            attach_session,
            UserConnectionCreate(
                user_id=user_id,
                provider="whoop",
                provider_user_id=identity,
                access_token="other-access-token",
                status=status,
            ),
        )


def _prepare_committed_reset(
    session_factory: sessionmaker[Session],
    *,
    user_id: UUID,
    operation_id: UUID,
) -> SDKHealthResetTransitionRequest:
    with session_factory() as reset_session:
        reviewed = sdk_source_reset_service.inspect(
            reset_session,
            user_id=user_id,
            request=_transition(operation_id),
        )
        request = _transition(operation_id, digest=reviewed.inventory_digest_sha256)
        sdk_source_reset_service.fence(reset_session, user_id=user_id, request=request)
        drained = sdk_source_reset_service.drain(reset_session, user_id=user_id, request=request)
        assert drained.drained is True
        return request


def _seed_all_provider_state(db: Session) -> tuple[User, SDKClientInstallation]:
    user = UserFactory(external_user_id="dashboard-subject-reset-proof")
    PersonalRecordFactory(user=user)
    whoop_connection = UserConnectionFactory(
        user=user,
        provider="whoop",
        provider_user_id="whoop-reset-provider-id",
        provider_username="whoop-reset-provider-username",
    )
    whoop_source = DataSourceFactory(
        user=user,
        provider="whoop",
        source="whoop",
        user_connection_id=whoop_connection.id,
    )
    apple_source = DataSourceFactory(user=user, provider="apple", source="apple_health_sdk")
    DataPointSeriesFactory(data_source=whoop_source, external_id="whoop-point")
    EventRecordFactory(data_source=apple_source, external_id="apple-event")
    HealthScoreFactory(data_source=whoop_source, user_id=user.id, provider="whoop")

    installation = sdk_client_installation_service.activate(
        db,
        user_id=user.id,
        registration=SDKClientRegistration(
            installation_id=uuid4(),
            bundle_id="fitness.dashboard.app",
            app_version="1.0.0",
            build_number="1",
            protocol_version=2,
        ),
    )
    now = datetime.now(timezone.utc)
    db.add(
        RefreshToken(
            id=f"rt-{uuid4().hex}",
            token_type=TokenType.SDK,
            user_id=user.id,
            app_id=installation.app_id,
            health_evidence_generation=0,
            developer_id=None,
            last_used_at=None,
            revoked_at=None,
            created_at=now,
        )
    )
    developer = DeveloperFactory()
    db.add(
        UserInvitationCode(
            id=uuid4(),
            code="RESET12345",
            user_id=user.id,
            created_by_id=developer.id,
            expires_at=now + timedelta(hours=1),
            redeemed_at=None,
            revoked_at=None,
            activation_policy=None,
            health_evidence_generation=0,
            created_at=now,
        )
    )

    batch_id = uuid4()
    batch_service = SDKBatchReceiptService()
    batch_service.prepare_submission(
        db,
        batch_id=batch_id,
        user_id=user.id,
        installation_id=installation.id,
        installation_generation=installation.generation,
        health_evidence_generation=0,
        provider="apple",
        payload_sha256=sha256(b"reset-payload").hexdigest(),
    )
    sdk_upload_inbox_service.put(
        db,
        batch_id=batch_id,
        user_id=user.id,
        installation_id=installation.id,
        installation_generation=installation.generation,
        health_evidence_generation=0,
        provider="apple",
        payload_sha256=sha256(b"reset-payload").hexdigest(),
        content_type="application/json",
        content="reset-payload",
    )
    claim = batch_service.claim_for_processing(db, batch_id)
    assert claim.attempt_count is not None
    staged = sdk_sleep_inbox_service.stage(
        db,
        user_id=user.id,
        provider="apple",
        batch_id=batch_id,
        records=[],
    )
    assert staged.error_code is None
    db.add(
        SDKSleepInbox(
            id=uuid4(),
            user_id=user.id,
            installation_id=installation.id,
            installation_generation=installation.generation,
            health_evidence_generation=0,
            provider="apple",
            external_id="reset-sleep",
            batch_ids=[batch_id],
            payload_sha256=sha256(b"sleep").hexdigest(),
            payload={
                "id": "reset-sleep",
                "stage": "light",
                "startDate": "2026-08-20T22:00:00Z",
                "endDate": "2026-08-20T23:00:00Z",
            },
            status="staged",
            attempt_count=0,
            next_attempt_at=now,
            last_attempt_at=None,
            materialized_at=None,
            last_error=None,
            updated_at=now,
            created_at=now,
        )
    )
    db.commit()
    return user, installation


def _seed_daily_summary_state(
    db: Session,
    *,
    external_user_id: str,
) -> tuple[User, SDKClientInstallation, UUID, UUID]:
    user = cast(User, UserFactory(external_user_id=external_user_id))
    installation = sdk_client_installation_service.activate(
        db,
        user_id=user.id,
        registration=SDKClientRegistration(
            installation_id=uuid4(),
            bundle_id="fitness.dashboard.app",
            app_version="1.0.0",
            build_number="1",
            protocol_version=2,
        ),
    )
    batch_id = uuid4()
    batch_service = SDKBatchReceiptService()
    batch_service.prepare_submission(
        db,
        batch_id=batch_id,
        user_id=user.id,
        installation_id=installation.id,
        installation_generation=installation.generation,
        health_evidence_generation=user.health_evidence_generation,
        provider="apple",
        payload_sha256=sha256(f"daily-summary-payload:{batch_id}".encode()).hexdigest(),
    )
    claim = batch_service.claim_for_processing(db, batch_id)
    assert claim.attempt_count is not None
    revision_set_digest = sha256(f"daily-summary-revisions:{batch_id}".encode()).hexdigest()
    batch_service.mark_succeeded(
        db,
        batch_id=batch_id,
        attempt_count=claim.attempt_count,
        result={
            "status_code": 200,
            "daily_summaries_saved": 1,
            "revision_set_digest": revision_set_digest,
        },
    )

    now = datetime.now(timezone.utc)
    summary_id = uuid4()
    db.add(
        AppleHealthDailySummary(
            id=summary_id,
            user_id=user.id,
            installation_id=installation.id,
            installation_generation=installation.generation,
            health_evidence_generation=user.health_evidence_generation,
            batch_id=batch_id,
            summary_kind="metric",
            stable_key=sha256(f"daily-summary-key:{batch_id}".encode()).hexdigest(),
            schema_version="apple-health-daily-summary.v1",
            revision_id=sha256(f"daily-summary-revision:{batch_id}".encode()).hexdigest(),
            supersedes_revision_id=None,
            local_date=now.date(),
            timezone="UTC",
            timezone_boundary_version="tzdb-test",
            series_type="HKQuantityTypeIdentifierStepCount",
            contributor_set_digest=sha256(f"daily-summary-contributors:{batch_id}".encode()).hexdigest(),
            input_set_digest=sha256(f"daily-summary-inputs:{batch_id}".encode()).hexdigest(),
            computed_at=now,
            payload={"value": 1},
            is_current=True,
        )
    )
    db.commit()
    return user, installation, batch_id, summary_id


def test_external_identity_scope_uses_only_provider_webhook_authorities(db: Session) -> None:
    user = UserFactory()
    UserConnectionFactory(
        user=user,
        provider="whoop",
        provider_user_id="whoop-provider-id",
        provider_username="non-authoritative-whoop-display-name",
    )
    UserConnectionFactory(
        user=user,
        provider="suunto",
        provider_user_id="suunto-provider-id",
        provider_username="authoritative-suunto-username",
    )
    batch_id = uuid4()
    SDKBatchReceiptService().prepare_submission(
        db,
        batch_id=batch_id,
        user_id=user.id,
        provider="apple",
        payload_sha256=sha256(b"internal-reset-locator").hexdigest(),
    )
    db.commit()

    scope = sdk_source_reset_service._provider_identity_scope(db, user_id=user.id)

    whoop = scope.for_provider("whoop")
    suunto = scope.for_provider("suunto")
    assert whoop is not None
    assert whoop.values == ("whoop-provider-id",)
    assert suunto is not None
    assert suunto.values == ("authoritative-suunto-username", "suunto-provider-id")
    internal = scope.for_provider(INTERNAL_LOCATOR_PROVIDER)
    assert internal is not None
    assert internal.values == (str(batch_id),)


def test_external_identity_scope_fails_closed_for_shared_provider_identity(db: Session) -> None:
    target = UserFactory()
    other = UserFactory()
    UserConnectionFactory(user=target, provider="whoop", provider_user_id="shared-whoop-id")
    UserConnectionFactory(user=other, provider="whoop", provider_user_id="shared-whoop-id")
    db.commit()

    scope = sdk_source_reset_service._provider_identity_scope(db, user_id=target.id)

    assert scope.ambiguous_providers == ("whoop",)
    assert scope.for_provider("whoop") is None


def test_reset_fence_rechecks_identity_attached_in_post_commit_gap(
    session_factory: sessionmaker[Session],
) -> None:
    target_user_id, other_user_id, identity = _seed_committed_identity_users(session_factory)
    fake_external = FakeExternalPlanes()
    fake_external.counts = {key: 0 for key in fake_external.counts}
    fake_provider_fence = FakeProviderFence()
    operation_id = uuid4()
    gap_open = Event()
    attach_done = Event()
    paused = False

    with (
        patch(
            "app.services.sdk_source_reset_service.sdk_source_reset_external_planes",
            fake_external,
        ),
        session_factory() as inspect_session,
    ):
        reviewed = sdk_source_reset_service.inspect(
            inspect_session,
            user_id=target_user_id,
            request=_transition(operation_id),
        )
        request = _transition(operation_id, digest=reviewed.inventory_digest_sha256)

    def pause_before_identity_lock(
        db_session: Session,
        identities: Iterable[ProviderIdentityFingerprint],
    ) -> tuple[ProviderIdentityFingerprint, ...]:
        nonlocal paused
        if not paused:
            paused = True
            gap_open.set()
            assert attach_done.wait(timeout=5)
        return acquire_provider_identity_locks(db_session, identities)

    def fence_reset() -> tuple[int, object]:
        with session_factory() as reset_session:
            try:
                sdk_source_reset_service.fence(
                    reset_session,
                    user_id=target_user_id,
                    request=request,
                )
            except HTTPException as exc:
                return exc.status_code, exc.detail
        return 0, "completed"

    try:
        with (
            patch(
                "app.services.sdk_source_reset_service.sdk_source_reset_external_planes",
                fake_external,
            ),
            patch(
                "app.services.sdk_source_reset_service.sdk_source_reset_provider_fence",
                fake_provider_fence,
            ),
            patch(
                "app.services.sdk_source_reset_service.acquire_provider_identity_locks",
                side_effect=pause_before_identity_lock,
            ),
            ThreadPoolExecutor(max_workers=1) as executor,
        ):
            future = executor.submit(fence_reset)
            assert gap_open.wait(timeout=5), future.result(timeout=1)
            _attach_shared_identity(
                session_factory,
                user_id=other_user_id,
                identity=identity,
            )
            attach_done.set()
            assert future.result(timeout=5)[0] == 409

        with session_factory() as verify:
            target = verify.get(User, target_user_id)
            assert target is not None
            assert target.health_write_state == "fenced"
            connection = verify.query(UserConnection).filter(UserConnection.user_id == target_user_id).one()
            assert connection.access_token == "target-access-token"
        assert fake_provider_fence.providers == []
    finally:
        attach_done.set()
        _cleanup_committed_users(session_factory, target_user_id, other_user_id)


def test_reset_fence_rechecks_identity_after_token_commit_before_queue_cleanup(
    session_factory: sessionmaker[Session],
) -> None:
    target_user_id, other_user_id, identity = _seed_committed_identity_users(session_factory)
    fake_external = FakeExternalPlanes()
    fake_external.counts = {key: 0 for key in fake_external.counts}
    fake_provider_fence = FakeProviderFence()
    operation_id = uuid4()
    second_gap_open = Event()
    attach_done = Event()
    lock_calls = 0

    with (
        patch(
            "app.services.sdk_source_reset_service.sdk_source_reset_external_planes",
            fake_external,
        ),
        session_factory() as inspect_session,
    ):
        reviewed = sdk_source_reset_service.inspect(
            inspect_session,
            user_id=target_user_id,
            request=_transition(operation_id),
        )
        request = _transition(operation_id, digest=reviewed.inventory_digest_sha256)

    def pause_post_token_identity_lock(
        db_session: Session,
        identities: Iterable[ProviderIdentityFingerprint],
    ) -> tuple[ProviderIdentityFingerprint, ...]:
        nonlocal lock_calls
        lock_calls += 1
        if lock_calls == 2:
            second_gap_open.set()
            assert attach_done.wait(timeout=5)
        return acquire_provider_identity_locks(db_session, identities)

    def fence_reset() -> tuple[int, object]:
        with session_factory() as reset_session:
            try:
                sdk_source_reset_service.fence(
                    reset_session,
                    user_id=target_user_id,
                    request=request,
                )
            except HTTPException as exc:
                return exc.status_code, exc.detail
        return 0, "completed"

    try:
        with (
            patch(
                "app.services.sdk_source_reset_service.sdk_source_reset_external_planes",
                fake_external,
            ),
            patch(
                "app.services.sdk_source_reset_service.sdk_source_reset_provider_fence",
                fake_provider_fence,
            ),
            patch(
                "app.services.sdk_source_reset_service.acquire_provider_identity_locks",
                side_effect=pause_post_token_identity_lock,
            ),
            ThreadPoolExecutor(max_workers=1) as executor,
        ):
            future = executor.submit(fence_reset)
            assert second_gap_open.wait(timeout=5), future.result(timeout=1)
            _attach_shared_identity(
                session_factory,
                user_id=other_user_id,
                identity=identity,
            )
            attach_done.set()
            assert future.result(timeout=5)[0] == 409

        with session_factory() as verify:
            target = verify.get(User, target_user_id)
            assert target is not None
            assert target.health_write_state == "fenced"
            connection = verify.query(UserConnection).filter(UserConnection.user_id == target_user_id).one()
            assert connection.status == ConnectionStatus.REVOKED
            assert connection.access_token is None
            assert connection.refresh_token is None
        assert fake_provider_fence.providers == ["whoop"]
        assert fake_external.erase_redis_calls == 0
    finally:
        attach_done.set()
        _cleanup_committed_users(session_factory, target_user_id, other_user_id)


def test_resumed_fence_releases_user_before_waiting_for_identity_writer(
    session_factory: sessionmaker[Session],
) -> None:
    target_user_id, other_user_id, identity = _seed_committed_identity_users(session_factory)
    fake_external = FakeExternalPlanes()
    fake_external.counts = {key: 0 for key in fake_external.counts}
    fake_provider_fence = FakeProviderFence()
    operation_id = uuid4()
    writer_identity_held = Event()
    resume_identity_attempted = Event()
    release_writer = Event()

    try:
        with (
            patch(
                "app.services.sdk_source_reset_service.sdk_source_reset_external_planes",
                fake_external,
            ),
            patch(
                "app.services.sdk_source_reset_service.sdk_source_reset_provider_fence",
                fake_provider_fence,
            ),
            session_factory() as initial_session,
        ):
            reviewed = sdk_source_reset_service.inspect(
                initial_session,
                user_id=target_user_id,
                request=_transition(operation_id),
            )
            request = _transition(operation_id, digest=reviewed.inventory_digest_sha256)
            sdk_source_reset_service.fence(
                initial_session,
                user_id=target_user_id,
                request=request,
            )

        def hold_writer_identity_before_user_lock(
            db_session: Session,
            identities: Iterable[ProviderIdentityFingerprint],
        ) -> tuple[ProviderIdentityFingerprint, ...]:
            locked = acquire_provider_identity_locks(db_session, identities)
            if db_session.info.get("resume_lock_order_writer"):
                writer_identity_held.set()
                assert release_writer.wait(timeout=5)
            return locked

        def observe_resume_identity_attempt(
            db_session: Session,
            identities: Iterable[ProviderIdentityFingerprint],
        ) -> tuple[ProviderIdentityFingerprint, ...]:
            resume_identity_attempted.set()
            return acquire_provider_identity_locks(db_session, identities)

        def update_fenced_connection() -> str:
            with session_factory() as writer_session:
                writer_session.info["resume_lock_order_writer"] = True
                connection = writer_session.query(UserConnection).filter(UserConnection.user_id == target_user_id).one()
                try:
                    UserConnectionRepository().update(
                        writer_session,
                        connection,
                        UserConnectionUpdate(provider_user_id=f"replacement-{identity}"),
                    )
                except HealthWriteAuthorityError as exc:
                    writer_session.rollback()
                    return str(exc)
            return "unexpectedly-updated"

        def resume_fence() -> str:
            with session_factory() as resume_session:
                return sdk_source_reset_service.fence(
                    resume_session,
                    user_id=target_user_id,
                    request=request,
                ).health_write_state

        executor = ThreadPoolExecutor(max_workers=2)
        try:
            with (
                patch(
                    "app.repositories.user_connection_repository.acquire_provider_identity_locks",
                    side_effect=hold_writer_identity_before_user_lock,
                ),
                patch(
                    "app.services.sdk_source_reset_service.acquire_provider_identity_locks",
                    side_effect=observe_resume_identity_attempt,
                ),
                patch(
                    "app.services.sdk_source_reset_service.sdk_source_reset_external_planes",
                    fake_external,
                ),
                patch(
                    "app.services.sdk_source_reset_service.sdk_source_reset_provider_fence",
                    fake_provider_fence,
                ),
            ):
                writer_future = executor.submit(update_fenced_connection)
                assert writer_identity_held.wait(timeout=5), writer_future.result(timeout=1)
                resume_future = executor.submit(resume_fence)
                assert resume_identity_attempted.wait(timeout=5), resume_future.result(timeout=1)
                release_writer.set()
                assert writer_future.result(timeout=5) == "Health writes are fenced"
                assert resume_future.result(timeout=5) == "fenced"
        finally:
            release_writer.set()
            executor.shutdown(wait=True, cancel_futures=True)
    finally:
        release_writer.set()
        _cleanup_committed_users(session_factory, target_user_id, other_user_id)


@pytest.mark.parametrize("other_status", [ConnectionStatus.ACTIVE, ConnectionStatus.REVOKED])
def test_apply_rechecks_identity_attached_before_database_delete(
    session_factory: sessionmaker[Session],
    other_status: ConnectionStatus,
) -> None:
    target_user_id, other_user_id, identity = _seed_committed_identity_users(session_factory)
    fake_external = FakeExternalPlanes()
    fake_external.counts = {key: 0 for key in fake_external.counts}
    fake_provider_fence = FakeProviderFence()
    operation_id = uuid4()
    try:
        with (
            patch(
                "app.services.sdk_source_reset_service.sdk_source_reset_external_planes",
                fake_external,
            ),
            patch(
                "app.services.sdk_source_reset_service.sdk_source_reset_provider_fence",
                fake_provider_fence,
            ),
        ):
            request = _prepare_committed_reset(
                session_factory,
                user_id=target_user_id,
                operation_id=operation_id,
            )
            with ThreadPoolExecutor(max_workers=1) as executor:
                executor.submit(
                    _attach_shared_identity,
                    session_factory,
                    user_id=other_user_id,
                    identity=identity,
                    status=other_status,
                ).result(timeout=5)
            with session_factory() as apply_session, pytest.raises(HTTPException) as exc_info:
                sdk_source_reset_service.apply(
                    apply_session,
                    user_id=target_user_id,
                    request=request,
                )
            assert exc_info.value.status_code == 409

        with session_factory() as verify:
            target = verify.get(User, target_user_id)
            assert target is not None
            assert target.health_write_state == "fenced"
            assert verify.query(UserConnection).filter(UserConnection.user_id == target_user_id).count() == 1
    finally:
        _cleanup_committed_users(session_factory, target_user_id, other_user_id)


def test_apply_reacquires_identity_and_blocks_attach_after_database_commit(
    session_factory: sessionmaker[Session],
) -> None:
    target_user_id, other_user_id, identity = _seed_committed_identity_users(session_factory)
    fake_external = FakeExternalPlanes()
    fake_external.counts = {key: 0 for key in fake_external.counts}
    fake_provider_fence = FakeProviderFence()
    operation_id = uuid4()
    post_commit_gap = Event()
    attach_done = Event()
    lock_calls = 0

    def pause_second_identity_lock(
        db_session: Session,
        identities: Iterable[ProviderIdentityFingerprint],
    ) -> tuple[ProviderIdentityFingerprint, ...]:
        nonlocal lock_calls
        lock_calls += 1
        if lock_calls == 2:
            post_commit_gap.set()
            assert attach_done.wait(timeout=5)
        return acquire_provider_identity_locks(db_session, identities)

    def apply_reset(request: SDKHealthResetTransitionRequest) -> int:
        with session_factory() as apply_session:
            try:
                sdk_source_reset_service.apply(
                    apply_session,
                    user_id=target_user_id,
                    request=request,
                )
            except HTTPException as exc:
                return exc.status_code
        return 0

    try:
        with (
            patch(
                "app.services.sdk_source_reset_service.sdk_source_reset_external_planes",
                fake_external,
            ),
            patch(
                "app.services.sdk_source_reset_service.sdk_source_reset_provider_fence",
                fake_provider_fence,
            ),
        ):
            request = _prepare_committed_reset(
                session_factory,
                user_id=target_user_id,
                operation_id=operation_id,
            )
            redis_calls_before_apply = fake_external.erase_redis_calls
            with (
                patch(
                    "app.services.sdk_source_reset_service.acquire_provider_identity_locks",
                    side_effect=pause_second_identity_lock,
                ),
                ThreadPoolExecutor(max_workers=1) as executor,
            ):
                future = executor.submit(apply_reset, request)
                assert post_commit_gap.wait(timeout=5)
                _attach_shared_identity(
                    session_factory,
                    user_id=other_user_id,
                    identity=identity,
                )
                attach_done.set()
                assert future.result(timeout=5) == 409

        with session_factory() as verify:
            target = verify.get(User, target_user_id)
            assert target is not None
            assert target.health_write_state == "fenced"
            assert target.health_evidence_generation == 1
            assert verify.query(UserConnection).filter(UserConnection.user_id == target_user_id).count() == 0
        assert fake_external.erase_object_calls == 0
        assert fake_external.erase_redis_calls == redis_calls_before_apply
    finally:
        attach_done.set()
        _cleanup_committed_users(session_factory, target_user_id, other_user_id)


def test_apply_holds_user_lock_through_cleanup_before_new_activation(
    session_factory: sessionmaker[Session],
) -> None:
    target_user_id, other_user_id, _identity = _seed_committed_identity_users(session_factory)
    fake_external = FakeExternalPlanes()
    fake_external.counts = {key: 0 for key in fake_external.counts}
    fake_provider_fence = FakeProviderFence()
    operation_id = uuid4()
    cleanup_started = Event()
    allow_cleanup = Event()
    external_inventory_calls = 0

    try:
        with (
            patch(
                "app.services.sdk_source_reset_service.sdk_source_reset_external_planes",
                fake_external,
            ),
            patch(
                "app.services.sdk_source_reset_service.sdk_source_reset_provider_fence",
                fake_provider_fence,
            ),
        ):
            request = _prepare_committed_reset(
                session_factory,
                user_id=target_user_id,
                operation_id=operation_id,
            )

        real_external_inventory = fake_external.inventory

        def pause_first_post_commit_inventory(
            user_id: UUID,
            *,
            identity_scope: ProviderIdentityScope | None = None,
        ) -> ExternalResetInventory:
            nonlocal external_inventory_calls
            external_inventory_calls += 1
            if external_inventory_calls == 2:
                cleanup_started.set()
                assert allow_cleanup.wait(timeout=5)
            return real_external_inventory(user_id, identity_scope=identity_scope)

        def apply_reset() -> bool:
            with session_factory() as apply_session:
                return sdk_source_reset_service.apply(
                    apply_session,
                    user_id=target_user_id,
                    request=request,
                ).verified_empty

        installation_id = uuid4()

        def activate_installation() -> UUID:
            with session_factory() as activation_session:
                installation = sdk_client_installation_service.activate(
                    activation_session,
                    user_id=target_user_id,
                    registration=SDKClientRegistration(
                        installation_id=installation_id,
                        bundle_id="fitness.dashboard.app",
                        app_version="2.0.0",
                        build_number="200",
                        protocol_version=2,
                    ),
                )
                activation_session.commit()
                return installation.id

        executor = ThreadPoolExecutor(max_workers=2)
        try:
            with (
                patch.object(
                    fake_external,
                    "inventory",
                    side_effect=pause_first_post_commit_inventory,
                ),
                patch(
                    "app.services.sdk_source_reset_service.sdk_source_reset_external_planes",
                    fake_external,
                ),
            ):
                apply_future = executor.submit(apply_reset)
                assert cleanup_started.wait(timeout=5), apply_future.result(timeout=1)
                activation_future = executor.submit(activate_installation)
                with pytest.raises(TimeoutError):
                    activation_future.result(timeout=0.25)

                allow_cleanup.set()
                assert apply_future.result(timeout=5) is True
                assert activation_future.result(timeout=5) == installation_id
        finally:
            allow_cleanup.set()
            executor.shutdown(wait=True, cancel_futures=True)

        with session_factory() as verify:
            user = verify.get(User, target_user_id)
            installation = verify.get(SDKClientInstallation, installation_id)
            assert user is not None
            assert user.health_evidence_generation == 1
            assert user.health_write_state == "activating"
            assert installation is not None
            assert installation.status == "active"
            assert installation.health_evidence_generation == 1
    finally:
        allow_cleanup.set()
        _cleanup_committed_users(session_factory, target_user_id, other_user_id)


def test_all_provider_reset_is_manifest_bound_idempotent_and_preserves_profile(db: Session) -> None:
    fake_external = FakeExternalPlanes()
    fake_provider_fence = FakeProviderFence()
    user, installation = _seed_all_provider_state(db)
    operation_id = uuid4()

    with (
        patch(
            "app.services.sdk_source_reset_service.sdk_source_reset_external_planes",
            fake_external,
        ),
        patch(
            "app.services.sdk_source_reset_service.sdk_source_reset_provider_fence",
            fake_provider_fence,
        ),
    ):
        reviewed = sdk_source_reset_service.inspect(
            db,
            user_id=user.id,
            request=_transition(operation_id),
        )
        assert tuple(reviewed.resource_counts) == RESOURCE_KEYS
        assert reviewed.resource_counts["open-wearables.connections"] == 1
        assert reviewed.resource_counts["open-wearables.provider-credentials"] == 1
        assert reviewed.resource_counts["open-wearables.source-mappings"] == 2
        assert reviewed.resource_counts["open-wearables.normalized-records"] >= 3
        assert reviewed.resource_counts["open-wearables.user-record"] == 0
        reviewed_counts = reviewed.resource_counts
        digest = reviewed.inventory_digest_sha256

        request = _transition(
            operation_id,
            installation_generation=installation.generation,
            digest=digest,
        )
        fenced = sdk_source_reset_service.fence(db, user_id=user.id, request=request)
        assert fenced.health_evidence_generation == 0
        assert fenced.health_write_state == "fenced"
        assert fenced.health_source_policy == "apple-mobile-v2-only"
        assert fenced.resulting_health_source_policy == "apple-mobile-v2-only"
        assert fenced.active_installation_id is None
        assert fenced.resource_counts == reviewed_counts
        assert fenced.inventory_digest_sha256 == digest

        resumed_inventory = sdk_source_reset_service.inspect(db, user_id=user.id, request=request)
        assert resumed_inventory.resource_counts == reviewed_counts
        assert resumed_inventory.inventory_digest_sha256 == digest

        drained = sdk_source_reset_service.drain(db, user_id=user.id, request=request)
        assert drained.drained is True
        assert drained.resource_counts == reviewed_counts
        assert drained.queued_or_processing_upload_count == 0
        assert drained.pending_sleep_projection_count == 0

        applied = sdk_source_reset_service.apply(db, user_id=user.id, request=request)
        assert applied.health_evidence_generation == 1
        assert applied.health_write_state == "awaiting-v2-pairing"
        assert applied.health_source_policy == "apple-mobile-v2-only"
        assert applied.resulting_health_source_policy == "apple-mobile-v2-only"
        assert applied.resource_counts == reviewed_counts
        assert applied.inventory_digest_sha256 == digest
        assert applied.verified_empty is True

        retry = sdk_source_reset_service.apply(db, user_id=user.id, request=request)
        assert retry.resource_counts == reviewed_counts
        assert retry.health_evidence_generation == 1

        verified = sdk_source_reset_service.verify(db, user_id=user.id, request=request)
        assert verified.inventory_digest_sha256 == digest
        assert verified.verified_empty is True
        assert verified.drained is True
        assert verified.resource_counts == {key: 0 for key in RESOURCE_KEYS}
        assert tuple(verified.resource_counts) == RESOURCE_KEYS

    assert fake_provider_fence.providers == ["whoop"]

    db.expire_all()
    preserved = db.get(User, user.id)
    assert preserved is not None
    assert preserved.external_user_id == "dashboard-subject-reset-proof"
    persisted_reset_receipt = str(preserved.health_reset_deleted_counts)
    assert "whoop-reset-provider-id" not in persisted_reset_receipt
    assert "whoop-reset-provider-username" not in persisted_reset_receipt
    assert any(scope.for_provider("whoop") is not None for scope in fake_external.identity_scopes)
    verified_identity = fake_external.identity_scopes[-1].for_provider("whoop")
    assert verified_identity is not None
    assert verified_identity.values == ()
    assert len(verified_identity.fingerprints) == 1
    assert db.query(PersonalRecord).filter(PersonalRecord.user_id == user.id).count() == 1
    assert db.query(UserConnection).filter(UserConnection.user_id == user.id).count() == 0
    assert db.query(DataSource).filter(DataSource.user_id == user.id).count() == 0
    assert db.query(DataPointSeries).join(DataSource).filter(DataSource.user_id == user.id).count() == 0
    assert db.query(EventRecord).join(DataSource).filter(DataSource.user_id == user.id).count() == 0
    assert db.query(HealthScore).filter(HealthScore.user_id == user.id).count() == 0
    assert db.query(SDKBatchReceipt).filter(SDKBatchReceipt.user_id == user.id).count() == 0
    assert db.query(SDKUploadInbox).filter(SDKUploadInbox.user_id == user.id).count() == 0
    assert db.query(SDKSleepInbox).filter(SDKSleepInbox.user_id == user.id).count() == 0
    assert db.query(SDKSyncWindowReceipt).filter(SDKSyncWindowReceipt.user_id == user.id).count() == 0
    assert db.query(SDKClientInstallation).filter(SDKClientInstallation.user_id == user.id).count() == 0
    assert db.query(RefreshToken).filter(RefreshToken.user_id == user.id).count() == 0
    assert db.query(UserInvitationCode).filter(UserInvitationCode.user_id == user.id).count() == 0


def test_reset_inventory_and_erasure_include_only_target_apple_daily_summaries(db: Session) -> None:
    fake_external = FakeExternalPlanes()
    fake_external.counts = {key: 0 for key in fake_external.counts}
    fake_provider_fence = FakeProviderFence()
    target, target_installation, target_batch_id, target_summary_id = _seed_daily_summary_state(
        db,
        external_user_id="daily-summary-reset-target",
    )
    other, other_installation, other_batch_id, other_summary_id = _seed_daily_summary_state(
        db,
        external_user_id="daily-summary-reset-other",
    )
    target_installation_id = target_installation.id
    target_installation_generation = target_installation.generation
    other_installation_id = other_installation.id
    operation_id = uuid4()

    with (
        patch(
            "app.services.sdk_source_reset_service.sdk_source_reset_external_planes",
            fake_external,
        ),
        patch(
            "app.services.sdk_source_reset_service.sdk_source_reset_provider_fence",
            fake_provider_fence,
        ),
    ):
        reviewed = sdk_source_reset_service.inspect(
            db,
            user_id=target.id,
            request=_transition(operation_id),
        )
        expected_counts = {key: 0 for key in RESOURCE_KEYS}
        expected_counts.update(
            {
                "open-wearables.normalized-records": 1,
                "open-wearables.sdk-batch-receipts": 1,
                "open-wearables.installations": 1,
            }
        )
        assert reviewed.resource_counts == expected_counts

        request = _transition(
            operation_id,
            installation_generation=target_installation_generation,
            digest=reviewed.inventory_digest_sha256,
        )
        sdk_source_reset_service.fence(db, user_id=target.id, request=request)
        drained = sdk_source_reset_service.drain(db, user_id=target.id, request=request)
        assert drained.drained is True
        applied = sdk_source_reset_service.apply(db, user_id=target.id, request=request)
        assert applied.verified_empty is True

    assert db.get(AppleHealthDailySummary, target_summary_id) is None
    assert db.get(SDKBatchReceipt, target_batch_id) is None
    assert db.get(SDKClientInstallation, target_installation_id) is None
    assert db.get(AppleHealthDailySummary, other_summary_id) is not None
    assert db.get(SDKBatchReceipt, other_batch_id) is not None
    assert db.get(SDKClientInstallation, other_installation_id) is not None
    assert db.get(User, other.id) is not None


def test_multi_source_reset_reopens_only_after_verified_empty_and_rejects_target_drift(db: Session) -> None:
    fake_external = FakeExternalPlanes()
    fake_provider_fence = FakeProviderFence()
    user, installation = _seed_all_provider_state(db)
    operation_id = uuid4()

    with (
        patch(
            "app.services.sdk_source_reset_service.sdk_source_reset_external_planes",
            fake_external,
        ),
        patch(
            "app.services.sdk_source_reset_service.sdk_source_reset_provider_fence",
            fake_provider_fence,
        ),
    ):
        reviewed = sdk_source_reset_service.inspect(
            db,
            user_id=user.id,
            request=_transition(operation_id, resulting_policy="multi-source"),
        )
        request = _transition(
            operation_id,
            installation_generation=installation.generation,
            digest=reviewed.inventory_digest_sha256,
            resulting_policy="multi-source",
        )
        changed_target = request.model_copy(update={"resulting_health_source_policy": "apple-mobile-v2-only"})

        fenced = sdk_source_reset_service.fence(db, user_id=user.id, request=request)
        assert fenced.health_write_state == "fenced"
        assert fenced.health_source_policy == "multi-source"
        assert fenced.resulting_health_source_policy == "multi-source"

        for transition in (
            sdk_source_reset_service.inspect,
            sdk_source_reset_service.fence,
            sdk_source_reset_service.drain,
            sdk_source_reset_service.apply,
            sdk_source_reset_service.verify,
        ):
            with pytest.raises(HTTPException) as exc_info:
                transition(db, user_id=user.id, request=changed_target)
            assert exc_info.value.status_code == 409
            assert exc_info.value.detail == "Health reset resulting source policy changed"
            db.rollback()

        drained = sdk_source_reset_service.drain(db, user_id=user.id, request=request)
        assert drained.drained is True
        applied = sdk_source_reset_service.apply(db, user_id=user.id, request=request)
        assert applied.health_evidence_generation == 1
        assert applied.health_write_state == "active"
        assert applied.health_source_policy == "multi-source"
        assert applied.resulting_health_source_policy == "multi-source"
        assert applied.verified_empty is True

        retry = sdk_source_reset_service.apply(db, user_id=user.id, request=request)
        assert retry.health_write_state == "active"
        assert retry.health_evidence_generation == 1
        assert retry.verified_empty is True

        verified = sdk_source_reset_service.verify(db, user_id=user.id, request=request)
        assert verified.health_write_state == "active"
        assert verified.verified_empty is True


@pytest.mark.parametrize("drift", ["credential-substitution", "normalized-addition"])
def test_apply_rejects_post_drain_identity_or_count_drift(db: Session, drift: str) -> None:
    fake_external = FakeExternalPlanes()
    fake_provider_fence = FakeProviderFence()
    user, installation = _seed_all_provider_state(db)
    operation_id = uuid4()

    with (
        patch(
            "app.services.sdk_source_reset_service.sdk_source_reset_external_planes",
            fake_external,
        ),
        patch(
            "app.services.sdk_source_reset_service.sdk_source_reset_provider_fence",
            fake_provider_fence,
        ),
    ):
        reviewed = sdk_source_reset_service.inspect(
            db,
            user_id=user.id,
            request=_transition(operation_id),
        )
        request = _transition(
            operation_id,
            installation_generation=installation.generation,
            digest=reviewed.inventory_digest_sha256,
        )
        sdk_source_reset_service.fence(db, user_id=user.id, request=request)
        sdk_source_reset_service.drain(db, user_id=user.id, request=request)

        if drift == "credential-substitution":
            connection = db.query(UserConnection).filter(UserConnection.user_id == user.id).one()
            original_id = connection.id
            connection.provider_user_id = "substituted-whoop-id"
            db.commit()
            assert db.query(UserConnection).filter(UserConnection.user_id == user.id).one().id == original_id
        else:
            source = db.query(DataSource).filter(DataSource.user_id == user.id).first()
            assert source is not None
            HealthScoreFactory(
                data_source=source,
                user_id=user.id,
                recorded_at=datetime.now(timezone.utc) + timedelta(minutes=1),
            )
            db.commit()

        with pytest.raises(HTTPException) as exc_info:
            sdk_source_reset_service.apply(db, user_id=user.id, request=request)
        assert exc_info.value.status_code == 409
        assert exc_info.value.detail == "Source reset drained inventory changed"
        assert db.query(DataSource).filter(DataSource.user_id == user.id).count() > 0


def test_provider_deregistration_failure_leaves_durable_fence_and_retries(db: Session) -> None:
    fake_external = FakeExternalPlanes()
    fake_provider_fence = FakeProviderFence()
    fake_provider_fence.fail_once = True
    user, installation = _seed_all_provider_state(db)
    operation_id = uuid4()

    with (
        patch(
            "app.services.sdk_source_reset_service.sdk_source_reset_external_planes",
            fake_external,
        ),
        patch(
            "app.services.sdk_source_reset_service.sdk_source_reset_provider_fence",
            fake_provider_fence,
        ),
    ):
        reviewed = sdk_source_reset_service.inspect(
            db,
            user_id=user.id,
            request=_transition(operation_id),
        )
        request = _transition(
            operation_id,
            installation_generation=installation.generation,
            digest=reviewed.inventory_digest_sha256,
        )

        with pytest.raises(HTTPException) as exc_info:
            sdk_source_reset_service.fence(db, user_id=user.id, request=request)
        assert exc_info.value.status_code == 503
        db.expire_all()
        persisted_user = db.get(User, user.id)
        assert persisted_user is not None
        assert persisted_user.health_write_state == "fenced"
        connection = db.query(UserConnection).filter(UserConnection.user_id == user.id).one()
        assert connection.access_token is not None

        retried = sdk_source_reset_service.fence(db, user_id=user.id, request=request)
        assert retried.health_write_state == "fenced"
        db.expire_all()
        connection = db.query(UserConnection).filter(UserConnection.user_id == user.id).one()
        assert connection.access_token is None
        assert connection.refresh_token is None


@pytest.mark.parametrize("failure_point", ["before-object-erase", "before-redis-erase"])
def test_apply_resumes_external_cleanup_after_committed_database_reset(
    db: Session,
    failure_point: str,
) -> None:
    fake_external = FakeExternalPlanes()
    fake_provider_fence = FakeProviderFence()
    user, installation = _seed_all_provider_state(db)
    original_invitation = db.query(UserInvitationCode).filter(UserInvitationCode.user_id == user.id).one()
    invitation_developer_id = original_invitation.created_by_id
    original_invitation_code = original_invitation.code
    operation_id = uuid4()

    with (
        patch(
            "app.services.sdk_source_reset_service.sdk_source_reset_external_planes",
            fake_external,
        ),
        patch(
            "app.services.sdk_source_reset_service.sdk_source_reset_provider_fence",
            fake_provider_fence,
        ),
    ):
        reviewed = sdk_source_reset_service.inspect(
            db,
            user_id=user.id,
            request=_transition(operation_id),
        )
        reviewed_counts = reviewed.resource_counts
        request = _transition(
            operation_id,
            installation_generation=installation.generation,
            digest=reviewed.inventory_digest_sha256,
        )
        sdk_source_reset_service.fence(db, user_id=user.id, request=request)
        sdk_source_reset_service.drain(db, user_id=user.id, request=request)

        if failure_point == "before-object-erase":
            fake_external.fail_object_erase_once = True
        else:
            fake_external.fail_redis_erase_once = True

        with pytest.raises(HTTPException) as exc_info:
            sdk_source_reset_service.apply(db, user_id=user.id, request=request)
        assert exc_info.value.status_code == 503

        db.expire_all()
        persisted = db.get(User, user.id)
        assert persisted is not None
        assert persisted.health_evidence_generation == 1
        assert persisted.health_write_state == "fenced"
        assert persisted.health_reset_applied_at is not None
        assert db.query(DataSource).filter(DataSource.user_id == user.id).count() == 0
        assert {
            key: int((persisted.health_reset_deleted_counts or {}).get(key, 0)) for key in RESOURCE_KEYS
        } == reviewed_counts
        if failure_point == "before-redis-erase":
            assert fake_external.counts[RAW_OBJECTS] == 0
            assert fake_external.counts[FIT_OBJECTS] == 0
            assert fake_external.counts[RESULT_BACKEND] == 1

        with pytest.raises(HTTPException) as generate_blocked:
            user_invitation_code_service.generate(
                db,
                user.id,
                invitation_developer_id,
            )
        assert generate_blocked.value.status_code == 423
        db.rollback()

        post_reset_installation_id = uuid4()
        registration = SDKClientRegistration(
            installation_id=post_reset_installation_id,
            bundle_id="fitness.dashboard.app",
            app_version="2.0.0",
            build_number="200",
            protocol_version=2,
        )
        with pytest.raises(HTTPException) as redeem_blocked:
            user_invitation_code_service.redeem(
                db,
                original_invitation_code,
                client=registration,
            )
        assert redeem_blocked.value.status_code == 404
        db.rollback()
        assert db.get(SDKClientInstallation, post_reset_installation_id) is None

        retried = sdk_source_reset_service.apply(db, user_id=user.id, request=request)
        assert retried.verified_empty is True
        assert retried.health_write_state == "awaiting-v2-pairing"
        assert retried.resource_counts == reviewed_counts
        assert fake_external.counts == {key: 0 for key in fake_external.counts}

        replacement_invitation = user_invitation_code_service.generate(
            db,
            user.id,
            invitation_developer_id,
        )
        redeemed = user_invitation_code_service.redeem(
            db,
            replacement_invitation.code,
            client=registration,
        )
        assert redeemed.installation_id == post_reset_installation_id
        active = db.get(SDKClientInstallation, post_reset_installation_id)
        assert active is not None
        assert active.status == "active"
        assert active.health_evidence_generation == 1


def test_apply_requires_drain_sealed_external_configuration(db: Session) -> None:
    fake_external = FakeExternalPlanes()
    fake_provider_fence = FakeProviderFence()
    user, installation = _seed_all_provider_state(db)
    operation_id = uuid4()

    with (
        patch(
            "app.services.sdk_source_reset_service.sdk_source_reset_external_planes",
            fake_external,
        ),
        patch(
            "app.services.sdk_source_reset_service.sdk_source_reset_provider_fence",
            fake_provider_fence,
        ),
    ):
        reviewed = sdk_source_reset_service.inspect(
            db,
            user_id=user.id,
            request=_transition(operation_id),
        )
        request = _transition(
            operation_id,
            installation_generation=installation.generation,
            digest=reviewed.inventory_digest_sha256,
        )
        sdk_source_reset_service.fence(db, user_id=user.id, request=request)
        sdk_source_reset_service.drain(db, user_id=user.id, request=request)

        fake_external.configuration_digest_sha256 = "d" * 64
        with pytest.raises(HTTPException) as exc_info:
            sdk_source_reset_service.apply(db, user_id=user.id, request=request)
        assert exc_info.value.status_code == 409
        db.expire_all()
        persisted = db.get(User, user.id)
        assert persisted is not None
        assert persisted.health_write_state == "fenced"

        fake_external.configuration_digest_sha256 = "c" * 64
        applied = sdk_source_reset_service.apply(db, user_id=user.id, request=request)
        assert applied.verified_empty is True


def test_already_applied_cleanup_retry_requires_sealed_external_configuration(db: Session) -> None:
    fake_external = FakeExternalPlanes()
    fake_provider_fence = FakeProviderFence()
    user, installation = _seed_all_provider_state(db)
    operation_id = uuid4()

    with (
        patch(
            "app.services.sdk_source_reset_service.sdk_source_reset_external_planes",
            fake_external,
        ),
        patch(
            "app.services.sdk_source_reset_service.sdk_source_reset_provider_fence",
            fake_provider_fence,
        ),
    ):
        reviewed = sdk_source_reset_service.inspect(
            db,
            user_id=user.id,
            request=_transition(operation_id),
        )
        request = _transition(
            operation_id,
            installation_generation=installation.generation,
            digest=reviewed.inventory_digest_sha256,
        )
        sdk_source_reset_service.fence(db, user_id=user.id, request=request)
        sdk_source_reset_service.drain(db, user_id=user.id, request=request)
        fake_external.fail_object_erase_once = True
        with pytest.raises(HTTPException) as interrupted:
            sdk_source_reset_service.apply(db, user_id=user.id, request=request)
        assert interrupted.value.status_code == 503
        calls_after_interruption = fake_external.erase_object_calls

        fake_external.configuration_digest_sha256 = "d" * 64
        with pytest.raises(HTTPException) as changed:
            sdk_source_reset_service.apply(db, user_id=user.id, request=request)
        assert changed.value.status_code == 409
        assert fake_external.erase_object_calls == calls_after_interruption

        fake_external.configuration_digest_sha256 = "c" * 64
        resumed = sdk_source_reset_service.apply(db, user_id=user.id, request=request)
        assert resumed.verified_empty is True


def test_reset_routes_require_api_key_and_preserved_external_subject_lookup(
    client: TestClient,
    db: Session,
    api_v1_prefix: str,
) -> None:
    developer = DeveloperFactory()
    api_key = ApiKeyFactory(developer=developer, scopes=["source-reset"])
    generic_api_key = ApiKeyFactory(developer=developer, scopes=[])
    user = UserFactory(external_user_id="dashboard-subject-lookup-proof")
    fake_external = FakeExternalPlanes()
    fake_external.counts = {key: 0 for key in fake_external.counts}
    payload = {
        "operation_id": str(uuid4()),
        "expected_health_evidence_generation": 0,
    }

    with patch(
        "app.services.sdk_source_reset_service.sdk_source_reset_external_planes",
        fake_external,
    ):
        unauthorized = client.post(
            f"{api_v1_prefix}/internal/source-resets/{user.id}/inventory",
            json=payload,
        )
        forbidden_generic = client.post(
            f"{api_v1_prefix}/internal/source-resets/{user.id}/inventory",
            headers=api_key_headers(generic_api_key.id),
            json=payload,
        )
        authorized = client.post(
            f"{api_v1_prefix}/internal/source-resets/{user.id}/inventory",
            headers=api_key_headers(api_key.id),
            json=payload,
        )
    assert unauthorized.status_code == 401
    assert forbidden_generic.status_code == 403
    assert authorized.status_code == 200
    assert tuple(authorized.json()["resource_counts"]) == RESOURCE_KEYS

    lookup = client.get(
        f"{api_v1_prefix}/users",
        headers=api_key_headers(api_key.id),
        params={
            "external_user_id": "dashboard-subject-lookup-proof",
            "page": 1,
            "limit": 2,
        },
    )
    assert lookup.status_code == 200
    assert lookup.json()["total"] == 1
    assert lookup.json()["items"][0]["id"] == str(user.id)
    assert lookup.json()["items"][0]["external_user_id"] == "dashboard-subject-lookup-proof"
