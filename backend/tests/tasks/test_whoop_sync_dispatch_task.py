from datetime import datetime, timedelta, timezone
from threading import Event
from typing import cast
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session

from app.integrations.celery.tasks.sync_vendor_data_task import _install_exact_whoop_commit_guard, sync_vendor_data
from app.integrations.celery.tasks.whoop_sync_dispatch_task import (
    _WhoopAuthorizationHeartbeat,
    drain_whoop_sync_dispatch_outbox,
    execute_whoop_sync_dispatch,
)
from app.models import User, UserConnection, WhoopAuthorizationLease, WhoopSyncDispatchReceipt
from app.repositories.health_write_authority import HealthWriteAuthorityError
from app.repositories.whoop_sync_dispatch_repository import (
    WHOOP_AUTHORIZATION_RECOVERY_GRACE,
    WhoopSyncDispatchRepository,
)
from app.schemas.auth import ConnectionStatus
from app.schemas.whoop_sync_dispatch import WhoopFullHistorySyncCommand, WhoopSyncDispatchStatus
from app.services.providers.whoop.exact_sync_authority import ExactWhoopSyncAuthority
from tests.factories import UserConnectionFactory, UserFactory


def _receipt(
    db: Session,
    *,
    generation: int = 1,
) -> tuple[User, UserConnection, WhoopSyncDispatchReceipt]:
    user = cast(User, UserFactory())
    connection = cast(
        UserConnection,
        UserConnectionFactory(
            user=user,
            provider="whoop",
            status=ConnectionStatus.ACTIVE,
            authorization_generation=generation,
        ),
    )
    dispatch_id = uuid4()
    receipt = WhoopSyncDispatchRepository().create_or_get(
        db,
        user_id=user.id,
        connection_id=connection.id,
        command=WhoopFullHistorySyncCommand(
            idempotency_key=dispatch_id,
            authorization_generation=generation,
            requested_start_at=datetime(2000, 1, 1, tzinfo=timezone.utc),
            requested_end_at=datetime(2026, 9, 2, tzinfo=timezone.utc),
        ),
    )
    return user, connection, receipt


class TestWhoopExactDispatchWorker:
    @patch("app.integrations.celery.tasks.sync_vendor_data_task.SessionLocal")
    def test_hidden_exact_selector_without_scoped_authority_fails_closed(
        self,
        mock_session_local: MagicMock,
        db: Session,
    ) -> None:
        user, connection, receipt = _receipt(db, generation=4)
        mock_session_local.return_value.__enter__.return_value = db
        mock_session_local.return_value.__exit__.return_value = None

        result = sync_vendor_data.run(
            user_id=str(user.id),
            providers=["whoop"],
            is_historical=True,
            _exact_connection_id=str(connection.id),
            _exact_authorization_generation=4,
        )

        assert result["providers_synced"] == {}
        assert result["errors"] == {"authority": "Missing exact WHOOP dispatch authority"}
        assert receipt.status == WhoopSyncDispatchStatus.QUEUED.value

    @patch("app.integrations.celery.tasks.whoop_sync_dispatch_task.execute_whoop_sync_dispatch.apply_async")
    @patch("app.integrations.celery.tasks.whoop_sync_dispatch_task.SessionLocal")
    def test_publish_failure_keeps_receipt_due_for_redelivery(
        self,
        mock_session_local: MagicMock,
        mock_publish: MagicMock,
        db: Session,
    ) -> None:
        _user, _connection, receipt = _receipt(db)
        mock_session_local.return_value.__enter__.return_value = db
        mock_session_local.return_value.__exit__.return_value = None
        mock_publish.side_effect = RuntimeError("broker unavailable")

        first = drain_whoop_sync_dispatch_outbox.run()
        assert first == {"selected": 1, "published": 0, "failed": 1}
        db.refresh(receipt)
        assert receipt.status == WhoopSyncDispatchStatus.QUEUED.value
        assert receipt.enqueue_attempt_count == 1

        receipt.next_enqueue_at = datetime.now(timezone.utc) - timedelta(seconds=1)
        db.commit()
        mock_publish.side_effect = None
        second = drain_whoop_sync_dispatch_outbox.run()
        assert second == {"selected": 1, "published": 1, "failed": 0}
        db.refresh(receipt)
        assert receipt.enqueue_attempt_count == 2

    @patch("app.integrations.celery.tasks.whoop_sync_dispatch_task.sync_vendor_data.run")
    @patch("app.integrations.celery.tasks.whoop_sync_dispatch_task.SessionLocal")
    def test_duplicate_delivery_executes_provider_once_with_exact_selector(
        self,
        mock_session_local: MagicMock,
        mock_sync: MagicMock,
        db: Session,
    ) -> None:
        user, connection, receipt = _receipt(db, generation=4)
        mock_session_local.return_value.__enter__.return_value = db
        mock_session_local.return_value.__exit__.return_value = None
        mock_sync.return_value = {
            "providers_synced": {"whoop": {"success": True, "params": {}}},
            "errors": {},
        }

        first = execute_whoop_sync_dispatch.run(str(receipt.id))
        duplicate = execute_whoop_sync_dispatch.run(str(receipt.id))

        assert first["status"] == "succeeded"
        assert duplicate["status"] == "succeeded"
        mock_sync.assert_called_once()
        kwargs = mock_sync.call_args.kwargs
        assert kwargs["providers"] == ["whoop"]
        assert kwargs["_exact_connection_id"] == str(connection.id)
        assert kwargs["_exact_authorization_generation"] == 4
        assert kwargs["_skip_linked_fan_out"] is True
        db.refresh(receipt)
        assert receipt.status == WhoopSyncDispatchStatus.SUCCEEDED.value
        assert receipt.execution_attempt_count == 1
        assert db.query(WhoopAuthorizationLease).filter_by(user_id=user.id).count() == 0

    @patch("app.integrations.celery.tasks.whoop_sync_dispatch_task.sync_vendor_data.run")
    @patch("app.integrations.celery.tasks.whoop_sync_dispatch_task.SessionLocal")
    def test_generation_advance_after_enqueue_supersedes_without_provider_call(
        self,
        mock_session_local: MagicMock,
        mock_sync: MagicMock,
        db: Session,
    ) -> None:
        _user, connection, receipt = _receipt(db, generation=1)
        connection.authorization_generation = 2
        db.commit()
        mock_session_local.return_value.__enter__.return_value = db
        mock_session_local.return_value.__exit__.return_value = None

        result = execute_whoop_sync_dispatch.run(str(receipt.id))

        assert result["status"] == "superseded"
        mock_sync.assert_not_called()
        db.refresh(receipt)
        assert receipt.status == WhoopSyncDispatchStatus.SUPERSEDED.value
        assert receipt.error_code == "authorization_generation_superseded"

    def test_preclaim_crash_lease_can_be_taken_over_without_replay(self, db: Session) -> None:
        user, connection, receipt = _receipt(db)
        repository = WhoopSyncDispatchRepository()
        crashed_token = uuid4()
        assert repository.try_acquire_authorization_lease(
            db,
            user_id=user.id,
            connection_id=connection.id,
            authorization_generation=1,
            lease_token=crashed_token,
            lease_kind="full_history_sync",
        )
        lease = db.get(WhoopAuthorizationLease, user.id)
        assert lease is not None
        now = datetime.now(timezone.utc)
        lease.acquired_at = now - timedelta(minutes=10)
        lease.lease_expires_at = now - WHOOP_AUTHORIZATION_RECOVERY_GRACE - timedelta(seconds=1)
        db.commit()

        takeover_token = uuid4()
        assert repository.try_acquire_authorization_lease(
            db,
            user_id=user.id,
            connection_id=connection.id,
            authorization_generation=1,
            lease_token=takeover_token,
            lease_kind="full_history_sync",
        )
        claimed = repository.claim_execution(db, dispatch_id=receipt.id, lease_token=takeover_token)

        assert claimed is not None
        assert claimed.status == WhoopSyncDispatchStatus.RUNNING.value
        assert claimed.execution_attempt_count == 1

    def test_oauth_callback_crash_lease_is_recoverable(self, db: Session) -> None:
        user, connection, receipt = _receipt(db)
        repository = WhoopSyncDispatchRepository()
        callback_token = uuid4()
        assert repository.try_acquire_authorization_lease(
            db,
            user_id=user.id,
            connection_id=connection.id,
            authorization_generation=1,
            lease_token=callback_token,
            lease_kind="oauth_callback",
        )
        lease = db.get(WhoopAuthorizationLease, user.id)
        assert lease is not None
        now = datetime.now(timezone.utc)
        lease.acquired_at = now - timedelta(minutes=10)
        lease.lease_expires_at = now - WHOOP_AUTHORIZATION_RECOVERY_GRACE - timedelta(seconds=1)
        db.commit()

        worker_token = uuid4()
        assert repository.try_acquire_authorization_lease(
            db,
            user_id=user.id,
            connection_id=connection.id,
            authorization_generation=1,
            lease_token=worker_token,
            lease_kind="full_history_sync",
        )
        assert receipt.status == WhoopSyncDispatchStatus.QUEUED.value

    @patch("app.integrations.celery.tasks.whoop_sync_dispatch_task.sync_vendor_data.run")
    @patch("app.integrations.celery.tasks.whoop_sync_dispatch_task.SessionLocal")
    def test_running_crash_expires_terminally_and_is_never_replayed(
        self,
        mock_session_local: MagicMock,
        mock_sync: MagicMock,
        db: Session,
    ) -> None:
        user, connection, receipt = _receipt(db)
        repository = WhoopSyncDispatchRepository()
        crashed_token = uuid4()
        assert repository.try_acquire_authorization_lease(
            db,
            user_id=user.id,
            connection_id=connection.id,
            authorization_generation=1,
            lease_token=crashed_token,
            lease_kind="full_history_sync",
        )
        assert repository.claim_execution(db, dispatch_id=receipt.id, lease_token=crashed_token)
        lease = db.get(WhoopAuthorizationLease, user.id)
        assert lease is not None
        now = datetime.now(timezone.utc)
        lease.acquired_at = now - timedelta(minutes=10)
        lease.lease_expires_at = now - WHOOP_AUTHORIZATION_RECOVERY_GRACE - timedelta(seconds=1)
        db.commit()

        assert repository.recover_expired_authorization_leases(db) == 1
        db.refresh(receipt)
        assert receipt.status == WhoopSyncDispatchStatus.FAILED.value
        assert receipt.error_code == "worker_lease_expired"
        assert db.get(WhoopAuthorizationLease, user.id) is None

        mock_session_local.return_value.__enter__.return_value = db
        mock_session_local.return_value.__exit__.return_value = None
        result = execute_whoop_sync_dispatch.run(str(receipt.id))
        assert result["status"] == "failed"
        mock_sync.assert_not_called()

    def test_finish_after_lease_expiry_fails_terminally(self, db: Session) -> None:
        user, connection, receipt = _receipt(db)
        repository = WhoopSyncDispatchRepository()
        lease_token = uuid4()
        assert repository.try_acquire_authorization_lease(
            db,
            user_id=user.id,
            connection_id=connection.id,
            authorization_generation=1,
            lease_token=lease_token,
            lease_kind="full_history_sync",
        )
        assert repository.claim_execution(db, dispatch_id=receipt.id, lease_token=lease_token)
        lease = db.get(WhoopAuthorizationLease, user.id)
        assert lease is not None
        now = datetime.now(timezone.utc)
        lease.acquired_at = now - timedelta(minutes=10)
        lease.lease_expires_at = now - timedelta(seconds=1)
        db.commit()

        assert not repository.finish_execution(
            db,
            dispatch_id=receipt.id,
            lease_token=lease_token,
            status=WhoopSyncDispatchStatus.SUCCEEDED,
            error_code=None,
        )
        db.refresh(receipt)
        assert receipt.status == WhoopSyncDispatchStatus.FAILED.value
        assert receipt.error_code == "worker_lease_expired"
        assert db.get(WhoopAuthorizationLease, user.id) is None

    @patch("app.integrations.celery.tasks.whoop_sync_dispatch_task.sync_vendor_data.run")
    @patch("app.integrations.celery.tasks.whoop_sync_dispatch_task.SessionLocal")
    def test_finalization_error_preserves_running_lease_for_terminal_recovery(
        self,
        mock_session_local: MagicMock,
        mock_sync: MagicMock,
        db: Session,
    ) -> None:
        user, _connection, receipt = _receipt(db)
        mock_session_local.return_value.__enter__.return_value = db
        mock_session_local.return_value.__exit__.return_value = None
        mock_sync.return_value = {
            "providers_synced": {"whoop": {"success": True, "params": {}}},
            "errors": {},
        }

        with (
            patch.object(
                WhoopSyncDispatchRepository,
                "finish_execution",
                side_effect=RuntimeError("database unavailable during finalization"),
            ),
            pytest.raises(RuntimeError, match="finalization"),
        ):
            execute_whoop_sync_dispatch.run(str(receipt.id))

        db.refresh(receipt)
        lease = db.get(WhoopAuthorizationLease, user.id)
        assert receipt.status == WhoopSyncDispatchStatus.RUNNING.value
        assert lease is not None
        now = datetime.now(timezone.utc)
        lease.acquired_at = now - timedelta(minutes=10)
        lease.lease_expires_at = now - WHOOP_AUTHORIZATION_RECOVERY_GRACE - timedelta(seconds=1)
        db.commit()

        assert WhoopSyncDispatchRepository().recover_expired_authorization_leases(db) == 1
        db.refresh(receipt)
        assert receipt.status == WhoopSyncDispatchStatus.FAILED.value
        assert receipt.error_code == "worker_lease_expired"

    @patch("app.integrations.celery.tasks.whoop_sync_dispatch_task.SessionLocal")
    def test_post_claim_refresh_error_cannot_orphan_running_receipt(
        self,
        mock_session_local: MagicMock,
        db: Session,
    ) -> None:
        user, _connection, receipt = _receipt(db)
        mock_session_local.return_value.__enter__.return_value = db
        mock_session_local.return_value.__exit__.return_value = None

        with (
            patch.object(db, "refresh", side_effect=RuntimeError("refresh failed after claim commit")),
            pytest.raises(RuntimeError, match="refresh failed"),
        ):
            execute_whoop_sync_dispatch.run(str(receipt.id))

        db.expire_all()
        running = db.get(WhoopSyncDispatchReceipt, receipt.id)
        lease = db.get(WhoopAuthorizationLease, user.id)
        assert running is not None
        assert running.status == WhoopSyncDispatchStatus.RUNNING.value
        assert lease is not None
        now = datetime.now(timezone.utc)
        lease.acquired_at = now - timedelta(minutes=10)
        lease.lease_expires_at = now - WHOOP_AUTHORIZATION_RECOVERY_GRACE - timedelta(seconds=1)
        db.commit()

        assert WhoopSyncDispatchRepository().recover_expired_authorization_leases(db) == 1
        db.refresh(running)
        assert running.status == WhoopSyncDispatchStatus.FAILED.value
        assert running.error_code == "worker_lease_expired"

    def test_orphaned_running_receipt_is_terminally_recovered_without_replay(self, db: Session) -> None:
        user, connection, receipt = _receipt(db)
        repository = WhoopSyncDispatchRepository()
        lease_token = uuid4()
        assert repository.try_acquire_authorization_lease(
            db,
            user_id=user.id,
            connection_id=connection.id,
            authorization_generation=1,
            lease_token=lease_token,
            lease_kind="full_history_sync",
        )
        assert repository.claim_execution(db, dispatch_id=receipt.id, lease_token=lease_token)
        lease = db.get(WhoopAuthorizationLease, user.id)
        assert lease is not None
        db.delete(lease)
        receipt.updated_at = datetime.now(timezone.utc) - WHOOP_AUTHORIZATION_RECOVERY_GRACE - timedelta(seconds=1)
        db.commit()

        assert repository.recover_expired_authorization_leases(db) == 1
        db.refresh(receipt)
        assert receipt.status == WhoopSyncDispatchStatus.FAILED.value
        assert receipt.error_code == "worker_lease_missing"
        assert receipt.execution_attempt_count == 1

    def test_expired_runtime_authority_rejects_the_next_persistence_commit(self, db: Session) -> None:
        user, connection, receipt = _receipt(db)
        repository = WhoopSyncDispatchRepository()
        lease_token = uuid4()
        assert repository.try_acquire_authorization_lease(
            db,
            user_id=user.id,
            connection_id=connection.id,
            authorization_generation=1,
            lease_token=lease_token,
            lease_kind="full_history_sync",
        )
        assert repository.claim_execution(db, dispatch_id=receipt.id, lease_token=lease_token)
        authority = ExactWhoopSyncAuthority(
            dispatch_id=receipt.id,
            user_id=user.id,
            connection_id=connection.id,
            authorization_generation=1,
            lease_token=lease_token,
        )
        remove_guard = _install_exact_whoop_commit_guard(db, authority)
        original_synced_at = connection.last_synced_at
        try:
            lease = db.get(WhoopAuthorizationLease, user.id)
            assert lease is not None
            now = datetime.now(timezone.utc)
            lease.acquired_at = now - timedelta(minutes=10)
            lease.lease_expires_at = now - timedelta(seconds=1)
            db.flush()
            connection.last_synced_at = datetime.now(timezone.utc)

            with pytest.raises(HealthWriteAuthorityError, match="lease was lost"):
                db.commit()
            db.rollback()
        finally:
            remove_guard()

        db.refresh(connection)
        assert connection.last_synced_at == original_synced_at
        assert authority.lease_lost.is_set()

    def test_runtime_heartbeat_renews_during_long_non_http_work(self) -> None:
        renewed = Event()
        authority = ExactWhoopSyncAuthority(
            dispatch_id=uuid4(),
            user_id=uuid4(),
            connection_id=uuid4(),
            authorization_generation=1,
            lease_token=uuid4(),
        )

        def record_renewal(*_args: object, **_kwargs: object) -> bool:
            renewed.set()
            return True

        session_local = MagicMock()
        session_local.return_value.__enter__.return_value = MagicMock()
        heartbeat = _WhoopAuthorizationHeartbeat(authority, interval_seconds=0.001)
        with (
            patch("app.integrations.celery.tasks.whoop_sync_dispatch_task.SessionLocal", session_local),
            patch.object(WhoopSyncDispatchRepository, "renew_runtime_authority", side_effect=record_renewal),
        ):
            heartbeat.start()
            try:
                assert renewed.wait(timeout=1)
            finally:
                heartbeat.stop()

        assert not authority.lease_lost.is_set()
