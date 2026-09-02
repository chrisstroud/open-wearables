import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from sqlalchemy import and_, delete, exists, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError

from app.database import DbSession
from app.models import User, UserConnection, WhoopAuthorizationLease, WhoopSyncDispatchReceipt
from app.schemas.auth import ConnectionStatus
from app.schemas.whoop_sync_dispatch import WhoopFullHistorySyncCommand, WhoopSyncDispatchStatus


class WhoopSyncDispatchConflictError(RuntimeError):
    """The requested command does not match current durable authority."""


@dataclass(frozen=True)
class WhoopDispatchDelivery:
    dispatch_id: UUID
    task_id: UUID


WHOOP_AUTHORIZATION_LEASE_TTL = timedelta(minutes=5)
WHOOP_AUTHORIZATION_RECOVERY_GRACE = timedelta(seconds=60)


def _fingerprint(
    *,
    user_id: UUID,
    connection_id: UUID,
    command: WhoopFullHistorySyncCommand,
) -> str:
    canonical = json.dumps(
        {
            "authorization_generation": command.authorization_generation,
            "connection_id": str(connection_id),
            "requested_end_at": command.requested_end_at.isoformat(),
            "requested_start_at": command.requested_start_at.isoformat(),
            "user_id": str(user_id),
        },
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


class WhoopSyncDispatchRepository:
    """Persistence boundary for exact WHOOP history commands and leases."""

    def create_or_get(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        connection_id: UUID,
        command: WhoopFullHistorySyncCommand,
    ) -> WhoopSyncDispatchReceipt:
        fingerprint = _fingerprint(user_id=user_id, connection_id=connection_id, command=command)
        existing = db_session.get(WhoopSyncDispatchReceipt, command.idempotency_key)
        if existing is not None:
            if existing.user_id != user_id or existing.request_fingerprint != fingerprint:
                raise WhoopSyncDispatchConflictError("Idempotency key was already used for a different command")
            return existing

        semantic_match = (
            db_session.query(WhoopSyncDispatchReceipt)
            .filter(
                WhoopSyncDispatchReceipt.user_id == user_id,
                WhoopSyncDispatchReceipt.connection_id == connection_id,
                WhoopSyncDispatchReceipt.authorization_generation == command.authorization_generation,
                WhoopSyncDispatchReceipt.requested_start_at == command.requested_start_at,
                WhoopSyncDispatchReceipt.requested_end_at == command.requested_end_at,
                WhoopSyncDispatchReceipt.request_fingerprint == fingerprint,
            )
            .one_or_none()
        )
        if semantic_match is not None:
            return semantic_match

        connection = (
            db_session.query(UserConnection)
            .join(User, User.id == UserConnection.user_id)
            .filter(
                UserConnection.id == connection_id,
                UserConnection.user_id == user_id,
                UserConnection.provider == "whoop",
                UserConnection.status == ConnectionStatus.ACTIVE,
                UserConnection.authorization_generation == command.authorization_generation,
                User.health_write_state == "active",
            )
            .populate_existing()
            .with_for_update()
            .one_or_none()
        )
        if connection is None:
            raise WhoopSyncDispatchConflictError("WHOOP connection generation is no longer active")

        now = datetime.now(timezone.utc)
        receipt = WhoopSyncDispatchReceipt(
            id=command.idempotency_key,
            user_id=user_id,
            connection_id=connection_id,
            authorization_generation=command.authorization_generation,
            request_fingerprint=fingerprint,
            requested_start_at=command.requested_start_at,
            requested_end_at=command.requested_end_at,
            task_id=uuid4(),
            status=WhoopSyncDispatchStatus.QUEUED.value,
            enqueue_attempt_count=0,
            execution_attempt_count=0,
            next_enqueue_at=now,
            enqueued_at=None,
            lease_token=None,
            processing_started_at=None,
            completed_at=None,
            error_code=None,
            updated_at=now,
        )
        db_session.add(receipt)
        try:
            db_session.commit()
        except IntegrityError:
            db_session.rollback()
            concurrent_by_key = db_session.get(WhoopSyncDispatchReceipt, command.idempotency_key)
            if concurrent_by_key is not None:
                if concurrent_by_key.user_id != user_id or concurrent_by_key.request_fingerprint != fingerprint:
                    raise WhoopSyncDispatchConflictError(
                        "Concurrent WHOOP command conflicts with this request"
                    ) from None
                return concurrent_by_key
            concurrent_semantic = (
                db_session.query(WhoopSyncDispatchReceipt)
                .filter(
                    WhoopSyncDispatchReceipt.user_id == user_id,
                    WhoopSyncDispatchReceipt.connection_id == connection_id,
                    WhoopSyncDispatchReceipt.authorization_generation == command.authorization_generation,
                    WhoopSyncDispatchReceipt.requested_start_at == command.requested_start_at,
                    WhoopSyncDispatchReceipt.requested_end_at == command.requested_end_at,
                    WhoopSyncDispatchReceipt.request_fingerprint == fingerprint,
                )
                .one_or_none()
            )
            if concurrent_semantic is None:
                raise WhoopSyncDispatchConflictError("Concurrent WHOOP command conflicts with this request") from None
            return concurrent_semantic
        db_session.refresh(receipt)
        return receipt

    def get_for_user(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        dispatch_id: UUID,
    ) -> WhoopSyncDispatchReceipt | None:
        return (
            db_session.query(WhoopSyncDispatchReceipt)
            .filter(
                WhoopSyncDispatchReceipt.id == dispatch_id,
                WhoopSyncDispatchReceipt.user_id == user_id,
            )
            .one_or_none()
        )

    def get(
        self,
        db_session: DbSession,
        *,
        dispatch_id: UUID,
    ) -> WhoopSyncDispatchReceipt | None:
        return db_session.get(WhoopSyncDispatchReceipt, dispatch_id)

    def due_deliveries(
        self,
        db_session: DbSession,
        *,
        limit: int = 100,
    ) -> tuple[WhoopDispatchDelivery, ...]:
        now = self._database_now(db_session)
        self._recover_expired_authorization_leases(db_session, now=now)
        receipts = (
            db_session.execute(
                select(WhoopSyncDispatchReceipt)
                .where(
                    WhoopSyncDispatchReceipt.status == WhoopSyncDispatchStatus.QUEUED.value,
                    WhoopSyncDispatchReceipt.next_enqueue_at <= now,
                )
                .order_by(WhoopSyncDispatchReceipt.created_at, WhoopSyncDispatchReceipt.id)
                .limit(limit)
                .with_for_update(skip_locked=True)
            )
            .scalars()
            .all()
        )
        deliveries: list[WhoopDispatchDelivery] = []
        for receipt in receipts:
            receipt.enqueue_attempt_count += 1
            receipt.enqueued_at = now
            receipt.next_enqueue_at = now + timedelta(seconds=60)
            receipt.updated_at = now
            deliveries.append(WhoopDispatchDelivery(receipt.id, receipt.task_id))
        db_session.commit()
        return tuple(deliveries)

    def try_acquire_authorization_lease(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        connection_id: UUID | None,
        authorization_generation: int,
        lease_token: UUID,
        lease_kind: str,
    ) -> bool:
        if lease_kind not in {"oauth_callback", "full_history_sync", "disconnect", "token_refresh"}:
            raise ValueError(f"Unsupported WHOOP authorization lease kind: {lease_kind}")

        user = db_session.query(User).filter(User.id == user_id).populate_existing().with_for_update().one_or_none()
        if user is None or user.health_write_state != "active":
            db_session.commit()
            return False

        now = self._database_now(db_session)
        recovered = self._recover_expired_authorization_leases(db_session, now=now, user_id=user_id)
        if recovered:
            db_session.flush()

        if connection_id is None:
            current_connection = (
                db_session.query(UserConnection)
                .filter(
                    UserConnection.user_id == user_id,
                    UserConnection.provider == "whoop",
                )
                .populate_existing()
                .with_for_update()
                .one_or_none()
            )
            valid_subject = (
                lease_kind == "oauth_callback" and authorization_generation == 0 and current_connection is None
            )
        else:
            current_connection = (
                db_session.query(UserConnection)
                .filter(
                    UserConnection.id == connection_id,
                    UserConnection.user_id == user_id,
                    UserConnection.provider == "whoop",
                )
                .populate_existing()
                .with_for_update()
                .one_or_none()
            )
            valid_subject = bool(
                current_connection is not None
                and current_connection.authorization_generation == authorization_generation
                and (
                    lease_kind in {"oauth_callback", "disconnect"}
                    or current_connection.status == ConnectionStatus.ACTIVE
                )
            )

        if not valid_subject:
            db_session.commit()
            return False

        inserted_token = db_session.execute(
            insert(WhoopAuthorizationLease)
            .values(
                user_id=user_id,
                connection_id=connection_id,
                authorization_generation=authorization_generation,
                lease_token=lease_token,
                lease_kind=lease_kind,
                acquired_at=func.clock_timestamp(),
                lease_expires_at=func.clock_timestamp() + WHOOP_AUTHORIZATION_LEASE_TTL,
                updated_at=func.clock_timestamp(),
            )
            .on_conflict_do_nothing(index_elements=[WhoopAuthorizationLease.user_id])
            .returning(WhoopAuthorizationLease.lease_token)
        ).scalar_one_or_none()
        acquired = inserted_token == lease_token
        db_session.commit()
        return acquired

    @staticmethod
    def _database_now(db_session: DbSession) -> datetime:
        return db_session.execute(select(func.clock_timestamp())).scalar_one()

    def _recover_expired_authorization_leases(
        self,
        db_session: DbSession,
        *,
        now: datetime,
        user_id: UUID | None = None,
    ) -> int:
        recovery_cutoff = now - WHOOP_AUTHORIZATION_RECOVERY_GRACE
        lease_query = select(WhoopAuthorizationLease).where(WhoopAuthorizationLease.lease_expires_at <= recovery_cutoff)
        if user_id is not None:
            lease_query = lease_query.where(WhoopAuthorizationLease.user_id == user_id)
        leases = (
            db_session.execute(lease_query.order_by(WhoopAuthorizationLease.user_id).with_for_update(skip_locked=True))
            .scalars()
            .all()
        )
        for lease in leases:
            if lease.lease_kind == "full_history_sync":
                receipt = (
                    db_session.query(WhoopSyncDispatchReceipt)
                    .filter(
                        WhoopSyncDispatchReceipt.user_id == lease.user_id,
                        WhoopSyncDispatchReceipt.status == WhoopSyncDispatchStatus.RUNNING.value,
                        WhoopSyncDispatchReceipt.lease_token == lease.lease_token,
                    )
                    .with_for_update()
                    .one_or_none()
                )
                if receipt is not None:
                    receipt.status = WhoopSyncDispatchStatus.FAILED.value
                    receipt.lease_token = None
                    receipt.completed_at = now
                    receipt.error_code = "worker_lease_expired"
                    receipt.updated_at = now
            db_session.delete(lease)
        if leases:
            db_session.flush()

        orphan_query = select(WhoopSyncDispatchReceipt).where(
            WhoopSyncDispatchReceipt.status == WhoopSyncDispatchStatus.RUNNING.value,
            WhoopSyncDispatchReceipt.lease_token.is_not(None),
            WhoopSyncDispatchReceipt.updated_at <= recovery_cutoff,
            ~exists().where(
                WhoopAuthorizationLease.user_id == WhoopSyncDispatchReceipt.user_id,
                WhoopAuthorizationLease.lease_token == WhoopSyncDispatchReceipt.lease_token,
            ),
        )
        if user_id is not None:
            orphan_query = orphan_query.where(WhoopSyncDispatchReceipt.user_id == user_id)
        orphaned_receipts = (
            db_session.execute(
                orphan_query.order_by(WhoopSyncDispatchReceipt.user_id).with_for_update(skip_locked=True)
            )
            .scalars()
            .all()
        )
        for receipt in orphaned_receipts:
            receipt.status = WhoopSyncDispatchStatus.FAILED.value
            receipt.lease_token = None
            receipt.completed_at = now
            receipt.error_code = "worker_lease_missing"
            receipt.updated_at = now
        return len(leases) + len(orphaned_receipts)

    def recover_expired_authorization_leases(self, db_session: DbSession) -> int:
        now = self._database_now(db_session)
        recovered = self._recover_expired_authorization_leases(db_session, now=now)
        db_session.commit()
        return recovered

    def renew_runtime_authority(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        connection_id: UUID,
        authorization_generation: int,
        lease_token: UUID,
    ) -> bool:
        return self.renew_authorization_lease(
            db_session,
            user_id=user_id,
            connection_id=connection_id,
            authorization_generation=authorization_generation,
            lease_token=lease_token,
            lease_kind="full_history_sync",
        )

    def renew_authorization_lease(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        connection_id: UUID,
        authorization_generation: int,
        lease_token: UUID,
        lease_kind: str,
    ) -> bool:
        current_connection = exists().where(
            UserConnection.id == WhoopAuthorizationLease.connection_id,
            UserConnection.user_id == WhoopAuthorizationLease.user_id,
            UserConnection.provider == "whoop",
            UserConnection.authorization_generation == WhoopAuthorizationLease.authorization_generation,
            *(
                (UserConnection.status == ConnectionStatus.ACTIVE,)
                if lease_kind in {"full_history_sync", "token_refresh"}
                else ()
            ),
        )
        renewed_token = db_session.execute(
            update(WhoopAuthorizationLease)
            .where(
                WhoopAuthorizationLease.user_id == user_id,
                WhoopAuthorizationLease.connection_id == connection_id,
                WhoopAuthorizationLease.authorization_generation == authorization_generation,
                WhoopAuthorizationLease.lease_token == lease_token,
                WhoopAuthorizationLease.lease_kind == lease_kind,
                WhoopAuthorizationLease.lease_expires_at > func.clock_timestamp(),
                current_connection,
            )
            .values(
                lease_expires_at=func.clock_timestamp() + WHOOP_AUTHORIZATION_LEASE_TTL,
                updated_at=func.clock_timestamp(),
            )
            .returning(WhoopAuthorizationLease.lease_token)
        ).scalar_one_or_none()
        renewed = renewed_token == lease_token
        if renewed and lease_kind == "full_history_sync":
            db_session.execute(
                update(WhoopSyncDispatchReceipt)
                .where(
                    WhoopSyncDispatchReceipt.user_id == user_id,
                    WhoopSyncDispatchReceipt.connection_id == connection_id,
                    WhoopSyncDispatchReceipt.authorization_generation == authorization_generation,
                    WhoopSyncDispatchReceipt.lease_token == lease_token,
                    WhoopSyncDispatchReceipt.status == WhoopSyncDispatchStatus.RUNNING.value,
                )
                .values(updated_at=func.clock_timestamp())
            )
        db_session.commit()
        return renewed

    def validate_runtime_authority(
        self,
        db_session: DbSession,
        *,
        dispatch_id: UUID,
        user_id: UUID,
        connection_id: UUID,
        authorization_generation: int,
        lease_token: UUID,
        for_update: bool = False,
    ) -> bool:
        """Validate the complete live exact-sync tuple using database time."""
        query = (
            select(WhoopAuthorizationLease.lease_token)
            .join(User, User.id == WhoopAuthorizationLease.user_id)
            .join(UserConnection, UserConnection.id == WhoopAuthorizationLease.connection_id)
            .join(
                WhoopSyncDispatchReceipt,
                and_(
                    WhoopSyncDispatchReceipt.user_id == WhoopAuthorizationLease.user_id,
                    WhoopSyncDispatchReceipt.connection_id == WhoopAuthorizationLease.connection_id,
                    WhoopSyncDispatchReceipt.lease_token == WhoopAuthorizationLease.lease_token,
                ),
            )
            .where(
                WhoopAuthorizationLease.user_id == user_id,
                WhoopAuthorizationLease.connection_id == connection_id,
                WhoopAuthorizationLease.authorization_generation == authorization_generation,
                WhoopAuthorizationLease.lease_token == lease_token,
                WhoopAuthorizationLease.lease_kind == "full_history_sync",
                WhoopAuthorizationLease.lease_expires_at > func.clock_timestamp(),
                User.health_write_state == "active",
                UserConnection.user_id == user_id,
                UserConnection.provider == "whoop",
                UserConnection.status == ConnectionStatus.ACTIVE,
                UserConnection.authorization_generation == authorization_generation,
                WhoopSyncDispatchReceipt.id == dispatch_id,
                WhoopSyncDispatchReceipt.authorization_generation == authorization_generation,
                WhoopSyncDispatchReceipt.status == WhoopSyncDispatchStatus.RUNNING.value,
            )
        )
        if for_update:
            query = query.with_for_update()
        return db_session.execute(query).scalar_one_or_none() == lease_token

    def renew_oauth_callback_authority(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        lease_token: UUID,
    ) -> bool:
        matching_existing_connection = exists().where(
            UserConnection.id == WhoopAuthorizationLease.connection_id,
            UserConnection.user_id == WhoopAuthorizationLease.user_id,
            UserConnection.provider == "whoop",
            UserConnection.authorization_generation == WhoopAuthorizationLease.authorization_generation,
        )
        no_connection_created_since_claim = ~exists().where(
            UserConnection.user_id == WhoopAuthorizationLease.user_id,
            UserConnection.provider == "whoop",
        )
        renewed_token = db_session.execute(
            update(WhoopAuthorizationLease)
            .where(
                WhoopAuthorizationLease.user_id == user_id,
                WhoopAuthorizationLease.lease_token == lease_token,
                WhoopAuthorizationLease.lease_kind == "oauth_callback",
                WhoopAuthorizationLease.lease_expires_at > func.clock_timestamp(),
                or_(
                    matching_existing_connection,
                    and_(
                        WhoopAuthorizationLease.connection_id.is_(None),
                        WhoopAuthorizationLease.authorization_generation == 0,
                        no_connection_created_since_claim,
                    ),
                ),
            )
            .values(
                lease_expires_at=func.clock_timestamp() + WHOOP_AUTHORIZATION_LEASE_TTL,
                updated_at=func.clock_timestamp(),
            )
            .returning(WhoopAuthorizationLease.lease_token)
        ).scalar_one_or_none()
        renewed = renewed_token == lease_token
        db_session.commit()
        return renewed

    def release_authorization_lease(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        lease_token: UUID,
        commit: bool = True,
    ) -> bool:
        running_receipt = exists().where(
            WhoopSyncDispatchReceipt.user_id == WhoopAuthorizationLease.user_id,
            WhoopSyncDispatchReceipt.lease_token == WhoopAuthorizationLease.lease_token,
            WhoopSyncDispatchReceipt.status == WhoopSyncDispatchStatus.RUNNING.value,
        )
        deleted_token = db_session.execute(
            delete(WhoopAuthorizationLease)
            .where(
                WhoopAuthorizationLease.user_id == user_id,
                WhoopAuthorizationLease.lease_token == lease_token,
                ~running_receipt,
            )
            .returning(WhoopAuthorizationLease.lease_token)
        ).scalar_one_or_none()
        if commit:
            db_session.commit()
        return deleted_token == lease_token

    def supersede_if_authority_stale(
        self,
        db_session: DbSession,
        *,
        dispatch_id: UUID,
    ) -> bool:
        """Terminalize a queued receipt only when its exact grant is no longer current."""
        receipt_snapshot = db_session.get(WhoopSyncDispatchReceipt, dispatch_id)
        if receipt_snapshot is None:
            return False
        user = (
            db_session.query(User)
            .filter(User.id == receipt_snapshot.user_id)
            .populate_existing()
            .with_for_update()
            .one_or_none()
        )
        receipt = (
            db_session.query(WhoopSyncDispatchReceipt)
            .filter(WhoopSyncDispatchReceipt.id == dispatch_id)
            .populate_existing()
            .with_for_update()
            .one_or_none()
        )
        if receipt is None or receipt.status != WhoopSyncDispatchStatus.QUEUED.value:
            db_session.commit()
            return bool(receipt is not None and receipt.status == WhoopSyncDispatchStatus.SUPERSEDED.value)
        connection = (
            db_session.query(UserConnection)
            .filter(
                UserConnection.id == receipt.connection_id,
                UserConnection.user_id == receipt.user_id,
                UserConnection.provider == "whoop",
                UserConnection.status == ConnectionStatus.ACTIVE,
                UserConnection.authorization_generation == receipt.authorization_generation,
            )
            .populate_existing()
            .with_for_update()
            .one_or_none()
        )
        if user is not None and user.health_write_state == "active" and connection is not None:
            db_session.commit()
            return False
        now = self._database_now(db_session)
        receipt.status = WhoopSyncDispatchStatus.SUPERSEDED.value
        receipt.next_enqueue_at = None
        receipt.completed_at = now
        receipt.error_code = "authorization_generation_superseded"
        receipt.updated_at = now
        db_session.commit()
        return True

    def claim_execution(
        self,
        db_session: DbSession,
        *,
        dispatch_id: UUID,
        lease_token: UUID,
    ) -> WhoopSyncDispatchReceipt | None:
        receipt = (
            db_session.query(WhoopSyncDispatchReceipt)
            .filter(WhoopSyncDispatchReceipt.id == dispatch_id)
            .populate_existing()
            .with_for_update()
            .one_or_none()
        )
        if receipt is None or receipt.status != WhoopSyncDispatchStatus.QUEUED.value:
            return None

        connection = (
            db_session.query(UserConnection)
            .join(User, User.id == UserConnection.user_id)
            .filter(
                UserConnection.id == receipt.connection_id,
                UserConnection.user_id == receipt.user_id,
                UserConnection.provider == "whoop",
                UserConnection.status == ConnectionStatus.ACTIVE,
                UserConnection.authorization_generation == receipt.authorization_generation,
                User.health_write_state == "active",
            )
            .populate_existing()
            .with_for_update()
            .one_or_none()
        )
        if connection is None:
            now = self._database_now(db_session)
            receipt.status = WhoopSyncDispatchStatus.SUPERSEDED.value
            receipt.next_enqueue_at = None
            receipt.completed_at = now
            receipt.error_code = "authorization_generation_superseded"
            receipt.updated_at = now
            db_session.commit()
            return receipt

        lease = (
            db_session.query(WhoopAuthorizationLease)
            .filter(
                WhoopAuthorizationLease.user_id == receipt.user_id,
                WhoopAuthorizationLease.connection_id == receipt.connection_id,
                WhoopAuthorizationLease.authorization_generation == receipt.authorization_generation,
                WhoopAuthorizationLease.lease_token == lease_token,
                WhoopAuthorizationLease.lease_kind == "full_history_sync",
                WhoopAuthorizationLease.lease_expires_at > func.clock_timestamp(),
            )
            .with_for_update()
            .one_or_none()
        )
        if lease is None:
            return None

        now = self._database_now(db_session)
        receipt.status = WhoopSyncDispatchStatus.RUNNING.value
        receipt.execution_attempt_count += 1
        receipt.next_enqueue_at = None
        receipt.lease_token = lease_token
        receipt.processing_started_at = now
        receipt.updated_at = now
        db_session.commit()
        db_session.refresh(receipt)
        return receipt

    def finish_execution(
        self,
        db_session: DbSession,
        *,
        dispatch_id: UUID,
        lease_token: UUID,
        status: WhoopSyncDispatchStatus,
        error_code: str | None,
    ) -> bool:
        if status not in {WhoopSyncDispatchStatus.SUCCEEDED, WhoopSyncDispatchStatus.FAILED}:
            raise ValueError("Execution may only finish as succeeded or failed")
        receipt = (
            db_session.query(WhoopSyncDispatchReceipt)
            .filter(
                WhoopSyncDispatchReceipt.id == dispatch_id,
                WhoopSyncDispatchReceipt.status == WhoopSyncDispatchStatus.RUNNING.value,
                WhoopSyncDispatchReceipt.lease_token == lease_token,
            )
            .populate_existing()
            .with_for_update()
            .one_or_none()
        )
        if receipt is None:
            return False
        lease = (
            db_session.query(WhoopAuthorizationLease)
            .filter(
                WhoopAuthorizationLease.user_id == receipt.user_id,
                WhoopAuthorizationLease.connection_id == receipt.connection_id,
                WhoopAuthorizationLease.authorization_generation == receipt.authorization_generation,
                WhoopAuthorizationLease.lease_token == lease_token,
                WhoopAuthorizationLease.lease_kind == "full_history_sync",
                WhoopAuthorizationLease.lease_expires_at > func.clock_timestamp(),
            )
            .with_for_update()
            .one_or_none()
        )
        if lease is None:
            now = self._database_now(db_session)
            receipt.status = WhoopSyncDispatchStatus.FAILED.value
            receipt.lease_token = None
            receipt.completed_at = now
            receipt.error_code = "worker_lease_expired"
            receipt.updated_at = now
            db_session.flush()
            self.release_authorization_lease(
                db_session,
                user_id=receipt.user_id,
                lease_token=lease_token,
                commit=False,
            )
            db_session.commit()
            return False
        now = self._database_now(db_session)
        receipt.status = status.value
        receipt.lease_token = None
        receipt.completed_at = now
        receipt.error_code = error_code
        receipt.updated_at = now
        db_session.flush()
        self.release_authorization_lease(
            db_session,
            user_id=receipt.user_id,
            lease_token=lease_token,
            commit=False,
        )
        db_session.commit()
        return True
