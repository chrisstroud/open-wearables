from datetime import datetime, timedelta, timezone
from uuid import UUID, uuid4

from sqlalchemy import or_

from app.database import DbSession
from app.models import User, WhoopHistoryDispatch
from app.repositories.health_write_authority import HealthWriteAuthorityError, require_user_connection_authority
from app.schemas.auth import ConnectionStatus


class HistoryConflictError(RuntimeError):
    pass


class WhoopHistoryRepository:
    def get(self, db: DbSession, user_id: UUID, dispatch_id: UUID) -> WhoopHistoryDispatch | None:
        return (
            db.query(WhoopHistoryDispatch).filter_by(id=dispatch_id, user_id=user_id).populate_existing().one_or_none()
        )

    def admit(
        self,
        db: DbSession,
        *,
        user_id: UUID,
        connection_id: UUID,
        generation: int,
        dispatch_id: UUID,
        start: datetime,
        end: datetime,
    ) -> WhoopHistoryDispatch:
        connection = require_user_connection_authority(
            db,
            connection_id=connection_id,
            expected_user_id=user_id,
            expected_provider="whoop",
        )
        if (
            connection.authorization_generation != generation
            or connection.status
            not in {
                ConnectionStatus.ACTIVE,
                ConnectionStatus.EXPIRED,
            }
            or not connection.access_token
        ):
            raise HealthWriteAuthorityError("Provider authorization changed")
        user = db.get(User, user_id)
        assert user is not None
        existing = db.get(WhoopHistoryDispatch, dispatch_id)
        if existing is not None:
            if (
                existing.user_id,
                existing.connection_id,
                existing.authorization_generation,
                existing.health_evidence_generation,
                existing.requested_start_at,
                existing.requested_end_at,
            ) != (
                user_id,
                connection_id,
                generation,
                user.health_evidence_generation,
                start,
                end,
            ):
                raise HistoryConflictError("idempotency_conflict")
            db.commit()
            return existing
        active = (
            db.query(WhoopHistoryDispatch)
            .filter(
                WhoopHistoryDispatch.user_id == user_id,
                WhoopHistoryDispatch.status.in_(("queued", "running")),
            )
            .with_for_update()
            .one_or_none()
        )
        if active is not None:
            if (active.connection_id, active.authorization_generation, active.health_evidence_generation) == (
                connection_id,
                generation,
                user.health_evidence_generation,
            ):
                raise HistoryConflictError("history_already_active")
            self._finish(active, "superseded", "authorization_changed")
            db.flush()
        dispatch = WhoopHistoryDispatch(
            id=dispatch_id,
            user_id=user_id,
            connection_id=connection_id,
            authorization_generation=generation,
            health_evidence_generation=user.health_evidence_generation,
            task_id=uuid4(),
            requested_start_at=start,
            requested_end_at=end,
            status="queued",
            expires_at=None,
            completed_at=None,
            error_code=None,
        )
        db.add(dispatch)
        db.commit()
        db.refresh(dispatch)
        return dispatch

    def claim(self, db: DbSession, dispatch_id: UUID) -> WhoopHistoryDispatch | None:
        initial = db.get(WhoopHistoryDispatch, dispatch_id)
        if initial is None or initial.status != "queued":
            return None
        user_id, connection_id, generation, health_generation = (
            initial.user_id,
            initial.connection_id,
            initial.authorization_generation,
            initial.health_evidence_generation,
        )
        try:
            connection = require_user_connection_authority(
                db,
                connection_id=connection_id,
                expected_user_id=user_id,
                expected_provider="whoop",
            )
            user = db.get(User, user_id)
            if (
                connection.authorization_generation != generation
                or user is None
                or user.health_evidence_generation != health_generation
                or connection.status not in {ConnectionStatus.ACTIVE, ConnectionStatus.EXPIRED}
                or not connection.access_token
            ):
                raise HealthWriteAuthorityError("Provider authorization changed")
        except HealthWriteAuthorityError:
            db.rollback()
            self.finish(db, dispatch_id, "superseded", "authorization_changed", expected_status="queued")
            return None
        dispatch = (
            db.query(WhoopHistoryDispatch).filter_by(id=dispatch_id).populate_existing().with_for_update().one_or_none()
        )
        if dispatch is None or dispatch.status != "queued":
            db.rollback()
            return None
        dispatch.status = "running"
        dispatch.expires_at = datetime.now(timezone.utc) + timedelta(minutes=15)
        db.commit()
        db.refresh(dispatch)
        return dispatch

    @staticmethod
    def _finish(dispatch: WhoopHistoryDispatch, status: str, error: str | None) -> None:
        dispatch.status = status
        dispatch.error_code = error
        dispatch.completed_at = datetime.now(timezone.utc)

    def finish(
        self,
        db: DbSession,
        dispatch_id: UUID,
        status: str,
        error: str | None,
        *,
        expected_status: str = "running",
    ) -> None:
        dispatch = (
            db.query(WhoopHistoryDispatch).filter_by(id=dispatch_id).populate_existing().with_for_update().one_or_none()
        )
        if dispatch is not None and dispatch.status == expected_status:
            self._finish(dispatch, status, error)
        db.commit()

    def pending(self, db: DbSession) -> list[UUID]:
        rows = (
            db.query(WhoopHistoryDispatch)
            .filter(
                or_(
                    WhoopHistoryDispatch.status == "queued",
                    (WhoopHistoryDispatch.status == "running")
                    & (WhoopHistoryDispatch.expires_at <= datetime.now(timezone.utc)),
                )
            )
            .order_by(WhoopHistoryDispatch.created_at)
            .limit(100)
            .with_for_update(skip_locked=True)
            .all()
        )
        queued = []
        for dispatch in rows:
            if dispatch.status == "running":
                self._finish(dispatch, "failed", "worker_timeout")
            else:
                queued.append(dispatch.id)
        db.commit()
        return queued
