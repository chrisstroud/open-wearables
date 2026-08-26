from datetime import datetime, timedelta, timezone
from logging import getLogger
from typing import cast
from uuid import UUID, uuid4

from sqlalchemy import CursorResult, and_, func, select, tuple_, update
from sqlalchemy.orm import Query
from sqlalchemy.orm.exc import MultipleResultsFound

from app.database import DbSession
from app.models import User, UserConnection
from app.repositories.health_write_authority import (
    HealthWriteAuthorityError,
    require_health_write_authority,
    require_user_connection_authority,
)
from app.repositories.repositories import CrudRepository
from app.schemas.auth import ConnectionStatus
from app.schemas.model_crud.user_management import (
    UserConnectionCreate,
    UserConnectionUpdate,
)
from app.services.provider_identity_authority import (
    acquire_provider_identity_locks,
    provider_identity_fingerprints,
)

logger = getLogger(__name__)


class UserConnectionRepository(CrudRepository[UserConnection, UserConnectionCreate, UserConnectionUpdate]):
    """Repository for managing OAuth user connections to fitness providers."""

    def __init__(self, model: type[UserConnection] = UserConnection):
        super().__init__(model)

    @staticmethod
    def _require_write_authority(
        db_session: DbSession,
        *,
        user_id: UUID,
        provider: str,
    ) -> None:
        require_health_write_authority(db_session, user_id=user_id, provider=provider)

    def create(self, db_session: DbSession, creator: UserConnectionCreate) -> UserConnection:
        acquire_provider_identity_locks(
            db_session,
            provider_identity_fingerprints(
                creator.provider,
                provider_user_id=creator.provider_user_id,
                provider_username=creator.provider_username,
            ),
        )
        self._require_write_authority(
            db_session,
            user_id=creator.user_id,
            provider=creator.provider,
        )
        created = super().create(db_session, creator)
        assert created is not None
        return created

    @staticmethod
    def _lock_identity_update(
        db_session: DbSession,
        *,
        connection_id: UUID,
        expected_user_id: UUID,
        expected_provider: str,
        change_provider_user_id: bool,
        provider_user_id: str | None,
        change_provider_username: bool,
        provider_username: str | None,
    ) -> UserConnection:
        """Lock old/new identities, then acquire and revalidate account authority.

        An identity writer that committed while this transaction waited is
        detected before the User lock is taken. The transaction restarts so
        every acquired multi-identity lock set remains globally sorted.
        """
        while True:
            observed = (
                db_session.query(UserConnection)
                .filter(UserConnection.id == connection_id)
                .populate_existing()
                .one_or_none()
            )
            if observed is None or observed.user_id != expected_user_id or observed.provider != expected_provider:
                raise HealthWriteAuthorityError("Health connection authority changed")
            # ``populate_existing`` refreshes an identity-mapped instance in
            # place. Keep immutable scalars: comparing ``observed`` with a
            # later query result would otherwise compare the same mutated
            # Python object and miss an identity writer that committed while
            # the advisory locks were being acquired.
            observed_provider = observed.provider
            observed_provider_user_id = observed.provider_user_id
            observed_provider_username = observed.provider_username
            next_provider_user_id = provider_user_id if change_provider_user_id else observed_provider_user_id
            next_provider_username = provider_username if change_provider_username else observed_provider_username
            identities = {
                *provider_identity_fingerprints(
                    observed_provider,
                    provider_user_id=observed_provider_user_id,
                    provider_username=observed_provider_username,
                ),
                *provider_identity_fingerprints(
                    observed_provider,
                    provider_user_id=next_provider_user_id,
                    provider_username=next_provider_username,
                ),
            }
            acquire_provider_identity_locks(db_session, identities)
            refreshed = (
                db_session.query(UserConnection)
                .filter(UserConnection.id == connection_id)
                .populate_existing()
                .one_or_none()
            )
            if refreshed is None:
                raise HealthWriteAuthorityError("Health connection authority changed")
            if (
                refreshed.provider != observed_provider
                or refreshed.provider_user_id != observed_provider_user_id
                or refreshed.provider_username != observed_provider_username
            ):
                db_session.rollback()
                continue
            current = require_user_connection_authority(
                db_session,
                connection_id=connection_id,
                expected_user_id=expected_user_id,
                expected_provider=expected_provider,
            )
            if (
                current.provider == observed_provider
                and current.provider_user_id == observed_provider_user_id
                and current.provider_username == observed_provider_username
            ):
                return current
            db_session.rollback()

    def update(
        self,
        db_session: DbSession,
        originator: UserConnection,
        updater: UserConnectionUpdate,
    ) -> UserConnection:
        changes = updater.model_dump(exclude_none=True, exclude_unset=True)
        current = self._lock_identity_update(
            db_session,
            connection_id=originator.id,
            expected_user_id=originator.user_id,
            expected_provider=originator.provider,
            change_provider_user_id="provider_user_id" in changes,
            provider_user_id=changes.get("provider_user_id"),
            change_provider_username="provider_username" in changes,
            provider_username=changes.get("provider_username"),
        )
        return super().update(db_session, current, updater)

    def delete(self, db_session: DbSession, originator: UserConnection) -> UserConnection:
        current = require_user_connection_authority(
            db_session,
            connection_id=originator.id,
            expected_user_id=originator.user_id,
            expected_provider=originator.provider,
        )
        return super().delete(db_session, current)

    def delete_flush(self, db_session: DbSession, originator: UserConnection) -> None:
        current = require_user_connection_authority(
            db_session,
            connection_id=originator.id,
            expected_user_id=originator.user_id,
            expected_provider=originator.provider,
        )
        super().delete_flush(db_session, current)

    def get_active_count(self, db_session: DbSession) -> int:
        """Get total count of active connections."""
        return (
            db_session.query(func.count(self.model.id)).filter(self.model.status == ConnectionStatus.ACTIVE).scalar()
            or 0
        )

    def get_active_count_in_range(self, db_session: DbSession, start_date: datetime, end_date: datetime) -> int:
        """Get count of active connections created within a date range."""
        return (
            db_session.query(func.count(self.model.id))
            .filter(
                and_(
                    self.model.status == ConnectionStatus.ACTIVE,
                    self.model.created_at >= start_date,
                    self.model.created_at < end_date,
                ),
            )
            .scalar()
            or 0
        )

    def get_users_with_active_conn_count(self, db_session: DbSession) -> int:
        """Count of distinct users with at least one active connection."""
        return (
            db_session.query(func.count(func.distinct(self.model.user_id)))
            .filter(self.model.status == ConnectionStatus.ACTIVE)
            .scalar()
            or 0
        )

    def get_users_with_multi_active_conn_count(self, db_session: DbSession) -> int:
        """Count of distinct users with more than one active connection."""
        subq = (
            select(self.model.user_id)
            .where(self.model.status == ConnectionStatus.ACTIVE)
            .group_by(self.model.user_id)
            .having(func.count(self.model.id) > 1)
            .subquery()
        )
        return db_session.query(func.count()).select_from(subq).scalar() or 0

    def get_top_providers_by_active_conn(self, db_session: DbSession, limit: int = 3) -> list[tuple[str, int]]:
        """Top providers by active connection count, returns (provider, count) pairs."""
        rows = (
            db_session.query(self.model.provider, func.count(self.model.id).label("cnt"))
            .filter(self.model.status == ConnectionStatus.ACTIVE)
            .group_by(self.model.provider)
            .order_by(func.count(self.model.id).desc())
            .limit(limit)
            .all()
        )
        return [(row.provider, row.cnt) for row in rows]

    def get_by_user_and_provider(
        self,
        db_session: DbSession,
        user_id: UUID,
        provider: str,
    ) -> UserConnection | None:
        """Get connection for specific user and provider."""
        return (
            db_session.query(self.model)
            .join(User, User.id == self.model.user_id)
            .filter(
                and_(
                    self.model.user_id == user_id,
                    self.model.provider == provider,
                ),
            )
            .one_or_none()
        )

    def get_active_connection(
        self,
        db_session: DbSession,
        user_id: UUID,
        provider: str,
    ) -> UserConnection | None:
        """Get active connection for specific user and provider."""
        return (
            db_session.query(self.model)
            .join(User, User.id == self.model.user_id)
            .filter(
                and_(
                    self.model.user_id == user_id,
                    self.model.provider == provider,
                    self.model.status == ConnectionStatus.ACTIVE,
                    User.health_write_state == "active",
                ),
            )
            .one_or_none()
        )

    def _active_by_provider_external_id(
        self, db_session: DbSession, provider: str, provider_user_id: str
    ) -> Query[UserConnection]:
        """Base query: active connections for a given (provider, provider_user_id) pair.

        Ordered by created_at asc, id asc so the oldest connection is always
        index 0 — stable primary attribution in webhook fan-out across query
        plans and restarts.
        """
        return (
            db_session.query(self.model)
            .join(User, User.id == self.model.user_id)
            .filter(
                and_(
                    self.model.provider == provider,
                    self.model.provider_user_id == provider_user_id,
                    self.model.status == ConnectionStatus.ACTIVE,
                    User.health_write_state == "active",
                )
            )
            .order_by(self.model.created_at.asc(), self.model.id.asc())
        )

    def get_all_by_provider_user_id(
        self,
        db_session: DbSession,
        provider: str,
        provider_user_id: str,
    ) -> list[UserConnection]:
        """Get all active connections sharing the same external provider account.

        Used for multi-account sync fan-out: one provider account connected to
        several OpenWearables profiles.
        """
        return self._active_by_provider_external_id(db_session, provider, provider_user_id).all()

    def get_by_provider_user_id(
        self,
        db_session: DbSession,
        provider: str,
        provider_user_id: str,
    ) -> UserConnection | None:
        """Get connection by provider and provider's user ID.

        Useful for webhook processing where we receive provider's user ID
        and need to find our internal user.
        """
        try:
            return self._active_by_provider_external_id(db_session, provider, provider_user_id).one_or_none()
        except MultipleResultsFound:
            logger.warning(
                "Multiple active connections found for provider_user_id — returning first",
                extra={"provider": provider, "provider_user_id": provider_user_id},
            )
            return self._active_by_provider_external_id(db_session, provider, provider_user_id).first()

    def get_by_provider_username(
        self,
        db_session: DbSession,
        provider: str,
        provider_username: str,
    ) -> UserConnection | None:
        """Get connection by provider and provider's display username.

        Used by Suunto webhooks — the ``username`` field in the payload matches
        the ``user`` JWT claim stored as ``provider_username``.
        """
        try:
            return (
                db_session.query(self.model)
                .join(User, User.id == self.model.user_id)
                .filter(
                    and_(
                        self.model.provider == provider,
                        self.model.provider_username == provider_username,
                        self.model.status == ConnectionStatus.ACTIVE,
                        User.health_write_state == "active",
                    ),
                )
                .one_or_none()
            )
        except MultipleResultsFound:
            logger.warning(
                "Multiple active connections found for provider_username — returning first",
                extra={"provider": provider, "provider_username": provider_username},
            )
            return (
                db_session.query(self.model)
                .filter(
                    and_(
                        self.model.provider == provider,
                        self.model.provider_username == provider_username,
                        self.model.status == ConnectionStatus.ACTIVE,
                    ),
                )
                .first()
            )

    def get_linked_user_ids(
        self,
        db_session: DbSession,
        exclude_user_id: UUID,
        provider_pairs: list[tuple[str, str]],
    ) -> dict[tuple[str, str], list[UUID]]:
        """For a list of (provider, provider_user_id) pairs, return other active OW users
        sharing the same external account, grouped by pair."""
        if not provider_pairs:
            return {}
        rows = (
            db_session.query(self.model.provider, self.model.provider_user_id, self.model.user_id)
            .join(User, User.id == self.model.user_id)
            .filter(
                and_(
                    self.model.status == ConnectionStatus.ACTIVE,
                    User.health_write_state == "active",
                    self.model.user_id != exclude_user_id,
                    tuple_(self.model.provider, self.model.provider_user_id).in_(provider_pairs),
                )
            )
            .all()
        )
        result: dict[tuple[str, str], list[UUID]] = {}
        for provider, provider_user_id, linked_user_id in rows:
            result.setdefault((provider, provider_user_id), []).append(linked_user_id)
        return result

    def get_by_user_id(
        self,
        db_session: DbSession,
        user_id: UUID,
    ) -> list[UserConnection]:
        """Get all connections for a specific user."""
        return (
            db_session.query(self.model)
            .filter(self.model.user_id == user_id)
            .order_by(self.model.created_at.desc())
            .all()
        )

    def get_expiring_tokens(self, db_session: DbSession, minutes_threshold: int = 5) -> list[UserConnection]:
        """Get connections with tokens expiring soon (for background refresh)."""
        now = datetime.now(timezone.utc)

        threshold_time = now + timedelta(minutes=minutes_threshold)

        return (
            db_session.query(self.model)
            .join(User, User.id == self.model.user_id)
            .filter(
                and_(
                    self.model.status == ConnectionStatus.ACTIVE,
                    User.health_write_state == "active",
                    self.model.token_expires_at <= threshold_time,
                ),
            )
            .all()
        )

    def disconnect(self, db_session: DbSession, user_id: UUID, provider: str) -> int:
        """Disconnect a provider in a single UPDATE query. Returns number of rows updated."""
        self._require_write_authority(db_session, user_id=user_id, provider=provider)
        result = cast(
            CursorResult[tuple[()]],
            db_session.execute(
                update(UserConnection)
                .where(
                    and_(
                        UserConnection.user_id == user_id,
                        UserConnection.provider == provider,
                        UserConnection.status != ConnectionStatus.REVOKED,
                    ),
                )
                .values(
                    status=ConnectionStatus.REVOKED,
                    access_token=None,
                    refresh_token=None,
                    token_expires_at=None,
                    updated_at=datetime.now(timezone.utc),
                ),
            ),
        )
        db_session.commit()
        return result.rowcount

    def mark_as_revoked(self, db_session: DbSession, connection: UserConnection) -> UserConnection:
        """Mark connection as revoked (when refresh token fails)."""
        connection = require_user_connection_authority(
            db_session,
            connection_id=connection.id,
            expected_user_id=connection.user_id,
            expected_provider=connection.provider,
        )
        connection.status = ConnectionStatus.REVOKED
        connection.updated_at = datetime.now(timezone.utc)
        db_session.add(connection)
        db_session.commit()
        db_session.refresh(connection)
        return connection

    def update_scope(self, db_session: DbSession, connection: UserConnection, scope: str | None) -> UserConnection:
        """Update connection scope (e.g. when user changes permissions on Garmin Connect)."""
        connection = require_user_connection_authority(
            db_session,
            connection_id=connection.id,
            expected_user_id=connection.user_id,
            expected_provider=connection.provider,
        )
        connection.scope = scope
        connection.updated_at = datetime.now(timezone.utc)
        db_session.add(connection)
        db_session.commit()
        db_session.refresh(connection)
        return connection

    def update_tokens(
        self,
        db_session: DbSession,
        connection: UserConnection,
        access_token: str,
        refresh_token: str | None,
        expires_in: int,
    ) -> UserConnection:
        """Update connection with new tokens after refresh."""

        connection = require_user_connection_authority(
            db_session,
            connection_id=connection.id,
            expected_user_id=connection.user_id,
            expected_provider=connection.provider,
        )

        connection.access_token = access_token
        if refresh_token:
            connection.refresh_token = refresh_token
        connection.token_expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)
        connection.updated_at = datetime.now(timezone.utc)
        db_session.add(connection)
        db_session.commit()
        db_session.refresh(connection)
        return connection

    def update_connection_info(
        self,
        db_session: DbSession,
        connection: UserConnection,
        access_token: str,
        refresh_token: str | None,
        expires_in: int,
        provider_user_id: str | None = None,
        provider_username: str | None = None,
        scope: str | None = None,
    ) -> UserConnection:
        """Update connection with new tokens and user info."""
        connection = self._lock_identity_update(
            db_session,
            connection_id=connection.id,
            expected_user_id=connection.user_id,
            expected_provider=connection.provider,
            change_provider_user_id=bool(provider_user_id and not connection.provider_user_id),
            provider_user_id=provider_user_id,
            change_provider_username=bool(provider_username and not connection.provider_username),
            provider_username=provider_username,
        )
        connection.access_token = access_token
        if refresh_token:
            connection.refresh_token = refresh_token
        connection.token_expires_at = datetime.now(timezone.utc) + timedelta(seconds=expires_in)

        if provider_user_id and not connection.provider_user_id:
            connection.provider_user_id = provider_user_id
        if provider_username and not connection.provider_username:
            connection.provider_username = provider_username
        if scope and connection.scope != scope:
            connection.scope = scope

        connection.status = ConnectionStatus.ACTIVE
        connection.updated_at = datetime.now(timezone.utc)
        db_session.add(connection)
        db_session.commit()
        db_session.refresh(connection)
        return connection

    def update_last_synced_at(
        self,
        db_session: DbSession,
        connection: UserConnection,
        *,
        commit: bool = True,
    ) -> UserConnection:
        """Update the last synced timestamp."""
        connection = require_user_connection_authority(
            db_session,
            connection_id=connection.id,
            expected_user_id=connection.user_id,
            expected_provider=connection.provider,
        )
        connection.last_synced_at = datetime.now(timezone.utc)
        db_session.add(connection)
        if commit:
            db_session.commit()
            db_session.refresh(connection)
        else:
            db_session.flush()
        return connection

    def get_all_active_by_user(self, db_session: DbSession, user_id: UUID) -> list[UserConnection]:
        """Get all active connections for a specific user."""
        return (
            db_session.query(self.model)
            .join(User, User.id == self.model.user_id)
            .filter(
                and_(
                    self.model.user_id == user_id,
                    self.model.status == ConnectionStatus.ACTIVE,
                    User.health_write_state == "active",
                ),
            )
            .all()
        )

    def get_all_active_users(self, db_session: DbSession) -> list[UUID]:
        """Get all unique user IDs that have active connections."""
        return [
            row.user_id
            for row in db_session.query(self.model.user_id)
            .join(User, User.id == self.model.user_id)
            .filter(
                self.model.status == ConnectionStatus.ACTIVE,
                User.health_write_state == "active",
            )
            .distinct()
            .all()
        ]

    def ensure_sdk_connection(
        self,
        db_session: DbSession,
        user_id: UUID,
        provider: str,
        *,
        commit: bool = True,
    ) -> UserConnection:
        """Ensure an SDK-based connection exists for a user and provider.

        SDK-based providers (like Apple Health) don't use OAuth tokens.
        This method creates or returns an existing connection without tokens.
        """
        self._require_write_authority(
            db_session,
            user_id=user_id,
            provider=provider,
        )
        existing = self.get_by_user_and_provider(db_session, user_id, provider)
        if existing:
            # Reactivate if revoked
            if existing.status != ConnectionStatus.ACTIVE:
                existing.status = ConnectionStatus.ACTIVE
                existing.updated_at = datetime.now(timezone.utc)
                db_session.add(existing)
                if commit:
                    db_session.commit()
                    db_session.refresh(existing)
                else:
                    db_session.flush()
            return existing

        # Create new SDK connection (no tokens needed)
        connection = UserConnection(
            id=uuid4(),
            user_id=user_id,
            provider=provider,
            access_token=None,
            refresh_token=None,
            token_expires_at=None,
            status=ConnectionStatus.ACTIVE,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        db_session.add(connection)
        if commit:
            db_session.commit()
            db_session.refresh(connection)
        else:
            db_session.flush()
        return connection
