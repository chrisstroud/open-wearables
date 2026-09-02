from collections.abc import Iterable
from uuid import UUID

from app.database import DbSession
from app.models import DataSource, EventRecord, SDKClientInstallation, User, UserConnection
from app.schemas.enums import ProviderName


class HealthWriteAuthorityError(RuntimeError):
    """A health writer does not hold the account's current write authority."""


def _provider_value(provider: ProviderName | str) -> str:
    return provider.value if isinstance(provider, ProviderName) else str(provider)


def _locked_users(db_session: DbSession, user_ids: Iterable[UUID]) -> dict[UUID, User]:
    unique_ids = sorted(set(user_ids), key=str)
    if not unique_ids:
        return {}
    users = (
        db_session.query(User)
        .filter(User.id.in_(unique_ids))
        .order_by(User.id)
        .populate_existing()
        .with_for_update()
        .all()
    )
    result = {user.id: user for user in users}
    if len(result) != len(unique_ids):
        raise HealthWriteAuthorityError("Health write owner does not exist")
    return result


def require_health_write_authorities(
    db_session: DbSession,
    targets: Iterable[tuple[UUID, ProviderName | str]],
    *,
    allow_internal_maintenance: bool = False,
) -> None:
    """Lock account owners and validate every normalized write in this transaction.

    Legacy accounts retain their existing provider behavior. Accounts that have
    completed source reset require the exact active mobile installation tuple.
    The only exception is an explicitly acquired, generation-bound maintenance
    authority for derived internal scores.
    """
    normalized_targets = {(user_id, _provider_value(provider)) for user_id, provider in targets}
    users = _locked_users(db_session, (user_id for user_id, _ in normalized_targets))
    mobile_authority = db_session.info.get("health_write_authority")
    maintenance_authority = db_session.info.get("health_maintenance_authority")

    for user_id, provider in sorted(normalized_targets, key=lambda item: (str(item[0]), item[1])):
        user = users[user_id]
        if user.health_write_state not in {"active", "activating"}:
            raise HealthWriteAuthorityError("Health writes are fenced")
        if user.health_source_policy != "apple-mobile-v2-only":
            continue

        if (
            allow_internal_maintenance
            and provider == ProviderName.INTERNAL.value
            and maintenance_authority == (user_id, user.health_evidence_generation)
        ):
            continue

        if (
            provider not in {ProviderName.APPLE.value, ProviderName.INTERNAL.value}
            or not isinstance(mobile_authority, tuple)
            or len(mobile_authority) != 4
            or mobile_authority[0] != user_id
            or mobile_authority[1] != user.health_evidence_generation
        ):
            raise HealthWriteAuthorityError("Current v2 mobile authority is required")
        installation = (
            db_session.query(SDKClientInstallation)
            .filter(SDKClientInstallation.id == mobile_authority[2])
            .populate_existing()
            .one_or_none()
        )
        if (
            installation is None
            or installation.user_id != user_id
            or installation.status != "active"
            or installation.health_evidence_generation != user.health_evidence_generation
            or installation.generation != mobile_authority[3]
        ):
            raise HealthWriteAuthorityError("Current v2 mobile authority is required")


def require_health_write_authority(
    db_session: DbSession,
    *,
    user_id: UUID,
    provider: ProviderName | str,
    allow_internal_maintenance: bool = False,
) -> None:
    require_health_write_authorities(
        db_session,
        ((user_id, provider),),
        allow_internal_maintenance=allow_internal_maintenance,
    )


def acquire_health_maintenance_authority(db_session: DbSession, *, user_id: UUID) -> User:
    """Lock an active account and bind maintenance work to its current generation."""
    user = _locked_users(db_session, (user_id,))[user_id]
    if user.health_write_state not in {"active", "activating"}:
        raise HealthWriteAuthorityError("Health writes are fenced")
    db_session.info["health_maintenance_authority"] = (user.id, user.health_evidence_generation)
    return user


def clear_health_maintenance_authority(db_session: DbSession) -> None:
    db_session.info.pop("health_maintenance_authority", None)


def require_data_source_authority(
    db_session: DbSession,
    *,
    data_source_id: UUID,
    expected_user_id: UUID | None = None,
    expected_provider: ProviderName | str | None = None,
) -> DataSource:
    data_source = db_session.query(DataSource).filter(DataSource.id == data_source_id).populate_existing().one_or_none()
    if data_source is None:
        raise HealthWriteAuthorityError("Health data source does not exist")
    if expected_user_id is not None and data_source.user_id != expected_user_id:
        raise HealthWriteAuthorityError("Health data source belongs to another user")
    if expected_provider is not None and _provider_value(data_source.provider) != _provider_value(expected_provider):
        raise HealthWriteAuthorityError("Health data source belongs to another provider")
    require_health_write_authority(
        db_session,
        user_id=data_source.user_id,
        provider=data_source.provider,
    )
    return data_source


def require_event_record_authorities(
    db_session: DbSession,
    record_ids: Iterable[UUID],
    *,
    expected_user_id: UUID | None = None,
) -> dict[UUID, tuple[UUID, ProviderName]]:
    unique_ids = set(record_ids)
    if not unique_ids:
        return {}
    rows = (
        db_session.query(EventRecord.id, DataSource.user_id, DataSource.provider)
        .join(DataSource, EventRecord.data_source_id == DataSource.id)
        .filter(EventRecord.id.in_(unique_ids))
        .all()
    )
    resolved = {record_id: (user_id, provider) for record_id, user_id, provider in rows}
    if set(resolved) != unique_ids:
        raise HealthWriteAuthorityError("Health event record does not exist")
    if expected_user_id is not None and any(user_id != expected_user_id for user_id, _ in resolved.values()):
        raise HealthWriteAuthorityError("Health event record belongs to another user")
    require_health_write_authorities(db_session, resolved.values())
    return resolved


def require_user_connection_authority(
    db_session: DbSession,
    *,
    connection_id: UUID,
    expected_user_id: UUID | None = None,
    expected_provider: ProviderName | str | None = None,
) -> UserConnection:
    initial = (
        db_session.query(UserConnection.user_id, UserConnection.provider)
        .filter(UserConnection.id == connection_id)
        .one_or_none()
    )
    if initial is None:
        raise HealthWriteAuthorityError("Health connection does not exist")
    user_id, provider = initial
    if expected_user_id is not None and user_id != expected_user_id:
        raise HealthWriteAuthorityError("Health connection belongs to another user")
    if expected_provider is not None and _provider_value(provider) != _provider_value(expected_provider):
        raise HealthWriteAuthorityError("Health connection belongs to another provider")
    require_health_write_authority(db_session, user_id=user_id, provider=provider)
    connection = (
        db_session.query(UserConnection)
        .filter(UserConnection.id == connection_id)
        .populate_existing()
        .with_for_update()
        .one_or_none()
    )
    if (
        connection is None
        or connection.user_id != user_id
        or _provider_value(connection.provider) != _provider_value(provider)
    ):
        raise HealthWriteAuthorityError("Health connection authority changed")
    return connection
