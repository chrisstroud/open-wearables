"""Transaction-scoped authority for provider identities shared with reset proofs."""

import hashlib
import hmac
from collections.abc import Iterable
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import text

from app.config import settings
from app.database import DbSession
from app.models import UserConnection


@dataclass(frozen=True, order=True)
class ProviderIdentityFingerprint:
    """A normalized provider plus its privacy-safe reset-proof fingerprint."""

    provider: str
    fingerprint: str


def provider_identity_fingerprint(provider: str, value: str) -> str:
    """Return the exact domain-separated HMAC used by durable reset proofs."""
    normalized_provider = provider.strip().lower()
    normalized_value = value.strip()
    message = f"sdk-source-reset:v1:{normalized_provider}\0{normalized_value}".encode()
    return hmac.new(settings.secret_key.encode(), message, hashlib.sha256).hexdigest()


def provider_identity_fingerprints(
    provider: str,
    *,
    provider_user_id: str | None,
    provider_username: str | None,
) -> tuple[ProviderIdentityFingerprint, ...]:
    """Return authoritative identities for one connection.

    Every provider webhook uses ``provider_user_id`` except Suunto, whose
    callback authority is its username. Unknown/blank placeholders never
    become durable identities.
    """
    normalized_provider = provider.strip().lower()
    values = {str(provider_user_id or "").strip()}
    if normalized_provider == "suunto":
        values.add(str(provider_username or "").strip())
    values = {value for value in values if value and value.lower() != "unknown"}
    return tuple(
        ProviderIdentityFingerprint(
            normalized_provider,
            provider_identity_fingerprint(normalized_provider, value),
        )
        for value in sorted(values)
    )


def _advisory_lock_key(identity: ProviderIdentityFingerprint) -> int:
    material = hashlib.sha256(
        f"open-wearables:provider-identity-lock:v1:{identity.provider}\0{identity.fingerprint}".encode()
    ).digest()[:8]
    return int.from_bytes(material, byteorder="big", signed=True)


def acquire_provider_identity_locks(
    db_session: DbSession,
    identities: Iterable[ProviderIdentityFingerprint],
) -> tuple[ProviderIdentityFingerprint, ...]:
    """Acquire globally ordered PostgreSQL transaction advisory locks."""
    ordered = tuple(sorted(set(identities)))
    for identity in ordered:
        db_session.execute(
            text("SELECT pg_advisory_xact_lock(:lock_key)"),
            {"lock_key": _advisory_lock_key(identity)},
        )
    return ordered


def acquire_provider_identity_value_locks(
    db_session: DbSession,
    identities: Iterable[tuple[str, str]],
) -> tuple[ProviderIdentityFingerprint, ...]:
    """Normalize raw identity values and acquire their fingerprint locks."""
    fingerprints = (
        ProviderIdentityFingerprint(
            provider.strip().lower(),
            provider_identity_fingerprint(provider, value),
        )
        for provider, value in identities
        if provider.strip() and value.strip() and value.strip().lower() != "unknown"
    )
    return acquire_provider_identity_locks(db_session, fingerprints)


def other_user_provider_identity_collisions(
    db_session: DbSession,
    *,
    identities: Iterable[ProviderIdentityFingerprint],
    exclude_user_id: UUID,
) -> tuple[ProviderIdentityFingerprint, ...]:
    """Re-HMAC every other-user identity for the selected providers.

    All connection statuses participate. A revoked/local historical row still
    proves that a provider identity is not exclusively owned by the reset
    target and therefore must fail closed before provider-wide side effects.
    """
    expected = set(identities)
    providers = tuple(sorted({identity.provider for identity in expected}))
    if not providers:
        return ()
    collisions: set[ProviderIdentityFingerprint] = set()
    rows = (
        db_session.query(
            UserConnection.provider,
            UserConnection.provider_user_id,
            UserConnection.provider_username,
        )
        .filter(
            UserConnection.user_id != exclude_user_id,
            UserConnection.provider.in_(providers),
        )
        .yield_per(1000)
    )
    for provider, provider_user_id, provider_username in rows:
        collisions.update(
            expected.intersection(
                provider_identity_fingerprints(
                    str(provider),
                    provider_user_id=provider_user_id,
                    provider_username=provider_username,
                )
            )
        )
    return tuple(sorted(collisions))
