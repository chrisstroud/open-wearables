"""Compatibility exports for the repository-layer provider identity authority."""

from app.repositories.provider_identity_authority import (
    ProviderIdentityFingerprint,
    acquire_provider_identity_locks,
    acquire_provider_identity_value_locks,
    other_user_provider_identity_collisions,
    provider_identity_fingerprint,
    provider_identity_fingerprints,
)

__all__ = [
    "ProviderIdentityFingerprint",
    "acquire_provider_identity_locks",
    "acquire_provider_identity_value_locks",
    "other_user_provider_identity_collisions",
    "provider_identity_fingerprint",
    "provider_identity_fingerprints",
]
