from datetime import datetime, timedelta, timezone

from jose import jwt

from app.config import settings


def create_sdk_user_token(
    app_id: str,
    user_id: str,
    *,
    installation_generation: int | None = None,
    bundle_id: str | None = None,
    app_version: str | None = None,
    build_number: str | None = None,
    protocol_version: int | None = None,
    health_evidence_generation: int | None = None,
) -> str:
    """Create JWT with SDK scope for a specific user.

    The token is scoped to SDK endpoints only and contains:
    - sub: The user_id (UUID string)
    - scope: "sdk" to identify this as an SDK token
    - app_id: The application ID that created this token
    - exp: Expiration timestamp (configured via access_token_expire_minutes)

    Args:
        app_id: The application ID that requested this token
        user_id: The OpenWearables User ID (UUID string)

    Returns:
        JWT token string
    """
    expire = datetime.now(timezone.utc) + timedelta(minutes=settings.access_token_expire_minutes)
    claims = {
        "sub": user_id,
        "scope": "sdk",
        "app_id": app_id,
        "exp": expire,
    }
    if installation_generation is not None:
        claims.update(
            {
                "installation_generation": installation_generation,
                "bundle_id": bundle_id,
                "app_version": app_version,
                "build_number": build_number,
                "protocol_version": protocol_version,
                "health_evidence_generation": health_evidence_generation,
            }
        )

    return jwt.encode(claims, settings.secret_key, algorithm=settings.algorithm)
