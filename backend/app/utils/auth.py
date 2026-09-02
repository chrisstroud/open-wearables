from typing import Annotated
from uuid import UUID

from fastapi import Depends, Header, HTTPException, status
from fastapi.security import OAuth2PasswordBearer
from jose import JWTError, jwt

from app.config import settings
from app.database import DbSession
from app.models import Developer, User
from app.repositories.developer_repository import DeveloperRepository
from app.schemas.auth import SDKAuthContext

oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/api/v1/auth/login", auto_error=False)
developer_repository = DeveloperRepository(Developer)


async def get_current_developer(
    db: DbSession,
    token: Annotated[str | None, Depends(oauth2_scheme)],
) -> Developer:
    """Get current authenticated developer from JWT token.

    SDK-scoped tokens are rejected - they can only access /sdk/ endpoints.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    if not token:
        raise credentials_exception

    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])

        # Reject SDK-scoped tokens - they can ONLY access /sdk/ endpoints
        if payload.get("scope") == "sdk":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="SDK tokens cannot access this endpoint",
                headers={"WWW-Authenticate": "Bearer"},
            )

        developer_id: str = payload.get("sub")
        if developer_id is None:
            raise credentials_exception
    except JWTError:
        raise credentials_exception

    developer = developer_repository.get(db, UUID(developer_id))
    if not developer:
        raise credentials_exception

    return developer


async def get_current_developer_optional(
    db: DbSession,
    token: Annotated[str | None, Depends(oauth2_scheme)] = None,
) -> Developer | None:
    """Get current authenticated developer from JWT token, or None if not authenticated.

    SDK-scoped tokens return None - they are not developer tokens.
    """
    if not token:
        return None

    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])

        # SDK tokens are not developer tokens - return None to allow fallback to API key
        if payload.get("scope") == "sdk":
            return None

        developer_id: str = payload.get("sub")
        if developer_id is None:
            return None

        # Validate that developer_id is a valid UUID
        try:
            developer_uuid = UUID(developer_id)
        except ValueError:
            return None

    except JWTError:
        return None

    return developer_repository.get(db, developer_uuid)


DeveloperDep = Annotated[Developer, Depends(get_current_developer)]
DeveloperOptionalDep = Annotated[Developer | None, Depends(get_current_developer_optional)]


async def get_sdk_auth(
    db: DbSession,
    token: Annotated[str | None, Depends(oauth2_scheme)] = None,
    x_open_wearables_api_key: str | None = Header(None, alias="X-Open-Wearables-API-Key"),
) -> SDKAuthContext:
    """Authenticate SDK requests using either SDK user token or API key.

    Accepts:
    - SDK token (Bearer token with scope="sdk")
    - API key (X-Open-Wearables-API-Key header)

    Returns SDKAuthContext with auth_type and relevant identifiers.
    """
    # Import here to avoid circular imports
    from app.services.api_key_service import api_key_service

    # Try SDK user token first
    if token:
        try:
            payload = jwt.decode(
                token,
                settings.secret_key,
                algorithms=[settings.algorithm],
            )
            if payload.get("scope") == "sdk":
                sub = payload.get("sub")
                try:
                    user_id = UUID(sub) if sub else None
                except (TypeError, ValueError):
                    user_id = None
                if user_id is None:
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail="Could not validate credentials",
                        headers={"WWW-Authenticate": "Bearer"},
                    )
                app_id = payload.get("app_id")
                if not isinstance(app_id, str):
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail="Could not validate credentials",
                        headers={"WWW-Authenticate": "Bearer"},
                    )
                if db.get(User, user_id) is None:
                    raise HTTPException(
                        status_code=status.HTTP_401_UNAUTHORIZED,
                        detail="Could not validate credentials",
                        headers={"WWW-Authenticate": "Bearer"},
                    )
                from app.services.sdk_client_installation_service import sdk_client_installation_service

                installation = sdk_client_installation_service.require_active(
                    db,
                    user_id=user_id,
                    app_id=app_id,
                    generation=payload.get("installation_generation"),
                    bundle_id=payload.get("bundle_id"),
                    app_version=payload.get("app_version"),
                    build_number=payload.get("build_number"),
                    protocol_version=payload.get("protocol_version"),
                    health_evidence_generation=payload.get("health_evidence_generation"),
                )
                return SDKAuthContext(
                    auth_type="sdk_token",
                    user_id=user_id,
                    app_id=app_id,
                    installation_id=installation.id if installation is not None else None,
                    installation_generation=installation.generation if installation is not None else None,
                    health_evidence_generation=(
                        payload.get("health_evidence_generation") if installation is not None else None
                    ),
                )
        except JWTError:
            pass  # Fall through to API key check

    # Fall back to API key (backwards compatibility)
    if x_open_wearables_api_key:
        api_key = api_key_service.validate_api_key(db, x_open_wearables_api_key)
        return SDKAuthContext(auth_type="api_key", api_key_id=api_key.id)

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Authentication required: provide SDK token or API key",
    )


SDKAuthDep = Annotated[SDKAuthContext, Depends(get_sdk_auth)]


async def get_sdk_revocation_auth(
    db: DbSession,
    token: Annotated[str | None, Depends(oauth2_scheme)] = None,
) -> SDKAuthContext:
    """Authenticate an exact first-class install for idempotent self-revocation.

    Unlike normal SDK authentication, the exact matching installation may
    already be revoked so a client can safely retry after losing the 200
    response. This dependency is intentionally used by only the revoke route.
    """
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate installation credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )
    if not token:
        raise credentials_exception
    try:
        payload = jwt.decode(token, settings.secret_key, algorithms=[settings.algorithm])
    except JWTError as exc:
        raise credentials_exception from exc
    if payload.get("scope") != "sdk":
        raise credentials_exception
    try:
        user_id = UUID(payload["sub"])
    except (KeyError, TypeError, ValueError) as exc:
        raise credentials_exception from exc
    app_id = payload.get("app_id")
    if not isinstance(app_id, str) or not app_id.startswith("dfi:"):
        raise credentials_exception

    from app.repositories.sdk_client_installation_repository import sdk_client_installation_repository

    installation = sdk_client_installation_repository.get_by_app_id(db, app_id)
    user = db.get(User, user_id)
    if (
        installation is None
        or user is None
        or installation.user_id != user_id
        or payload.get("installation_generation") != installation.generation
        or payload.get("bundle_id") != installation.bundle_id
        or payload.get("app_version") != installation.app_version
        or payload.get("build_number") != installation.build_number
        or payload.get("protocol_version") != installation.protocol_version
        or payload.get("health_evidence_generation") != user.health_evidence_generation
    ):
        raise credentials_exception
    return SDKAuthContext(
        auth_type="sdk_token",
        user_id=user_id,
        app_id=app_id,
        installation_id=installation.id,
        installation_generation=installation.generation,
        health_evidence_generation=payload.get("health_evidence_generation"),
    )


SDKRevocationAuthDep = Annotated[SDKAuthContext, Depends(get_sdk_revocation_auth)]
