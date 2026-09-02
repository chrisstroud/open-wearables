import secrets
from datetime import datetime, timezone
from logging import Logger, getLogger
from uuid import UUID

from fastapi import HTTPException, status

from app.config import settings
from app.database import DbSession
from app.models import RefreshToken, User
from app.repositories.refresh_token_repository import refresh_token_repository
from app.schemas.auth import SDKClientMetadataRefresh, TokenResponse, TokenType
from app.services.sdk_client_installation_service import sdk_client_installation_service
from app.services.sdk_token_service import create_sdk_user_token
from app.utils.security import create_access_token


class RefreshTokenService:
    """Service for managing refresh tokens."""

    def __init__(self, log: Logger) -> None:
        self.logger = log
        self.repo = refresh_token_repository

    @staticmethod
    def _generate_refresh_token_id() -> str:
        """Generate an opaque refresh token ID with rt- prefix."""
        return f"rt-{secrets.token_hex(16)}"

    def create_sdk_refresh_token(
        self,
        db_session: DbSession,
        user_id: UUID,
        app_id: str,
        *,
        health_evidence_generation: int | None = None,
        commit: bool = True,
    ) -> str:
        """Create a refresh token for an SDK token.

        Args:
            db_session: Database session
            user_id: The OpenWearables User ID
            app_id: The application ID that created the token

        Returns:
            The refresh token string (rt-{hex})
        """
        token_id = self._generate_refresh_token_id()
        token = RefreshToken(
            id=token_id,
            token_type=TokenType.SDK,
            user_id=user_id,
            app_id=app_id,
            health_evidence_generation=health_evidence_generation,
            developer_id=None,
            created_at=datetime.now(timezone.utc),
            last_used_at=None,
            revoked_at=None,
        )
        if commit:
            self.repo.create(db_session, token)
        else:
            db_session.add(token)
            db_session.flush()
        self.logger.debug(f"Created SDK refresh token for user {user_id}, app {app_id}")
        return token_id

    def create_developer_refresh_token(
        self,
        db_session: DbSession,
        developer_id: UUID,
        *,
        commit: bool = True,
    ) -> str:
        """Create a refresh token for a developer token.

        Args:
            db_session: Database session
            developer_id: The developer ID

        Returns:
            The refresh token string (rt-{hex})
        """
        token_id = self._generate_refresh_token_id()
        token = RefreshToken(
            id=token_id,
            token_type=TokenType.DEVELOPER,
            user_id=None,
            app_id=None,
            health_evidence_generation=None,
            developer_id=developer_id,
            created_at=datetime.now(timezone.utc),
            last_used_at=None,
            revoked_at=None,
        )
        if commit:
            self.repo.create(db_session, token)
        else:
            db_session.add(token)
            db_session.flush()
        self.logger.debug(f"Created developer refresh token for developer {developer_id}")
        return token_id

    def refresh_token(
        self,
        db_session: DbSession,
        refresh_token_str: str,
        *,
        client: SDKClientMetadataRefresh | None = None,
    ) -> TokenResponse:
        """Exchange a refresh token for a new access token.

        Implements refresh token rotation: the old refresh token is revoked and
        a new one is issued with each refresh request.

        Args:
            db_session: Database session
            refresh_token_str: The refresh token string

        Returns:
            TokenResponse with new access token and new refresh token

        Raises:
            HTTPException: If the refresh token is invalid or revoked
        """
        # Read only enough authority to acquire locks in the global order used
        # by installation replacement and reset: User first, then credential.
        # The second read is the authoritative one. This prevents both replay
        # of a single-use refresh token and a refresh racing past an account
        # generation fence.
        candidate = self.repo.get_valid_token(db_session, refresh_token_str)
        if not candidate:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or revoked refresh token",
                headers={"WWW-Authenticate": "Bearer"},
            )
        if candidate.token_type == TokenType.SDK:
            locked_user = db_session.query(User).filter(User.id == candidate.user_id).with_for_update().one_or_none()
            if locked_user is None:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Invalid or revoked refresh token",
                    headers={"WWW-Authenticate": "Bearer"},
                )
        token = self.repo.get_valid_token(db_session, refresh_token_str, for_update=True)
        if token is None or token.token_type != candidate.token_type:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid or revoked refresh token",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Validate authority before consuming the old token, then publish the
        # revocation and replacement in one commit.
        if token.token_type == TokenType.SDK:
            installation_for_claims = (
                sdk_client_installation_service.crud.get_by_app_id(db_session, token.app_id) if token.app_id else None
            )
            installation = sdk_client_installation_service.require_active(
                db_session,
                user_id=token.user_id,  # ty:ignore[invalid-argument-type]
                app_id=token.app_id,
                generation=installation_for_claims.generation if installation_for_claims is not None else None,
                bundle_id=installation_for_claims.bundle_id if installation_for_claims is not None else None,
                app_version=installation_for_claims.app_version if installation_for_claims is not None else None,
                build_number=installation_for_claims.build_number if installation_for_claims is not None else None,
                protocol_version=(
                    installation_for_claims.protocol_version if installation_for_claims is not None else None
                ),
                health_evidence_generation=token.health_evidence_generation,
                touch=False,
            )
            if client is not None:
                if installation is None:
                    raise HTTPException(
                        status_code=status.HTTP_400_BAD_REQUEST,
                        detail="Client metadata is supported only for a permanent mobile installation",
                    )
                sdk_client_installation_service.refresh_metadata(
                    db_session,
                    installation=installation,
                    metadata=client,
                )
            access_token = create_sdk_user_token(
                app_id=token.app_id,  # ty:ignore[invalid-argument-type]
                user_id=str(token.user_id),
                installation_generation=installation.generation if installation is not None else None,
                bundle_id=installation.bundle_id if installation is not None else None,
                app_version=installation.app_version if installation is not None else None,
                build_number=installation.build_number if installation is not None else None,
                protocol_version=installation.protocol_version if installation is not None else None,
                health_evidence_generation=token.health_evidence_generation,
            )
            new_refresh_token = self.create_sdk_refresh_token(
                db_session,
                user_id=token.user_id,  # ty:ignore[invalid-argument-type]
                app_id=token.app_id,  # ty:ignore[invalid-argument-type]
                health_evidence_generation=token.health_evidence_generation,
                commit=False,
            )
            self.logger.debug(f"Refreshed SDK token for user {token.user_id} (rotated)")
        elif token.token_type == TokenType.DEVELOPER:
            if client is not None:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Client metadata is not valid for a developer token",
                )
            access_token = create_access_token(subject=str(token.developer_id))
            new_refresh_token = self.create_developer_refresh_token(
                db_session,
                developer_id=token.developer_id,  # ty:ignore[invalid-argument-type]
                commit=False,
            )
            self.logger.debug(f"Refreshed developer token for developer {token.developer_id} (rotated)")
        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unknown token type: {token.token_type}",
            )

        self.repo.revoke_token(db_session, token, commit=False)
        db_session.commit()

        return TokenResponse(
            access_token=access_token,
            token_type="bearer",
            refresh_token=new_refresh_token,
            expires_in=settings.access_token_expire_minutes * 60,
        )

    def revoke_token(self, db_session: DbSession, refresh_token_str: str) -> bool:
        """Revoke a refresh token.

        Args:
            db_session: Database session
            refresh_token_str: The refresh token string

        Returns:
            True if the token was revoked, False if not found

        Raises:
            HTTPException: If the refresh token is not found
        """
        token = self.repo.get_valid_token(db_session, refresh_token_str)
        if not token:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Refresh token not found",
            )

        self.repo.revoke_token(db_session, token)
        self.logger.debug("Revoked refresh token")
        return True


refresh_token_service = RefreshTokenService(log=getLogger(__name__))
