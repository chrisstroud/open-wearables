import hashlib
import json
import logging
import secrets
from abc import ABC, abstractmethod
from base64 import b64encode, urlsafe_b64encode
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

import httpx
from fastapi import HTTPException
from redis import Redis
from sqlalchemy import func
from starlette.status import (
    HTTP_400_BAD_REQUEST,
    HTTP_401_UNAUTHORIZED,
    HTTP_409_CONFLICT,
    HTTP_500_INTERNAL_SERVER_ERROR,
)

from app.database import DbSession
from app.integrations.redis_client import get_redis_client
from app.models import UserConnection, WhoopAuthorizationLease
from app.repositories.user_connection_repository import UserConnectionRepository
from app.repositories.user_repository import UserRepository
from app.repositories.whoop_sync_dispatch_repository import WhoopSyncDispatchRepository
from app.schemas.auth import AuthenticationMethod, ConnectionStatus
from app.schemas.model_crud.credentials import (
    OAuthState,
    OAuthTokenResponse,
    ProviderCredentials,
    ProviderEndpoints,
)
from app.schemas.model_crud.user_management import UserConnectionCreate
from app.services.outgoing_webhooks.events import on_connection_created, on_connection_revoked
from app.services.providers.whoop.exact_sync_authority import current_exact_whoop_sync_authority
from app.utils.structured_logging import log_structured

logger = logging.getLogger(__name__)


class BaseOAuthTemplate(ABC):
    """Base template for OAuth 2.0 authentication flow."""

    def __init__(
        self,
        user_repo: UserRepository,
        connection_repo: UserConnectionRepository,
        provider_name: str,
        api_base_url: str,
    ):
        self.user_repo = user_repo
        self.connection_repo = connection_repo
        self.provider_name = provider_name
        self.api_base_url = api_base_url
        self.state_ttl = 900  # 15 minutes

    @property
    def redis_client(self) -> Redis:
        """Lazy-loaded Redis client."""
        return get_redis_client()

    @property
    @abstractmethod
    def endpoints(self) -> ProviderEndpoints:
        """Returns provider OAuth endpoints configuration."""
        pass

    @property
    @abstractmethod
    def credentials(self) -> ProviderCredentials:
        """Returns provider OAuth credentials."""
        pass

    use_pkce: bool = False
    auth_method: AuthenticationMethod = AuthenticationMethod.BASIC_AUTH

    def get_authorization_url(self, user_id: UUID, redirect_uri: str | None = None) -> tuple[str, str]:
        """Generates the provider's authorization URL.

        Returns:
            tuple[str, str]: The authorization URL and the state.
        """
        state = secrets.token_urlsafe(32)  # random state 32-bytes (Base64 encoding)

        oauth_state = OAuthState(
            user_id=user_id,
            provider=self.provider_name,
            redirect_uri=redirect_uri,  # Only store if explicitly provided by frontend
        )

        auth_url, pkce_data = self._build_auth_url(state)

        redis_key = f"oauth_state:{state}"
        state_data = oauth_state.model_dump(mode="json")
        if pkce_data:
            state_data.update(pkce_data)
        self.redis_client.setex(redis_key, self.state_ttl, json.dumps(state_data))

        log_structured(
            logger,
            "info",
            "OAuth authorization URL generated",
            provider=self.provider_name,
            task="get_authorization_url",
            user_id=str(user_id),
        )

        return auth_url, state

    def handle_callback(self, db: DbSession, code: str, state: str) -> OAuthState:
        """Handles the OAuth callback, exchanges code, and saves the connection."""
        oauth_state, code_verifier = self._validate_state(state)

        if oauth_state.provider != self.provider_name:
            log_structured(
                logger,
                "error",
                "Provider mismatch in OAuth state",
                provider=self.provider_name,
                task="handle_callback",
                user_id=str(oauth_state.user_id),
                state_provider=oauth_state.provider,
            )
            raise HTTPException(status_code=HTTP_400_BAD_REQUEST, detail="Provider mismatch in state")

        whoop_lease_token: UUID | None = None
        whoop_dispatch_repository: WhoopSyncDispatchRepository | None = None
        if self.provider_name == "whoop":
            connection = self.connection_repo.get_by_user_and_provider(
                db,
                oauth_state.user_id,
                self.provider_name,
            )
            whoop_lease_token = uuid4()
            whoop_dispatch_repository = WhoopSyncDispatchRepository()
            acquired = whoop_dispatch_repository.try_acquire_authorization_lease(
                db,
                user_id=oauth_state.user_id,
                connection_id=connection.id if connection else None,
                authorization_generation=connection.authorization_generation if connection else 0,
                lease_token=whoop_lease_token,
                lease_kind="oauth_callback",
            )
            if not acquired:
                raise HTTPException(
                    status_code=HTTP_409_CONFLICT,
                    detail="WHOOP authorization is busy; start authorization again after the current sync finishes",
                )

        try:
            token_response = self._exchange_token(code, code_verifier)

            user_info = self._get_provider_user_info(token_response, str(oauth_state.user_id))

            if (
                whoop_dispatch_repository is not None
                and whoop_lease_token is not None
                and not whoop_dispatch_repository.renew_oauth_callback_authority(
                    db,
                    user_id=oauth_state.user_id,
                    lease_token=whoop_lease_token,
                )
            ):
                raise HTTPException(
                    status_code=HTTP_409_CONFLICT,
                    detail="WHOOP authorization authority expired; start authorization again",
                )

            self._save_connection(
                db,
                oauth_state.user_id,
                token_response,
                user_info,
                oauth_state,
                expected_whoop_oauth_lease_token=whoop_lease_token,
            )
        finally:
            if whoop_dispatch_repository is not None and whoop_lease_token is not None:
                db.rollback()
                whoop_dispatch_repository.release_authorization_lease(
                    db,
                    user_id=oauth_state.user_id,
                    lease_token=whoop_lease_token,
                )

        log_structured(
            logger,
            "info",
            "OAuth callback handled successfully",
            provider=self.provider_name,
            task="handle_callback",
            user_id=str(oauth_state.user_id),
        )

        return oauth_state

    def refresh_access_token(self, db: DbSession, user_id: UUID, refresh_token: str) -> OAuthTokenResponse:
        """Refreshes the access token using the refresh token."""
        refresh_lease_token: UUID | None = None
        refresh_connection_id: UUID | None = None
        refresh_generation: int | None = None
        whoop_dispatch_repository: WhoopSyncDispatchRepository | None = None
        exact_authority = current_exact_whoop_sync_authority()

        if self.provider_name == "whoop" and exact_authority is None:
            observed = self.connection_repo.get_by_user_and_provider(db, user_id, self.provider_name)
            if (
                observed is None
                or observed.status != ConnectionStatus.ACTIVE
                or observed.refresh_token != refresh_token
            ):
                raise HTTPException(
                    status_code=HTTP_401_UNAUTHORIZED,
                    detail="Current WHOOP refresh credentials are required",
                )
            refresh_connection_id = observed.id
            refresh_generation = observed.authorization_generation
            refresh_lease_token = uuid4()
            whoop_dispatch_repository = WhoopSyncDispatchRepository()
            acquired = whoop_dispatch_repository.try_acquire_authorization_lease(
                db,
                user_id=user_id,
                connection_id=refresh_connection_id,
                authorization_generation=refresh_generation,
                lease_token=refresh_lease_token,
                lease_kind="token_refresh",
            )
            if not acquired:
                raise HTTPException(
                    status_code=HTTP_409_CONFLICT,
                    detail="WHOOP authorization is busy; retry token refresh",
                )

        try:
            connection = self._resolve_refresh_connection(
                db,
                user_id,
                for_update=False,
                connection_id=refresh_connection_id,
                authorization_generation=refresh_generation,
                lease_token=refresh_lease_token,
                lease_kind="token_refresh" if refresh_lease_token is not None else None,
            )
            if self.provider_name == "whoop" and (connection is None or connection.refresh_token != refresh_token):
                raise HTTPException(
                    status_code=HTTP_409_CONFLICT,
                    detail="WHOOP authorization changed before token refresh",
                )

            data, headers = self._prepare_refresh_request(refresh_token)
            response = httpx.post(
                self.endpoints.token_url,
                data=data,
                headers=headers,
                timeout=30.0,
            )
            response.raise_for_status()
            token_response = OAuthTokenResponse.model_validate(response.json())

            if (
                whoop_dispatch_repository is not None
                and refresh_connection_id is not None
                and refresh_generation is not None
                and refresh_lease_token is not None
                and not whoop_dispatch_repository.renew_authorization_lease(
                    db,
                    user_id=user_id,
                    connection_id=refresh_connection_id,
                    authorization_generation=refresh_generation,
                    lease_token=refresh_lease_token,
                    lease_kind="token_refresh",
                )
            ):
                raise HTTPException(
                    status_code=HTTP_409_CONFLICT,
                    detail="WHOOP authorization changed during token refresh",
                )

            connection = self._resolve_refresh_connection(
                db,
                user_id,
                for_update=self.provider_name == "whoop",
                connection_id=refresh_connection_id,
                authorization_generation=refresh_generation,
                lease_token=refresh_lease_token,
                lease_kind="token_refresh" if refresh_lease_token is not None else None,
            )
            if connection:
                mutation_generation = (
                    exact_authority.authorization_generation
                    if exact_authority is not None and self.provider_name == "whoop"
                    else refresh_generation
                )
                mutation_lease_token = (
                    exact_authority.lease_token
                    if exact_authority is not None and self.provider_name == "whoop"
                    else refresh_lease_token
                )
                mutation_lease_kind = (
                    "full_history_sync"
                    if exact_authority is not None and self.provider_name == "whoop"
                    else ("token_refresh" if refresh_lease_token is not None else None)
                )
                self.connection_repo.update_tokens(
                    db,
                    connection,
                    token_response.access_token,
                    token_response.refresh_token or refresh_token,  # Use old refresh token if new one not provided
                    token_response.expires_in,
                    expected_whoop_authorization_generation=mutation_generation,
                    expected_whoop_lease_token=mutation_lease_token,
                    expected_whoop_lease_kind=mutation_lease_kind,
                )
            elif self.provider_name == "whoop":
                raise HTTPException(
                    status_code=HTTP_401_UNAUTHORIZED,
                    detail="WHOOP refresh authority expired",
                )

            log_structured(
                logger,
                "info",
                "OAuth token refreshed successfully",
                provider=self.provider_name,
                task="refresh_access_token",
                user_id=str(user_id),
            )

            return token_response

        except httpx.HTTPStatusError as e:
            log_structured(
                logger,
                "error",
                f"Failed to refresh OAuth token: {e.response.text}",
                provider=self.provider_name,
                task="refresh_access_token",
                user_id=str(user_id),
                status_code=e.response.status_code,
            )
            # 400/401 = refresh token is dead so revoke + notify
            if e.response.status_code in (HTTP_400_BAD_REQUEST, HTTP_401_UNAUTHORIZED):
                self._revoke_connection(
                    db,
                    user_id,
                    reason="refresh_failed",
                    connection_id=refresh_connection_id,
                    authorization_generation=refresh_generation,
                    lease_token=refresh_lease_token,
                    lease_kind="token_refresh" if refresh_lease_token is not None else None,
                )
                raise HTTPException(
                    status_code=HTTP_401_UNAUTHORIZED,
                    detail=f"Refresh token rejected for {self.provider_name}; reconnection required",
                )
            raise HTTPException(status_code=HTTP_400_BAD_REQUEST, detail=f"Failed to refresh token: {e.response.text}")
        except HTTPException:
            raise
        except Exception as e:
            log_structured(
                logger,
                "warning",
                f"OAuth token refresh failed: {e}",
                provider=self.provider_name,
                task="refresh_access_token",
                user_id=str(user_id),
            )
            raise HTTPException(status_code=HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Token refresh failed: {str(e)}")
        finally:
            if whoop_dispatch_repository is not None and refresh_lease_token is not None:
                db.rollback()
                whoop_dispatch_repository.release_authorization_lease(
                    db,
                    user_id=user_id,
                    lease_token=refresh_lease_token,
                )

    def _revoke_connection(
        self,
        db: DbSession,
        user_id: UUID,
        *,
        reason: str,
        connection_id: UUID | None = None,
        authorization_generation: int | None = None,
        lease_token: UUID | None = None,
        lease_kind: str | None = None,
    ) -> None:
        """Mark the connection revoked and emit a connection.revoked webhook."""
        connection = self._resolve_refresh_connection(
            db,
            user_id,
            for_update=self.provider_name == "whoop",
            connection_id=connection_id,
            authorization_generation=authorization_generation,
            lease_token=lease_token,
            lease_kind=lease_kind,
        )
        if not connection or connection.status == ConnectionStatus.REVOKED:
            return
        exact_authority = current_exact_whoop_sync_authority()
        self.connection_repo.mark_as_revoked(
            db,
            connection,
            expected_whoop_authorization_generation=(
                exact_authority.authorization_generation
                if exact_authority is not None and self.provider_name == "whoop"
                else authorization_generation
            ),
            expected_whoop_lease_token=(
                exact_authority.lease_token
                if exact_authority is not None and self.provider_name == "whoop"
                else lease_token
            ),
            expected_whoop_lease_kind=(
                "full_history_sync" if exact_authority is not None and self.provider_name == "whoop" else lease_kind
            ),
        )
        on_connection_revoked(
            user_id=user_id,
            provider=self.provider_name,
            connection_id=connection.id,
            reason=reason,
            revoked_at=connection.updated_at.isoformat(),
        )

    def _resolve_refresh_connection(
        self,
        db: DbSession,
        user_id: UUID,
        *,
        for_update: bool,
        connection_id: UUID | None = None,
        authorization_generation: int | None = None,
        lease_token: UUID | None = None,
        lease_kind: str | None = None,
    ) -> UserConnection | None:
        """Resolve refresh credentials without escaping an exact WHOOP authority."""
        exact_authority = current_exact_whoop_sync_authority()
        if exact_authority is None:
            if self.provider_name == "whoop" and lease_token is not None:
                if connection_id is None or authorization_generation is None or lease_kind is None:
                    return None
                return self.connection_repo.get_active_whoop_lease_authority(
                    db,
                    user_id=user_id,
                    connection_id=connection_id,
                    authorization_generation=authorization_generation,
                    lease_token=lease_token,
                    lease_kind=lease_kind,
                    for_update=for_update,
                )
            return self.connection_repo.get_by_user_and_provider(db, user_id, self.provider_name)
        if self.provider_name != "whoop" or exact_authority.user_id != user_id:
            return None
        renewed = WhoopSyncDispatchRepository().renew_runtime_authority(
            db,
            user_id=user_id,
            connection_id=exact_authority.connection_id,
            authorization_generation=exact_authority.authorization_generation,
            lease_token=exact_authority.lease_token,
        )
        if not renewed:
            return None
        return self.connection_repo.get_active_exact_whoop_authority(
            db,
            user_id=user_id,
            connection_id=exact_authority.connection_id,
            authorization_generation=exact_authority.authorization_generation,
            dispatch_id=exact_authority.dispatch_id,
            lease_token=exact_authority.lease_token,
            for_update=for_update,
        )

    def _build_auth_url(self, state: str) -> tuple[str, dict[str, Any] | None]:
        """Builds the authorization URL.

        Returns:
            tuple: (authorization_url, pkce_data_or_None)
        """
        pkce_data = None
        extra_params = ""

        if self.use_pkce:
            code_verifier, code_challenge = self._generate_pkce_pair()
            extra_params = f"&code_challenge={code_challenge}&code_challenge_method=S256"
            pkce_data = {"code_verifier": code_verifier}

        auth_url = (
            f"{self.endpoints.authorize_url}?"
            f"response_type=code&"
            f"client_id={self.credentials.client_id}&"
            f"redirect_uri={self.credentials.redirect_uri}&"
            f"state={state}"
            f"{extra_params}"
        )

        if self.credentials.default_scope:
            from urllib.parse import quote

            encoded_scope = quote(self.credentials.default_scope)
            auth_url += f"&scope={encoded_scope}"

        # pkce_data will be None for non-PKCE providers
        return auth_url, pkce_data

    def _generate_pkce_pair(self) -> tuple[str, str]:
        """Generates PKCE code verifier and challenge."""
        code_verifier = secrets.token_urlsafe(43)
        challenge_bytes = hashlib.sha256(code_verifier.encode()).digest()
        code_challenge = urlsafe_b64encode(challenge_bytes).decode().rstrip("=")

        return code_verifier, code_challenge

    def _validate_state(self, state: str) -> tuple[OAuthState, str | None]:
        """Validates and consumes the OAuth state from Redis."""
        redis_key = f"oauth_state:{state}"
        state_data = self.redis_client.get(redis_key)

        if not state_data:
            raise HTTPException(status_code=HTTP_400_BAD_REQUEST, detail="Invalid or expired state parameter")

        # Delete state immediately (one-time use)
        self.redis_client.delete(redis_key)

        # Redis data can be str or bytes, ensure it's str
        state_data_str: str = state_data.decode("utf-8") if isinstance(state_data, bytes) else str(state_data)
        state_dict = json.loads(state_data_str)
        code_verifier = state_dict.pop("code_verifier", None)
        oauth_state = OAuthState.model_validate(state_dict)

        # code_verifier will be None for non-PKCE providers
        return oauth_state, code_verifier

    def _exchange_token(self, code: str, code_verifier: str | None) -> OAuthTokenResponse:
        """Exchanges authorization code for tokens."""
        data, headers = self._prepare_token_request(code, code_verifier)

        try:
            response = httpx.post(
                self.endpoints.token_url,
                data=data,
                headers=headers,
                timeout=30.0,
            )
            response.raise_for_status()
            return OAuthTokenResponse.model_validate(response.json())
        except httpx.HTTPStatusError as e:
            log_structured(
                logger,
                "error",
                f"Failed to exchange authorization code: {e.response.text}",
                provider=self.provider_name,
                task="exchange_token",
                status_code=e.response.status_code,
            )
            raise HTTPException(
                status_code=HTTP_400_BAD_REQUEST,
                detail=f"Failed to exchange authorization code: {e.response.text}",
            )
        except Exception as e:
            log_structured(
                logger,
                "error",
                f"Token exchange failed: {e}",
                provider=self.provider_name,
                task="exchange_token",
            )
            raise HTTPException(status_code=HTTP_500_INTERNAL_SERVER_ERROR, detail=f"Token exchange failed: {str(e)}")

    def _prepare_token_request(self, code: str, code_verifier: str | None) -> tuple[dict, dict]:
        """Prepares the token exchange request. Default implementation uses Basic Auth."""

        token_data = {
            "grant_type": "authorization_code",
            "code": code,
            "redirect_uri": self.credentials.redirect_uri,
        }

        if self.use_pkce and code_verifier:
            token_data["code_verifier"] = code_verifier

        if self.auth_method == AuthenticationMethod.BODY:
            token_data["client_id"] = self.credentials.client_id
            token_data["client_secret"] = self.credentials.client_secret
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
        else:
            headers = self._get_basic_auth_headers()

        return token_data, headers

    def _prepare_refresh_request(self, refresh_token: str) -> tuple[dict, dict]:
        """Prepares the token refresh request. Default implementation uses Basic Auth."""
        token_data = {
            "grant_type": "refresh_token",
            "refresh_token": refresh_token,
        }

        if self.auth_method == AuthenticationMethod.BODY:
            token_data["client_id"] = self.credentials.client_id
            token_data["client_secret"] = self.credentials.client_secret
            headers = {"Content-Type": "application/x-www-form-urlencoded"}
        else:
            headers = self._get_basic_auth_headers()

        return token_data, headers

    def _get_basic_auth_headers(self) -> dict:
        """Generates Basic Auth headers for token requests."""
        credentials = f"{self.credentials.client_id}:{self.credentials.client_secret}"
        b64_credentials = b64encode(credentials.encode()).decode()

        return {
            "Authorization": f"Basic {b64_credentials}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

    def deregister_user(self, access_token: str, provider_user_id: str | None = None) -> None:
        """Notify provider that user is disconnecting. Override in subclasses that support deregistration."""
        log_structured(
            logger,
            "warning",
            "Deregistering not supported",
            provider=self.provider_name,
            action="deregister_user",
        )

    def _get_provider_user_info(self, token_response: OAuthTokenResponse, user_id: str) -> dict[str, str | None]:
        """Extracts provider user info. Default implementation returns None."""
        return {"user_id": None, "username": None}

    def _save_connection(
        self,
        db: DbSession,
        user_id: UUID,
        token_response: OAuthTokenResponse,
        user_info: dict[str, str | None],
        oauth_state: OAuthState,
        *,
        expected_whoop_oauth_lease_token: UUID | None = None,
    ) -> None:
        """Saves or updates the user connection."""
        provider_user_id = user_info.get("user_id")
        provider_username = user_info.get("username")

        scope = user_info.get("scope") or token_response.scope

        token_expires_at = datetime.now(timezone.utc) + timedelta(seconds=token_response.expires_in)

        existing_connection = self.connection_repo.get_by_user_and_provider(
            db,
            user_id,
            self.provider_name,
        )

        if existing_connection:
            was_inactive = existing_connection.status != ConnectionStatus.ACTIVE
            # Update tokens, user info, and scope
            self.connection_repo.update_connection_info(
                db,
                existing_connection,
                access_token=token_response.access_token,
                refresh_token=token_response.refresh_token,
                expires_in=token_response.expires_in,
                provider_user_id=provider_user_id,
                provider_username=provider_username,
                scope=scope,
                expected_whoop_oauth_lease_token=expected_whoop_oauth_lease_token,
            )
            if was_inactive:
                on_connection_created(
                    user_id=user_id,
                    provider=self.provider_name,
                    connection_id=existing_connection.id,
                    connected_at=datetime.now(timezone.utc).isoformat(),
                )
        else:
            if expected_whoop_oauth_lease_token is not None:
                lease = (
                    db.query(WhoopAuthorizationLease)
                    .filter(
                        WhoopAuthorizationLease.user_id == user_id,
                        WhoopAuthorizationLease.connection_id.is_(None),
                        WhoopAuthorizationLease.authorization_generation == 0,
                        WhoopAuthorizationLease.lease_token == expected_whoop_oauth_lease_token,
                        WhoopAuthorizationLease.lease_kind == "oauth_callback",
                        WhoopAuthorizationLease.lease_expires_at > func.clock_timestamp(),
                    )
                    .with_for_update()
                    .one_or_none()
                )
                if lease is None:
                    raise HTTPException(
                        status_code=HTTP_409_CONFLICT,
                        detail="WHOOP authorization authority expired; start authorization again",
                    )
            connection_create = UserConnectionCreate(
                user_id=user_id,
                provider=self.provider_name,
                provider_user_id=provider_user_id,
                provider_username=provider_username,
                access_token=token_response.access_token,
                refresh_token=token_response.refresh_token,
                token_expires_at=token_expires_at,
                scope=scope,
            )
            new_connection = self.connection_repo.create(db, connection_create)
            on_connection_created(
                user_id=user_id,
                provider=self.provider_name,
                connection_id=new_connection.id,
                connected_at=new_connection.created_at.isoformat(),
            )
