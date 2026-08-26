import secrets
from logging import Logger, getLogger
from typing import Annotated
from uuid import UUID

from fastapi import Depends, Header, HTTPException

from app.database import DbSession
from app.models import ApiKey, Developer
from app.repositories.api_key_repository import ApiKeyRepository
from app.schemas.model_crud.credentials import ApiKeyCreate, ApiKeyUpdate
from app.schemas.model_crud.credentials.api_key import ApiKeyScope
from app.services.services import AppService
from app.utils.auth import get_current_developer_optional


class ApiKeyService(AppService[ApiKeyRepository, ApiKey, ApiKeyCreate, ApiKeyUpdate]):
    def __init__(self, log: Logger, **kwargs):
        super().__init__(
            crud_model=ApiKeyRepository,
            model=ApiKey,
            log=log,
            **kwargs,
        )

    def _generate_key_value(self) -> str:
        """Generate random API key with sk- prefix and 32 hex characters."""
        return f"sk-{secrets.token_hex(16)}"

    def create_api_key(
        self,
        db: DbSession,
        created_by: UUID | None,
        name: str = "Default",
        *,
        scopes: list[ApiKeyScope] | None = None,
    ) -> ApiKey:
        key_value = self._generate_key_value()
        creator = ApiKeyCreate(id=key_value, name=name, created_by=created_by, scopes=scopes or [])
        api_key = self.create(db, creator)
        self.logger.debug(f"Created API key {api_key.id} by developer {created_by} with name {name}")
        return api_key

    def list_api_keys(self, db: DbSession) -> list[ApiKey]:
        """List all API keys ordered by creation date."""
        keys = self.crud.get_all_ordered(db)
        self.logger.debug(f"Listed {len(keys)} API keys")
        return keys

    def rotate_api_key(self, db: DbSession, old_key: str, created_by: UUID | None) -> ApiKey:
        """Rotate API key - delete old and create new."""
        current = self.get(db, old_key, raise_404=True)
        assert current is not None
        scopes = ApiKeyUpdate.model_validate({"scopes": current.scopes}).scopes
        assert scopes is not None
        self.delete(db, old_key, raise_404=True)
        new_key = self.create_api_key(db, created_by, scopes=scopes)
        self.logger.debug(f"Rotated API key from {old_key} to {new_key.id}")
        return new_key

    def validate_api_key(self, db: DbSession, key: str) -> ApiKey:
        """Validate API key exists in database. Raises 401 if invalid."""
        if not (api_key := self.get(db, key)):
            raise HTTPException(status_code=401, detail="Invalid or missing API key")
        return api_key


api_key_service = ApiKeyService(log=getLogger(__name__))


async def _require_api_key(
    db: DbSession,
    developer: Developer | None = Depends(get_current_developer_optional),
    x_open_wearables_api_key: str | None = Header(None, alias="X-Open-Wearables-API-Key"),
) -> str:
    if developer:
        return str(developer.id)
    if x_open_wearables_api_key:
        return api_key_service.validate_api_key(db, x_open_wearables_api_key).id
    raise HTTPException(status_code=401, detail="Authentication required: provide JWT token or API key")


ApiKeyDep = Annotated[str, Depends(_require_api_key)]


async def _require_source_reset_api_key(
    db: DbSession,
    x_open_wearables_api_key: str | None = Header(None, alias="X-Open-Wearables-API-Key"),
) -> str:
    """Require a dedicated destructive-reset credential, never a generic key."""
    if not x_open_wearables_api_key:
        raise HTTPException(status_code=401, detail="A source-reset scoped API key is required")
    api_key = api_key_service.validate_api_key(db, x_open_wearables_api_key)
    if "source-reset" not in api_key.scopes:
        raise HTTPException(status_code=403, detail="API key is not authorized for source reset")
    return api_key.id


SourceResetApiKeyDep = Annotated[str, Depends(_require_source_reset_api_key)]
