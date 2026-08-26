import secrets
from datetime import datetime, timedelta, timezone
from logging import Logger, getLogger
from uuid import UUID, uuid4

from fastapi import HTTPException, status

from app.config import settings
from app.database import DbSession
from app.models import User
from app.models.user_invitation_code import UserInvitationCode
from app.repositories.user_invitation_code_repository import UserInvitationCodeRepository
from app.schemas.model_crud.credentials import (
    InvitationCodeRedeemResponse,
    UserInvitationActivationPolicy,
    UserInvitationCodeCreate,
    UserInvitationCodeRead,
)
from app.schemas.model_crud.credentials.sdk_client_installation import SDKClientRegistration
from app.services.refresh_token_service import refresh_token_service
from app.services.sdk_client_installation_service import sdk_client_installation_service
from app.services.sdk_token_service import create_sdk_user_token

CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789"
CODE_LENGTH = 8


class UserInvitationCodeService:
    def __init__(self, log: Logger) -> None:
        self.crud = UserInvitationCodeRepository(UserInvitationCode)
        self.logger = log

    def _generate_code(self) -> str:
        """Generate an 8-character alphanumeric code from unambiguous charset."""
        return "".join(secrets.choice(CODE_ALPHABET) for _ in range(CODE_LENGTH))

    def generate(
        self,
        db_session: DbSession,
        user_id: UUID,
        developer_id: UUID,
        activation_policy: UserInvitationActivationPolicy | None = None,
    ) -> UserInvitationCodeRead:
        """Generate a new invitation code for a user."""
        user = db_session.query(User).filter(User.id == user_id).with_for_update().one_or_none()
        if user is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
        assert user is not None
        if user.health_write_state == "fenced":
            raise HTTPException(
                status_code=status.HTTP_423_LOCKED,
                detail="Health data changes are temporarily fenced",
            )
        self.crud.revoke_active_for_user(db_session, user_id, commit=False)

        now = datetime.now(timezone.utc)
        code_data = UserInvitationCodeCreate(
            id=uuid4(),
            code=self._generate_code(),
            user_id=user_id,
            created_by_id=developer_id,
            expires_at=now + timedelta(days=settings.user_invitation_code_expire_days),
            activation_policy=(activation_policy.storage_value() if activation_policy is not None else None),
            health_evidence_generation=user.health_evidence_generation,
            created_at=now,
        )
        row = self.crud.create(db_session, code_data)
        return UserInvitationCodeRead.model_validate(row)

    def redeem(
        self,
        db_session: DbSession,
        code: str,
        *,
        client: SDKClientRegistration | None = None,
    ) -> InvitationCodeRedeemResponse:
        """Redeem an invitation code and return SDK tokens."""
        invitation_preview = self.crud.get_valid_by_code(db_session, code.upper(), for_update=False)

        if not invitation_preview:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Invalid or expired invitation code",
            )

        user = db_session.query(User).filter(User.id == invitation_preview.user_id).with_for_update().one_or_none()
        if user is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Invalid or expired invitation code")
        invitation_code = self.crud.get_valid_by_code(db_session, code.upper())
        if invitation_code is None or invitation_code.user_id != user.id:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Invalid or expired invitation code")
        if invitation_code.health_evidence_generation != user.health_evidence_generation:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail="Invalid or expired invitation code",
            )
        if user.health_write_state == "fenced":
            raise HTTPException(
                status_code=status.HTTP_423_LOCKED,
                detail="Health data changes are temporarily fenced",
            )
        if client is None and user.health_source_policy == "apple-mobile-v2-only":
            raise HTTPException(
                status_code=status.HTTP_426_UPGRADE_REQUIRED,
                detail="Permanent mobile client registration is required",
            )

        installation = None
        if client is not None:
            activation_policy = (
                UserInvitationActivationPolicy.model_validate(invitation_code.activation_policy)
                if invitation_code.activation_policy is not None
                else None
            )
            installation = sdk_client_installation_service.activate(
                db_session,
                user_id=invitation_code.user_id,
                registration=client,
                activation_policy=activation_policy,
            )
            app_id = installation.app_id
        else:
            app_id = f"invite:{invitation_code.created_by_id}"

        invitation_code.redeemed_at = datetime.now(timezone.utc)
        db_session.flush()

        access_token = create_sdk_user_token(
            app_id=app_id,
            user_id=str(invitation_code.user_id),
            installation_generation=installation.generation if installation is not None else None,
            bundle_id=installation.bundle_id if installation is not None else None,
            app_version=installation.app_version if installation is not None else None,
            build_number=installation.build_number if installation is not None else None,
            protocol_version=installation.protocol_version if installation is not None else None,
            health_evidence_generation=user.health_evidence_generation,
        )

        refresh_token = refresh_token_service.create_sdk_refresh_token(
            db_session,
            user_id=invitation_code.user_id,
            app_id=app_id,
            health_evidence_generation=user.health_evidence_generation,
            commit=False,
        )

        db_session.commit()

        return InvitationCodeRedeemResponse(
            access_token=access_token,
            token_type="bearer",
            refresh_token=refresh_token,
            expires_in=settings.access_token_expire_minutes * 60,
            user_id=invitation_code.user_id,
            activation_policy=invitation_code.activation_policy,
            installation_id=installation.id if installation is not None else None,
        )


user_invitation_code_service = UserInvitationCodeService(log=getLogger(__name__))
