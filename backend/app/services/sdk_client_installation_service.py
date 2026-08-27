from datetime import datetime, timedelta, timezone
from logging import getLogger
from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy import update

from app.constants.sdk_history import (
    DASHBOARD_FITNESS_APPLE_HEALTH_V1_TYPES,
    DASHBOARD_FITNESS_COVERAGE_POLICY_VERSION,
)
from app.database import DbSession
from app.models import RefreshToken, User, UserInvitationCode
from app.models.sdk_client_installation import SDKClientInstallation
from app.repositories.sdk_client_installation_repository import sdk_client_installation_repository
from app.schemas.auth import SDKClientMetadataRefresh
from app.schemas.model_crud.credentials.sdk_client_installation import SDKClientRegistration
from app.schemas.model_crud.credentials.user_invitation_code import UserInvitationActivationPolicy

logger = getLogger(__name__)


class SDKClientInstallationService:
    app_id_prefix = "dfi:"
    contact_write_interval = timedelta(minutes=5)

    def __init__(self) -> None:
        self.crud = sdk_client_installation_repository

    @staticmethod
    def _now() -> datetime:
        return datetime.now(timezone.utc)

    @classmethod
    def app_id_for(cls, installation_id: UUID, generation: int) -> str:
        return f"{cls.app_id_prefix}{installation_id}:{generation}"

    def health_write_error(
        self,
        db_session: DbSession,
        *,
        user: User,
        installation_id: UUID | None,
        installation_generation: int | None,
        health_evidence_generation: int | None,
    ) -> str | None:
        """Return a stable failure code when queued health work has lost authority."""
        if user.health_write_state not in {"active", "activating"}:
            return "health_write_fenced"

        has_first_class_installation = bool(self.crud.list_for_user(db_session, user.id))
        if installation_id is None and installation_generation is None and health_evidence_generation is None:
            if (
                user.health_evidence_generation != 0
                or user.health_source_policy != "legacy-mixed"
                or has_first_class_installation
            ):
                return "legacy_writer_fenced"
            return None

        if installation_id is None or installation_generation is None or health_evidence_generation is None:
            return "installation_scope_invalid"
        if health_evidence_generation != user.health_evidence_generation:
            return "health_generation_mismatch"

        installation = self.crud.get(db_session, installation_id)
        if (
            installation is None
            or installation.user_id != user.id
            or installation.status != "active"
            or installation.generation != installation_generation
            or installation.health_evidence_generation != health_evidence_generation
        ):
            return "installation_generation_mismatch"
        return None

    def activate(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        registration: SDKClientRegistration,
        activation_policy: UserInvitationActivationPolicy | None = None,
    ) -> SDKClientInstallation:
        """Atomically replace the active phone while retaining its accepted evidence."""
        now = self._now()
        # Serialize replacement per account before inspecting the partial unique
        # active-installation slot.
        user = db_session.query(User).filter(User.id == user_id).populate_existing().with_for_update().one()
        if user.health_write_state == "fenced":
            raise HTTPException(
                status_code=status.HTTP_423_LOCKED,
                detail="Health data changes are temporarily fenced",
            )
        # Once an account has ever accepted a first-class client, legacy invite
        # credentials stay fenced even if every installation is later revoked.
        if user.health_source_policy != "multi-source":
            user.health_source_policy = "apple-mobile-v2-only"
        if user.health_write_state == "awaiting-v2-pairing":
            user.health_write_state = "activating"
        existing = self.crud.get_for_update(db_session, registration.installation_id)
        if existing is not None and existing.user_id != user_id:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Installation identity is already bound to another user",
            )

        active = self.crud.get_active_for_user(db_session, user_id)
        if active is not None and active.id != registration.installation_id:
            active.status = "revoked"
            active.revoked_at = now

        # Replacement invalidates every outstanding SDK refresh credential for
        # the account, including legacy invite:* credentials. Access JWTs are
        # fenced independently by get_sdk_auth on their next request.
        db_session.execute(
            update(RefreshToken)
            .where(
                RefreshToken.user_id == user_id,
                RefreshToken.revoked_at.is_(None),
            )
            .values(revoked_at=now)
        )

        generation = self.crud.next_generation(db_session, user_id)
        stored_activation_policy = None
        if activation_policy is not None:
            stored_activation_policy = {
                **activation_policy.storage_value(),
                "coverage_policy_version": DASHBOARD_FITNESS_COVERAGE_POLICY_VERSION,
                "required_type_identifiers": sorted(DASHBOARD_FITNESS_APPLE_HEALTH_V1_TYPES),
            }
        if existing is None:
            existing = SDKClientInstallation(
                id=registration.installation_id,
                user_id=user_id,
                app_id=self.app_id_for(registration.installation_id, generation),
                bundle_id=registration.bundle_id,
                app_version=registration.app_version,
                build_number=registration.build_number,
                protocol_version=registration.protocol_version,
                activation_policy=stored_activation_policy,
                health_evidence_generation=user.health_evidence_generation,
                generation=generation,
                status="active",
                connected_at=now,
                last_contact_at=now,
                last_terminal_receipt_at=None,
                revoked_at=None,
                created_at=now,
            )
            db_session.add(existing)
        else:
            existing.app_id = self.app_id_for(registration.installation_id, generation)
            existing.bundle_id = registration.bundle_id
            existing.app_version = registration.app_version
            existing.build_number = registration.build_number
            existing.protocol_version = registration.protocol_version
            existing.activation_policy = stored_activation_policy
            existing.health_evidence_generation = user.health_evidence_generation
            existing.generation = generation
            existing.status = "active"
            existing.connected_at = now
            existing.last_contact_at = now
            existing.last_terminal_receipt_at = None
            existing.revoked_at = None

        db_session.flush()
        return existing

    def refresh_metadata(
        self,
        db_session: DbSession,
        *,
        installation: SDKClientInstallation,
        metadata: SDKClientMetadataRefresh,
    ) -> None:
        """Refresh non-secret release metadata for the exact token-bound install."""
        if (
            metadata.installation_id != installation.id
            or metadata.bundle_id != installation.bundle_id
            or metadata.protocol_version != installation.protocol_version
        ):
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Mobile installation metadata does not match",
                headers={"WWW-Authenticate": "Bearer"},
            )
        installation.app_version = metadata.app_version
        installation.build_number = metadata.build_number
        installation.last_contact_at = self._now()
        db_session.flush()

    def require_active(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        app_id: str | None,
        generation: int | None = None,
        bundle_id: str | None = None,
        app_version: str | None = None,
        build_number: str | None = None,
        protocol_version: int | None = None,
        health_evidence_generation: int | None = None,
        touch: bool = True,
    ) -> SDKClientInstallation | None:
        """Fence replaced installs while allowing untouched legacy users to continue."""
        user = db_session.get(User, user_id)
        if user is None or user.health_write_state not in {"active", "activating"}:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Health data access is not active",
                headers={"WWW-Authenticate": "Bearer"},
            )
        if app_id and app_id.startswith(self.app_id_prefix):
            installation = self.crud.get_by_app_id(db_session, app_id)
            if (
                installation is None
                or installation.user_id != user_id
                or installation.status != "active"
                or generation != installation.generation
                or bundle_id != installation.bundle_id
                or app_version != installation.app_version
                or build_number != installation.build_number
                or protocol_version != installation.protocol_version
                or health_evidence_generation != user.health_evidence_generation
            ):
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail="Mobile installation is no longer active",
                    headers={"WWW-Authenticate": "Bearer"},
                )
            if touch:
                now = self._now()
                if installation.last_contact_at <= now - self.contact_write_interval:
                    installation.last_contact_at = now
                    db_session.commit()
            return installation

        # A first-class active installation permanently fences legacy invite:*
        # access tokens. Accounts not yet migrated retain their existing flow.
        if self.crud.get_active_for_user(db_session, user_id) is not None:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Mobile installation has been replaced",
                headers={"WWW-Authenticate": "Bearer"},
            )
        if user.health_evidence_generation != 0 or user.health_source_policy != "legacy-mixed":
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Legacy mobile credentials are no longer accepted",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return None

    def revoke(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        installation_id: UUID,
        expected_generation: int,
        expected_health_evidence_generation: int,
    ) -> SDKClientInstallation:
        # Serialize revocation with replacement, reset fencing, and queued
        # workers, all of which use the user row as the account write lock.
        user = db_session.query(User).filter(User.id == user_id).populate_existing().with_for_update().one_or_none()
        if user is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Mobile installation not found")
        installation = self.crud.get_for_update(db_session, installation_id)
        if installation is None or installation.user_id != user_id:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Mobile installation not found")
        if installation.generation != expected_generation:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Mobile installation generation changed",
            )
        if (
            user.health_evidence_generation != expected_health_evidence_generation
            or installation.health_evidence_generation != expected_health_evidence_generation
        ):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Health evidence generation changed",
            )
        if installation.status == "active":
            now = self._now()
            installation.status = "revoked"
            installation.revoked_at = now
            db_session.execute(
                update(RefreshToken)
                .where(
                    RefreshToken.user_id == user_id,
                    RefreshToken.app_id == installation.app_id,
                    RefreshToken.revoked_at.is_(None),
                )
                .values(revoked_at=now)
            )
            db_session.commit()
            db_session.refresh(installation)
        return installation

    def fence_for_reset(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        operation_id: UUID,
        expected_health_evidence_generation: int,
        resulting_health_source_policy: str = "apple-mobile-v2-only",
        commit: bool = True,
    ) -> User:
        """Idempotently stop every health writer without deleting any evidence."""
        user = db_session.query(User).filter(User.id == user_id).populate_existing().with_for_update().one_or_none()
        if user is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
        if user.health_evidence_generation != expected_health_evidence_generation:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Health evidence generation changed")
        if user.health_write_state == "fenced":
            if user.health_reset_operation_id == operation_id:
                if user.health_reset_resulting_source_policy != resulting_health_source_policy:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail="Health reset resulting source policy changed",
                    )
                return user
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Another health reset is already active")
        if user.health_reset_operation_id == operation_id and user.health_write_state == "awaiting-v2-pairing":
            # The advance response may have been lost; do not reopen or rewind it.
            return user

        now = self._now()
        user.health_source_policy = resulting_health_source_policy
        user.health_write_state = "fenced"
        user.health_reset_operation_id = operation_id
        user.health_reset_resulting_source_policy = resulting_health_source_policy
        active = self.crud.get_active_for_user(db_session, user_id)
        if active is not None:
            active.status = "revoked"
            active.revoked_at = now
        db_session.execute(
            update(RefreshToken)
            .where(RefreshToken.user_id == user_id, RefreshToken.revoked_at.is_(None))
            .values(revoked_at=now)
        )
        db_session.execute(
            update(UserInvitationCode)
            .where(
                UserInvitationCode.user_id == user_id,
                UserInvitationCode.redeemed_at.is_(None),
                UserInvitationCode.revoked_at.is_(None),
            )
            .values(revoked_at=now)
        )
        if commit:
            db_session.commit()
            db_session.refresh(user)
        else:
            db_session.flush()
        return user


sdk_client_installation_service = SDKClientInstallationService()
