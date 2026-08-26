from datetime import datetime
from uuid import UUID

from sqlalchemy import func, select

from app.database import DbSession
from app.models import SDKSyncWindowReceipt
from app.models.sdk_client_installation import SDKClientInstallation


class SDKClientInstallationRepository:
    def get(self, db_session: DbSession, installation_id: UUID) -> SDKClientInstallation | None:
        return db_session.get(SDKClientInstallation, installation_id)

    def get_for_update(self, db_session: DbSession, installation_id: UUID) -> SDKClientInstallation | None:
        stmt = (
            select(SDKClientInstallation)
            .where(SDKClientInstallation.id == installation_id)
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        return db_session.execute(stmt).scalar_one_or_none()

    def get_by_app_id(self, db_session: DbSession, app_id: str) -> SDKClientInstallation | None:
        stmt = select(SDKClientInstallation).where(SDKClientInstallation.app_id == app_id)
        return db_session.execute(stmt).scalar_one_or_none()

    def get_active_for_user(self, db_session: DbSession, user_id: UUID) -> SDKClientInstallation | None:
        stmt = select(SDKClientInstallation).where(
            SDKClientInstallation.user_id == user_id,
            SDKClientInstallation.status == "active",
        )
        return db_session.execute(stmt).scalar_one_or_none()

    def list_for_user(self, db_session: DbSession, user_id: UUID) -> list[SDKClientInstallation]:
        stmt = (
            select(SDKClientInstallation)
            .where(SDKClientInstallation.user_id == user_id)
            .order_by(
                SDKClientInstallation.generation.desc(),
                SDKClientInstallation.connected_at.desc(),
            )
        )
        return list(db_session.execute(stmt).scalars().all())

    def next_generation(self, db_session: DbSession, user_id: UUID) -> int:
        stmt = select(func.coalesce(func.max(SDKClientInstallation.generation), 0)).where(
            SDKClientInstallation.user_id == user_id
        )
        return int(db_session.execute(stmt).scalar_one()) + 1

    def readiness_for(
        self,
        db_session: DbSession,
        installation: SDKClientInstallation,
    ) -> tuple[datetime | None, datetime | None]:
        """Return exact recent readiness and a contiguous backward archive frontier."""
        rows = (
            db_session.query(SDKSyncWindowReceipt)
            .filter(
                SDKSyncWindowReceipt.user_id == installation.user_id,
                SDKSyncWindowReceipt.installation_id == installation.id,
                SDKSyncWindowReceipt.installation_generation == installation.generation,
                SDKSyncWindowReceipt.health_evidence_generation == installation.health_evidence_generation,
                SDKSyncWindowReceipt.provider == "apple",
                SDKSyncWindowReceipt.window_version == 2,
                SDKSyncWindowReceipt.purpose.in_(("activation", "archive")),
            )
            .order_by(
                SDKSyncWindowReceipt.accepted_at.desc(),
                SDKSyncWindowReceipt.id.desc(),
            )
            .all()
        )
        activation = next((row for row in rows if row.purpose == "activation"), None)
        if activation is None:
            return None, None

        frontier = activation.lower_bound_inclusive
        remaining = [row for row in rows if row.purpose == "archive"]
        while True:
            adjacent = [row for row in remaining if row.upper_bound_exclusive == frontier]
            if not adjacent:
                break
            selected = min(adjacent, key=lambda row: (row.lower_bound_inclusive, row.id))
            frontier = selected.lower_bound_inclusive
            remaining.remove(selected)
        return activation.accepted_at, frontier


sdk_client_installation_repository = SDKClientInstallationRepository()
