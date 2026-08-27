"""One-off, fail-closed cleanup for a founder shadow WHOOP account.

This module intentionally is not wired to an HTTP route.  It exists only to
repair one known duplicate Open Wearables user while preserving the real user
that shares the same WHOOP provider identity.  It must not become a general
account deletion path.
"""

import hashlib
import hmac
import json
import re
from dataclasses import dataclass, field
from typing import Any
from uuid import UUID

from sqlalchemy import or_

from app.config import settings
from app.database import DbSession
from app.models import (
    DETAIL_MODELS,
    DataPointSeries,
    DataPointSeriesArchive,
    DataSource,
    EventRecord,
    HealthScore,
    RefreshToken,
    SDKBatchReceipt,
    SDKClientInstallation,
    SDKSleepInbox,
    SDKSourceResetSeal,
    SDKSyncWindowReceipt,
    SDKUploadInbox,
    User,
    UserConnection,
    UserInvitationCode,
)
from app.schemas.auth import ConnectionStatus
from app.schemas.enums import ProviderName
from app.services.provider_identity_authority import acquire_provider_identity_value_locks
from app.services.sdk_source_reset_external import (
    ExternalResetInventory,
    ProviderIdentityScope,
    RedisReference,
    SDKSourceResetExternalPlanes,
    sdk_source_reset_external_planes,
)

WHOOP = ProviderName.WHOOP.value
_PLAN_VERSION = 1
_SDK_MODELS = (
    SDKUploadInbox,
    SDKSyncWindowReceipt,
    SDKSleepInbox,
    SDKBatchReceipt,
    RefreshToken,
    UserInvitationCode,
    SDKClientInstallation,
    SDKSourceResetSeal,
)


class FounderShadowWhoopCleanupError(RuntimeError):
    """A bounded cleanup precondition or postcondition failed."""

    def __init__(self, *blockers: str) -> None:
        normalized = tuple(sorted({str(blocker) for blocker in blockers if str(blocker)}))
        self.blockers = normalized or ("founder-shadow.cleanup-failed",)
        super().__init__("; ".join(self.blockers))


@dataclass(frozen=True)
class FounderShadowWhoopCleanupPlan:
    phase: str
    plan_digest_sha256: str
    database_digest_sha256: str
    external_digest_sha256: str
    external_configuration_digest_sha256: str
    counts: dict[str, int]
    blockers: tuple[str, ...]
    execution_state_digest_sha256: str = field(repr=False)

    @property
    def executable(self) -> bool:
        return not self.blockers and self.phase in {"planned", "prepared"}

    def public_dict(self) -> dict[str, object]:
        """Return a value-minimized operator response."""

        return {
            "phase": self.phase,
            "plan_digest_sha256": self.plan_digest_sha256,
            "database_digest_sha256": self.database_digest_sha256,
            "external_digest_sha256": self.external_digest_sha256,
            "external_configuration_digest_sha256": self.external_configuration_digest_sha256,
            "counts": dict(sorted(self.counts.items())),
            "blockers": list(self.blockers),
            "executable": self.executable,
        }


@dataclass(frozen=True)
class FounderShadowWhoopCleanupVerification:
    verified: bool
    verification_digest_sha256: str
    counts: dict[str, int]
    blockers: tuple[str, ...]

    def public_dict(self) -> dict[str, object]:
        return {
            "verified": self.verified,
            "verification_digest_sha256": self.verification_digest_sha256,
            "counts": dict(sorted(self.counts.items())),
            "blockers": list(self.blockers),
        }


@dataclass(frozen=True)
class _DatabaseSnapshot:
    phase: str
    counts: dict[str, int]
    blockers: tuple[str, ...]
    database_digest_sha256: str
    target_user_digest_sha256: str
    keeper_user_digest_sha256: str
    keeper_connections_digest_sha256: str
    target_connection_full_digest_sha256: str
    target_connection_id: UUID | None = field(default=None, repr=False)
    provider_user_id: str | None = field(default=None, repr=False)


class FounderShadowWhoopCleanupService:
    """Plan, execute, and verify one shadow-only WHOOP cleanup."""

    @staticmethod
    def _keyed_digest(label: str, payload: object) -> str:
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        return hmac.new(
            settings.secret_key.encode(),
            f"founder-shadow-whoop-cleanup:v1:{label}\0{canonical}".encode(),
            hashlib.sha256,
        ).hexdigest()

    @classmethod
    def _row_digest(cls, label: str, row: Any, *, excluded: frozenset[str] = frozenset()) -> str:
        if row is None:
            return cls._keyed_digest(label, None)
        payload = {
            column.name: getattr(row, column.name) for column in row.__table__.columns if column.name not in excluded
        }
        return cls._keyed_digest(label, payload)

    @classmethod
    def _ids_digest(cls, label: str, values: tuple[UUID, ...]) -> str:
        return cls._keyed_digest(label, [str(value) for value in sorted(values, key=str)])

    @classmethod
    def _rows_digest(cls, label: str, rows: tuple[Any, ...]) -> str:
        ordered = sorted(rows, key=lambda row: str(row.id))
        return cls._keyed_digest(
            label,
            [cls._row_digest(f"{label}-row", row) for row in ordered],
        )

    @staticmethod
    def _count(db_session: DbSession, model: type[Any], user_id: UUID) -> int:
        return int(db_session.query(model).filter(model.user_id == user_id).count())

    @staticmethod
    def _provider_value(value: object) -> str:
        return value.value if isinstance(value, ProviderName) else str(value)

    @classmethod
    def _database_snapshot(
        cls,
        db_session: DbSession,
        *,
        target_user_id: UUID,
        keeper_user_id: UUID,
        for_update: bool,
    ) -> _DatabaseSnapshot:
        blockers: list[str] = []
        if target_user_id == keeper_user_id:
            blockers.append("founder-shadow.target-is-keeper")

        user_query = (
            db_session.query(User)
            .filter(User.id.in_((target_user_id, keeper_user_id)))
            .order_by(User.id)
            .populate_existing()
        )
        if for_update:
            user_query = user_query.with_for_update()
        users = {row.id: row for row in user_query.all()}
        target = users.get(target_user_id)
        keeper = users.get(keeper_user_id)
        if target is None:
            blockers.append("founder-shadow.target-user-missing")
        if keeper is None:
            blockers.append("founder-shadow.keeper-user-missing")
        if target is None or keeper is None or target_user_id == keeper_user_id:
            counts = {"users": len(users)}
            digest = cls._keyed_digest("database", {"counts": counts, "blockers": sorted(blockers)})
            return _DatabaseSnapshot(
                phase="blocked",
                counts=counts,
                blockers=tuple(sorted(set(blockers))),
                database_digest_sha256=digest,
                target_user_digest_sha256=cls._row_digest("target-user", target),
                keeper_user_digest_sha256=cls._row_digest("keeper-user", keeper),
                keeper_connections_digest_sha256=cls._keyed_digest("keeper-connections", None),
                target_connection_full_digest_sha256=cls._keyed_digest("target-connection-full", None),
            )

        connection_query = (
            db_session.query(UserConnection)
            .filter(UserConnection.user_id.in_((target_user_id, keeper_user_id)))
            .order_by(UserConnection.id)
            .populate_existing()
        )
        if for_update:
            connection_query = connection_query.with_for_update()
        pair_connections = connection_query.all()
        target_connections = [row for row in pair_connections if row.user_id == target_user_id]
        keeper_connections = [row for row in pair_connections if row.user_id == keeper_user_id]
        keeper_whoop_connections = [row for row in keeper_connections if row.provider == WHOOP]
        target_connection = target_connections[0] if len(target_connections) == 1 else None
        keeper_connection = keeper_whoop_connections[0] if len(keeper_whoop_connections) == 1 else None

        if len(target_connections) != 1:
            blockers.append("founder-shadow.target-connection-count-invalid")
        elif target_connection is not None and target_connection.provider != WHOOP:
            blockers.append("founder-shadow.target-connection-is-not-whoop")
        if len(keeper_whoop_connections) != 1:
            blockers.append("founder-shadow.keeper-whoop-connection-count-invalid")
        elif keeper_connection is not None and keeper_connection.status != ConnectionStatus.ACTIVE:
            blockers.append("founder-shadow.keeper-whoop-connection-not-active")

        provider_user_id = str(target_connection.provider_user_id or "").strip() if target_connection else ""
        if not provider_user_id or provider_user_id.lower() == "unknown":
            blockers.append("founder-shadow.target-provider-identity-invalid")
        if keeper_connection is not None and str(keeper_connection.provider_user_id or "").strip() != provider_user_id:
            blockers.append("founder-shadow.keeper-provider-identity-mismatch")

        identity_owner_ids: tuple[UUID, ...] = ()
        if provider_user_id:
            identity_rows = (
                db_session.query(UserConnection)
                .filter(
                    UserConnection.provider == WHOOP,
                    UserConnection.provider_user_id == provider_user_id,
                )
                .order_by(UserConnection.id)
                .all()
            )
            identity_owner_ids = tuple(row.user_id for row in identity_rows)
            if len(identity_rows) != 2 or set(identity_owner_ids) != {target_user_id, keeper_user_id}:
                blockers.append("founder-shadow.provider-identity-owner-set-invalid")

        prepared = False
        if target_connection is not None:
            credentialless = all(
                value is None
                for value in (
                    target_connection.access_token,
                    target_connection.refresh_token,
                    target_connection.token_expires_at,
                    target_connection.scope,
                )
            )
            prepared = (
                target.health_write_state == "fenced"
                and target_connection.status == ConnectionStatus.REVOKED
                and credentialless
            )
            active = target.health_write_state == "active" and target_connection.status == ConnectionStatus.ACTIVE
            if not active and not prepared:
                blockers.append("founder-shadow.target-state-is-neither-planned-nor-prepared")
            if target.health_write_state == "fenced" and not prepared:
                blockers.append("founder-shadow.target-fence-is-not-owned-by-cleanup")
        if target.health_reset_operation_id is not None and target.health_reset_applied_at is None:
            blockers.append("founder-shadow.health-reset-active")

        target_source_rows = (
            db_session.query(DataSource).filter(DataSource.user_id == target_user_id).order_by(DataSource.id).all()
        )
        whoop_sources = [row for row in target_source_rows if cls._provider_value(row.provider) == WHOOP]
        other_sources = [row for row in target_source_rows if cls._provider_value(row.provider) != WHOOP]
        if other_sources:
            blockers.append("founder-shadow.other-provider-data-source-present")
        if target_connection is not None and any(
            row.user_connection_id is not None and row.user_connection_id != target_connection.id
            for row in whoop_sources
        ):
            blockers.append("founder-shadow.target-source-connection-mismatch")
        if (
            target_connection is not None
            and db_session.query(DataSource)
            .filter(
                DataSource.user_connection_id == target_connection.id,
                or_(DataSource.user_id != target_user_id, DataSource.provider != ProviderName.WHOOP),
            )
            .count()
        ):
            blockers.append("founder-shadow.cross-owner-data-source-reference")

        source_ids = tuple(row.id for row in whoop_sources)
        event_ids = tuple(
            row[0]
            for row in db_session.query(EventRecord.id)
            .filter(EventRecord.data_source_id.in_(source_ids or (UUID(int=0),)))
            .all()
        )
        target_score_rows = (
            db_session.query(HealthScore).filter(HealthScore.user_id == target_user_id).order_by(HealthScore.id).all()
        )
        whoop_scores = [row for row in target_score_rows if cls._provider_value(row.provider) == WHOOP]
        if any(cls._provider_value(row.provider) != WHOOP for row in target_score_rows):
            blockers.append("founder-shadow.other-provider-health-score-present")
        allowed_source_ids = set(source_ids)
        allowed_event_ids = set(event_ids)
        if any(row.data_source_id is not None and row.data_source_id not in allowed_source_ids for row in whoop_scores):
            blockers.append("founder-shadow.target-score-source-mismatch")
        if any(
            row.sleep_record_id is not None and row.sleep_record_id not in allowed_event_ids for row in whoop_scores
        ):
            blockers.append("founder-shadow.target-score-event-mismatch")
        if (
            source_ids
            and db_session.query(HealthScore)
            .filter(
                HealthScore.data_source_id.in_(source_ids),
                or_(HealthScore.user_id != target_user_id, HealthScore.provider != ProviderName.WHOOP),
            )
            .count()
        ):
            blockers.append("founder-shadow.cross-owner-health-score-source-reference")
        if (
            event_ids
            and db_session.query(HealthScore)
            .filter(
                HealthScore.sleep_record_id.in_(event_ids),
                or_(HealthScore.user_id != target_user_id, HealthScore.provider != ProviderName.WHOOP),
            )
            .count()
        ):
            blockers.append("founder-shadow.cross-owner-health-score-event-reference")

        sdk_counts = {model.__tablename__: cls._count(db_session, model, target_user_id) for model in _SDK_MODELS}
        if any(sdk_counts.values()):
            blockers.append("founder-shadow.sdk-or-reset-state-present")

        target_user_digest = cls._row_digest(
            "target-user",
            target,
            excluded=frozenset({"health_write_state"}),
        )
        keeper_user_digest = cls._row_digest("keeper-user", keeper)
        keeper_connections_digest = cls._rows_digest("keeper-connections", tuple(keeper_connections))
        target_connection_digest = cls._row_digest(
            "target-connection-stable",
            target_connection,
            excluded=frozenset(
                {
                    "status",
                    "access_token",
                    "refresh_token",
                    "token_expires_at",
                    "scope",
                    "updated_at",
                }
            ),
        )
        target_connection_full_digest = cls._row_digest("target-connection-full", target_connection)
        counts = {
            "users": 2,
            "target_connections": len(target_connections),
            "keeper_connections": len(keeper_connections),
            "keeper_whoop_connections": len(keeper_whoop_connections),
            "identity_connections": len(identity_owner_ids),
            "whoop_data_sources": len(whoop_sources),
            "whoop_health_scores": len(whoop_scores),
            "whoop_event_records": len(event_ids),
            "other_provider_rows": len(other_sources)
            + sum(cls._provider_value(row.provider) != WHOOP for row in target_score_rows)
            + sum(row.provider != WHOOP for row in target_connections),
            "sdk_or_reset_rows": sum(sdk_counts.values()),
        }
        database_payload = {
            "counts": counts,
            "target_user": target_user_digest,
            "keeper_user": keeper_user_digest,
            "target_connection": target_connection_digest,
            "keeper_connections": keeper_connections_digest,
            "identity_owners": cls._ids_digest("identity-owners", identity_owner_ids),
            "source_ids": cls._ids_digest("source-ids", source_ids),
            "event_ids": cls._ids_digest("event-ids", event_ids),
            "score_ids": cls._ids_digest("score-ids", tuple(row.id for row in whoop_scores)),
            "sdk_counts": sdk_counts,
            "blockers": sorted(set(blockers)),
        }
        database_digest = cls._keyed_digest("database", database_payload)
        return _DatabaseSnapshot(
            phase="prepared" if prepared and not blockers else "planned" if not blockers else "blocked",
            counts=counts,
            blockers=tuple(sorted(set(blockers))),
            database_digest_sha256=database_digest,
            target_user_digest_sha256=target_user_digest,
            keeper_user_digest_sha256=keeper_user_digest,
            keeper_connections_digest_sha256=keeper_connections_digest,
            target_connection_full_digest_sha256=target_connection_full_digest,
            target_connection_id=target_connection.id if target_connection is not None else None,
            provider_user_id=provider_user_id or None,
        )

    @classmethod
    def _external_digest(cls, inventory: ExternalResetInventory) -> str:
        return cls._keyed_digest(
            "external",
            {
                "counts": dict(sorted(inventory.counts.items())),
                "identity_tokens": {key: list(values) for key, values in sorted(inventory.identity_tokens.items())},
                "blockers": list(inventory.blockers),
                "configuration": inventory.configuration_digest_sha256,
            },
        )

    @staticmethod
    def _redis_reference_is_exact_whoop(reference: RedisReference, *, target_user_id: UUID) -> bool:
        try:
            values = SDKSourceResetExternalPlanes._walk_text_values(
                {
                    "key": reference.key,
                    "locator": reference.locator,
                    "value": reference.raw_value,
                }
            )
        except (TypeError, ValueError):
            return False
        user_pattern = re.compile(
            rf"(?<![0-9a-f-]){re.escape(str(target_user_id))}(?![0-9a-f-])",
            re.IGNORECASE,
        )
        provider_pattern = re.compile(r"(?<![a-z0-9])whoop(?![a-z0-9])", re.IGNORECASE)
        return any(user_pattern.search(value) for value in values) and any(
            provider_pattern.search(value) for value in values
        )

    @classmethod
    def _external_blockers(
        cls,
        inventory: ExternalResetInventory,
        *,
        target_user_id: UUID,
    ) -> tuple[str, ...]:
        blockers = list(inventory.blockers)
        user_segment = f"/{target_user_id}/"
        for row in inventory.objects:
            segments = tuple(part.strip().lower() for part in row.key.split("/"))
            normalized_key = f"/{row.key.strip('/')}/"
            if user_segment not in normalized_key or WHOOP not in segments:
                blockers.append("founder-shadow.external-object-is-not-exact-whoop")
        if any(
            not cls._redis_reference_is_exact_whoop(row, target_user_id=target_user_id)
            for row in inventory.redis_references
        ):
            blockers.append("founder-shadow.redis-reference-is-not-exact-whoop")
        if inventory.active_task_ids:
            # The shared inventory exposes task IDs, not enough task metadata
            # to prove the provider. Running/reserved work must disappear before
            # this bounded tool is allowed to mutate anything.
            blockers.append("founder-shadow.active-task-provider-unverifiable")
        return tuple(sorted(set(blockers)))

    @classmethod
    def _stable_external_inventory(
        cls,
        *,
        target_user_id: UUID,
    ) -> tuple[ExternalResetInventory, str, tuple[str, ...]]:
        scope = ProviderIdentityScope()
        first = sdk_source_reset_external_planes.inventory(target_user_id, identity_scope=scope)
        second = sdk_source_reset_external_planes.inventory(target_user_id, identity_scope=scope)
        first_digest = cls._external_digest(first)
        second_digest = cls._external_digest(second)
        blockers = list(cls._external_blockers(second, target_user_id=target_user_id))
        if not hmac.compare_digest(first_digest, second_digest):
            blockers.append("founder-shadow.external-inventory-unstable")
        return second, second_digest, tuple(sorted(set(blockers)))

    @classmethod
    def plan(
        cls,
        db_session: DbSession,
        *,
        target_user_id: UUID,
        keeper_user_id: UUID,
    ) -> FounderShadowWhoopCleanupPlan:
        database = cls._database_snapshot(
            db_session,
            target_user_id=target_user_id,
            keeper_user_id=keeper_user_id,
            for_update=False,
        )
        if database.blockers:
            external = ExternalResetInventory({}, {}, (), (), (), (), "")
            external_digest = cls._external_digest(external)
            external_blockers: tuple[str, ...] = ()
        else:
            external, external_digest, external_blockers = cls._stable_external_inventory(target_user_id=target_user_id)
        blockers = tuple(sorted(set((*database.blockers, *external_blockers))))
        counts = {
            **database.counts,
            "external_objects": len(external.objects),
            "external_redis_references": len(external.redis_references),
            "external_active_tasks": len(external.active_task_ids),
        }
        plan_digest = cls._keyed_digest(
            "plan",
            {
                "version": _PLAN_VERSION,
                "phase": database.phase,
                "database": database.database_digest_sha256,
                "external": external_digest,
                "external_configuration": external.configuration_digest_sha256,
                "execution_state": database.target_connection_full_digest_sha256,
                "counts": counts,
                "blockers": list(blockers),
            },
        )
        return FounderShadowWhoopCleanupPlan(
            phase=database.phase,
            plan_digest_sha256=plan_digest,
            database_digest_sha256=database.database_digest_sha256,
            external_digest_sha256=external_digest,
            external_configuration_digest_sha256=external.configuration_digest_sha256,
            counts=counts,
            blockers=blockers,
            execution_state_digest_sha256=database.target_connection_full_digest_sha256,
        )

    @classmethod
    def _lock_database_scope(
        cls,
        db_session: DbSession,
        *,
        target_user_id: UUID,
        keeper_user_id: UUID,
        provider_user_id: str,
    ) -> _DatabaseSnapshot:
        acquire_provider_identity_value_locks(db_session, ((WHOOP, provider_user_id),))
        return cls._database_snapshot(
            db_session,
            target_user_id=target_user_id,
            keeper_user_id=keeper_user_id,
            for_update=True,
        )

    @classmethod
    def execute(
        cls,
        db_session: DbSession,
        *,
        target_user_id: UUID,
        keeper_user_id: UUID,
        expected_plan_sha256: str,
    ) -> FounderShadowWhoopCleanupVerification:
        planned = cls.plan(
            db_session,
            target_user_id=target_user_id,
            keeper_user_id=keeper_user_id,
        )
        if not planned.executable:
            raise FounderShadowWhoopCleanupError(*planned.blockers)
        if not hmac.compare_digest(planned.plan_digest_sha256, expected_plan_sha256):
            raise FounderShadowWhoopCleanupError("founder-shadow.plan-digest-mismatch")

        initial = cls._database_snapshot(
            db_session,
            target_user_id=target_user_id,
            keeper_user_id=keeper_user_id,
            for_update=False,
        )
        if initial.provider_user_id is None:
            raise FounderShadowWhoopCleanupError("founder-shadow.target-provider-identity-invalid")
        locked = cls._lock_database_scope(
            db_session,
            target_user_id=target_user_id,
            keeper_user_id=keeper_user_id,
            provider_user_id=initial.provider_user_id,
        )
        if (
            locked.phase != planned.phase
            or locked.blockers
            or not hmac.compare_digest(
                locked.database_digest_sha256,
                planned.database_digest_sha256,
            )
            or not hmac.compare_digest(
                locked.target_connection_full_digest_sha256,
                planned.execution_state_digest_sha256,
            )
        ):
            db_session.rollback()
            raise FounderShadowWhoopCleanupError(*(locked.blockers or ("founder-shadow.database-changed-after-plan",)))

        if locked.phase == "planned":
            target = db_session.query(User).filter(User.id == target_user_id).one()
            connection = db_session.query(UserConnection).filter(UserConnection.id == locked.target_connection_id).one()
            target.health_write_state = "fenced"
            connection.status = ConnectionStatus.REVOKED
            connection.access_token = None
            connection.refresh_token = None
            connection.token_expires_at = None
            connection.scope = None
        db_session.commit()

        external, external_digest, external_blockers = cls._stable_external_inventory(target_user_id=target_user_id)
        if external_blockers:
            raise FounderShadowWhoopCleanupError(*external_blockers)
        if not hmac.compare_digest(external_digest, planned.external_digest_sha256):
            raise FounderShadowWhoopCleanupError("founder-shadow.external-changed-after-plan")
        try:
            sdk_source_reset_external_planes.erase_objects(external.objects)
            sdk_source_reset_external_planes.erase_redis(external.redis_references, include_results=True)
        except RuntimeError as exc:
            raise FounderShadowWhoopCleanupError("founder-shadow.external-deletion-unavailable") from exc

        after_external, _external_digest, after_external_blockers = cls._stable_external_inventory(
            target_user_id=target_user_id
        )
        if after_external_blockers or after_external.objects or after_external.redis_references:
            raise FounderShadowWhoopCleanupError(
                *(after_external_blockers or ("founder-shadow.external-verification-failed",))
            )

        prepared = cls._database_snapshot(
            db_session,
            target_user_id=target_user_id,
            keeper_user_id=keeper_user_id,
            for_update=False,
        )
        if prepared.provider_user_id is None:
            raise FounderShadowWhoopCleanupError("founder-shadow.target-provider-identity-invalid")
        locked = cls._lock_database_scope(
            db_session,
            target_user_id=target_user_id,
            keeper_user_id=keeper_user_id,
            provider_user_id=prepared.provider_user_id,
        )
        if (
            locked.phase != "prepared"
            or locked.blockers
            or not hmac.compare_digest(
                locked.database_digest_sha256,
                planned.database_digest_sha256,
            )
        ):
            db_session.rollback()
            raise FounderShadowWhoopCleanupError(*(locked.blockers or ("founder-shadow.prepared-database-changed",)))

        source_ids = tuple(
            row[0]
            for row in db_session.query(DataSource.id)
            .filter(DataSource.user_id == target_user_id, DataSource.provider == ProviderName.WHOOP)
            .all()
        )
        event_ids = tuple(
            row[0]
            for row in db_session.query(EventRecord.id)
            .filter(EventRecord.data_source_id.in_(source_ids or (UUID(int=0),)))
            .all()
        )
        db_session.query(HealthScore).filter(
            HealthScore.user_id == target_user_id,
            HealthScore.provider == ProviderName.WHOOP,
        ).delete(synchronize_session=False)
        db_session.query(DataSource).filter(
            DataSource.user_id == target_user_id,
            DataSource.provider == ProviderName.WHOOP,
        ).delete(synchronize_session=False)
        db_session.flush()
        for model in (DataPointSeries, DataPointSeriesArchive, EventRecord):
            if source_ids and db_session.query(model).filter(model.data_source_id.in_(source_ids)).count():
                db_session.rollback()
                raise FounderShadowWhoopCleanupError("founder-shadow.normalized-cascade-incomplete")
        for model in DETAIL_MODELS.values():
            if event_ids and db_session.query(model).filter(model.record_id.in_(event_ids)).count():
                db_session.rollback()
                raise FounderShadowWhoopCleanupError("founder-shadow.detail-cascade-incomplete")
        db_session.query(UserConnection).filter(
            UserConnection.id == locked.target_connection_id,
            UserConnection.user_id == target_user_id,
            UserConnection.provider == WHOOP,
            UserConnection.status == ConnectionStatus.REVOKED,
            UserConnection.access_token.is_(None),
            UserConnection.refresh_token.is_(None),
        ).delete(synchronize_session=False)
        target = db_session.query(User).filter(User.id == target_user_id).one()
        target.health_write_state = "active"
        db_session.flush()
        if db_session.query(UserConnection).filter(UserConnection.user_id == target_user_id).count():
            db_session.rollback()
            raise FounderShadowWhoopCleanupError("founder-shadow.target-connection-delete-incomplete")
        if db_session.query(DataSource).filter(DataSource.user_id == target_user_id).count():
            db_session.rollback()
            raise FounderShadowWhoopCleanupError("founder-shadow.target-source-delete-incomplete")
        if db_session.query(HealthScore).filter(HealthScore.user_id == target_user_id).count():
            db_session.rollback()
            raise FounderShadowWhoopCleanupError("founder-shadow.target-score-delete-incomplete")
        if not hmac.compare_digest(
            cls._row_digest("target-user", target, excluded=frozenset({"health_write_state"})),
            locked.target_user_digest_sha256,
        ):
            db_session.rollback()
            raise FounderShadowWhoopCleanupError("founder-shadow.target-user-changed")
        keeper = db_session.query(User).filter(User.id == keeper_user_id).one()
        keeper_connections = tuple(
            db_session.query(UserConnection)
            .filter(UserConnection.user_id == keeper_user_id)
            .order_by(UserConnection.id)
            .populate_existing()
            .all()
        )
        keeper_user_unchanged = hmac.compare_digest(
            cls._row_digest("keeper-user", keeper),
            locked.keeper_user_digest_sha256,
        )
        keeper_connections_unchanged = hmac.compare_digest(
            cls._rows_digest("keeper-connections", keeper_connections),
            locked.keeper_connections_digest_sha256,
        )
        if not keeper_user_unchanged or not keeper_connections_unchanged:
            db_session.rollback()
            raise FounderShadowWhoopCleanupError("founder-shadow.keeper-changed")
        db_session.commit()
        return cls.verify(
            db_session,
            target_user_id=target_user_id,
            keeper_user_id=keeper_user_id,
        )

    @classmethod
    def verify(
        cls,
        db_session: DbSession,
        *,
        target_user_id: UUID,
        keeper_user_id: UUID,
    ) -> FounderShadowWhoopCleanupVerification:
        blockers: list[str] = []
        target = db_session.get(User, target_user_id)
        keeper = db_session.get(User, keeper_user_id)
        if target is None:
            blockers.append("founder-shadow.target-user-missing")
        elif target.health_write_state != "active":
            blockers.append("founder-shadow.target-user-not-active")
        if keeper is None:
            blockers.append("founder-shadow.keeper-user-missing")
        keeper_connections = (
            db_session.query(UserConnection)
            .filter(
                UserConnection.user_id == keeper_user_id,
                UserConnection.provider == WHOOP,
                UserConnection.status == ConnectionStatus.ACTIVE,
            )
            .count()
        )
        if keeper_connections != 1:
            blockers.append("founder-shadow.keeper-whoop-connection-not-active")
        counts = {
            "target_users": int(target is not None),
            "target_connections": db_session.query(UserConnection)
            .filter(UserConnection.user_id == target_user_id)
            .count(),
            "target_data_sources": db_session.query(DataSource).filter(DataSource.user_id == target_user_id).count(),
            "target_health_scores": db_session.query(HealthScore).filter(HealthScore.user_id == target_user_id).count(),
            "target_sdk_or_reset_rows": sum(cls._count(db_session, model, target_user_id) for model in _SDK_MODELS),
            "keeper_active_whoop_connections": keeper_connections,
        }
        if any(
            counts[key]
            for key in (
                "target_connections",
                "target_data_sources",
                "target_health_scores",
                "target_sdk_or_reset_rows",
            )
        ):
            blockers.append("founder-shadow.database-state-not-empty")

        external, external_digest, external_blockers = cls._stable_external_inventory(target_user_id=target_user_id)
        blockers.extend(external_blockers)
        counts["external_objects"] = len(external.objects)
        counts["external_redis_references"] = len(external.redis_references)
        counts["external_active_tasks"] = len(external.active_task_ids)
        if external.objects or external.redis_references or external.active_task_ids:
            blockers.append("founder-shadow.external-state-not-empty")
        normalized_blockers = tuple(sorted(set(blockers)))
        verification_digest = cls._keyed_digest(
            "verification",
            {
                "counts": counts,
                "blockers": list(normalized_blockers),
                "target_user": cls._row_digest("target-user-final", target),
                "keeper_user": cls._row_digest("keeper-user-final", keeper),
                "external": external_digest,
            },
        )
        return FounderShadowWhoopCleanupVerification(
            verified=not normalized_blockers,
            verification_digest_sha256=verification_digest,
            counts=counts,
            blockers=normalized_blockers,
        )


founder_shadow_whoop_cleanup_service = FounderShadowWhoopCleanupService()
