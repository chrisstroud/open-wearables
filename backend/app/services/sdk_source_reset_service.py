import hashlib
import hmac
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Literal, cast
from uuid import UUID

from fastapi import HTTPException, status
from sqlalchemy import update

from app.config import settings
from app.database import DbSession
from app.models import (
    DETAIL_MODELS,
    AppleHealthDailySummary,
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
from app.schemas.model_crud.credentials import SDKHealthResetStateRead, SDKHealthResetTransitionRequest
from app.services.provider_identity_authority import (
    ProviderIdentityFingerprint,
    acquire_provider_identity_locks,
    other_user_provider_identity_collisions,
)
from app.services.sdk_client_installation_service import sdk_client_installation_service
from app.services.sdk_source_reset_external import (
    FIT_OBJECTS,
    INTERNAL_LOCATOR_PROVIDER,
    QUEUED_TASKS,
    RAW_OBJECTS,
    REDIS_COORDINATION,
    RESULT_BACKEND,
    WEBHOOK_IDENTITY_PROVIDERS,
    ExternalResetInventory,
    ProviderIdentityScope,
    sdk_source_reset_external_planes,
)
from app.services.sdk_source_reset_provider_fence import sdk_source_reset_provider_fence

RESOURCE_KEYS = (
    "open-wearables.connections",
    "open-wearables.normalized-records",
    "open-wearables.sdk-batch-receipts",
    "open-wearables.sdk-window-receipts",
    "open-wearables.sleep-inbox",
    "open-wearables.invitations",
    "open-wearables.installations",
    "open-wearables.refresh-tokens",
    "open-wearables.source-mappings",
    QUEUED_TASKS,
    RESULT_BACKEND,
    RAW_OBJECTS,
    FIT_OBJECTS,
    "open-wearables.provider-credentials",
    REDIS_COORDINATION,
    "open-wearables.user-record",
)

_EXTERNAL_IDENTITY_PROOF_KEY = "__external_identity_hmac_sha256_v1"


@dataclass(frozen=True)
class ResetInventory:
    counts: dict[str, int]
    inventory_digest_sha256: str
    operational_digest_sha256: str
    blockers: tuple[str, ...]
    external: ExternalResetInventory
    queued_or_processing_upload_count: int
    pending_sleep_projection_count: int

    @property
    def drained(self) -> bool:
        return (
            self.queued_or_processing_upload_count == 0
            and self.pending_sleep_projection_count == 0
            and self.counts[QUEUED_TASKS] == 0
        )

    @property
    def verified_empty(self) -> bool:
        return not self.blockers and self.drained and all(self.counts[key] == 0 for key in RESOURCE_KEYS)


class SDKSourceResetService:
    """Fail-closed erasure of every user-bound health/provider plane while preserving User/profile."""

    @staticmethod
    def _hash_query(hasher: Any, resource_key: str, label: str, query: Any) -> int:
        count = int(query.count())
        column = query.column_descriptions[0]["expr"]
        for (value,) in query.order_by(column).yield_per(5000):
            hasher.update(resource_key.encode())
            hasher.update(b":")
            hasher.update(label.encode())
            hasher.update(b":")
            hasher.update(str(value).encode())
            hasher.update(b"\n")
        return count

    @staticmethod
    def _public_counts(counts: dict[str, Any] | None) -> dict[str, int]:
        source = counts or {}
        return {key: int(source.get(key, 0)) for key in RESOURCE_KEYS}

    @staticmethod
    def _keyed_inventory_token(label: str, payload: dict[str, Any]) -> str:
        canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
        return hmac.new(
            settings.secret_key.encode(),
            f"sdk-source-reset-inventory:v1:{label}\0{canonical}".encode(),
            hashlib.sha256,
        ).hexdigest()

    @staticmethod
    def _target_connection_identity_scope(
        db_session: DbSession,
        *,
        user_id: UUID,
    ) -> ProviderIdentityScope:
        provider_values: dict[str, set[str]] = {}
        incomplete_providers: set[str] = set()
        rows = (
            db_session.query(
                UserConnection.provider,
                UserConnection.provider_user_id,
                UserConnection.provider_username,
            )
            .filter(UserConnection.user_id == user_id)
            .all()
        )
        for raw_provider, provider_user_id, provider_username in rows:
            provider = str(raw_provider).strip().lower()
            normalized_provider_user_id = str(provider_user_id or "").strip()
            normalized_provider_username = str(provider_username or "").strip()
            if normalized_provider_user_id.lower() == "unknown":
                normalized_provider_user_id = ""
            if normalized_provider_username.lower() == "unknown":
                normalized_provider_username = ""
            values = {normalized_provider_user_id} if normalized_provider_user_id else set()
            if provider == "suunto" and normalized_provider_username:
                values.add(normalized_provider_username)
            if values:
                provider_values.setdefault(provider, set()).update(values)
            if (provider == "suunto" and not normalized_provider_username) or (
                provider in WEBHOOK_IDENTITY_PROVIDERS and provider != "suunto" and not normalized_provider_user_id
            ):
                incomplete_providers.add(provider)
        return ProviderIdentityScope.from_values(
            provider_values,
            incomplete_providers=incomplete_providers,
        )

    @staticmethod
    def _scope_lock_identities(scope: ProviderIdentityScope) -> tuple[ProviderIdentityFingerprint, ...]:
        return tuple(
            ProviderIdentityFingerprint(identity.provider, fingerprint)
            for identity in scope.identities
            if identity.provider != INTERNAL_LOCATOR_PROVIDER
            for fingerprint in identity.fingerprints
        )

    def _lock_and_require_exclusive_provider_identities(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        identity_scope: ProviderIdentityScope,
    ) -> None:
        identities = self._scope_lock_identities(identity_scope)
        acquire_provider_identity_locks(db_session, identities)
        collisions = other_user_provider_identity_collisions(
            db_session,
            identities=identities,
            exclude_user_id=user_id,
        )
        if collisions:
            db_session.rollback()
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Source reset provider identity became shared",
            )

    @staticmethod
    def _persisted_identity_scope(user: User) -> ProviderIdentityScope:
        deleted_state = cast(dict[str, Any], user.health_reset_deleted_counts or {})
        persisted_proof = deleted_state.get(_EXTERNAL_IDENTITY_PROOF_KEY)
        proof_required = (
            int((user.health_reset_manifest_counts or {}).get("open-wearables.connections", 0)) > 0
            or int((user.health_reset_manifest_counts or {}).get("open-wearables.sdk-batch-receipts", 0)) > 0
        )
        return ProviderIdentityScope.from_proof(persisted_proof, required=proof_required)

    def _provider_identity_scope(self, db_session: DbSession, *, user_id: UUID) -> ProviderIdentityScope:
        rows = (
            db_session.query(
                UserConnection.provider,
                UserConnection.provider_user_id,
                UserConnection.provider_username,
            )
            .filter(UserConnection.user_id == user_id)
            .all()
        )
        provider_values: dict[str, set[str]] = {}
        incomplete_providers: set[str] = set()
        for raw_provider, provider_user_id, provider_username in rows:
            provider = str(raw_provider).strip().lower()
            values: set[str] = set()
            normalized_provider_user_id = str(provider_user_id or "").strip()
            normalized_provider_username = str(provider_username or "").strip()
            if normalized_provider_user_id.lower() == "unknown":
                normalized_provider_user_id = ""
            if normalized_provider_username.lower() == "unknown":
                normalized_provider_username = ""
            if normalized_provider_user_id:
                values.add(normalized_provider_user_id)
            # Suunto webhooks resolve by username.  Other webhook handlers use
            # provider_user_id, so unrelated display usernames must not widen
            # deletion scope and risk removing another user's raw object.
            if provider == "suunto" and normalized_provider_username:
                values.add(normalized_provider_username)
            if values:
                provider_values.setdefault(provider, set()).update(values)
            required_identity_missing = (provider == "suunto" and not normalized_provider_username) or (
                provider in WEBHOOK_IDENTITY_PROVIDERS and provider != "suunto" and not normalized_provider_user_id
            )
            if required_identity_missing:
                incomplete_providers.add(provider)

        ambiguous_providers: set[str] = set()
        if provider_values:
            linked_rows = (
                db_session.query(
                    UserConnection.provider,
                    UserConnection.provider_user_id,
                    UserConnection.provider_username,
                )
                .filter(
                    UserConnection.user_id != user_id,
                    UserConnection.provider.in_(tuple(provider_values)),
                )
                .all()
            )
            for raw_provider, provider_user_id, provider_username in linked_rows:
                provider = str(raw_provider).strip().lower()
                linked_values = {
                    str(provider_user_id or "").strip(),
                    str(provider_username or "").strip() if provider == "suunto" else "",
                }
                linked_values = {value for value in linked_values if value and value.lower() != "unknown"}
                if linked_values.intersection(provider_values.get(provider, set())):
                    ambiguous_providers.add(provider)

        # Durable v2 Celery envelopes and result rows intentionally carry a
        # batch UUID instead of a user UUID or health payload. Bind those
        # account-owned locators into the same keyed proof so reset can find
        # queued/running/result state before and after the database rows vanish.
        batch_ids = db_session.query(SDKBatchReceipt.id).filter(SDKBatchReceipt.user_id == user_id).all()
        if batch_ids:
            provider_values[INTERNAL_LOCATOR_PROVIDER] = {str(batch_id) for (batch_id,) in batch_ids}

        current = ProviderIdentityScope.from_values(
            provider_values,
            incomplete_providers=incomplete_providers,
            ambiguous_providers=ambiguous_providers,
        )
        user = db_session.get(User, user_id)
        if user is None:
            return current
        deleted_state = cast(dict[str, Any], user.health_reset_deleted_counts or {})
        persisted_proof = deleted_state.get(_EXTERNAL_IDENTITY_PROOF_KEY)
        manifest_connections = int((user.health_reset_manifest_counts or {}).get("open-wearables.connections", 0))
        manifest_batches = int((user.health_reset_manifest_counts or {}).get("open-wearables.sdk-batch-receipts", 0))
        proof_required = (
            (manifest_connections > 0 or manifest_batches > 0)
            and user.health_reset_operation_id is not None
            and user.health_reset_applied_at is not None
        )
        persisted = ProviderIdentityScope.from_proof(persisted_proof, required=proof_required)
        return current.merge(persisted)

    def _database_inventory(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
    ) -> tuple[dict[str, int], dict[str, tuple[str, ...]], int, int]:
        source_ids = db_session.query(DataSource.id).filter(DataSource.user_id == user_id)
        data_points = (
            db_session.query(DataPointSeries.id)
            .join(DataSource, DataPointSeries.data_source_id == DataSource.id)
            .filter(DataSource.user_id == user_id)
        )
        archive_points = (
            db_session.query(DataPointSeriesArchive.id)
            .join(DataSource, DataPointSeriesArchive.data_source_id == DataSource.id)
            .filter(DataSource.user_id == user_id)
        )
        events = (
            db_session.query(EventRecord.id)
            .join(DataSource, EventRecord.data_source_id == DataSource.id)
            .filter(DataSource.user_id == user_id)
        )
        health_scores = db_session.query(HealthScore.id).filter(HealthScore.user_id == user_id)
        daily_summaries = db_session.query(AppleHealthDailySummary.id).filter(
            AppleHealthDailySummary.user_id == user_id
        )
        normalized_queries: list[tuple[str, Any]] = [
            ("data-point-series", data_points),
            ("data-point-series-archive", archive_points),
            ("event-record", events),
            ("health-score", health_scores),
            ("apple-health-daily-summary", daily_summaries),
        ]
        for detail_type, model in sorted(DETAIL_MODELS.items()):
            normalized_queries.append(
                (
                    f"{detail_type}-details",
                    db_session.query(model.record_id)
                    .join(EventRecord, model.record_id == EventRecord.id)
                    .join(DataSource, EventRecord.data_source_id == DataSource.id)
                    .filter(DataSource.user_id == user_id),
                )
            )

        receipts = db_session.query(SDKBatchReceipt.id).filter(SDKBatchReceipt.user_id == user_id)
        upload_inbox = db_session.query(SDKUploadInbox.id).filter(SDKUploadInbox.user_id == user_id)
        windows = db_session.query(SDKSyncWindowReceipt.id).filter(SDKSyncWindowReceipt.user_id == user_id)
        sleep_inbox = db_session.query(SDKSleepInbox.id).filter(SDKSleepInbox.user_id == user_id)
        invitations = db_session.query(UserInvitationCode.id).filter(UserInvitationCode.user_id == user_id)
        installations = db_session.query(SDKClientInstallation.id).filter(SDKClientInstallation.user_id == user_id)
        refresh_tokens = db_session.query(RefreshToken.id).filter(RefreshToken.user_id == user_id)
        connection_rows = db_session.query(UserConnection).filter(UserConnection.user_id == user_id).all()
        pending_upload_count = receipts.filter(SDKBatchReceipt.status.in_(("queued", "processing"))).count()
        pending_sleep_count = sleep_inbox.filter(
            SDKSleepInbox.status.in_(("staged", "projecting", "projected"))
        ).count()

        identity_tokens: dict[str, list[str]] = {key: [] for key in RESOURCE_KEYS}
        counts = {key: 0 for key in RESOURCE_KEYS}

        def accumulate(resource_key: str, label: str, query: Any) -> None:
            local = hashlib.sha256()
            count = self._hash_query(local, resource_key, label, query)
            counts[resource_key] += count
            if count:
                token = local.hexdigest()
                identity_tokens[resource_key].append(token)

        def accumulate_connection_projections() -> None:
            counts["open-wearables.connections"] = len(connection_rows)
            credential_count = 0
            for connection in connection_rows:
                connection_payload = {
                    "id": connection.id,
                    "user_id": connection.user_id,
                    "provider": connection.provider,
                    "provider_user_id": connection.provider_user_id,
                    "provider_username": connection.provider_username,
                    "access_token": connection.access_token,
                    "refresh_token": connection.refresh_token,
                    "token_expires_at": connection.token_expires_at,
                    "scope": connection.scope,
                    "status": connection.status,
                    "last_synced_at": connection.last_synced_at,
                    "updated_at": connection.updated_at,
                    "created_at": connection.created_at,
                }
                identity_tokens["open-wearables.connections"].append(
                    self._keyed_inventory_token("connection", connection_payload)
                )
                credential_payload = {
                    key: connection_payload[key]
                    for key in (
                        "id",
                        "provider",
                        "provider_user_id",
                        "provider_username",
                        "access_token",
                        "refresh_token",
                        "token_expires_at",
                        "scope",
                    )
                }
                if any(value is not None for key, value in credential_payload.items() if key not in {"id", "provider"}):
                    credential_count += 1
                    identity_tokens["open-wearables.provider-credentials"].append(
                        self._keyed_inventory_token("provider-credential", credential_payload)
                    )
            counts["open-wearables.provider-credentials"] = credential_count

        accumulate("open-wearables.source-mappings", "data-source", source_ids)
        for label, query in normalized_queries:
            accumulate("open-wearables.normalized-records", label, query)
        accumulate("open-wearables.sdk-batch-receipts", "batch-receipt", receipts)
        accumulate("open-wearables.sdk-batch-receipts", "upload-inbox", upload_inbox)
        accumulate("open-wearables.sdk-window-receipts", "window-receipt", windows)
        accumulate("open-wearables.sleep-inbox", "sleep-inbox", sleep_inbox)
        accumulate("open-wearables.invitations", "invitation", invitations)
        accumulate("open-wearables.installations", "installation", installations)
        accumulate("open-wearables.refresh-tokens", "refresh-token", refresh_tokens)
        accumulate_connection_projections()
        # The preserved User/profile identity is explicitly outside deletion scope.
        # This key represents deletable health payload on the User row; this schema has none.
        counts["open-wearables.user-record"] = 0

        return (
            counts,
            {key: tuple(sorted(values)) for key, values in identity_tokens.items()},
            int(pending_upload_count),
            int(pending_sleep_count),
        )

    def inventory(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        identity_scope: ProviderIdentityScope | None = None,
    ) -> ResetInventory:
        counts, identity_tokens, pending_uploads, pending_sleep = self._database_inventory(
            db_session,
            user_id=user_id,
        )
        scope = identity_scope or self._provider_identity_scope(db_session, user_id=user_id)
        identity_tokens["open-wearables.provider-credentials"] = tuple(
            sorted(
                (
                    *identity_tokens["open-wearables.provider-credentials"],
                    *scope.manifest_tokens(),
                )
            )
        )
        external = sdk_source_reset_external_planes.inventory(user_id, identity_scope=scope)
        for key, count in external.counts.items():
            counts[key] = count
        for key, tokens in external.identity_tokens.items():
            identity_tokens[key] = tokens

        canonical = {
            "user_id": str(user_id),
            "resources": [
                {
                    "resource_key": key,
                    "count": counts[key],
                    "identity_tokens": list(identity_tokens[key]),
                }
                for key in RESOURCE_KEYS
            ],
            "blockers": list(external.blockers),
        }
        inventory_digest = hashlib.sha256(
            json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        operational = {
            "queued_or_processing_upload_count": pending_uploads,
            "pending_sleep_projection_count": pending_sleep,
            "queued_task_count": counts[QUEUED_TASKS],
        }
        operational_digest = hashlib.sha256(
            json.dumps(operational, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        return ResetInventory(
            counts={key: counts[key] for key in RESOURCE_KEYS},
            inventory_digest_sha256=inventory_digest,
            operational_digest_sha256=operational_digest,
            blockers=external.blockers,
            external=external,
            queued_or_processing_upload_count=pending_uploads,
            pending_sleep_projection_count=pending_sleep,
        )

    @staticmethod
    def _response(
        db_session: DbSession,
        user: User,
        inventory: ResetInventory,
        *,
        resource_counts: dict[str, int] | None = None,
        inventory_digest_sha256: str | None = None,
        verified_empty: bool | None = None,
    ) -> SDKHealthResetStateRead:
        active = sdk_client_installation_service.crud.get_active_for_user(db_session, user.id)
        public_counts = SDKSourceResetService._public_counts(
            resource_counts if resource_counts is not None else inventory.counts
        )
        return SDKHealthResetStateRead(
            user_id=user.id,
            operation_id=user.health_reset_operation_id,
            health_evidence_generation=user.health_evidence_generation,
            health_write_state=cast(
                Literal["active", "fenced", "awaiting-v2-pairing", "activating"],
                user.health_write_state,
            ),
            health_source_policy=cast(
                Literal["legacy-mixed", "apple-mobile-v2-only"],
                user.health_source_policy,
            ),
            active_installation_id=active.id if active is not None else None,
            active_installation_generation=active.generation if active is not None else None,
            queued_or_processing_upload_count=inventory.queued_or_processing_upload_count,
            pending_sleep_projection_count=inventory.pending_sleep_projection_count,
            drained=inventory.drained,
            operational_digest_sha256=inventory.operational_digest_sha256,
            resource_counts=public_counts,
            inventory_digest_sha256=inventory_digest_sha256 or inventory.inventory_digest_sha256,
            blockers=list(inventory.blockers),
            verified_empty=inventory.verified_empty if verified_empty is None else verified_empty,
        )

    @staticmethod
    def _require_user(db_session: DbSession, user_id: UUID, *, for_update: bool) -> User:
        query = db_session.query(User).filter(User.id == user_id)
        if for_update:
            query = query.populate_existing().with_for_update()
        user = query.one_or_none()
        if user is None:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="User not found")
        return user

    def inspect(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        request: SDKHealthResetTransitionRequest,
    ) -> SDKHealthResetStateRead:
        user = self._require_user(db_session, user_id, for_update=False)
        same_applied_operation = (
            user.health_reset_operation_id == request.operation_id
            and user.health_write_state in {"fenced", "awaiting-v2-pairing"}
            and user.health_evidence_generation == request.expected_health_evidence_generation + 1
            and user.health_reset_applied_at is not None
        )
        if (
            user.health_evidence_generation != request.expected_health_evidence_generation
            and not same_applied_operation
        ):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Health evidence generation changed")
        current = self.inventory(db_session, user_id=user_id)
        if (
            user.health_reset_operation_id == request.operation_id
            and user.health_reset_manifest_sha256 is not None
            and user.health_reset_manifest_counts is not None
        ):
            return self._response(
                db_session,
                user,
                current,
                resource_counts=user.health_reset_manifest_counts,
                inventory_digest_sha256=user.health_reset_manifest_sha256,
            )
        return self._response(db_session, user, current)

    def fence(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        request: SDKHealthResetTransitionRequest,
    ) -> SDKHealthResetStateRead:
        user = self._require_user(db_session, user_id, for_update=True)
        resuming = (
            user.health_write_state == "fenced"
            and user.health_reset_operation_id == request.operation_id
            and user.health_evidence_generation == request.expected_health_evidence_generation
            and user.health_reset_manifest_sha256 == request.expected_inventory_digest_sha256
        )

        if not resuming:
            identity_scope = self._provider_identity_scope(db_session, user_id=user_id)
            inventory = self.inventory(db_session, user_id=user_id, identity_scope=identity_scope)
            if inventory.blockers:
                raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=list(inventory.blockers))
            if request.expected_inventory_digest_sha256 != inventory.inventory_digest_sha256:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Source reset inventory changed")
            active = sdk_client_installation_service.crud.get_active_for_user(db_session, user_id)
            if active is None:
                if request.expected_installation_generation is not None:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail="Mobile installation generation changed",
                    )
            elif request.expected_installation_generation != active.generation:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Mobile installation generation changed",
                )

            user = sdk_client_installation_service.fence_for_reset(
                db_session,
                user_id=user_id,
                operation_id=request.operation_id,
                expected_health_evidence_generation=request.expected_health_evidence_generation,
                commit=False,
            )
            user.health_reset_manifest_sha256 = inventory.inventory_digest_sha256
            user.health_reset_manifest_counts = inventory.counts
            user.health_reset_deleted_counts = cast(
                dict[str, int],
                {_EXTERNAL_IDENTITY_PROOF_KEY: identity_scope.to_proof()},
            )

            now = datetime.now(timezone.utc)
            pending_ids = [
                row[0]
                for row in db_session.query(SDKBatchReceipt.id)
                .filter(
                    SDKBatchReceipt.user_id == user_id,
                    SDKBatchReceipt.status.in_(("queued", "processing")),
                )
                .all()
            ]
            if pending_ids:
                db_session.query(SDKUploadInbox).filter(SDKUploadInbox.id.in_(pending_ids)).delete(
                    synchronize_session=False
                )
                db_session.execute(
                    update(SDKBatchReceipt)
                    .where(SDKBatchReceipt.id.in_(pending_ids))
                    .values(
                        status="failed",
                        retryable=False,
                        error_code="source_reset_fenced",
                        updated_at=now,
                        completed_at=now,
                    )
                )
            db_session.execute(
                update(SDKSleepInbox)
                .where(
                    SDKSleepInbox.user_id == user_id,
                    SDKSleepInbox.status.in_(("staged", "projecting", "projected")),
                )
                .values(status="quarantined", last_error="source_reset_fenced", updated_at=now)
            )
            # Commit the durable account fence before any irreversible provider
            # or queue call. A crash from this point is a resumable fenced saga.
            db_session.commit()
        else:
            # A resumed request entered with the User row locked. Release that
            # validation lock before taking provider-identity locks so every
            # path follows the canonical identity -> User order used by
            # connection writers, webhooks, and apply.
            db_session.commit()

        identity_scope = self._target_connection_identity_scope(db_session, user_id=user_id)
        self._lock_and_require_exclusive_provider_identities(
            db_session,
            user_id=user_id,
            identity_scope=identity_scope,
        )
        user = self._require_user(db_session, user_id, for_update=True)
        connections = (
            db_session.query(UserConnection)
            .filter(UserConnection.user_id == user_id)
            .order_by(UserConnection.id)
            .populate_existing()
            .all()
        )
        try:
            sdk_source_reset_provider_fence.deregister(connections)
        except RuntimeError as exc:
            db_session.rollback()
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Source reset provider deregistration is unavailable",
            ) from exc
        now = datetime.now(timezone.utc)
        db_session.execute(
            update(UserConnection)
            .where(UserConnection.user_id == user_id)
            .values(
                status="revoked",
                access_token=None,
                refresh_token=None,
                token_expires_at=None,
                updated_at=now,
            )
        )
        db_session.commit()

        # Token clearing committed and released the first identity lock set.
        # Reacquire the immutable proof identities before inspecting or
        # deleting shared queue state, then revalidate the fenced operation
        # under the User lock. These locks remain held through cleanup and the
        # post-delete inventory.
        post_commit_user = self._require_user(db_session, user_id, for_update=False)
        identity_scope = self._persisted_identity_scope(post_commit_user)
        self._lock_and_require_exclusive_provider_identities(
            db_session,
            user_id=user_id,
            identity_scope=identity_scope,
        )
        user = self._require_user(db_session, user_id, for_update=True)
        if (
            user.health_write_state != "fenced"
            or user.health_reset_operation_id != request.operation_id
            or user.health_evidence_generation != request.expected_health_evidence_generation
            or user.health_reset_manifest_sha256 != request.expected_inventory_digest_sha256
        ):
            db_session.rollback()
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Health reset authority changed")

        inventory = self.inventory(db_session, user_id=user_id, identity_scope=identity_scope)
        if inventory.blockers:
            db_session.rollback()
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=list(inventory.blockers))
        try:
            sdk_source_reset_external_planes.revoke_tasks(inventory.external.active_task_ids)
            sdk_source_reset_external_planes.erase_redis(
                inventory.external.redis_references,
                include_results=False,
            )
        except RuntimeError as exc:
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Source reset queue fencing is unavailable",
            ) from exc
        inventory = self.inventory(db_session, user_id=user_id, identity_scope=identity_scope)
        db_session.refresh(user)
        response = self._response(
            db_session,
            user,
            inventory,
            resource_counts=user.health_reset_manifest_counts,
            inventory_digest_sha256=user.health_reset_manifest_sha256,
        )
        db_session.commit()
        return response

    def drain(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        request: SDKHealthResetTransitionRequest,
    ) -> SDKHealthResetStateRead:
        user = self._require_user(db_session, user_id, for_update=True)
        if (
            user.health_reset_operation_id != request.operation_id
            or user.health_write_state != "fenced"
            or user.health_evidence_generation != request.expected_health_evidence_generation
            or user.health_reset_manifest_sha256 != request.expected_inventory_digest_sha256
        ):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Health reset is not fenced")
        inventory = self.inventory(db_session, user_id=user_id)
        if inventory.blockers:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=list(inventory.blockers))
        seal = (
            db_session.query(SDKSourceResetSeal)
            .filter(
                SDKSourceResetSeal.operation_id == request.operation_id,
                SDKSourceResetSeal.user_id == user_id,
            )
            .one_or_none()
        )
        if inventory.drained:
            if seal is None:
                db_session.add(
                    SDKSourceResetSeal(
                        operation_id=request.operation_id,
                        user_id=user_id,
                        health_evidence_generation=user.health_evidence_generation,
                        inventory_digest_sha256=inventory.inventory_digest_sha256,
                        configuration_digest_sha256=inventory.external.configuration_digest_sha256,
                        resource_counts=inventory.counts,
                    )
                )
                db_session.commit()
                db_session.refresh(user)
            elif (
                seal.health_evidence_generation != user.health_evidence_generation
                or seal.inventory_digest_sha256 != inventory.inventory_digest_sha256
                or seal.configuration_digest_sha256 != inventory.external.configuration_digest_sha256
                or self._public_counts(seal.resource_counts) != inventory.counts
            ):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Source reset drained inventory changed",
                )
        return self._response(
            db_session,
            user,
            inventory,
            resource_counts=user.health_reset_manifest_counts,
            inventory_digest_sha256=user.health_reset_manifest_sha256,
        )

    @staticmethod
    def _delete_database_state(db_session: DbSession, user_id: UUID) -> None:
        db_session.query(SDKUploadInbox).filter(SDKUploadInbox.user_id == user_id).delete(synchronize_session=False)
        db_session.query(SDKSyncWindowReceipt).filter(SDKSyncWindowReceipt.user_id == user_id).delete(
            synchronize_session=False
        )
        db_session.query(SDKSleepInbox).filter(SDKSleepInbox.user_id == user_id).delete(synchronize_session=False)
        # Both summary parents use restrictive foreign keys, so erase child rows first.
        db_session.query(AppleHealthDailySummary).filter(AppleHealthDailySummary.user_id == user_id).delete(
            synchronize_session=False
        )
        db_session.query(SDKBatchReceipt).filter(SDKBatchReceipt.user_id == user_id).delete(synchronize_session=False)
        db_session.query(RefreshToken).filter(RefreshToken.user_id == user_id).delete(synchronize_session=False)
        db_session.query(UserInvitationCode).filter(UserInvitationCode.user_id == user_id).delete(
            synchronize_session=False
        )
        db_session.query(SDKClientInstallation).filter(SDKClientInstallation.user_id == user_id).delete(
            synchronize_session=False
        )
        db_session.query(HealthScore).filter(HealthScore.user_id == user_id).delete(synchronize_session=False)
        db_session.query(DataSource).filter(DataSource.user_id == user_id).delete(synchronize_session=False)
        db_session.query(UserConnection).filter(UserConnection.user_id == user_id).delete(synchronize_session=False)

    def apply(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        request: SDKHealthResetTransitionRequest,
    ) -> SDKHealthResetStateRead:
        observed_user = self._require_user(db_session, user_id, for_update=False)
        observed_database_applied = (
            observed_user.health_reset_operation_id == request.operation_id
            and observed_user.health_evidence_generation == request.expected_health_evidence_generation + 1
            and observed_user.health_write_state in {"fenced", "awaiting-v2-pairing"}
            and observed_user.health_reset_manifest_sha256 == request.expected_inventory_digest_sha256
            and observed_user.health_reset_applied_at is not None
        )
        identity_scope = (
            self._persisted_identity_scope(observed_user)
            if observed_database_applied
            else self._target_connection_identity_scope(db_session, user_id=user_id)
        )
        self._lock_and_require_exclusive_provider_identities(
            db_session,
            user_id=user_id,
            identity_scope=identity_scope,
        )
        user = self._require_user(db_session, user_id, for_update=True)
        database_applied = (
            user.health_reset_operation_id == request.operation_id
            and user.health_evidence_generation == request.expected_health_evidence_generation + 1
            and user.health_write_state in {"fenced", "awaiting-v2-pairing"}
            and user.health_reset_manifest_sha256 == request.expected_inventory_digest_sha256
            and user.health_reset_applied_at is not None
        )
        if not database_applied and (
            user.health_reset_operation_id != request.operation_id
            or user.health_write_state != "fenced"
            or user.health_evidence_generation != request.expected_health_evidence_generation
            or user.health_reset_manifest_sha256 != request.expected_inventory_digest_sha256
            or user.health_reset_manifest_counts is None
        ):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Health reset authority changed")
        if database_applied != observed_database_applied:
            db_session.rollback()
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Health reset authority changed")

        if not database_applied:
            # Identity advisory locks are already held for every connection
            # value; now enrich the scope with durable internal batch locators.
            identity_scope = self._provider_identity_scope(db_session, user_id=user_id)
            inventory = self.inventory(db_session, user_id=user_id, identity_scope=identity_scope)
            if inventory.blockers:
                raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=list(inventory.blockers))
            if not inventory.drained:
                raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Health writers have not drained")

            seal = (
                db_session.query(SDKSourceResetSeal)
                .filter(
                    SDKSourceResetSeal.operation_id == request.operation_id,
                    SDKSourceResetSeal.user_id == user_id,
                )
                .one_or_none()
            )
            if seal is None:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Source reset drain inventory is not sealed",
                )
            if (
                seal.health_evidence_generation != user.health_evidence_generation
                or seal.inventory_digest_sha256 != inventory.inventory_digest_sha256
                or seal.configuration_digest_sha256 != inventory.external.configuration_digest_sha256
                or self._public_counts(seal.resource_counts) != inventory.counts
            ):
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail="Source reset drained inventory changed",
                )

            # Commit the database half of the saga first. The persisted keyed
            # identity proof is sufficient to rediscover every external object,
            # queue entry and result after connection/receipt rows are gone.
            self._delete_database_state(db_session, user_id)
            user.health_evidence_generation += 1
            user.health_source_policy = "apple-mobile-v2-only"
            # Keep the durable account fence closed until every external plane
            # is verified empty. ``health_reset_applied_at`` plus the advanced
            # generation is the resumable database-half receipt.
            user.health_write_state = "fenced"
            user.health_reset_applied_at = datetime.now(timezone.utc)
            deleted_state: dict[str, Any] = self._public_counts(user.health_reset_manifest_counts)
            deleted_state[_EXTERNAL_IDENTITY_PROOF_KEY] = identity_scope.to_proof()
            user.health_reset_deleted_counts = cast(dict[str, int], deleted_state)
            db_session.commit()
            user = self._require_user(db_session, user_id, for_update=False)
            identity_scope = self._persisted_identity_scope(user)
            self._lock_and_require_exclusive_provider_identities(
                db_session,
                user_id=user_id,
                identity_scope=identity_scope,
            )

        # Initial apply committed the database half of the saga above, while
        # retry apply already holds this row from its authority check. In both
        # cases, hold the account lock through external cleanup so pairing or
        # activation cannot create new generation-bound state that cleanup
        # could erase.
        user = self._require_user(db_session, user_id, for_update=True)
        if (
            user.health_reset_operation_id != request.operation_id
            or user.health_evidence_generation != request.expected_health_evidence_generation + 1
            or user.health_write_state not in {"fenced", "awaiting-v2-pairing"}
            or user.health_reset_manifest_sha256 != request.expected_inventory_digest_sha256
            or user.health_reset_applied_at is None
        ):
            db_session.rollback()
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Health reset authority changed")

        remaining = self.inventory(db_session, user_id=user_id, identity_scope=identity_scope)
        if remaining.blockers:
            db_session.rollback()
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=list(remaining.blockers))
        seal = (
            db_session.query(SDKSourceResetSeal)
            .filter(
                SDKSourceResetSeal.operation_id == request.operation_id,
                SDKSourceResetSeal.user_id == user_id,
            )
            .one_or_none()
        )
        if seal is None or seal.configuration_digest_sha256 != remaining.external.configuration_digest_sha256:
            db_session.rollback()
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="Source reset external configuration changed",
            )
        try:
            sdk_source_reset_external_planes.revoke_tasks(remaining.external.active_task_ids)
            sdk_source_reset_external_planes.erase_objects(remaining.external.objects)
            sdk_source_reset_external_planes.erase_redis(
                remaining.external.redis_references,
                include_results=True,
            )
        except RuntimeError as exc:
            db_session.rollback()
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Source reset external deletion is unavailable",
            ) from exc

        verified = self.inventory(db_session, user_id=user_id, identity_scope=identity_scope)
        if not verified.verified_empty:
            db_session.rollback()
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail={
                    "error_code": "source_reset_verification_failed",
                    "blockers": list(verified.blockers),
                },
            )
        deleted_counts = self._public_counts(user.health_reset_deleted_counts)
        user.health_write_state = "awaiting-v2-pairing"
        response = self._response(
            db_session,
            user,
            verified,
            resource_counts=deleted_counts,
            inventory_digest_sha256=user.health_reset_manifest_sha256,
            verified_empty=True,
        )
        db_session.commit()
        return response

    def verify(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        request: SDKHealthResetTransitionRequest,
    ) -> SDKHealthResetStateRead:
        user = self._require_user(db_session, user_id, for_update=False)
        if (
            user.health_reset_operation_id != request.operation_id
            or user.health_reset_manifest_sha256 != request.expected_inventory_digest_sha256
        ):
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Health reset operation changed")
        if user.health_evidence_generation != request.expected_health_evidence_generation + 1:
            raise HTTPException(status_code=status.HTTP_409_CONFLICT, detail="Health reset generation was not advanced")
        return self._response(
            db_session,
            user,
            self.inventory(db_session, user_id=user_id),
            inventory_digest_sha256=user.health_reset_manifest_sha256,
        )


sdk_source_reset_service = SDKSourceResetService()
