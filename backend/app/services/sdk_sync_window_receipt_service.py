import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from pydantic import ValidationError

from app.constants.sdk_history import (
    DASHBOARD_FITNESS_APPLE_HEALTH_V1_TYPES,
    DASHBOARD_FITNESS_COVERAGE_POLICY_VERSION,
)
from app.database import DbSession
from app.models import SDKSyncWindowReceipt
from app.repositories import (
    SDKBatchReceiptRepository,
    SDKSleepInboxRepository,
    SDKSyncWindowReceiptRepository,
    sdk_client_installation_repository,
)
from app.schemas.model_crud.credentials import UserInvitationActivationPolicy
from app.schemas.providers.mobile_sdk import SyncWindowManifest
from app.schemas.responses.upload import SDKBatchReceiptStatus, SDKSyncWindowReceiptResponse


@dataclass(frozen=True)
class WindowAcceptance:
    accepted: bool
    error_code: str | None = None
    receipt: SDKSyncWindowReceipt | None = None


class SDKSyncWindowReceiptService:
    def __init__(self) -> None:
        self.receipts = SDKSyncWindowReceiptRepository()
        self.batches = SDKBatchReceiptRepository()
        self.sleep_inbox = SDKSleepInboxRepository()

    @staticmethod
    def _canonical_manifest(manifest: SyncWindowManifest) -> dict:
        def iso_utc(value: datetime | None) -> str | None:
            if value is None:
                return None
            return value.astimezone(timezone.utc).isoformat()

        return {
            "windowId": str(manifest.windowId),
            "purpose": manifest.purpose,
            "windowVersion": manifest.windowVersion,
            "lowerBoundInclusive": iso_utc(manifest.lowerBoundInclusive),
            "upperBoundExclusive": iso_utc(manifest.upperBoundExclusive),
            "batchIds": sorted({str(batch_id) for batch_id in manifest.batchIds}),
            "emptyOrNoAccessTypes": sorted(set(manifest.emptyOrNoAccessTypes)),
            "reconciliationStartInclusive": iso_utc(manifest.reconciliationStartInclusive),
            "reconciliationEndExclusive": iso_utc(manifest.reconciliationEndExclusive),
        }

    @classmethod
    def manifest_sha256(cls, manifest: SyncWindowManifest) -> str:
        encoded = json.dumps(cls._canonical_manifest(manifest), separators=(",", ":"), sort_keys=True).encode()
        return hashlib.sha256(encoded).hexdigest()

    @staticmethod
    def _activation_policy(raw_policy: object) -> UserInvitationActivationPolicy | None:
        if not isinstance(raw_policy, dict):
            return None
        if raw_policy.get("coverage_policy_version") != DASHBOARD_FITNESS_COVERAGE_POLICY_VERSION:
            return None
        required_types = raw_policy.get("required_type_identifiers")
        if (
            not isinstance(required_types, list)
            or len(required_types) != len(DASHBOARD_FITNESS_APPLE_HEALTH_V1_TYPES)
            or set(required_types) != DASHBOARD_FITNESS_APPLE_HEALTH_V1_TYPES
        ):
            return None
        policy_fields = {
            key: raw_policy.get(key)
            for key in (
                "purpose",
                "window_version",
                "lower_bound_inclusive",
                "upper_bound_exclusive",
                "timezone",
                "completed_day_count",
            )
        }
        try:
            return UserInvitationActivationPolicy.model_validate(policy_fields)
        except ValidationError:
            return None

    @staticmethod
    def _same_instant(first: datetime, second: datetime) -> bool:
        return first.astimezone(timezone.utc) == second.astimezone(timezone.utc)

    def accept(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        provider: str,
        terminal_batch_id: UUID,
        manifest: SyncWindowManifest,
    ) -> WindowAcceptance:
        if manifest.windowId != terminal_batch_id:
            return WindowAcceptance(False, "window_id_mismatch")

        canonical = self._canonical_manifest(manifest)
        referenced_ids = {UUID(value) for value in canonical["batchIds"]}
        if terminal_batch_id in referenced_ids:
            return WindowAcceptance(False, "window_self_reference")

        manifest_hash = self.manifest_sha256(manifest)
        existing = self.receipts.get(db_session, manifest.windowId, for_update=True)
        if existing is not None:
            if (
                existing.user_id != user_id
                or existing.provider != provider
                or existing.manifest_sha256 != manifest_hash
            ):
                return WindowAcceptance(False, "window_manifest_conflict")
            return WindowAcceptance(True, receipt=existing)

        terminal_batch = self.batches.get(db_session, terminal_batch_id)
        if terminal_batch is None or terminal_batch.user_id != user_id or terminal_batch.provider != provider:
            return WindowAcceptance(False, "window_batch_not_accepted")

        installation = None
        activation_policy = None
        if terminal_batch.installation_id is not None:
            installation = sdk_client_installation_repository.get_for_update(
                db_session,
                terminal_batch.installation_id,
            )
            if (
                installation is None
                or installation.user_id != user_id
                or installation.generation != terminal_batch.installation_generation
                or installation.health_evidence_generation != terminal_batch.health_evidence_generation
            ):
                return WindowAcceptance(False, "window_installation_generation_mismatch")
            if manifest.purpose in {"activation", "archive"}:
                if provider != "apple":
                    return WindowAcceptance(False, "window_provider_mismatch")
                activation_policy = self._activation_policy(installation.activation_policy)
                if activation_policy is None:
                    return WindowAcceptance(False, "activation_policy_required")
                if manifest.purpose == "activation" and (
                    not self._same_instant(manifest.lowerBoundInclusive, activation_policy.lower_bound_inclusive)
                    or not self._same_instant(manifest.upperBoundExclusive, activation_policy.upper_bound_exclusive)
                ):
                    return WindowAcceptance(False, "activation_policy_mismatch")
                if manifest.purpose == "activation":
                    previous_activation = (
                        db_session.query(SDKSyncWindowReceipt.id)
                        .filter(
                            SDKSyncWindowReceipt.user_id == user_id,
                            SDKSyncWindowReceipt.installation_id == installation.id,
                            SDKSyncWindowReceipt.installation_generation == installation.generation,
                            SDKSyncWindowReceipt.health_evidence_generation == installation.health_evidence_generation,
                            SDKSyncWindowReceipt.provider == provider,
                            SDKSyncWindowReceipt.purpose == "activation",
                        )
                        .first()
                    )
                    if previous_activation is not None:
                        return WindowAcceptance(False, "activation_already_accepted")
                else:
                    recent_ready_at, archive_frontier = sdk_client_installation_repository.readiness_for(
                        db_session,
                        installation,
                    )
                    if recent_ready_at is None or archive_frontier is None:
                        return WindowAcceptance(False, "archive_activation_required")
                    if not self._same_instant(manifest.upperBoundExclusive, archive_frontier):
                        return WindowAcceptance(False, "archive_window_not_adjacent")

        batches = self.batches.list_by_ids(db_session, referenced_ids)
        if len(batches) != len(referenced_ids):
            return WindowAcceptance(False, "window_batch_not_accepted")
        if any(
            batch.user_id != user_id
            or batch.provider != provider
            or SDKBatchReceiptStatus(batch.status) != SDKBatchReceiptStatus.SUCCEEDED
            or batch.dropped_count != 0
            or batch.tombstones_unresolved != 0
            for batch in batches
        ):
            return WindowAcceptance(False, "window_batch_not_accepted")
        if any(batch.installation_id != terminal_batch.installation_id for batch in batches):
            return WindowAcceptance(False, "window_installation_mismatch")
        if any(batch.installation_generation != terminal_batch.installation_generation for batch in batches):
            return WindowAcceptance(False, "window_installation_generation_mismatch")
        if any(batch.health_evidence_generation != terminal_batch.health_evidence_generation for batch in batches):
            return WindowAcceptance(False, "window_generation_mismatch")
        covered_types: set[str] = set()
        for batch in batches:
            batch_types = set(batch.covered_type_identifiers or [])
            if not batch_types:
                return WindowAcceptance(False, "window_batch_coverage_missing")
            if batch.content_lower_bound_inclusive is None or batch.content_upper_bound_exclusive is None:
                return WindowAcceptance(False, "window_batch_bounds_missing")
            if (
                batch.content_lower_bound_inclusive < manifest.lowerBoundInclusive
                or batch.content_upper_bound_exclusive > manifest.upperBoundExclusive
            ):
                return WindowAcceptance(False, "window_batch_bounds_mismatch")
            covered_types.update(batch_types)
        empty_types = set(manifest.emptyOrNoAccessTypes)
        if covered_types & empty_types:
            return WindowAcceptance(False, "window_type_coverage_overlap")
        if activation_policy is not None and covered_types | empty_types != DASHBOARD_FITNESS_APPLE_HEALTH_V1_TYPES:
            return WindowAcceptance(False, "window_type_coverage_incomplete")
        if self.sleep_inbox.has_unmaterialized_for_batches(db_session, referenced_ids):
            # Sleep batches are terminal once their source payload is durable,
            # but the dashboard must not treat a whole window as canonical until
            # the asynchronous sleep projection has been verified materialized.
            return WindowAcceptance(False, "window_sleep_projection_pending")

        now = datetime.now(timezone.utc)
        receipt = SDKSyncWindowReceipt(
            id=manifest.windowId,
            user_id=user_id,
            installation_id=terminal_batch.installation_id,
            installation_generation=terminal_batch.installation_generation,
            health_evidence_generation=terminal_batch.health_evidence_generation,
            provider=provider,
            manifest_sha256=manifest_hash,
            purpose=manifest.purpose,
            window_version=manifest.windowVersion,
            lower_bound_inclusive=manifest.lowerBoundInclusive,
            upper_bound_exclusive=manifest.upperBoundExclusive,
            batch_ids=canonical["batchIds"],
            empty_or_no_access_types=canonical["emptyOrNoAccessTypes"],
            reconciliation_start_inclusive=manifest.reconciliationStartInclusive,
            reconciliation_end_exclusive=manifest.reconciliationEndExclusive,
            accepted_at=now,
        )
        self.receipts.add(db_session, receipt)
        return WindowAcceptance(True, receipt=receipt)

    def get_for_user(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        window_id: UUID,
    ) -> SDKSyncWindowReceipt | None:
        receipt = self.receipts.get(db_session, window_id)
        if receipt is None or receipt.user_id != user_id:
            return None
        return receipt

    def list_for_user(
        self,
        db_session: DbSession,
        *,
        user_id: UUID,
        provider: str | None,
        limit: int,
    ) -> list[SDKSyncWindowReceipt]:
        return self.receipts.list_for_user(
            db_session,
            user_id=user_id,
            provider=provider,
            limit=limit,
        )

    @staticmethod
    def to_response(receipt: SDKSyncWindowReceipt) -> SDKSyncWindowReceiptResponse:
        return SDKSyncWindowReceiptResponse(
            windowId=receipt.id,
            userId=receipt.user_id,
            installationId=receipt.installation_id,
            installationGeneration=receipt.installation_generation,
            healthEvidenceGeneration=receipt.health_evidence_generation,
            provider=receipt.provider,
            purpose=receipt.purpose,
            windowVersion=receipt.window_version,
            lowerBoundInclusive=receipt.lower_bound_inclusive,
            upperBoundExclusive=receipt.upper_bound_exclusive,
            batchIds=[UUID(value) for value in receipt.batch_ids],
            emptyOrNoAccessTypes=receipt.empty_or_no_access_types,
            reconciliationStartInclusive=receipt.reconciliation_start_inclusive,
            reconciliationEndExclusive=receipt.reconciliation_end_exclusive,
            acceptedAt=receipt.accepted_at,
        )


sdk_sync_window_receipt_service = SDKSyncWindowReceiptService()
