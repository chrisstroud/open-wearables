import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from uuid import UUID

from app.database import DbSession
from app.models import SDKSyncWindowReceipt
from app.repositories import (
    SDKBatchReceiptRepository,
    SDKSleepInboxRepository,
    SDKSyncWindowReceiptRepository,
)
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
        if self.sleep_inbox.has_unmaterialized_for_batches(db_session, referenced_ids):
            # Sleep batches are terminal once their source payload is durable,
            # but the dashboard must not treat a whole window as canonical until
            # the asynchronous sleep projection has been verified materialized.
            return WindowAcceptance(False, "window_sleep_projection_pending")

        now = datetime.now(timezone.utc)
        receipt = SDKSyncWindowReceipt(
            id=manifest.windowId,
            user_id=user_id,
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
