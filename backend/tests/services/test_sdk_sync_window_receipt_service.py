from hashlib import sha256
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy.orm import Session

from app.schemas.providers.mobile_sdk import SyncWindowManifest
from app.services.sdk_batch_receipt_service import SDKBatchReceiptService
from app.services.sdk_sync_window_receipt_service import SDKSyncWindowReceiptService
from tests.factories import UserFactory


def manifest(
    window_id: UUID,
    batch_ids: tuple[UUID, ...] | list[UUID] = (),
    *,
    purpose: str = "activation",
) -> SyncWindowManifest:
    return SyncWindowManifest(
        windowId=window_id,
        purpose=purpose,
        windowVersion=2,
        lowerBoundInclusive="2026-07-25T00:00:00Z",
        upperBoundExclusive="2026-08-25T00:00:00Z",
        batchIds=list(batch_ids),
        emptyOrNoAccessTypes=[] if batch_ids else ["HKQuantityTypeIdentifierBodyMass"],
    )


class TestSDKSyncWindowReceiptService:
    @pytest.mark.parametrize(
        ("patch", "message"),
        [
            ({"purpose": "unsupported"}, "purpose"),
            ({"purpose": "incremental"}, "requires reconciliation bounds"),
            ({"windowVersion": 1}, "windowVersion"),
            (
                {"batchIds": [], "emptyOrNoAccessTypes": []},
                "accepted batches or terminal empty/no-access types",
            ),
            (
                {
                    "reconciliationStartInclusive": "2026-08-24T00:00:00Z",
                    "reconciliationEndExclusive": None,
                },
                "supplied together",
            ),
            (
                {
                    "reconciliationStartInclusive": "2026-08-25T00:00:00Z",
                    "reconciliationEndExclusive": "2026-08-24T00:00:00Z",
                },
                "start must precede its end",
            ),
        ],
    )
    def test_manifest_rejects_malformed_authority(self, patch: dict, message: str) -> None:
        values = manifest(uuid4()).model_dump()
        values.update(patch)

        with pytest.raises(ValidationError, match=message):
            SyncWindowManifest.model_validate(values)

    def test_failed_or_foreign_batch_cannot_authorize_window(self, db: Session) -> None:
        user = UserFactory()
        other_user = UserFactory()
        batch_service = SDKBatchReceiptService()
        window_service = SDKSyncWindowReceiptService()
        failed_batch_id = uuid4()
        batch_service.prepare_submission(
            db,
            batch_id=failed_batch_id,
            user_id=other_user.id,
            provider="apple",
            payload_sha256=sha256(b"failed").hexdigest(),
        )
        claim = batch_service.claim_for_processing(db, failed_batch_id)
        batch_service.mark_failed(
            db,
            batch_id=failed_batch_id,
            attempt_count=claim.attempt_count,
            error_code="deletion_projection_unsupported",
            retryable=False,
        )

        window_id = uuid4()
        decision = window_service.accept(
            db,
            user_id=user.id,
            provider="apple",
            terminal_batch_id=window_id,
            manifest=manifest(window_id, [failed_batch_id]),
        )

        assert decision.accepted is False
        assert decision.error_code == "window_batch_not_accepted"
        assert window_service.get_for_user(db, user_id=user.id, window_id=window_id) is None

    @pytest.mark.parametrize("purpose", ["activation", "archive"])
    def test_v2_bounded_manifest_accepts_terminal_empty_coverage(self, db: Session, purpose: str) -> None:
        user = UserFactory()
        batch_service = SDKBatchReceiptService()
        window_service = SDKSyncWindowReceiptService()
        window_id = uuid4()
        batch_service.prepare_submission(
            db,
            batch_id=window_id,
            user_id=user.id,
            provider="apple",
            payload_sha256=sha256(f"{purpose}-window".encode()).hexdigest(),
        )

        accepted = window_service.accept(
            db,
            user_id=user.id,
            provider="apple",
            terminal_batch_id=window_id,
            manifest=manifest(window_id, purpose=purpose),
        )

        assert accepted.accepted is True
        assert accepted.receipt is not None
        assert accepted.receipt.purpose == purpose
        assert accepted.receipt.window_version == 2

    def test_incremental_reconciliation_manifest_accepts_catch_up_batches(self, db: Session) -> None:
        user = UserFactory()
        batch_service = SDKBatchReceiptService()
        window_service = SDKSyncWindowReceiptService()
        catch_up_batch_id = uuid4()
        batch_service.prepare_submission(
            db,
            batch_id=catch_up_batch_id,
            user_id=user.id,
            provider="apple",
            payload_sha256=sha256(b"catch-up").hexdigest(),
        )
        catch_up_claim = batch_service.claim_for_processing(db, catch_up_batch_id)
        assert catch_up_claim.attempt_count is not None
        batch_service.mark_succeeded(
            db,
            batch_id=catch_up_batch_id,
            attempt_count=catch_up_claim.attempt_count,
            result={"status_code": 200, "records_saved": 3, "dropped_count": 0},
        )
        window_id = uuid4()
        batch_service.prepare_submission(
            db,
            batch_id=window_id,
            user_id=user.id,
            provider="apple",
            payload_sha256=sha256(b"incremental-window").hexdigest(),
        )
        catch_up_manifest = SyncWindowManifest(
            windowId=window_id,
            purpose="incremental",
            windowVersion=2,
            lowerBoundInclusive="2014-09-17T00:00:00Z",
            upperBoundExclusive="2026-08-25T00:00:00Z",
            batchIds=[catch_up_batch_id],
            reconciliationStartInclusive="2026-08-24T12:00:00Z",
            reconciliationEndExclusive="2026-08-25T12:00:00Z",
        )

        accepted = window_service.accept(
            db,
            user_id=user.id,
            provider="apple",
            terminal_batch_id=window_id,
            manifest=catch_up_manifest,
        )

        assert accepted.accepted is True
        assert accepted.receipt is not None
        assert accepted.receipt.purpose == "incremental"
        assert accepted.receipt.reconciliation_start_inclusive == catch_up_manifest.reconciliationStartInclusive
        assert accepted.receipt.reconciliation_end_exclusive == catch_up_manifest.reconciliationEndExclusive

    def test_manifest_is_immutable_but_exact_retry_is_idempotent(self, db: Session) -> None:
        user = UserFactory()
        batch_service = SDKBatchReceiptService()
        window_service = SDKSyncWindowReceiptService()
        window_id = uuid4()
        batch_service.prepare_submission(
            db,
            batch_id=window_id,
            user_id=user.id,
            provider="apple",
            payload_sha256=sha256(b"window").hexdigest(),
        )

        first = window_service.accept(
            db,
            user_id=user.id,
            provider="apple",
            terminal_batch_id=window_id,
            manifest=manifest(window_id),
        )
        db.commit()
        exact_retry = window_service.accept(
            db,
            user_id=user.id,
            provider="apple",
            terminal_batch_id=window_id,
            manifest=manifest(window_id),
        )
        conflict = window_service.accept(
            db,
            user_id=user.id,
            provider="apple",
            terminal_batch_id=window_id,
            manifest=manifest(window_id, purpose="archive"),
        )

        assert first.accepted is True
        assert exact_retry.accepted is True
        assert exact_retry.receipt is not None
        assert first.receipt is not None
        assert exact_retry.receipt.id == first.receipt.id
        assert conflict.accepted is False
        assert conflict.error_code == "window_manifest_conflict"
