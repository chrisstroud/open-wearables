from datetime import timedelta
from hashlib import sha256
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session

from app.models import SDKUploadInbox
from app.schemas.model_crud.credentials.sdk_client_installation import SDKClientRegistration
from app.schemas.responses.upload import SDKBatchReceiptStatus
from app.services.sdk_batch_receipt_service import BatchReceiptConflictError, SDKBatchReceiptService
from app.services.sdk_client_installation_service import sdk_client_installation_service
from app.services.sdk_upload_inbox_service import sdk_upload_inbox_service
from tests.factories import UserFactory


class TestSDKBatchReceiptService:
    def test_success_metadata_fails_closed_when_type_provenance_exceeds_bound(self, db: Session) -> None:
        user = UserFactory()
        service = SDKBatchReceiptService()
        batch_id = uuid4()
        service.prepare_submission(
            db,
            batch_id=batch_id,
            user_id=user.id,
            provider="apple",
            payload_sha256=sha256(b"bounded-provenance").hexdigest(),
        )
        claim = service.claim_for_processing(db, batch_id)
        assert claim.attempt_count is not None

        service.mark_succeeded(
            db,
            batch_id=batch_id,
            attempt_count=claim.attempt_count,
            result={
                "status_code": 200,
                "covered_type_identifiers": [f"HKQuantityTypeIdentifierSynthetic{index}" for index in range(257)],
                "content_lower_bound_inclusive": "2026-08-24T00:00:00+00:00",
                "content_upper_bound_exclusive": "2026-08-25T00:00:00+00:00",
            },
        )

        receipt = service.crud.get(db, batch_id)
        assert receipt is not None
        assert receipt.status == SDKBatchReceiptStatus.SUCCEEDED
        assert receipt.covered_type_identifiers == []

    def test_queued_is_not_terminal_and_zero_drop_success_is_terminal(self, db: Session) -> None:
        user = UserFactory()
        service = SDKBatchReceiptService()
        batch_id = uuid4()
        digest = sha256(b"payload").hexdigest()

        queued = service.prepare_submission(
            db,
            batch_id=batch_id,
            user_id=user.id,
            provider="apple",
            payload_sha256=digest,
        )
        assert queued.http_status == 202
        assert queued.should_dispatch is True
        assert service.to_response(queued.receipt).accepted is False

        claim = service.claim_for_processing(db, batch_id)
        assert claim.receipt_exists is True
        assert claim.should_process is True
        service.mark_succeeded(
            db,
            batch_id=batch_id,
            attempt_count=claim.attempt_count or 0,
            result={"status_code": 200, "records_saved": 12, "dropped_count": 0},
        )

        terminal = service.prepare_submission(
            db,
            batch_id=batch_id,
            user_id=user.id,
            provider="apple",
            payload_sha256=digest,
        )
        response = service.to_response(terminal.receipt)
        assert terminal.http_status == 200
        assert terminal.should_dispatch is False
        assert response.status == SDKBatchReceiptStatus.SUCCEEDED
        assert response.terminal is True
        assert response.accepted is True
        assert response.records_saved == 12

    @pytest.mark.parametrize(
        ("result", "error_code"),
        [
            ({"status_code": 200, "dropped_count": 1}, "dropped_records"),
            (
                {
                    "status_code": 409,
                    "tombstones_unresolved": 1,
                    "tombstone_error_code": "legacy_workout_lineage_missing",
                },
                "tombstones_unresolved",
            ),
        ],
    )
    def test_drop_or_unresolved_tombstone_never_becomes_accepted(
        self,
        db: Session,
        result: dict,
        error_code: str,
    ) -> None:
        user = UserFactory()
        service = SDKBatchReceiptService()
        batch_id = uuid4()
        digest = sha256(b"payload").hexdigest()
        service.prepare_submission(
            db,
            batch_id=batch_id,
            user_id=user.id,
            provider="apple",
            payload_sha256=digest,
        )
        claim = service.claim_for_processing(db, batch_id)
        service.mark_succeeded(
            db,
            batch_id=batch_id,
            attempt_count=claim.attempt_count or 0,
            result=result,
        )

        decision = service.prepare_submission(
            db,
            batch_id=batch_id,
            user_id=user.id,
            provider="apple",
            payload_sha256=digest,
        )
        response = service.to_response(decision.receipt)
        assert decision.http_status == 409
        assert response.status == SDKBatchReceiptStatus.FAILED
        assert response.accepted is False
        assert response.error_code == error_code

    def test_retryable_worker_failure_requeues_same_batch(self, db: Session) -> None:
        user = UserFactory()
        service = SDKBatchReceiptService()
        batch_id = uuid4()
        digest = sha256(b"payload").hexdigest()
        service.prepare_submission(
            db,
            batch_id=batch_id,
            user_id=user.id,
            provider="apple",
            payload_sha256=digest,
        )
        claim = service.claim_for_processing(db, batch_id)
        service.mark_failed(
            db,
            batch_id=batch_id,
            attempt_count=claim.attempt_count,
            error_code="worker_exception",
            retryable=True,
        )

        retry = service.prepare_submission(
            db,
            batch_id=batch_id,
            user_id=user.id,
            provider="apple",
            payload_sha256=digest,
        )
        assert retry.http_status == 202
        assert retry.should_dispatch is True
        assert retry.receipt.status == SDKBatchReceiptStatus.QUEUED

    def test_batch_id_cannot_be_reused_for_different_payload(self, db: Session) -> None:
        user = UserFactory()
        service = SDKBatchReceiptService()
        batch_id = uuid4()
        service.prepare_submission(
            db,
            batch_id=batch_id,
            user_id=user.id,
            provider="apple",
            payload_sha256=sha256(b"first").hexdigest(),
        )

        with pytest.raises(BatchReceiptConflictError):
            service.prepare_submission(
                db,
                batch_id=batch_id,
                user_id=user.id,
                provider="apple",
                payload_sha256=sha256(b"different").hexdigest(),
            )

    def test_stale_worker_cannot_publish_over_newer_attempt(self, db: Session) -> None:
        user = UserFactory()
        service = SDKBatchReceiptService()
        batch_id = uuid4()
        digest = sha256(b"payload").hexdigest()
        service.prepare_submission(
            db,
            batch_id=batch_id,
            user_id=user.id,
            provider="apple",
            payload_sha256=digest,
        )
        first = service.claim_for_processing(db, batch_id)
        assert first.attempt_count == 1

        receipt = service.crud.get(db, batch_id)
        assert receipt is not None
        receipt.processing_started_at = service._now() - service.stale_processing_after - timedelta(seconds=1)
        db.commit()

        requeued = service.prepare_submission(
            db,
            batch_id=batch_id,
            user_id=user.id,
            provider="apple",
            payload_sha256=digest,
        )
        assert requeued.should_dispatch is True
        second = service.claim_for_processing(db, batch_id)
        assert second.attempt_count == 2

        service.mark_succeeded(
            db,
            batch_id=batch_id,
            attempt_count=first.attempt_count,
            result={"status_code": 200, "records_saved": 1, "dropped_count": 0},
        )
        current = service.crud.get(db, batch_id)
        assert current is not None
        assert current.status == SDKBatchReceiptStatus.PROCESSING
        assert current.attempt_count == 2

        service.mark_failed(
            db,
            batch_id=batch_id,
            attempt_count=second.attempt_count,
            error_code="newer_attempt_failed",
            retryable=False,
        )
        service.mark_succeeded(
            db,
            batch_id=batch_id,
            attempt_count=first.attempt_count,
            result={"status_code": 200, "records_saved": 1, "dropped_count": 0},
        )
        terminal = service.crud.get(db, batch_id)
        assert terminal is not None
        assert terminal.status == SDKBatchReceiptStatus.FAILED
        assert terminal.error_code == "newer_attempt_failed"

    def test_success_publication_rechecks_exact_installation_generation(self, db: Session) -> None:
        user = UserFactory()
        service = SDKBatchReceiptService()
        registration = SDKClientRegistration(
            installation_id=uuid4(),
            bundle_id="fitness.dashboard.app",
            app_version="1.0.0",
            build_number="1",
            protocol_version=2,
        )
        installation = sdk_client_installation_service.activate(
            db,
            user_id=user.id,
            registration=registration,
        )
        generation = installation.generation
        batch_id = uuid4()
        digest = sha256(b"payload").hexdigest()
        service.prepare_submission(
            db,
            batch_id=batch_id,
            user_id=user.id,
            installation_id=installation.id,
            installation_generation=generation,
            health_evidence_generation=0,
            provider="apple",
            payload_sha256=digest,
        )
        sdk_upload_inbox_service.put(
            db,
            batch_id=batch_id,
            user_id=user.id,
            installation_id=installation.id,
            installation_generation=generation,
            health_evidence_generation=0,
            provider="apple",
            payload_sha256=digest,
            content_type="application/json",
            content="payload",
        )
        claim = service.claim_for_processing(db, batch_id)
        assert claim.attempt_count is not None

        repaired = sdk_client_installation_service.activate(
            db,
            user_id=user.id,
            registration=registration,
        )
        assert repaired.generation == generation + 1
        db.commit()

        service.mark_succeeded(
            db,
            batch_id=batch_id,
            attempt_count=claim.attempt_count,
            result={"status_code": 200, "records_saved": 1},
        )

        receipt = service.crud.get(db, batch_id)
        assert receipt is not None
        assert receipt.status == SDKBatchReceiptStatus.FAILED
        assert receipt.error_code == "installation_generation_mismatch"
        assert db.query(SDKUploadInbox).filter_by(id=batch_id).one_or_none() is not None
