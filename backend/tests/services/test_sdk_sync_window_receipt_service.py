from hashlib import sha256
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError
from sqlalchemy.orm import Session

from app.constants.sdk_history import DASHBOARD_FITNESS_APPLE_HEALTH_V1_TYPES
from app.schemas.model_crud.credentials import UserInvitationActivationPolicy
from app.schemas.model_crud.credentials.sdk_client_installation import SDKClientRegistration
from app.schemas.providers.mobile_sdk import SyncWindowManifest
from app.services.sdk_batch_receipt_service import SDKBatchReceiptService
from app.services.sdk_client_installation_service import sdk_client_installation_service
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
        lowerBoundInclusive="2026-07-26T00:00:00Z",
        upperBoundExclusive="2026-08-25T00:00:00Z",
        batchIds=list(batch_ids),
        emptyOrNoAccessTypes=[] if batch_ids else ["HKQuantityTypeIdentifierBodyMass"],
    )


def activation_policy() -> UserInvitationActivationPolicy:
    return UserInvitationActivationPolicy.model_validate(
        {
            "purpose": "activation",
            "window_version": 2,
            "lower_bound_inclusive": "2026-07-26T00:00:00Z",
            "upper_bound_exclusive": "2026-08-25T00:00:00Z",
            "timezone": "UTC",
            "completed_day_count": 30,
        }
    )


def test_server_registry_is_the_exact_reviewed_28_type_surface() -> None:
    assert len(DASHBOARD_FITNESS_APPLE_HEALTH_V1_TYPES) == 28
    assert "HKQuantityTypeIdentifierStepCount" in DASHBOARD_FITNESS_APPLE_HEALTH_V1_TYPES
    assert "HKCategoryTypeIdentifierSleepAnalysis" in DASHBOARD_FITNESS_APPLE_HEALTH_V1_TYPES
    assert "HKWorkoutType" in DASHBOARD_FITNESS_APPLE_HEALTH_V1_TYPES


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
            result={
                "status_code": 200,
                "records_saved": 3,
                "dropped_count": 0,
                "covered_type_identifiers": ["HKQuantityTypeIdentifierBodyMass"],
                "content_lower_bound_inclusive": "2026-08-01T00:00:00+00:00",
                "content_upper_bound_exclusive": "2026-08-02T00:00:00+00:00",
            },
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

    def test_window_cannot_mix_installation_generations(self, db: Session) -> None:
        user = UserFactory()
        batch_service = SDKBatchReceiptService()
        window_service = SDKSyncWindowReceiptService()
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
            activation_policy=activation_policy(),
        )
        old_generation = installation.generation
        data_batch_id = uuid4()
        batch_service.prepare_submission(
            db,
            batch_id=data_batch_id,
            user_id=user.id,
            installation_id=installation.id,
            installation_generation=old_generation,
            health_evidence_generation=0,
            provider="apple",
            payload_sha256=sha256(b"data").hexdigest(),
        )
        claim = batch_service.claim_for_processing(db, data_batch_id)
        assert claim.attempt_count is not None
        batch_service.mark_succeeded(
            db,
            batch_id=data_batch_id,
            attempt_count=claim.attempt_count,
            result={"status_code": 200, "records_saved": 1},
        )

        repaired = sdk_client_installation_service.activate(
            db,
            user_id=user.id,
            registration=registration,
            activation_policy=activation_policy(),
        )
        assert repaired.generation == old_generation + 1
        window_id = uuid4()
        batch_service.prepare_submission(
            db,
            batch_id=window_id,
            user_id=user.id,
            installation_id=repaired.id,
            installation_generation=repaired.generation,
            health_evidence_generation=0,
            provider="apple",
            payload_sha256=sha256(b"window").hexdigest(),
        )

        decision = window_service.accept(
            db,
            user_id=user.id,
            provider="apple",
            terminal_batch_id=window_id,
            manifest=manifest(window_id, [data_batch_id]),
        )

        assert decision.accepted is False
        assert decision.error_code == "window_installation_generation_mismatch"

    @pytest.mark.parametrize(
        ("manifest_patch", "expected_error"),
        [
            (
                {"lowerBoundInclusive": "2026-07-25T00:00:00Z"},
                "activation_policy_mismatch",
            ),
            (
                {
                    "emptyOrNoAccessTypes": sorted(
                        DASHBOARD_FITNESS_APPLE_HEALTH_V1_TYPES - {"HKQuantityTypeIdentifierStepCount"}
                    )
                },
                "window_type_coverage_incomplete",
            ),
        ],
    )
    def test_first_class_activation_rejects_client_chosen_policy_or_partial_type_coverage(
        self,
        db: Session,
        manifest_patch: dict,
        expected_error: str,
    ) -> None:
        user = UserFactory()
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
            activation_policy=activation_policy(),
        )
        window_id = uuid4()
        SDKBatchReceiptService().prepare_submission(
            db,
            batch_id=window_id,
            user_id=user.id,
            installation_id=installation.id,
            installation_generation=installation.generation,
            health_evidence_generation=installation.health_evidence_generation,
            provider="apple",
            payload_sha256=sha256(b"first-class-window").hexdigest(),
        )
        values = manifest(window_id).model_dump()
        values["emptyOrNoAccessTypes"] = sorted(DASHBOARD_FITNESS_APPLE_HEALTH_V1_TYPES)
        values.update(manifest_patch)

        decision = SDKSyncWindowReceiptService().accept(
            db,
            user_id=user.id,
            provider="apple",
            terminal_batch_id=window_id,
            manifest=SyncWindowManifest.model_validate(values),
        )

        assert decision.accepted is False
        assert decision.error_code == expected_error

    def test_first_class_activation_rejects_referenced_batch_outside_bound_policy(self, db: Session) -> None:
        user = UserFactory()
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
            activation_policy=activation_policy(),
        )
        batch_service = SDKBatchReceiptService()
        data_batch_id = uuid4()
        batch_service.prepare_submission(
            db,
            batch_id=data_batch_id,
            user_id=user.id,
            installation_id=installation.id,
            installation_generation=installation.generation,
            health_evidence_generation=installation.health_evidence_generation,
            provider="apple",
            payload_sha256=sha256(b"out-of-window").hexdigest(),
        )
        claim = batch_service.claim_for_processing(db, data_batch_id)
        assert claim.attempt_count is not None
        batch_service.mark_succeeded(
            db,
            batch_id=data_batch_id,
            attempt_count=claim.attempt_count,
            result={
                "status_code": 200,
                "records_saved": 1,
                "covered_type_identifiers": ["HKQuantityTypeIdentifierStepCount"],
                "content_lower_bound_inclusive": "2026-07-25T23:59:59+00:00",
                "content_upper_bound_exclusive": "2026-07-26T00:00:00+00:00",
            },
        )
        window_id = uuid4()
        batch_service.prepare_submission(
            db,
            batch_id=window_id,
            user_id=user.id,
            installation_id=installation.id,
            installation_generation=installation.generation,
            health_evidence_generation=installation.health_evidence_generation,
            provider="apple",
            payload_sha256=sha256(b"bounded-window").hexdigest(),
        )
        bounded_manifest = manifest(window_id, [data_batch_id]).model_copy(
            update={
                "emptyOrNoAccessTypes": sorted(
                    DASHBOARD_FITNESS_APPLE_HEALTH_V1_TYPES - {"HKQuantityTypeIdentifierStepCount"}
                )
            }
        )

        decision = SDKSyncWindowReceiptService().accept(
            db,
            user_id=user.id,
            provider="apple",
            terminal_batch_id=window_id,
            manifest=bounded_manifest,
        )

        assert decision.accepted is False
        assert decision.error_code == "window_batch_bounds_mismatch"

    def test_exact_activation_is_ready_but_archive_must_extend_the_contiguous_frontier(self, db: Session) -> None:
        user = UserFactory()
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
            activation_policy=activation_policy(),
        )
        batch_service = SDKBatchReceiptService()
        activation_id = uuid4()
        batch_service.prepare_submission(
            db,
            batch_id=activation_id,
            user_id=user.id,
            installation_id=installation.id,
            installation_generation=installation.generation,
            health_evidence_generation=installation.health_evidence_generation,
            provider="apple",
            payload_sha256=sha256(b"exact-activation").hexdigest(),
        )
        exact = manifest(activation_id).model_copy(
            update={"emptyOrNoAccessTypes": sorted(DASHBOARD_FITNESS_APPLE_HEALTH_V1_TYPES)}
        )
        accepted = SDKSyncWindowReceiptService().accept(
            db,
            user_id=user.id,
            provider="apple",
            terminal_batch_id=activation_id,
            manifest=exact,
        )
        assert accepted.accepted is True

        archive_id = uuid4()
        batch_service.prepare_submission(
            db,
            batch_id=archive_id,
            user_id=user.id,
            installation_id=installation.id,
            installation_generation=installation.generation,
            health_evidence_generation=installation.health_evidence_generation,
            provider="apple",
            payload_sha256=sha256(b"gapped-archive").hexdigest(),
        )
        gapped_archive = SyncWindowManifest(
            windowId=archive_id,
            purpose="archive",
            windowVersion=2,
            lowerBoundInclusive="2026-06-25T00:00:00Z",
            upperBoundExclusive="2026-07-25T00:00:00Z",
            emptyOrNoAccessTypes=sorted(DASHBOARD_FITNESS_APPLE_HEALTH_V1_TYPES),
        )
        rejected = SDKSyncWindowReceiptService().accept(
            db,
            user_id=user.id,
            provider="apple",
            terminal_batch_id=archive_id,
            manifest=gapped_archive,
        )

        assert rejected.accepted is False
        assert rejected.error_code == "archive_window_not_adjacent"

        adjacent_id = uuid4()
        batch_service.prepare_submission(
            db,
            batch_id=adjacent_id,
            user_id=user.id,
            installation_id=installation.id,
            installation_generation=installation.generation,
            health_evidence_generation=installation.health_evidence_generation,
            provider="apple",
            payload_sha256=sha256(b"adjacent-archive").hexdigest(),
        )
        adjacent_archive = SyncWindowManifest(
            windowId=adjacent_id,
            purpose="archive",
            windowVersion=2,
            lowerBoundInclusive="2026-06-26T00:00:00Z",
            upperBoundExclusive="2026-07-26T00:00:00Z",
            emptyOrNoAccessTypes=sorted(DASHBOARD_FITNESS_APPLE_HEALTH_V1_TYPES),
        )
        adjacent = SDKSyncWindowReceiptService().accept(
            db,
            user_id=user.id,
            provider="apple",
            terminal_batch_id=adjacent_id,
            manifest=adjacent_archive,
        )
        recent_ready_at, archive_frontier = sdk_client_installation_service.crud.readiness_for(
            db,
            installation,
        )

        assert adjacent.accepted is True
        assert recent_ready_at is not None
        assert archive_frontier == adjacent_archive.lowerBoundInclusive
