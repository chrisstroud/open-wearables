"""Whole-account saga: exact durable bindings, closed write fence, verified receipt."""

import hashlib
import hmac
import json
from datetime import datetime, timezone
from typing import Any
from uuid import UUID

from fastapi import HTTPException

from app.config import settings
from app.database import DbSession
from app.models import AccountErasureOperation, User
from app.repositories.account_erasure_repository import account_erasure_repository as repository
from app.schemas.account_erasure import (
    AccountErasureOperationRequest,
    AccountErasureOperationResponse,
    AccountErasurePendingResponse,
    AccountErasurePlanRequest,
    AccountErasurePlanResponse,
    AccountErasureTerminalResponse,
    PendingStatus,
)
from app.schemas.model_crud.credentials import SDKHealthResetTransitionRequest
from app.services.account_erasure_runtime import require_account_erasure_workers
from app.services.sdk_source_reset_external import ProviderIdentityScope
from app.services.sdk_source_reset_service import sdk_source_reset_service as cleanup


def _digest(label: str, payload: Any) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hmac.new(
        settings.secret_key.encode(), f"account-erasure:v1:{label}\0{canonical}".encode(), hashlib.sha256
    ).hexdigest()


def _binding(request: AccountErasurePlanRequest) -> str:
    payload = request.model_dump(by_alias=True)
    payload["authorization"].pop("planFingerprint", None)
    return _digest("binding", payload)


def _matches(row: AccountErasureOperation, request: AccountErasurePlanRequest) -> bool:
    return row.open_wearables_user_id == UUID(request.account.open_wearables_user_id) and hmac.compare_digest(
        row.binding_digest, _binding(request)
    )


def _owner_matches(user: User | None, request: AccountErasurePlanRequest) -> bool:
    return user is not None and user.external_user_id == request.account.subject_id


def _transition(row: AccountErasureOperation) -> SDKHealthResetTransitionRequest:
    return SDKHealthResetTransitionRequest(
        operation_id=row.operation_id,
        expected_health_evidence_generation=row.health_evidence_generation,
        expected_installation_generation=row.installation_generation,
        expected_inventory_digest_sha256=row.inventory_digest,
        resulting_health_source_policy="multi-source",
    )


def _pending(request: AccountErasureOperationRequest, status: PendingStatus) -> AccountErasurePendingResponse:
    return AccountErasurePendingResponse(**request.model_dump(), status=status)


def _receipt(row: AccountErasureOperation) -> str:
    assert row.recorded_at is not None
    return _digest(
        "terminal",
        {
            "binding": row.binding_digest,
            "plan": row.plan_fingerprint,
            "inventory": row.inventory_digest,
            "configuration": row.configuration_digest,
            "workers": row.worker_set_digest,
            "proof": row.identity_proof,
            "recordedAt": row.recorded_at.astimezone(timezone.utc).isoformat(),
        },
    )


def _workers_match(row: AccountErasureOperation) -> bool:
    return hmac.compare_digest(row.worker_set_digest, _digest("workers", require_account_erasure_workers()))


class AccountErasureService:
    def plan(self, db: DbSession, request: AccountErasurePlanRequest) -> AccountErasurePlanResponse:
        row = repository.lock_operation(db, UUID(request.authorization.operation_id))
        if row is not None:
            result = "planned" if _matches(row, request) else "scope-rejected"
            db.rollback()
            return AccountErasurePlanResponse(**request.model_dump(), status=result)
        user_id = UUID(request.account.open_wearables_user_id)
        user = repository.user(db, user_id, lock=True)
        if not _owner_matches(user, request):
            db.rollback()
            return AccountErasurePlanResponse(**request.model_dump(), status="identity_mismatch")
        assert user is not None
        if user.health_write_state != "active" or user.account_erasure_operation_id is not None:
            db.rollback()
            return AccountErasurePlanResponse(**request.model_dump(), status="unavailable")
        try:
            worker_set_digest = _digest("workers", require_account_erasure_workers())
        except Exception:
            db.rollback()
            return AccountErasurePlanResponse(**request.model_dump(), status="upstream_unavailable")
        state = cleanup.inspect(
            db,
            user_id=user_id,
            request=SDKHealthResetTransitionRequest(
                operation_id=UUID(request.authorization.operation_id),
                expected_health_evidence_generation=user.health_evidence_generation,
                resulting_health_source_policy="multi-source",
            ),
        )
        if state.blockers:
            db.rollback()
            return AccountErasurePlanResponse(**request.model_dump(), status="upstream_unavailable")
        repository.create(
            db,
            AccountErasureOperation(
                operation_id=UUID(request.authorization.operation_id),
                open_wearables_user_id=user_id,
                binding_digest=_binding(request),
                inventory_digest=state.inventory_digest_sha256,
                worker_set_digest=worker_set_digest,
                health_evidence_generation=state.health_evidence_generation,
                installation_generation=state.active_installation_generation,
                state="planned",
            ),
        )
        db.commit()
        return AccountErasurePlanResponse(**request.model_dump(), status="planned")

    def erase(self, db: DbSession, request: AccountErasureOperationRequest) -> AccountErasureOperationResponse:
        row = repository.lock_operation(db, UUID(request.authorization.operation_id))
        if row is None or not _matches(row, request):
            db.rollback()
            return _pending(request, "scope-rejected")
        if row.plan_fingerprint not in {None, request.authorization.plan_fingerprint}:
            db.rollback()
            return _pending(request, "stale-plan")
        if row.state == "deleted":
            db.rollback()
            return self.verify(db, request)
        user_id = row.open_wearables_user_id
        user = repository.user(db, user_id, lock=True)
        if not _owner_matches(user, request):
            db.rollback()
            return _pending(request, "identity_mismatch")
        assert user is not None
        if user.account_erasure_operation_id not in {None, row.operation_id}:
            db.rollback()
            return _pending(request, "scope-rejected")
        # An active authorization lease can own in-flight provider I/O. Never
        # revoke/remove credentials until that owner has released its lease.
        if repository.has_authorization_writer(db, user_id):
            db.rollback()
            return _pending(request, "cleanup_pending")
        transition = _transition(row)
        applied = user.health_reset_operation_id == row.operation_id and user.health_reset_applied_at is not None
        row.plan_fingerprint = request.authorization.plan_fingerprint
        db.commit()
        try:
            if not _workers_match(row):
                raise RuntimeError("Reviewed worker inventory changed")
        except Exception:
            db.rollback()
            return _pending(request, "upstream_unavailable")
        try:
            if not applied:
                fenced = cleanup.fence(db, user_id=user_id, request=transition, account_erasure=True)
                if fenced.blockers:
                    db.rollback()
                    return _pending(request, "upstream_unavailable")
                bound = repository.lock_operation(db, transition.operation_id)
                if bound is None or not _matches(bound, request):
                    db.rollback()
                    return _pending(request, "scope-rejected")
                bound.state = "fenced"
                db.commit()
                drained = cleanup.drain(db, user_id=user_id, request=transition)
                if not drained.drained:
                    db.rollback()
                    return _pending(request, "cleanup_pending")
            cleaned = cleanup.apply(db, user_id=user_id, request=transition)
            if not cleaned.verified_empty or cleaned.health_write_state != "fenced":
                db.rollback()
                return _pending(request, "cleanup_verification_failed")
            return self._finish(db, request)
        except HTTPException as exc:
            db.rollback()
            # Once the durable fence exists, preserve this operation for retry.
            # An upstream 404 is explicitly not a deletion receipt.
            current = repository.user(db, user_id)
            not_fenced = current is not None and current.account_erasure_operation_id is None
            db.rollback()
            if exc.status_code == 409 and not_fenced:
                return _pending(request, "stale-plan")
            return _pending(request, "upstream_unavailable" if exc.status_code == 503 else "cleanup_pending")

    def _finish(self, db: DbSession, request: AccountErasureOperationRequest) -> AccountErasureOperationResponse:
        user_id = UUID(request.account.open_wearables_user_id)
        observed = repository.user(db, user_id)
        if not _owner_matches(observed, request):
            db.rollback()
            return _pending(request, "identity_mismatch")
        assert observed is not None
        identity_scope = cleanup._persisted_identity_scope(observed)
        # Same canonical identity -> user lock order as connection writers.
        cleanup._lock_and_require_exclusive_provider_identities(db, user_id=user_id, identity_scope=identity_scope)
        row = repository.lock_operation(db, UUID(request.authorization.operation_id))
        user = repository.user(db, user_id, lock=True)
        if row is None or not _matches(row, request) or row.plan_fingerprint != request.authorization.plan_fingerprint:
            db.rollback()
            return _pending(request, "scope-rejected")
        if (
            not _owner_matches(user, request)
            or user is None
            or (
                user.health_write_state != "fenced"
                or not user.account_erasure_provider_fence_verified
                or user.account_erasure_operation_id != row.operation_id
                or user.health_reset_operation_id != row.operation_id
                or user.health_evidence_generation != row.health_evidence_generation + 1
                or user.health_reset_applied_at is None
            )
        ):
            db.rollback()
            return _pending(request, "cleanup_verification_failed")
        inventory = cleanup.inventory(db, user_id=user_id, identity_scope=identity_scope)
        try:
            if not _workers_match(row):
                raise RuntimeError("Reviewed worker inventory changed")
        except Exception:
            db.rollback()
            return _pending(request, "upstream_unavailable")
        if not inventory.verified_empty or repository.has_authorization_writer(db, user_id):
            db.rollback()
            return _pending(request, "cleanup_verification_failed")
        row.identity_proof = identity_scope.to_proof()
        row.configuration_digest = inventory.external.configuration_digest_sha256
        # User/profile and cascade-owned rows disappear in the SAME transaction
        # that creates the receipt. A lost response can only replay that receipt.
        repository.delete_user(db, user_id)
        if any(repository.owned_row_counts(db, user_id).values()):
            db.rollback()
            return _pending(request, "cleanup_verification_failed")
        row.recorded_at = datetime.now(timezone.utc)
        row.receipt_fingerprint = _receipt(row)
        row.state = "deleted"
        response = self._terminal(row, request, replay=False)
        db.commit()
        return response

    def verify(self, db: DbSession, request: AccountErasureOperationRequest) -> AccountErasureOperationResponse:
        row = repository.lock_operation(db, UUID(request.authorization.operation_id))
        if row is None or not _matches(row, request):
            db.rollback()
            return _pending(request, "scope-rejected")
        if row.plan_fingerprint != request.authorization.plan_fingerprint:
            db.rollback()
            return _pending(request, "stale-plan")
        if row.state != "deleted" or not row.receipt_fingerprint or row.recorded_at is None:
            db.rollback()
            return _pending(request, "cleanup_pending")
        if not hmac.compare_digest(row.receipt_fingerprint, _receipt(row)):
            db.rollback()
            return _pending(request, "cleanup_verification_failed")
        user_id = row.open_wearables_user_id
        if repository.user(db, user_id) is not None or any(repository.owned_row_counts(db, user_id).values()):
            db.rollback()
            return _pending(request, "cleanup_verification_failed")
        inventory = cleanup.inventory(
            db, user_id=user_id, identity_scope=ProviderIdentityScope.from_proof(row.identity_proof, required=True)
        )
        try:
            if not _workers_match(row):
                raise RuntimeError("Reviewed worker inventory changed")
        except Exception:
            db.rollback()
            return _pending(request, "upstream_unavailable")
        if not inventory.verified_empty or inventory.external.configuration_digest_sha256 != row.configuration_digest:
            db.rollback()
            return _pending(request, "cleanup_verification_failed")
        response = self._terminal(row, request, replay=True)
        db.rollback()
        return response

    @staticmethod
    def _terminal(
        row: AccountErasureOperation, request: AccountErasureOperationRequest, *, replay: bool
    ) -> AccountErasureTerminalResponse:
        assert row.recorded_at is not None
        assert row.receipt_fingerprint is not None
        return AccountErasureTerminalResponse(
            **request.model_dump(),
            status="already_deleted" if replay else "deleted",
            receipt_fingerprint=row.receipt_fingerprint,
            recorded_at=row.recorded_at.astimezone(timezone.utc).isoformat(),
        )


account_erasure_service = AccountErasureService()
