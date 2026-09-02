import json
from collections.abc import Generator
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest
from sqlalchemy.orm import Session
from starlette.testclient import TestClient

from app.config import settings
from app.models import SDKBatchReceipt
from app.schemas.providers.mobile_sdk import SyncWindowManifest
from app.services.sdk_batch_receipt_service import sdk_batch_receipt_service
from app.services.sdk_sync_window_receipt_service import sdk_sync_window_receipt_service
from app.services.sdk_token_service import create_sdk_user_token
from tests.factories import ApiKeyFactory, UserFactory
from tests.utils import api_key_headers


@pytest.fixture(autouse=True)
def mock_sdk_worker() -> Generator[MagicMock, None, None]:
    with patch("app.api.routes.v1.sdk_sync.process_sdk_upload") as task:
        task.delay.return_value = None
        yield task


def payload(value: int = 1) -> dict:
    return {
        "provider": "apple",
        "sdkVersion": "1.0.0",
        "syncTimestamp": "2026-08-25T12:00:00Z",
        "data": {
            "records": [
                {
                    "id": "66666666-6666-6666-6666-666666666666",
                    "type": "HKQuantityTypeIdentifierStepCount",
                    "startDate": "2026-08-25T10:00:00Z",
                    "endDate": "2026-08-25T10:01:00Z",
                    "value": value,
                    "unit": "count",
                }
            ]
        },
    }


def auth_headers(user_id: UUID, batch_id: UUID) -> dict[str, str]:
    token = create_sdk_user_token("test-app", str(user_id))
    return {
        "Authorization": f"Bearer {token}",
        "X-Open-Wearables-Batch-ID": str(batch_id),
    }


def succeed_batch(db: Session, *, batch_id: UUID, user_id: UUID, payload_digest: str) -> None:
    sdk_batch_receipt_service.prepare_submission(
        db,
        batch_id=batch_id,
        user_id=user_id,
        provider="apple",
        payload_sha256=payload_digest,
    )
    claim = sdk_batch_receipt_service.claim_for_processing(db, batch_id)
    assert claim.attempt_count is not None
    sdk_batch_receipt_service.mark_succeeded(
        db,
        batch_id=batch_id,
        attempt_count=claim.attempt_count,
        result={
            "status_code": 200,
            "records_saved": 1,
            "dropped_count": 0,
            "covered_type_identifiers": ["HKQuantityTypeIdentifierStepCount"],
            "content_lower_bound_inclusive": "2026-08-24T10:00:00+00:00",
            "content_upper_bound_exclusive": "2026-08-24T10:01:00+00:00",
        },
    )


def window_payload(window_id: UUID, batch_ids: list[UUID]) -> dict:
    return {
        "provider": "apple",
        "sdkVersion": "2.0.0",
        "syncTimestamp": "2026-08-25T12:00:00Z",
        "data": {"records": [], "sleep": [], "workouts": [], "deletions": []},
        "syncWindow": {
            "windowId": str(window_id),
            "purpose": "activation",
            "windowVersion": 2,
            "lowerBoundInclusive": "2026-07-25T00:00:00Z",
            "upperBoundExclusive": "2026-08-25T00:00:00Z",
            "batchIds": [str(batch_id) for batch_id in batch_ids],
            "emptyOrNoAccessTypes": ["HKQuantityTypeIdentifierBodyMass"],
            "reconciliationStartInclusive": "2026-08-24T00:00:00Z",
            "reconciliationEndExclusive": "2026-08-25T00:00:00Z",
        },
    }


class TestSDKBatchReceiptRoutes:
    def test_wire_body_limit_rejects_oversized_json_before_canonicalization(
        self,
        client: TestClient,
        db: Session,
        api_v1_prefix: str,
        mock_sdk_worker: MagicMock,
    ) -> None:
        user = UserFactory()
        batch_id = uuid4()
        canonical = json.dumps(payload(), separators=(",", ":"), sort_keys=True).encode()
        wire_body = canonical + (b" " * 256)

        with patch.object(settings, "sdk_upload_max_size_bytes", len(canonical) + 1):
            response = client.post(
                f"{api_v1_prefix}/sdk/users/{user.id}/sync",
                headers={**auth_headers(user.id, batch_id), "Content-Type": "application/json"},
                content=wire_body,
            )

        assert response.status_code == 413
        assert response.json()["detail"] == {"error_code": "sdk_upload_too_large", "retryable": False}
        mock_sdk_worker.delay.assert_not_called()
        assert db.get(SDKBatchReceipt, batch_id) is None

    def test_headerless_legacy_client_gets_retryable_non_2xx_without_dispatch_or_receipt(
        self,
        client: TestClient,
        db: Session,
        api_v1_prefix: str,
        mock_sdk_worker: MagicMock,
    ) -> None:
        user = UserFactory()
        headers = auth_headers(user.id, uuid4())
        headers.pop("X-Open-Wearables-Batch-ID")

        rejected = client.post(
            f"{api_v1_prefix}/sdk/users/{user.id}/sync",
            headers=headers,
            json=payload(),
        )

        assert rejected.status_code == 425
        assert rejected.json()["detail"] == {
            "error_code": "batch_id_required",
            "retryable": True,
            "message": "Upgrade the SDK to send X-Open-Wearables-Batch-ID",
        }
        mock_sdk_worker.delay.assert_not_called()
        assert db.query(SDKBatchReceipt).filter(SDKBatchReceipt.user_id == user.id).count() == 0

    def test_202_is_queued_and_duplicate_becomes_200_only_after_terminal_success(
        self,
        client: TestClient,
        db: Session,
        api_v1_prefix: str,
        mock_sdk_worker: MagicMock,
    ) -> None:
        user = UserFactory()
        batch_id = uuid4()
        url = f"{api_v1_prefix}/sdk/users/{user.id}/sync"
        headers = auth_headers(user.id, batch_id)

        queued = client.post(url, headers=headers, json=payload())
        assert queued.status_code == 202
        assert queued.json()["batch_id"] == str(batch_id)
        assert queued.json()["terminal"] is False
        assert queued.json()["accepted"] is False
        mock_sdk_worker.delay.assert_called_once()
        assert mock_sdk_worker.delay.call_args.kwargs["require_terminal_receipt"] is True

        # A duplicate task cannot process concurrently; only the claimed worker
        # can publish the terminal receipt.
        claim = sdk_batch_receipt_service.claim_for_processing(db, batch_id)
        assert claim.should_process is True
        sdk_batch_receipt_service.mark_succeeded(
            db,
            batch_id=batch_id,
            attempt_count=claim.attempt_count or 0,
            result={"status_code": 200, "records_saved": 1, "dropped_count": 0},
        )

        terminal = client.post(url, headers=headers, json=payload())
        assert terminal.status_code == 200
        assert terminal.json()["status"] == "succeeded"
        assert terminal.json()["terminal"] is True
        assert terminal.json()["accepted"] is True
        assert terminal.json()["dropped_count"] == 0
        mock_sdk_worker.delay.assert_called_once()

        status_response = client.get(f"{url}/{batch_id}", headers=headers)
        assert status_response.status_code == 200
        assert status_response.json()["accepted"] is True

    def test_daily_summary_digest_round_trips_on_duplicate_post_and_get(
        self,
        client: TestClient,
        db: Session,
        api_v1_prefix: str,
        mock_sdk_worker: MagicMock,
    ) -> None:
        user = UserFactory()
        batch_id = uuid4()
        url = f"{api_v1_prefix}/sdk/users/{user.id}/sync"
        headers = auth_headers(user.id, batch_id)
        body = payload()
        revision_set_digest = "d" * 64

        # The API intentionally stores the body opaquely for its worker. The
        # daily-summary parser is covered in service tests; this proves the
        # terminal receipt field survives both public replay surfaces.
        assert client.post(url, headers=headers, json=body).status_code == 202
        claim = sdk_batch_receipt_service.claim_for_processing(db, batch_id)
        assert claim.attempt_count is not None
        sdk_batch_receipt_service.mark_succeeded(
            db,
            batch_id=batch_id,
            attempt_count=claim.attempt_count,
            result={
                "status_code": 200,
                "daily_summaries_saved": 1,
                "revision_set_digest": revision_set_digest,
            },
        )

        duplicate = client.post(url, headers=headers, json=body)
        assert duplicate.status_code == 200
        assert duplicate.json()["revision_set_digest"] == revision_set_digest
        assert duplicate.json()["accepted"] is True

        status_response = client.get(f"{url}/{batch_id}", headers=headers)
        assert status_response.status_code == 200
        assert status_response.json()["revision_set_digest"] == revision_set_digest
        assert status_response.json()["accepted"] is True
        mock_sdk_worker.delay.assert_called_once()

    def test_terminal_drop_returns_409_and_never_redispatches(
        self,
        client: TestClient,
        db: Session,
        api_v1_prefix: str,
        mock_sdk_worker: MagicMock,
    ) -> None:
        user = UserFactory()
        batch_id = uuid4()
        url = f"{api_v1_prefix}/sdk/users/{user.id}/sync"
        headers = auth_headers(user.id, batch_id)
        assert client.post(url, headers=headers, json=payload()).status_code == 202
        claim = sdk_batch_receipt_service.claim_for_processing(db, batch_id)
        sdk_batch_receipt_service.mark_succeeded(
            db,
            batch_id=batch_id,
            attempt_count=claim.attempt_count or 0,
            result={"status_code": 200, "dropped_count": 2},
        )

        failed = client.post(url, headers=headers, json=payload())
        assert failed.status_code == 409
        assert failed.json()["status"] == "failed"
        assert failed.json()["accepted"] is False
        assert failed.json()["dropped_count"] == 2
        assert failed.json()["error_code"] == "dropped_records"
        mock_sdk_worker.delay.assert_called_once()

    def test_batch_id_payload_mismatch_is_rejected(
        self,
        client: TestClient,
        api_v1_prefix: str,
    ) -> None:
        user = UserFactory()
        batch_id = uuid4()
        url = f"{api_v1_prefix}/sdk/users/{user.id}/sync"
        headers = auth_headers(user.id, batch_id)
        assert client.post(url, headers=headers, json=payload(1)).status_code == 202

        conflict = client.post(url, headers=headers, json=payload(2))
        assert conflict.status_code == 409

    def test_window_authority_round_trip_and_api_key_dashboard_reads(
        self,
        client: TestClient,
        db: Session,
        api_v1_prefix: str,
        mock_sdk_worker: MagicMock,
    ) -> None:
        user = UserFactory()
        api_key = ApiKeyFactory()
        referenced_batch_id = uuid4()
        succeed_batch(
            db,
            batch_id=referenced_batch_id,
            user_id=user.id,
            payload_digest="a" * 64,
        )
        window_id = uuid4()
        body = window_payload(window_id, [referenced_batch_id, referenced_batch_id])
        sync_url = f"{api_v1_prefix}/sdk/users/{user.id}/sync"

        queued = client.post(sync_url, headers=auth_headers(user.id, window_id), json=body)
        assert queued.status_code == 202
        assert mock_sdk_worker.delay.call_args.kwargs["require_terminal_receipt"] is True

        claim = sdk_batch_receipt_service.claim_for_processing(db, window_id)
        assert claim.attempt_count is not None
        acceptance = sdk_sync_window_receipt_service.accept(
            db,
            user_id=user.id,
            provider="apple",
            terminal_batch_id=window_id,
            manifest=SyncWindowManifest.model_validate(body["syncWindow"]),
        )
        assert acceptance.accepted is True
        db.commit()
        sdk_batch_receipt_service.mark_succeeded(
            db,
            batch_id=window_id,
            attempt_count=claim.attempt_count,
            result={"status_code": 200, "dropped_count": 0},
        )

        terminal = client.post(sync_url, headers=auth_headers(user.id, window_id), json=body)
        assert terminal.status_code == 200
        assert terminal.json()["accepted"] is True

        dashboard_headers = api_key_headers(api_key.id)
        exact = client.get(
            f"{api_v1_prefix}/sdk/users/{user.id}/sync-windows/{window_id}",
            headers=dashboard_headers,
        )
        assert exact.status_code == 200
        assert exact.json()["windowId"] == str(window_id)
        assert exact.json()["batchIds"] == [str(referenced_batch_id)]
        assert exact.json()["purpose"] == "activation"

        listed = client.get(
            f"{api_v1_prefix}/sdk/users/{user.id}/sync-windows?provider=apple",
            headers=dashboard_headers,
        )
        assert listed.status_code == 200
        assert [item["windowId"] for item in listed.json()] == [str(window_id)]

    def test_window_header_must_equal_manifest_id(
        self,
        client: TestClient,
        api_v1_prefix: str,
        mock_sdk_worker: MagicMock,
    ) -> None:
        user = UserFactory()
        header_id = uuid4()
        response = client.post(
            f"{api_v1_prefix}/sdk/users/{user.id}/sync",
            headers=auth_headers(user.id, header_id),
            json=window_payload(uuid4(), []),
        )

        assert response.status_code == 409
        assert response.json()["detail"] == {
            "error_code": "window_id_mismatch",
            "retryable": False,
        }
        mock_sdk_worker.delay.assert_not_called()
