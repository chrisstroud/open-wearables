from datetime import datetime, timezone
from unittest.mock import MagicMock, patch
from uuid import uuid4

from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.models import WhoopSyncDispatchReceipt
from app.schemas.auth import ConnectionStatus
from tests.factories import ApiKeyFactory, UserConnectionFactory, UserFactory
from tests.utils.auth import api_key_headers


def _command(*, idempotency_key: str, end_at: str = "2026-09-02T05:00:00Z") -> dict[str, object]:
    return {
        "idempotency_key": idempotency_key,
        "authorization_generation": 7,
        "requested_start_at": "2000-01-01T00:00:00Z",
        "requested_end_at": end_at,
    }


class TestExactWhoopSyncDispatchApi:
    @patch("app.integrations.celery.tasks.whoop_sync_dispatch_task.drain_whoop_sync_dispatch_outbox.delay")
    def test_command_and_replay_return_same_durable_receipt(
        self,
        mock_nudge: MagicMock,
        client: TestClient,
        db: Session,
    ) -> None:
        user = UserFactory()
        connection = UserConnectionFactory(
            user=user,
            provider="whoop",
            status=ConnectionStatus.ACTIVE,
            authorization_generation=7,
        )
        api_key = ApiKeyFactory()
        key = str(uuid4())
        path = f"/api/v1/providers/whoop/users/{user.id}/connections/{connection.id}/sync/full-history"

        first = client.post(path, headers=api_key_headers(str(api_key.id)), json=_command(idempotency_key=key))
        replay = client.post(path, headers=api_key_headers(str(api_key.id)), json=_command(idempotency_key=key))

        assert first.status_code == 202
        assert replay.status_code == 202
        assert first.json() == replay.json()
        body = first.json()
        assert body["dispatch_id"] == key
        assert body["connection_id"] == str(connection.id)
        assert body["authorization_generation"] == 7
        assert body["status"] == "queued"
        assert body["async"] is True
        assert body["requested_start_at"] == "2000-01-01T00:00:00Z"
        assert db.query(WhoopSyncDispatchReceipt).count() == 1
        assert mock_nudge.call_count == 2

        read = client.get(
            f"/api/v1/providers/whoop/users/{user.id}/sync/full-history/{key}",
            headers=api_key_headers(str(api_key.id)),
        )
        assert read.status_code == 200
        assert read.json()["dispatch_id"] == key
        assert read.json()["task_id"] == body["task_id"]

    @patch("app.integrations.celery.tasks.whoop_sync_dispatch_task.drain_whoop_sync_dispatch_outbox.delay")
    def test_reused_key_with_different_window_conflicts(
        self,
        mock_nudge: MagicMock,
        client: TestClient,
        db: Session,
    ) -> None:
        user = UserFactory()
        connection = UserConnectionFactory(
            user=user,
            provider="whoop",
            authorization_generation=7,
        )
        api_key = ApiKeyFactory()
        key = str(uuid4())
        path = f"/api/v1/providers/whoop/users/{user.id}/connections/{connection.id}/sync/full-history"
        first = client.post(path, headers=api_key_headers(str(api_key.id)), json=_command(idempotency_key=key))
        assert first.status_code == 202
        mock_nudge.assert_called_once()

        conflict = client.post(
            path,
            headers=api_key_headers(str(api_key.id)),
            json=_command(idempotency_key=key, end_at="2026-09-02T05:01:00Z"),
        )

        assert conflict.status_code == 409
        assert db.query(WhoopSyncDispatchReceipt).count() == 1

    @patch("app.integrations.celery.tasks.whoop_sync_dispatch_task.drain_whoop_sync_dispatch_outbox.delay")
    def test_semantic_replay_is_exact_and_never_crosses_route_user(
        self,
        mock_nudge: MagicMock,
        client: TestClient,
        db: Session,
    ) -> None:
        owner = UserFactory()
        attacker = UserFactory()
        connection = UserConnectionFactory(
            user=owner,
            provider="whoop",
            authorization_generation=7,
        )
        api_key = ApiKeyFactory()
        path = f"/api/v1/providers/whoop/users/{owner.id}/connections/{connection.id}/sync/full-history"
        first_key = str(uuid4())

        first = client.post(path, headers=api_key_headers(str(api_key.id)), json=_command(idempotency_key=first_key))
        semantic_replay = client.post(
            path,
            headers=api_key_headers(str(api_key.id)),
            json=_command(idempotency_key=str(uuid4())),
        )
        cross_user = client.post(
            f"/api/v1/providers/whoop/users/{attacker.id}/connections/{connection.id}/sync/full-history",
            headers=api_key_headers(str(api_key.id)),
            json=_command(idempotency_key=str(uuid4())),
        )
        cross_user_read = client.get(
            f"/api/v1/providers/whoop/users/{attacker.id}/sync/full-history/{first_key}",
            headers=api_key_headers(str(api_key.id)),
        )

        assert first.status_code == 202
        assert semantic_replay.status_code == 202
        assert semantic_replay.json() == first.json()
        assert cross_user.status_code == 409
        assert cross_user_read.status_code == 404
        assert db.query(WhoopSyncDispatchReceipt).count() == 1
        assert mock_nudge.call_count == 2

    @patch("app.integrations.celery.tasks.whoop_sync_dispatch_task.drain_whoop_sync_dispatch_outbox.delay")
    def test_stale_generation_is_rejected_before_outbox_insert(
        self,
        mock_nudge: MagicMock,
        client: TestClient,
        db: Session,
    ) -> None:
        user = UserFactory()
        connection = UserConnectionFactory(
            user=user,
            provider="whoop",
            authorization_generation=8,
        )
        api_key = ApiKeyFactory()
        key = str(uuid4())

        response = client.post(
            f"/api/v1/providers/whoop/users/{user.id}/connections/{connection.id}/sync/full-history",
            headers=api_key_headers(str(api_key.id)),
            json=_command(idempotency_key=key),
        )

        assert response.status_code == 409
        assert db.query(WhoopSyncDispatchReceipt).count() == 0
        mock_nudge.assert_not_called()

    def test_connection_projection_exposes_authorization_generation(
        self,
        client: TestClient,
        db: Session,
    ) -> None:
        user = UserFactory()
        connection = UserConnectionFactory(
            user=user,
            provider="whoop",
            authorization_generation=7,
            updated_at=datetime.now(timezone.utc),
        )
        api_key = ApiKeyFactory()

        response = client.get(
            f"/api/v1/users/{user.id}/connections",
            headers=api_key_headers(str(api_key.id)),
        )

        assert response.status_code == 200
        projected = next(item for item in response.json() if item["id"] == str(connection.id))
        assert projected["authorization_generation"] == 7
