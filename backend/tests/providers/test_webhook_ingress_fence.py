"""Provider webhook ingress must be fenced before raw storage or queue dispatch."""

from concurrent.futures import ThreadPoolExecutor, TimeoutError
from dataclasses import dataclass
from datetime import datetime, timezone
from threading import Event
from typing import Any, Callable
from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from sqlalchemy.orm import Session, sessionmaker

from app.models import User, UserConnection
from app.repositories.user_connection_repository import UserConnectionRepository
from app.schemas.auth import ConnectionStatus
from app.schemas.model_crud.user_management import UserConnectionCreate
from app.schemas.providers.polar import PolarWebhookEvent, PolarWebhookEventType
from app.schemas.providers.strava import StravaWebhookEvent
from app.services.providers.garmin.webhook_handler import GarminWebhookHandler
from app.services.providers.google.health_api.webhook_handler import GoogleWebhookHandler
from app.services.providers.oura.webhook_handler import OuraWebhookHandler
from app.services.providers.polar.webhook_handler import PolarWebhookHandler
from app.services.providers.strava.webhook_handler import StravaWebhookHandler
from app.services.providers.suunto.webhook_handler import SuuntoWebhookHandler
from app.services.providers.whoop.webhook_handler import WhoopWebhookHandler
from tests.factories import UserConnectionFactory, UserFactory


@dataclass(frozen=True)
class WebhookCase:
    provider: str
    module: str
    handler: Any
    payload_factory: Callable[[str], Any]
    identity_kind: str = "provider_user_id"


def _handler_cases() -> list[WebhookCase]:
    return [
        WebhookCase(
            provider="garmin",
            module="app.services.providers.garmin.webhook_handler",
            handler=GarminWebhookHandler(MagicMock(), MagicMock()),
            payload_factory=lambda identity: {"activities": [{"userId": identity, "activityId": 1}]},
        ),
        WebhookCase(
            provider="whoop",
            module="app.services.providers.whoop.webhook_handler",
            handler=WhoopWebhookHandler(MagicMock(), MagicMock()),
            payload_factory=lambda identity: {"user_id": int(identity), "id": "workout-1", "type": "workout.updated"},
        ),
        WebhookCase(
            provider="suunto",
            module="app.services.providers.suunto.webhook_handler",
            handler=SuuntoWebhookHandler(MagicMock(), MagicMock()),
            payload_factory=lambda identity: {"username": identity, "type": "WORKOUT_CREATED"},
            identity_kind="provider_username",
        ),
        WebhookCase(
            provider="strava",
            module="app.services.providers.strava.webhook_handler",
            handler=StravaWebhookHandler(MagicMock()),
            payload_factory=lambda identity: StravaWebhookEvent(
                object_type="activity",
                object_id=1,
                aspect_type="create",
                owner_id=int(identity),
                subscription_id=2,
                event_time=1_700_000_000,
            ),
        ),
        WebhookCase(
            provider="polar",
            module="app.services.providers.polar.webhook_handler",
            handler=PolarWebhookHandler(),
            payload_factory=lambda identity: PolarWebhookEvent(
                event=PolarWebhookEventType.EXERCISE,
                user_id=int(identity),
                entity_id="exercise-1",
                url="https://www.polaraccesslink.com/v3/exercises/exercise-1",
            ),
        ),
        WebhookCase(
            provider="oura",
            module="app.services.providers.oura.webhook_handler",
            handler=OuraWebhookHandler(MagicMock(), MagicMock()),
            payload_factory=lambda identity: {
                "event_type": "create",
                "data_type": "daily_sleep",
                "user_id": identity,
                "object_id": "sleep-1",
            },
        ),
        WebhookCase(
            provider="google",
            module="app.services.providers.google.health_api.webhook_handler",
            handler=GoogleWebhookHandler(MagicMock(), MagicMock()),
            payload_factory=lambda identity: {
                "data": {
                    "healthUserId": identity,
                    "operation": "UPSERT",
                    "dataType": "steps",
                    "intervals": [],
                }
            },
        ),
    ]


@pytest.mark.parametrize("case", _handler_cases(), ids=lambda case: case.provider)
@pytest.mark.parametrize("account_state", ["active", "fenced", "revoked", "unknown", "v2-only"])
def test_provider_dispatch_requires_current_legacy_account_authority(
    db: Session,
    case: WebhookCase,
    account_state: str,
) -> None:
    identity = "4242" if case.identity_kind == "provider_user_id" else "athlete-4242"
    if account_state != "unknown":
        user = UserFactory(
            health_write_state="fenced" if account_state == "fenced" else "active",
            health_source_policy="apple-mobile-v2-only" if account_state == "v2-only" else "legacy-mixed",
        )
        connection_kwargs = {
            "user": user,
            "provider": case.provider,
            "status": "revoked" if account_state == "revoked" else "active",
        }
        connection_kwargs[case.identity_kind] = identity
        UserConnectionFactory(**connection_kwargs)
        db.flush()

    payload = case.payload_factory(identity)
    with (
        patch(f"{case.module}.store_raw_payload") as store_raw_payload,
        patch(f"{case.module}.celery_app") as celery_app,
    ):
        celery_app.send_task.return_value = MagicMock(id="queued-task")
        result = case.handler.dispatch(db, payload)

    assert result == {"status": "accepted"}
    if account_state == "active":
        store_raw_payload.assert_called_once()
        celery_app.send_task.assert_called_once()
    else:
        store_raw_payload.assert_not_called()
        celery_app.send_task.assert_not_called()


@pytest.mark.parametrize("provider", ["garmin", "google"])
def test_batch_dispatch_filters_each_identity_without_cross_user_widening(db: Session, provider: str) -> None:
    allowed = UserFactory(health_write_state="active", health_source_policy="legacy-mixed")
    fenced = UserFactory(health_write_state="fenced", health_source_policy="legacy-mixed")
    UserConnectionFactory(user=allowed, provider=provider, provider_user_id="allowed", status="active")
    UserConnectionFactory(user=fenced, provider=provider, provider_user_id="fenced", status="active")
    db.flush()

    if provider == "garmin":
        module = "app.services.providers.garmin.webhook_handler"
        handler = GarminWebhookHandler(MagicMock(), MagicMock())
        payload: Any = {
            "activities": [
                {"userId": "allowed", "activityId": 1},
                {"userId": "fenced", "activityId": 2},
            ]
        }
    else:
        module = "app.services.providers.google.health_api.webhook_handler"
        handler = GoogleWebhookHandler(MagicMock(), MagicMock())
        payload = [
            {"data": {"healthUserId": "allowed", "operation": "UPSERT", "dataType": "steps"}},
            {"data": {"healthUserId": "fenced", "operation": "UPSERT", "dataType": "steps"}},
        ]

    with (
        patch(f"{module}.store_raw_payload") as store_raw_payload,
        patch(f"{module}.celery_app") as celery_app,
    ):
        celery_app.send_task.return_value = MagicMock(id="queued-task")
        assert handler.dispatch(db, payload) == {"status": "accepted"}

    stored_payload = store_raw_payload.call_args.kwargs["payload"]
    queued_payload = celery_app.send_task.call_args.kwargs["args"][1]
    assert stored_payload == queued_payload
    assert "allowed" in str(stored_payload)
    assert "fenced" not in str(stored_payload)


@pytest.mark.parametrize("case", _handler_cases(), ids=lambda case: case.provider)
def test_shared_provider_identity_with_active_accounts_is_dispatched(
    db: Session,
    case: WebhookCase,
) -> None:
    identity = "4242" if case.identity_kind == "provider_user_id" else "athlete-4242"
    for _ in range(2):
        user = UserFactory(health_write_state="active", health_source_policy="legacy-mixed")
        connection_kwargs = {
            "user": user,
            "provider": case.provider,
            "status": "active",
        }
        connection_kwargs[case.identity_kind] = identity
        UserConnectionFactory(**connection_kwargs)
    db.flush()

    with (
        patch(f"{case.module}.store_raw_payload") as store_raw_payload,
        patch(f"{case.module}.celery_app") as celery_app,
    ):
        celery_app.send_task.return_value = MagicMock(id="queued-task")
        result = case.handler.dispatch(db, case.payload_factory(identity))

    assert result == {"status": "accepted"}
    store_raw_payload.assert_called_once()
    celery_app.send_task.assert_called_once()


@pytest.mark.parametrize("case", _handler_cases(), ids=lambda case: case.provider)
def test_shared_provider_identity_with_fenced_account_is_not_dispatched(
    db: Session,
    case: WebhookCase,
) -> None:
    identity = "4242" if case.identity_kind == "provider_user_id" else "athlete-4242"
    active = UserFactory(health_write_state="active", health_source_policy="legacy-mixed")
    fenced = UserFactory(health_write_state="fenced", health_source_policy="legacy-mixed")
    for user in (active, fenced):
        connection_kwargs = {
            "user": user,
            "provider": case.provider,
            "status": "active",
        }
        connection_kwargs[case.identity_kind] = identity
        UserConnectionFactory(**connection_kwargs)
    db.flush()

    with (
        patch(f"{case.module}.store_raw_payload") as store_raw_payload,
        patch(f"{case.module}.celery_app") as celery_app,
    ):
        result = case.handler.dispatch(db, case.payload_factory(identity))

    assert result == {"status": "accepted"}
    store_raw_payload.assert_not_called()
    celery_app.send_task.assert_not_called()


def test_webhook_holds_identity_lock_through_raw_store_and_enqueue(
    session_factory: sessionmaker[Session],
) -> None:
    target_user_id = uuid4()
    attach_user_id = uuid4()
    identity = "424242"
    now = datetime.now(timezone.utc)
    with session_factory() as setup:
        for user_id, label in ((target_user_id, "target"), (attach_user_id, "attach")):
            setup.add(
                User(
                    id=user_id,
                    first_name=None,
                    last_name=None,
                    email=None,
                    external_user_id=f"webhook-lock-{label}-{user_id}",
                    health_evidence_generation=0,
                    health_write_state="active",
                    health_source_policy="legacy-mixed",
                )
            )
        setup.add(
            UserConnection(
                id=uuid4(),
                user_id=target_user_id,
                provider="whoop",
                provider_user_id=identity,
                provider_username=None,
                access_token="target-token",
                refresh_token=None,
                token_expires_at=None,
                scope=None,
                status=ConnectionStatus.ACTIVE,
                last_synced_at=None,
                created_at=now,
                updated_at=now,
            )
        )
        setup.commit()

    enqueue_entered = Event()
    release_enqueue = Event()

    def dispatch() -> dict[str, Any]:
        with session_factory() as webhook_session:
            return WhoopWebhookHandler(MagicMock(), MagicMock()).dispatch(
                webhook_session,
                {"user_id": int(identity), "id": "workout-1", "type": "workout.updated"},
            )

    def attach() -> None:
        with session_factory() as attach_session:
            UserConnectionRepository().create(
                attach_session,
                UserConnectionCreate(
                    user_id=attach_user_id,
                    provider="whoop",
                    provider_user_id=identity,
                    access_token="attach-token",
                ),
            )

    def block_enqueue(*_args: Any, **_kwargs: Any) -> MagicMock:
        enqueue_entered.set()
        assert release_enqueue.wait(timeout=5)
        return MagicMock(id="queued-task")

    try:
        with (
            patch("app.services.providers.whoop.webhook_handler.store_raw_payload") as store_raw_payload,
            patch("app.services.providers.whoop.webhook_handler.celery_app") as celery_app,
            ThreadPoolExecutor(max_workers=2) as executor,
        ):
            celery_app.send_task.side_effect = block_enqueue
            dispatch_future = executor.submit(dispatch)
            assert enqueue_entered.wait(timeout=5)
            attach_future = executor.submit(attach)
            with pytest.raises(TimeoutError):
                attach_future.result(timeout=0.25)
            release_enqueue.set()
            assert dispatch_future.result(timeout=5) == {"status": "accepted"}
            attach_future.result(timeout=5)
            store_raw_payload.assert_called_once()
    finally:
        release_enqueue.set()
        with session_factory() as cleanup:
            cleanup.query(User).filter(User.id.in_((target_user_id, attach_user_id))).delete()
            cleanup.commit()
