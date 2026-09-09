from datetime import datetime, timezone
from uuid import uuid4

import httpx
import pytest

from app.models import UserConnection
from app.services.account_erasure_provider_fence import WHOOP_API_ROOT, AccountErasureProviderFence


def connection() -> UserConnection:
    return UserConnection(
        id=uuid4(),
        user_id=uuid4(),
        provider="whoop",
        provider_user_id="123",
        access_token="synthetic-access",
        refresh_token="synthetic-refresh",
        status="active",
        updated_at=datetime.now(timezone.utc),
    )


def test_verifies_exact_provider_identity_before_authoritative_revocation() -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        assert request.headers["authorization"] == "Bearer synthetic-access"
        return httpx.Response(200, json={"user_id": 123}) if request.method == "GET" else httpx.Response(204)

    AccountErasureProviderFence(httpx.MockTransport(handler)).deregister([connection()], already_verified=False)
    assert [(request.method, str(request.url)) for request in calls] == [
        ("GET", f"{WHOOP_API_ROOT}/user/profile/basic"),
        ("DELETE", f"{WHOOP_API_ROOT}/user/access"),
    ]


@pytest.mark.parametrize("status", [200, 202, 301, 307, 401, 404, 500])
def test_non_204_revoke_never_proves_completion(status: int) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"user_id": 123}) if request.method == "GET" else httpx.Response(status)

    target = connection()
    with pytest.raises(RuntimeError):
        AccountErasureProviderFence(httpx.MockTransport(handler)).deregister([target], already_verified=False)
    assert target.access_token == "synthetic-access"
    assert target.refresh_token == "synthetic-refresh"


@pytest.mark.parametrize("identity", [{"user_id": 456}, {"user_id": True}, {}, None])
def test_unknown_or_other_provider_identity_never_reaches_delete(identity: object) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        return httpx.Response(200, json=identity)

    with pytest.raises(RuntimeError):
        AccountErasureProviderFence(httpx.MockTransport(handler)).deregister([connection()], already_verified=False)
    assert calls == ["GET"]


@pytest.mark.parametrize("status", [301, 401, 404, 500])
def test_unavailable_identity_never_reaches_delete(status: int) -> None:
    calls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request.method)
        return httpx.Response(status)

    with pytest.raises(RuntimeError):
        AccountErasureProviderFence(httpx.MockTransport(handler)).deregister([connection()], already_verified=False)
    assert calls == ["GET"]


def test_only_durable_verified_cleared_credentials_can_skip_provider_io() -> None:
    target = connection()
    target.access_token = None
    target.refresh_token = None
    target.status = "revoked"
    with pytest.raises(RuntimeError):
        AccountErasureProviderFence().deregister([target], already_verified=False)
    AccountErasureProviderFence().deregister([target], already_verified=True)
    target.refresh_token = "changed"
    with pytest.raises(RuntimeError):
        AccountErasureProviderFence().deregister([target], already_verified=True)


def test_unknown_provider_is_not_silently_skipped() -> None:
    target = connection()
    target.provider = "another-provider"
    with pytest.raises(RuntimeError):
        AccountErasureProviderFence().deregister([target], already_verified=False)


def test_offline_apple_records_require_no_cloud_revocation() -> None:
    target = connection()
    target.provider = "apple"
    target.access_token = None
    target.refresh_token = None
    AccountErasureProviderFence().deregister([target], already_verified=False)
