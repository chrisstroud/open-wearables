"""Exact WHOOP grant revocation, without legacy 401/404 success inference."""

from collections.abc import Iterable

import httpx

from app.models import UserConnection

WHOOP_API_ROOT = "https://api.prod.whoop.com/developer/v2"


class AccountErasureProviderFence:
    def __init__(self, transport: httpx.BaseTransport | None = None) -> None:
        self.transport = transport

    def deregister(self, connections: Iterable[UserConnection], *, already_verified: bool) -> None:
        rows = sorted(connections, key=lambda row: (row.provider, str(row.id)))
        if already_verified:
            if any(row.access_token or row.refresh_token or row.status != "revoked" for row in rows):
                raise RuntimeError("Verified account-erasure credential state changed")
            return
        with httpx.Client(transport=self.transport, timeout=15.0, follow_redirects=False) as client:
            for connection in rows:
                if connection.provider == "apple" and not connection.access_token and not connection.refresh_token:
                    continue
                if connection.provider != "whoop" or not connection.access_token or not connection.provider_user_id:
                    raise RuntimeError("Account-erasure provider revocation requires verified credentials")
                headers = {"Authorization": f"Bearer {connection.access_token}"}
                try:
                    profile = client.get(f"{WHOOP_API_ROOT}/user/profile/basic", headers=headers)
                    if profile.status_code != 200:
                        raise RuntimeError("Account-erasure provider identity is unavailable")
                    identity = profile.json()
                    if (
                        not isinstance(identity, dict)
                        or type(identity.get("user_id")) not in {str, int}
                        or (str(identity["user_id"]) != connection.provider_user_id)
                    ):
                        raise RuntimeError("Account-erasure provider identity does not match")
                    result = client.delete(f"{WHOOP_API_ROOT}/user/access", headers=headers)
                    if result.status_code != 204:
                        # Expired/invalid credentials and a lost successful revoke
                        # are indistinguishable here. Both require reconciliation,
                        # never a fabricated terminal receipt.
                        raise RuntimeError("Account-erasure provider revocation is unverified")
                except (httpx.HTTPError, ValueError) as exc:
                    raise RuntimeError("Account-erasure provider revocation is unavailable") from exc


account_erasure_provider_fence = AccountErasureProviderFence()
