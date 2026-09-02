from datetime import datetime, timedelta, timezone
from hashlib import sha256
from unittest.mock import MagicMock, patch
from uuid import UUID, uuid4

import pytest
from httpx import Response
from jose import jwt
from sqlalchemy.orm import Session
from starlette.testclient import TestClient

from app.config import settings
from app.models import (
    RefreshToken,
    SDKBatchReceipt,
    SDKClientInstallation,
    SDKSyncWindowReceipt,
    SDKUploadInbox,
)
from app.services.sdk_batch_receipt_service import sdk_batch_receipt_service
from tests.factories import ApiKeyFactory, DeveloperFactory, UserFactory
from tests.utils import api_key_headers, developer_auth_headers

ACTIVATION_POLICY = {
    "purpose": "activation",
    "window_version": 2,
    "lower_bound_inclusive": "2026-07-26T04:00:00Z",
    "upper_bound_exclusive": "2026-08-25T04:00:00Z",
    "timezone": "America/Toronto",
    "completed_day_count": 30,
}


def registration(installation_id: UUID | None = None, *, build_number: str = "1") -> dict[str, object]:
    return {
        "installation_id": str(installation_id or uuid4()),
        "bundle_id": "fitness.dashboard.app",
        "app_version": "1.0.0",
        "build_number": build_number,
        "protocol_version": 2,
    }


def generate_code(client: TestClient, api_v1_prefix: str, *, user_id: UUID, developer_id: UUID) -> str:
    response = client.post(
        f"{api_v1_prefix}/users/{user_id}/invitation-code",
        headers=developer_auth_headers(developer_id),
        json={"activation_policy": ACTIVATION_POLICY},
    )
    assert response.status_code == 201
    return str(response.json()["code"])


def redeem(
    client: TestClient,
    api_v1_prefix: str,
    *,
    code: str,
    client_registration: dict[str, object],
) -> Response:
    return client.post(
        f"{api_v1_prefix}/invitation-code/redeem",
        json={"code": code, "client": client_registration},
    )


class TestSDKClientInstallationPairing:
    def test_redeem_registers_permanent_installation_and_scoped_tokens(
        self,
        client: TestClient,
        db: Session,
        api_v1_prefix: str,
    ) -> None:
        developer = DeveloperFactory()
        user = UserFactory()
        installation_id = uuid4()
        code = generate_code(client, api_v1_prefix, user_id=user.id, developer_id=developer.id)

        response = redeem(
            client,
            api_v1_prefix,
            code=code,
            client_registration=registration(installation_id),
        )

        assert response.status_code == 200
        body = response.json()
        assert body["installation_id"] == str(installation_id)
        claims = jwt.decode(
            body["access_token"],
            settings.secret_key,
            algorithms=[settings.algorithm],
            options={"verify_exp": False},
        )
        assert claims["app_id"] == f"dfi:{installation_id}:1"
        assert claims["installation_generation"] == 1
        assert claims["bundle_id"] == "fitness.dashboard.app"
        assert claims["app_version"] == "1.0.0"
        assert claims["build_number"] == "1"
        assert claims["protocol_version"] == 2
        assert claims["health_evidence_generation"] == 0
        row = db.query(SDKClientInstallation).filter_by(id=installation_id).one()
        assert row.user_id == user.id
        assert row.status == "active"
        assert row.generation == 1
        assert row.bundle_id == "fitness.dashboard.app"
        assert row.health_evidence_generation == 0
        refresh = db.query(RefreshToken).filter_by(id=body["refresh_token"]).one()
        assert refresh.app_id == row.app_id

    def test_redeem_rejects_non_release_identity_without_consuming_code(
        self,
        client: TestClient,
        db: Session,
        api_v1_prefix: str,
    ) -> None:
        developer = DeveloperFactory()
        user = UserFactory()
        code = generate_code(client, api_v1_prefix, user_id=user.id, developer_id=developer.id)
        invalid = registration()
        invalid["bundle_id"] = "fitness.dashboard.app.founderproof"

        response = redeem(client, api_v1_prefix, code=code, client_registration=invalid)

        assert response.status_code in {400, 422}
        retry = redeem(client, api_v1_prefix, code=code, client_registration=registration())
        assert retry.status_code == 200
        assert db.query(SDKClientInstallation).filter_by(user_id=user.id).count() == 1

    def test_replacement_revokes_old_installation_and_all_old_refresh_tokens(
        self,
        client: TestClient,
        db: Session,
        api_v1_prefix: str,
    ) -> None:
        developer = DeveloperFactory()
        user = UserFactory()
        old_id = uuid4()
        first = redeem(
            client,
            api_v1_prefix,
            code=generate_code(client, api_v1_prefix, user_id=user.id, developer_id=developer.id),
            client_registration=registration(old_id),
        )
        old_access = first.json()["access_token"]
        old_refresh = first.json()["refresh_token"]

        new_id = uuid4()
        second = redeem(
            client,
            api_v1_prefix,
            code=generate_code(client, api_v1_prefix, user_id=user.id, developer_id=developer.id),
            client_registration=registration(new_id, build_number="2"),
        )

        assert second.status_code == 200
        old = db.query(SDKClientInstallation).filter_by(id=old_id).one()
        new = db.query(SDKClientInstallation).filter_by(id=new_id).one()
        assert old.status == "revoked"
        assert old.revoked_at is not None
        assert new.status == "active"
        assert new.generation == 2
        assert db.query(RefreshToken).filter_by(id=old_refresh).one().revoked_at is not None

        fenced = client.get(
            f"{api_v1_prefix}/sdk/users/{user.id}/sync/{uuid4()}",
            headers={"Authorization": f"Bearer {old_access}"},
        )
        assert fenced.status_code == 401

    def test_first_class_pairing_fences_preexisting_legacy_invite_token(
        self,
        client: TestClient,
        api_v1_prefix: str,
    ) -> None:
        developer = DeveloperFactory()
        user = UserFactory()
        legacy_code = generate_code(client, api_v1_prefix, user_id=user.id, developer_id=developer.id)
        legacy = client.post(
            f"{api_v1_prefix}/invitation-code/redeem",
            json={"code": legacy_code},
        )
        assert legacy.status_code == 200

        before = client.get(
            f"{api_v1_prefix}/sdk/users/{user.id}/sync/{uuid4()}",
            headers={"Authorization": f"Bearer {legacy.json()['access_token']}"},
        )
        assert before.status_code == 404

        permanent = redeem(
            client,
            api_v1_prefix,
            code=generate_code(client, api_v1_prefix, user_id=user.id, developer_id=developer.id),
            client_registration=registration(),
        )
        assert permanent.status_code == 200

        after = client.get(
            f"{api_v1_prefix}/sdk/users/{user.id}/sync/{uuid4()}",
            headers={"Authorization": f"Bearer {legacy.json()['access_token']}"},
        )
        assert after.status_code == 401

    def test_health_generation_change_fences_access_before_jwt_expiry(
        self,
        client: TestClient,
        db: Session,
        api_v1_prefix: str,
    ) -> None:
        developer = DeveloperFactory()
        user = UserFactory()
        paired = redeem(
            client,
            api_v1_prefix,
            code=generate_code(client, api_v1_prefix, user_id=user.id, developer_id=developer.id),
            client_registration=registration(),
        )
        assert paired.status_code == 200

        user.health_evidence_generation += 1
        db.commit()
        fenced = client.get(
            f"{api_v1_prefix}/sdk/users/{user.id}/sync/{uuid4()}",
            headers={"Authorization": f"Bearer {paired.json()['access_token']}"},
        )

        assert fenced.status_code == 401

    def test_generation_change_invalidates_unredeemed_code(
        self,
        client: TestClient,
        db: Session,
        api_v1_prefix: str,
    ) -> None:
        developer = DeveloperFactory()
        user = UserFactory()
        code = generate_code(client, api_v1_prefix, user_id=user.id, developer_id=developer.id)
        user.health_evidence_generation += 1
        db.commit()

        response = redeem(
            client,
            api_v1_prefix,
            code=code,
            client_registration=registration(),
        )

        assert response.status_code == 404
        assert db.query(SDKClientInstallation).filter_by(user_id=user.id).count() == 0

    def test_generation_change_fences_legacy_token_without_waiting_for_repair(
        self,
        client: TestClient,
        db: Session,
        api_v1_prefix: str,
    ) -> None:
        developer = DeveloperFactory()
        user = UserFactory()
        code = generate_code(client, api_v1_prefix, user_id=user.id, developer_id=developer.id)
        legacy = client.post(f"{api_v1_prefix}/invitation-code/redeem", json={"code": code})
        assert legacy.status_code == 200

        user.health_evidence_generation = 1
        db.commit()
        fenced = client.get(
            f"{api_v1_prefix}/sdk/users/{user.id}/sync/{uuid4()}",
            headers={"Authorization": f"Bearer {legacy.json()['access_token']}"},
        )
        assert fenced.status_code == 401

    def test_repairing_same_installation_fences_prior_generation_token(
        self,
        client: TestClient,
        api_v1_prefix: str,
    ) -> None:
        developer = DeveloperFactory()
        user = UserFactory()
        installation_id = uuid4()
        first = redeem(
            client,
            api_v1_prefix,
            code=generate_code(client, api_v1_prefix, user_id=user.id, developer_id=developer.id),
            client_registration=registration(installation_id),
        )
        second = redeem(
            client,
            api_v1_prefix,
            code=generate_code(client, api_v1_prefix, user_id=user.id, developer_id=developer.id),
            client_registration=registration(installation_id, build_number="2"),
        )
        assert second.status_code == 200

        old = client.get(
            f"{api_v1_prefix}/sdk/users/{user.id}/sync/{uuid4()}",
            headers={"Authorization": f"Bearer {first.json()['access_token']}"},
        )
        current = client.get(
            f"{api_v1_prefix}/sdk/users/{user.id}/sync/{uuid4()}",
            headers={"Authorization": f"Bearer {second.json()['access_token']}"},
        )
        assert old.status_code == 401
        assert current.status_code == 404

    def test_post_reset_account_requires_v2_pairing_and_never_reopens_legacy_writers(
        self,
        client: TestClient,
        db: Session,
        api_v1_prefix: str,
    ) -> None:
        developer = DeveloperFactory()
        api_key = ApiKeyFactory(developer=developer)
        user = UserFactory(
            health_evidence_generation=1,
            health_write_state="awaiting-v2-pairing",
            health_source_policy="apple-mobile-v2-only",
        )
        installation_id = uuid4()
        code = generate_code(client, api_v1_prefix, user_id=user.id, developer_id=developer.id)

        legacy_redeem = client.post(
            f"{api_v1_prefix}/invitation-code/redeem",
            json={"code": code},
        )
        paired = redeem(
            client,
            api_v1_prefix,
            code=code,
            client_registration=registration(installation_id),
        )

        assert legacy_redeem.status_code == 426
        assert paired.status_code == 200
        claims = jwt.decode(
            paired.json()["access_token"],
            settings.secret_key,
            algorithms=[settings.algorithm],
            options={"verify_exp": False},
        )
        assert claims["health_evidence_generation"] == 1
        installation = db.query(SDKClientInstallation).filter_by(id=installation_id).one()
        assert installation.health_evidence_generation == 1
        db.refresh(user)
        assert user.health_write_state == "activating"
        assert user.health_source_policy == "apple-mobile-v2-only"

        api_key_upload = client.post(
            f"{api_v1_prefix}/sdk/users/{user.id}/sync",
            headers={
                **api_key_headers(api_key.id),
                "X-Open-Wearables-Batch-ID": str(uuid4()),
            },
            json={
                "provider": "apple",
                "sdkVersion": "1.0.0",
                "syncTimestamp": "2026-08-26T12:00:00Z",
                "data": {"records": [], "sleep": [], "workouts": [], "deletions": []},
            },
        )
        assert api_key_upload.status_code == 403

        # Even after activation transitions the account back to an active
        # write state, the generation/source-policy fence is irreversible.
        user.health_write_state = "active"
        db.commit()
        legacy_mint = client.post(
            f"{api_v1_prefix}/users/{user.id}/token",
            headers=developer_auth_headers(developer.id),
        )
        assert legacy_mint.status_code == 409


class TestSDKClientInstallationManagement:
    def test_api_key_lists_safe_projection_and_revoke_is_idempotent(
        self,
        client: TestClient,
        api_v1_prefix: str,
    ) -> None:
        developer = DeveloperFactory()
        api_key = ApiKeyFactory(developer=developer)
        user = UserFactory()
        installation_id = uuid4()
        paired = redeem(
            client,
            api_v1_prefix,
            code=generate_code(client, api_v1_prefix, user_id=user.id, developer_id=developer.id),
            client_registration=registration(installation_id),
        )
        assert paired.status_code == 200

        listed = client.get(
            f"{api_v1_prefix}/users/{user.id}/sdk-installations",
            headers=api_key_headers(api_key.id),
        )
        assert listed.status_code == 200
        assert len(listed.json()) == 1
        projection = listed.json()[0]
        assert projection["id"] == str(installation_id)
        assert projection["generation"] == 1
        assert projection["health_evidence_generation"] == 0
        assert projection["status"] == "active"
        assert projection["recent_history_ready_at"] is None
        assert projection["archive_earliest_confirmed_at"] is None
        assert "app_id" not in projection
        assert "refresh_token" not in projection

        endpoint = f"{api_v1_prefix}/users/{user.id}/sdk-installations/{installation_id}/revoke"
        first = client.post(
            endpoint,
            headers=api_key_headers(api_key.id),
            json={"expected_generation": 1, "expected_health_evidence_generation": 0},
        )
        second = client.post(
            endpoint,
            headers=api_key_headers(api_key.id),
            json={"expected_generation": 1, "expected_health_evidence_generation": 0},
        )
        assert first.status_code == 200
        assert second.status_code == 200
        assert second.json()["status"] == "revoked"

        fenced = client.get(
            f"{api_v1_prefix}/sdk/users/{user.id}/sync/{uuid4()}",
            headers={"Authorization": f"Bearer {paired.json()['access_token']}"},
        )
        assert fenced.status_code == 401

    def test_dashboard_revoke_rejects_stale_generation_after_same_installation_repair(
        self,
        client: TestClient,
        api_v1_prefix: str,
    ) -> None:
        developer = DeveloperFactory()
        api_key = ApiKeyFactory(developer=developer)
        user = UserFactory()
        installation_id = uuid4()
        first = redeem(
            client,
            api_v1_prefix,
            code=generate_code(client, api_v1_prefix, user_id=user.id, developer_id=developer.id),
            client_registration=registration(installation_id),
        )
        assert first.status_code == 200
        repaired = redeem(
            client,
            api_v1_prefix,
            code=generate_code(client, api_v1_prefix, user_id=user.id, developer_id=developer.id),
            client_registration=registration(installation_id, build_number="2"),
        )
        assert repaired.status_code == 200

        endpoint = f"{api_v1_prefix}/users/{user.id}/sdk-installations/{installation_id}/revoke"
        stale = client.post(
            endpoint,
            headers=api_key_headers(api_key.id),
            json={"expected_generation": 1, "expected_health_evidence_generation": 0},
        )
        listed = client.get(
            f"{api_v1_prefix}/users/{user.id}/sdk-installations",
            headers=api_key_headers(api_key.id),
        )

        assert stale.status_code == 409
        assert listed.status_code == 200
        assert listed.json()[0]["generation"] == 2
        assert listed.json()[0]["status"] == "active"

    def test_refresh_atomically_rotates_token_and_updates_exact_installation_metadata(
        self,
        client: TestClient,
        db: Session,
        api_v1_prefix: str,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        developer = DeveloperFactory()
        user = UserFactory()
        installation_id = uuid4()
        paired = redeem(
            client,
            api_v1_prefix,
            code=generate_code(client, api_v1_prefix, user_id=user.id, developer_id=developer.id),
            client_registration=registration(installation_id),
        )
        original_access = paired.json()["access_token"]
        original_refresh = paired.json()["refresh_token"]
        refreshed_client = registration(installation_id, build_number="2")
        refreshed_client["app_version"] = "1.1.0"

        with caplog.at_level("DEBUG"):
            refreshed = client.post(
                f"{api_v1_prefix}/token/refresh",
                json={"refresh_token": original_refresh, "client": refreshed_client},
            )

        assert refreshed.status_code == 200
        body = refreshed.json()
        assert body["refresh_token"] != original_refresh
        claims = jwt.decode(
            body["access_token"],
            settings.secret_key,
            algorithms=[settings.algorithm],
            options={"verify_exp": False},
        )
        assert claims["app_version"] == "1.1.0"
        assert claims["build_number"] == "2"
        row = db.query(SDKClientInstallation).filter_by(id=installation_id).one()
        assert row.app_version == "1.1.0"
        assert row.build_number == "2"
        assert db.query(RefreshToken).filter_by(id=original_refresh).one().revoked_at is not None
        assert original_refresh not in caplog.text
        assert body["refresh_token"] not in caplog.text

        old_access_result = client.get(
            f"{api_v1_prefix}/sdk/users/{user.id}/sync/{uuid4()}",
            headers={"Authorization": f"Bearer {original_access}"},
        )
        current_access_result = client.get(
            f"{api_v1_prefix}/sdk/users/{user.id}/sync/{uuid4()}",
            headers={"Authorization": f"Bearer {body['access_token']}"},
        )
        assert old_access_result.status_code == 401
        assert current_access_result.status_code == 404

    def test_metadata_mismatch_does_not_consume_refresh_token(
        self,
        client: TestClient,
        api_v1_prefix: str,
    ) -> None:
        developer = DeveloperFactory()
        user = UserFactory()
        installation_id = uuid4()
        paired = redeem(
            client,
            api_v1_prefix,
            code=generate_code(client, api_v1_prefix, user_id=user.id, developer_id=developer.id),
            client_registration=registration(installation_id),
        )
        refresh_token = paired.json()["refresh_token"]
        wrong = registration(uuid4(), build_number="2")
        rejected = client.post(
            f"{api_v1_prefix}/token/refresh",
            json={"refresh_token": refresh_token, "client": wrong},
        )
        accepted = client.post(
            f"{api_v1_prefix}/token/refresh",
            json={
                "refresh_token": refresh_token,
                "client": registration(installation_id, build_number="2"),
            },
        )

        assert rejected.status_code == 401
        assert accepted.status_code == 200

    def test_installation_readiness_uses_only_exact_receipt_generation(
        self,
        client: TestClient,
        db: Session,
        api_v1_prefix: str,
    ) -> None:
        developer = DeveloperFactory()
        api_key = ApiKeyFactory(developer=developer)
        user = UserFactory()
        installation_id = uuid4()
        paired = redeem(
            client,
            api_v1_prefix,
            code=generate_code(client, api_v1_prefix, user_id=user.id, developer_id=developer.id),
            client_registration=registration(installation_id),
        )
        assert paired.status_code == 200
        installation = db.query(SDKClientInstallation).filter_by(id=installation_id).one()
        now = datetime.now(timezone.utc)

        def add_window(
            *,
            purpose: str,
            lower: datetime,
            upper: datetime,
            accepted_at: datetime,
            generation: int,
        ) -> None:
            window_id = uuid4()
            sdk_batch_receipt_service.prepare_submission(
                db,
                batch_id=window_id,
                user_id=user.id,
                installation_id=installation_id,
                installation_generation=generation,
                health_evidence_generation=0,
                provider="apple",
                payload_sha256=sha256(str(window_id).encode()).hexdigest(),
            )
            db.add(
                SDKSyncWindowReceipt(
                    id=window_id,
                    user_id=user.id,
                    installation_id=installation_id,
                    installation_generation=generation,
                    health_evidence_generation=0,
                    provider="apple",
                    manifest_sha256=sha256(f"{purpose}:{window_id}".encode()).hexdigest(),
                    purpose=purpose,
                    window_version=2,
                    lower_bound_inclusive=lower,
                    upper_bound_exclusive=upper,
                    batch_ids=[],
                    empty_or_no_access_types=["HKQuantityTypeIdentifierBodyMass"],
                    reconciliation_start_inclusive=None,
                    reconciliation_end_exclusive=None,
                    accepted_at=accepted_at,
                )
            )
            db.commit()

        activation_lower = now - timedelta(days=30)
        add_window(
            purpose="activation",
            lower=activation_lower,
            upper=now,
            accepted_at=now - timedelta(minutes=2),
            generation=installation.generation,
        )
        archive_lower = activation_lower - timedelta(days=31)
        add_window(
            purpose="archive",
            lower=archive_lower,
            upper=activation_lower,
            accepted_at=now - timedelta(minutes=1),
            generation=installation.generation,
        )

        listed = client.get(
            f"{api_v1_prefix}/users/{user.id}/sdk-installations",
            headers=api_key_headers(api_key.id),
        )
        assert listed.status_code == 200
        projection = listed.json()[0]
        assert projection["recent_history_ready_at"] is not None
        assert datetime.fromisoformat(projection["archive_earliest_confirmed_at"]) == archive_lower

        repaired = redeem(
            client,
            api_v1_prefix,
            code=generate_code(client, api_v1_prefix, user_id=user.id, developer_id=developer.id),
            client_registration=registration(installation_id, build_number="2"),
        )
        assert repaired.status_code == 200
        after_repair = client.get(
            f"{api_v1_prefix}/users/{user.id}/sdk-installations",
            headers=api_key_headers(api_key.id),
        ).json()[0]
        assert after_repair["generation"] == 2
        assert after_repair["recent_history_ready_at"] is None
        assert after_repair["archive_earliest_confirmed_at"] is None

    def test_phone_self_revoke_returns_same_terminal_projection_on_retry(
        self,
        client: TestClient,
        api_v1_prefix: str,
    ) -> None:
        developer = DeveloperFactory()
        user = UserFactory()
        paired = redeem(
            client,
            api_v1_prefix,
            code=generate_code(client, api_v1_prefix, user_id=user.id, developer_id=developer.id),
            client_registration=registration(),
        )
        headers = {"Authorization": f"Bearer {paired.json()['access_token']}"}
        endpoint = f"{api_v1_prefix}/sdk/users/{user.id}/installation/revoke"

        first = client.post(endpoint, headers=headers)
        retry = client.post(endpoint, headers=headers)

        assert first.status_code == 200
        assert retry.status_code == 200
        assert first.json() == retry.json()
        assert retry.json() == {
            "installation_id": paired.json()["installation_id"],
            "status": "revoked",
            "revoked_at": retry.json()["revoked_at"],
        }
        assert retry.json()["revoked_at"]

        expired_claims = jwt.decode(
            paired.json()["access_token"],
            settings.secret_key,
            algorithms=[settings.algorithm],
            options={"verify_exp": False},
        )
        expired_claims["exp"] = datetime.now(timezone.utc) - timedelta(minutes=1)
        expired_access_token = jwt.encode(expired_claims, settings.secret_key, algorithm=settings.algorithm)
        expired_retry = client.post(
            endpoint,
            headers={"Authorization": f"Bearer {expired_access_token}"},
        )

        assert expired_retry.status_code == 200
        assert expired_retry.json() == first.json()

        mismatched_claims = {**expired_claims, "installation_generation": expired_claims["installation_generation"] + 1}
        mismatched_token = jwt.encode(mismatched_claims, settings.secret_key, algorithm=settings.algorithm)
        mismatched_retry = client.post(
            endpoint,
            headers={"Authorization": f"Bearer {mismatched_token}"},
        )
        wrong_signature_token = jwt.encode(expired_claims, "wrong-signing-key", algorithm=settings.algorithm)
        wrong_signature_retry = client.post(
            endpoint,
            headers={"Authorization": f"Bearer {wrong_signature_token}"},
        )

        assert mismatched_retry.status_code == 401
        assert wrong_signature_retry.status_code == 401

    def test_expired_phone_token_cannot_revoke_an_active_installation(
        self,
        client: TestClient,
        db: Session,
        api_v1_prefix: str,
    ) -> None:
        developer = DeveloperFactory()
        user = UserFactory()
        paired = redeem(
            client,
            api_v1_prefix,
            code=generate_code(client, api_v1_prefix, user_id=user.id, developer_id=developer.id),
            client_registration=registration(),
        )
        claims = jwt.decode(
            paired.json()["access_token"],
            settings.secret_key,
            algorithms=[settings.algorithm],
            options={"verify_exp": False},
        )
        claims["exp"] = datetime.now(timezone.utc) - timedelta(minutes=1)
        expired_access_token = jwt.encode(claims, settings.secret_key, algorithm=settings.algorithm)

        response = client.post(
            f"{api_v1_prefix}/sdk/users/{user.id}/installation/revoke",
            headers={"Authorization": f"Bearer {expired_access_token}"},
        )
        normal_sdk_response = client.get(
            f"{api_v1_prefix}/sdk/users/{user.id}/sync/{uuid4()}",
            headers={"Authorization": f"Bearer {expired_access_token}"},
        )

        assert response.status_code == 401
        assert normal_sdk_response.status_code == 401
        installation = db.get(SDKClientInstallation, UUID(paired.json()["installation_id"]))
        assert installation is not None
        assert installation.status == "active"
        assert installation.revoked_at is None

    def test_api_key_cannot_upload_for_first_class_mobile_account(
        self,
        client: TestClient,
        api_v1_prefix: str,
    ) -> None:
        developer = DeveloperFactory()
        api_key = ApiKeyFactory(developer=developer)
        user = UserFactory()
        paired = redeem(
            client,
            api_v1_prefix,
            code=generate_code(client, api_v1_prefix, user_id=user.id, developer_id=developer.id),
            client_registration=registration(),
        )
        assert paired.status_code == 200

        response = client.post(
            f"{api_v1_prefix}/sdk/users/{user.id}/sync",
            headers={
                **api_key_headers(api_key.id),
                "X-Open-Wearables-Batch-ID": str(uuid4()),
            },
            json={
                "provider": "apple",
                "sdkVersion": "1.0.0",
                "syncTimestamp": "2026-08-26T12:00:00Z",
                "data": {"records": [], "sleep": [], "workouts": [], "deletions": []},
            },
        )

        assert response.status_code == 403

        installation_id = UUID(paired.json()["installation_id"])
        revoked = client.post(
            f"{api_v1_prefix}/users/{user.id}/sdk-installations/{installation_id}/revoke",
            headers=api_key_headers(api_key.id),
            json={"expected_generation": 1, "expected_health_evidence_generation": 0},
        )
        assert revoked.status_code == 200
        still_blocked = client.post(
            f"{api_v1_prefix}/sdk/users/{user.id}/sync",
            headers={
                **api_key_headers(api_key.id),
                "X-Open-Wearables-Batch-ID": str(uuid4()),
            },
            json={
                "provider": "apple",
                "sdkVersion": "1.0.0",
                "syncTimestamp": "2026-08-26T12:00:00Z",
                "data": {"records": [], "sleep": [], "workouts": [], "deletions": []},
            },
        )
        assert still_blocked.status_code == 403

    @patch("app.api.routes.v1.sdk_sync.process_sdk_upload.delay")
    def test_installation_is_bound_into_receipt_and_terminal_contact(
        self,
        dispatch: MagicMock,
        client: TestClient,
        db: Session,
        api_v1_prefix: str,
    ) -> None:
        developer = DeveloperFactory()
        user = UserFactory()
        installation_id = uuid4()
        paired = redeem(
            client,
            api_v1_prefix,
            code=generate_code(client, api_v1_prefix, user_id=user.id, developer_id=developer.id),
            client_registration=registration(installation_id),
        )
        batch_id = uuid4()

        queued = client.post(
            f"{api_v1_prefix}/sdk/users/{user.id}/sync",
            headers={
                "Authorization": f"Bearer {paired.json()['access_token']}",
                "X-Open-Wearables-Batch-ID": str(batch_id),
            },
            json={
                "provider": "apple",
                "sdkVersion": "1.0.0",
                "syncTimestamp": "2026-08-26T12:00:00Z",
                "data": {"records": [], "sleep": [], "workouts": [], "deletions": []},
            },
        )

        assert queued.status_code == 202
        dispatch.assert_called_once_with(
            batch_id=str(batch_id),
            require_terminal_receipt=True,
        )
        receipt = db.query(SDKBatchReceipt).filter_by(id=batch_id).one()
        assert receipt.installation_id == installation_id
        assert receipt.installation_generation == 1
        assert receipt.health_evidence_generation == 0
        inbox = db.query(SDKUploadInbox).filter_by(id=batch_id).one()
        assert inbox.installation_id == installation_id
        assert inbox.installation_generation == 1
        assert inbox.health_evidence_generation == 0
        assert inbox.user_id == user.id
        assert "records" in inbox.content
        claim = sdk_batch_receipt_service.claim_for_processing(db, batch_id)
        sdk_batch_receipt_service.mark_succeeded(
            db,
            batch_id=batch_id,
            attempt_count=claim.attempt_count or 1,
            result={"status_code": 200},
        )
        installation = db.query(SDKClientInstallation).filter_by(id=installation_id).one()
        assert installation.last_terminal_receipt_at is not None
        assert db.query(SDKUploadInbox).filter_by(id=batch_id).one_or_none() is None

    @patch("app.api.routes.v1.sdk_sync.process_sdk_upload.delay")
    def test_replacement_phone_cannot_read_prior_installation_receipt(
        self,
        dispatch: MagicMock,
        client: TestClient,
        api_v1_prefix: str,
    ) -> None:
        developer = DeveloperFactory()
        user = UserFactory()
        first = redeem(
            client,
            api_v1_prefix,
            code=generate_code(client, api_v1_prefix, user_id=user.id, developer_id=developer.id),
            client_registration=registration(),
        )
        batch_id = uuid4()
        queued = client.post(
            f"{api_v1_prefix}/sdk/users/{user.id}/sync",
            headers={
                "Authorization": f"Bearer {first.json()['access_token']}",
                "X-Open-Wearables-Batch-ID": str(batch_id),
            },
            json={
                "provider": "apple",
                "sdkVersion": "1.0.0",
                "syncTimestamp": "2026-08-26T12:00:00Z",
                "data": {"records": [], "sleep": [], "workouts": [], "deletions": []},
            },
        )
        assert queued.status_code == 202

        replacement = redeem(
            client,
            api_v1_prefix,
            code=generate_code(client, api_v1_prefix, user_id=user.id, developer_id=developer.id),
            client_registration=registration(),
        )
        assert replacement.status_code == 200
        hidden = client.get(
            f"{api_v1_prefix}/sdk/users/{user.id}/sync/{batch_id}",
            headers={"Authorization": f"Bearer {replacement.json()['access_token']}"},
        )
        assert hidden.status_code == 404
