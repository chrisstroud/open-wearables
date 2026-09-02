from uuid import UUID, uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy.orm import Session, sessionmaker

from app.models import SDKClientInstallation, User
from app.repositories.sdk_client_installation_repository import sdk_client_installation_repository
from app.schemas.model_crud.credentials.sdk_client_installation import SDKClientRegistration
from app.services.sdk_client_installation_service import sdk_client_installation_service


def _registration(installation_id: UUID, *, build_number: str) -> SDKClientRegistration:
    return SDKClientRegistration(
        installation_id=installation_id,
        bundle_id="fitness.dashboard.app",
        app_version="1.0.0",
        build_number=build_number,
        protocol_version=2,
    )


def test_stale_identity_map_cannot_revoke_repaired_installation(
    session_factory: sessionmaker[Session],
) -> None:
    user_id = uuid4()
    installation_id = uuid4()
    with session_factory() as setup:
        setup.add(
            User(
                id=user_id,
                first_name=None,
                last_name=None,
                email=None,
                external_user_id=f"revoke-race-{user_id}",
                health_evidence_generation=0,
                health_write_state="active",
                health_source_policy="legacy-mixed",
            )
        )
        setup.commit()
        original = sdk_client_installation_service.activate(
            setup,
            user_id=user_id,
            registration=_registration(installation_id, build_number="1"),
        )
        setup.commit()
        original_generation = original.generation

    try:
        with session_factory() as stale_request:
            cached = sdk_client_installation_repository.get(stale_request, installation_id)
            assert cached is not None
            assert cached.generation == original_generation

            with session_factory() as repair:
                repaired = sdk_client_installation_service.activate(
                    repair,
                    user_id=user_id,
                    registration=_registration(installation_id, build_number="2"),
                )
                repair.commit()
                repaired_generation = repaired.generation

            with pytest.raises(HTTPException) as raised:
                sdk_client_installation_service.revoke(
                    stale_request,
                    user_id=user_id,
                    installation_id=installation_id,
                    expected_generation=original_generation,
                    expected_health_evidence_generation=0,
                )
            assert raised.value.status_code == 409
            stale_request.rollback()

        with session_factory() as verify:
            current = verify.get(SDKClientInstallation, installation_id)
            assert current is not None
            assert current.status == "active"
            assert current.generation == repaired_generation
            assert current.build_number == "2"
    finally:
        with session_factory() as cleanup:
            cleanup.query(SDKClientInstallation).filter_by(id=installation_id).delete()
            cleanup.query(User).filter_by(id=user_id).delete()
            cleanup.commit()


def test_revoke_cannot_cross_a_newer_account_health_generation(
    session_factory: sessionmaker[Session],
) -> None:
    user_id = uuid4()
    installation_id = uuid4()
    with session_factory() as setup:
        setup.add(
            User(
                id=user_id,
                first_name=None,
                last_name=None,
                email=None,
                external_user_id=f"revoke-health-race-{user_id}",
                health_evidence_generation=0,
                health_write_state="active",
                health_source_policy="legacy-mixed",
            )
        )
        setup.commit()
        installation = sdk_client_installation_service.activate(
            setup,
            user_id=user_id,
            registration=_registration(installation_id, build_number="1"),
        )
        setup.commit()
        installation_generation = installation.generation

    try:
        with session_factory() as reset:
            user = reset.query(User).filter(User.id == user_id).with_for_update().one()
            user.health_evidence_generation = 1
            reset.commit()

        with session_factory() as stale_revoke:
            with pytest.raises(HTTPException) as raised:
                sdk_client_installation_service.revoke(
                    stale_revoke,
                    user_id=user_id,
                    installation_id=installation_id,
                    expected_generation=installation_generation,
                    expected_health_evidence_generation=0,
                )
            assert raised.value.status_code == 409
            stale_revoke.rollback()

        with session_factory() as verify:
            current = verify.get(SDKClientInstallation, installation_id)
            assert current is not None
            assert current.status == "active"
    finally:
        with session_factory() as cleanup:
            cleanup.query(SDKClientInstallation).filter_by(id=installation_id).delete()
            cleanup.query(User).filter_by(id=user_id).delete()
            cleanup.commit()


def test_stale_identity_map_cannot_activate_across_committed_reset_fence(
    session_factory: sessionmaker[Session],
) -> None:
    user_id = uuid4()
    installation_id = uuid4()
    with session_factory() as setup:
        setup.add(
            User(
                id=user_id,
                first_name=None,
                last_name=None,
                email=None,
                external_user_id=f"activate-fence-race-{user_id}",
                health_evidence_generation=0,
                health_write_state="active",
                health_source_policy="legacy-mixed",
            )
        )
        setup.commit()

    try:
        with session_factory() as stale_pairing:
            cached = stale_pairing.get(User, user_id)
            assert cached is not None
            assert cached.health_write_state == "active"

            with session_factory() as reset:
                current = reset.query(User).filter(User.id == user_id).with_for_update().one()
                current.health_write_state = "fenced"
                reset.commit()

            with pytest.raises(HTTPException) as raised:
                sdk_client_installation_service.activate(
                    stale_pairing,
                    user_id=user_id,
                    registration=_registration(installation_id, build_number="1"),
                )
            assert raised.value.status_code == 423
            stale_pairing.rollback()

        with session_factory() as verify:
            assert verify.get(SDKClientInstallation, installation_id) is None
    finally:
        with session_factory() as cleanup:
            cleanup.query(SDKClientInstallation).filter_by(id=installation_id).delete()
            cleanup.query(User).filter_by(id=user_id).delete()
            cleanup.commit()
