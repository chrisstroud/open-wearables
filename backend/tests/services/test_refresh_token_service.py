"""
Unit tests for refresh token service.
"""

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import pytest
from fastapi import HTTPException
from sqlalchemy.orm import Session, sessionmaker

from app.models import RefreshToken, User
from app.schemas.auth import TokenType
from app.services.refresh_token_service import refresh_token_service
from tests.factories import DeveloperFactory, UserFactory


class TestCreateSDKRefreshToken:
    """Tests for create_sdk_refresh_token."""

    def test_create_sdk_refresh_token_format(self, db: Session) -> None:
        """SDK refresh token should have rt- prefix and be 35 characters total."""
        # Arrange
        user = UserFactory()
        app_id = "test_app_123"

        # Act
        token = refresh_token_service.create_sdk_refresh_token(db, user.id, app_id)

        # Assert
        assert token.startswith("rt-")
        assert len(token) == 35  # "rt-" (3) + 32 hex chars

    def test_create_sdk_refresh_token_stored_in_db(self, db: Session) -> None:
        """SDK refresh token should be stored in database with correct metadata."""
        # Arrange
        user = UserFactory()
        app_id = "test_app_123"

        # Act
        token = refresh_token_service.create_sdk_refresh_token(db, user.id, app_id)

        # Assert
        db_token = db.query(RefreshToken).filter(RefreshToken.id == token).first()
        assert db_token is not None
        assert db_token.token_type == TokenType.SDK
        assert db_token.user_id == user.id
        assert db_token.app_id == app_id
        assert db_token.developer_id is None
        assert db_token.revoked_at is None


class TestCreateDeveloperRefreshToken:
    """Tests for create_developer_refresh_token."""

    def test_create_developer_refresh_token_format(self, db: Session) -> None:
        """Developer refresh token should have rt- prefix and be 35 characters total."""
        # Arrange
        developer = DeveloperFactory()

        # Act
        token = refresh_token_service.create_developer_refresh_token(db, developer.id)

        # Assert
        assert token.startswith("rt-")
        assert len(token) == 35  # "rt-" (3) + 32 hex chars

    def test_create_developer_refresh_token_stored_in_db(self, db: Session) -> None:
        """Developer refresh token should be stored in database with correct metadata."""
        # Arrange
        developer = DeveloperFactory()

        # Act
        token = refresh_token_service.create_developer_refresh_token(db, developer.id)

        # Assert
        db_token = db.query(RefreshToken).filter(RefreshToken.id == token).first()
        assert db_token is not None
        assert db_token.token_type == TokenType.DEVELOPER
        assert db_token.developer_id == developer.id
        assert db_token.user_id is None
        assert db_token.app_id is None
        assert db_token.revoked_at is None


class TestRefreshToken:
    """Tests for refresh_token method."""

    def test_refresh_sdk_token_success(self, db: Session) -> None:
        """Refreshing SDK token should return new access token and rotated refresh token."""
        # Arrange
        user = UserFactory()
        app_id = "test_app_123"
        refresh_token = refresh_token_service.create_sdk_refresh_token(db, user.id, app_id)

        # Act
        result = refresh_token_service.refresh_token(db, refresh_token)

        # Assert
        assert result.access_token is not None
        assert result.token_type == "bearer"
        # Refresh token should be rotated
        assert result.refresh_token != refresh_token
        assert result.refresh_token.startswith("rt-")

    def test_refresh_developer_token_success(self, db: Session) -> None:
        """Refreshing developer token should return new access token and rotated refresh token."""
        # Arrange
        developer = DeveloperFactory()
        refresh_token = refresh_token_service.create_developer_refresh_token(db, developer.id)

        # Act
        result = refresh_token_service.refresh_token(db, refresh_token)

        # Assert
        assert result.access_token is not None
        assert result.token_type == "bearer"
        # Refresh token should be rotated
        assert result.refresh_token != refresh_token
        assert result.refresh_token.startswith("rt-")

    def test_refresh_invalid_token_raises_401(self, db: Session) -> None:
        """Refreshing invalid token should raise 401."""
        # Arrange
        invalid_token = "rt-invalidtoken12345678901234567890"

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            refresh_token_service.refresh_token(db, invalid_token)

        assert exc_info.value.status_code == 401
        assert exc_info.value.detail == "Invalid or revoked refresh token"

    def test_refresh_revoked_token_raises_401(self, db: Session) -> None:
        """Refreshing revoked token should raise 401."""
        # Arrange
        user = UserFactory()
        refresh_token = refresh_token_service.create_sdk_refresh_token(db, user.id, "test_app")
        refresh_token_service.revoke_token(db, refresh_token)

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            refresh_token_service.refresh_token(db, refresh_token)

        assert exc_info.value.status_code == 401

    def test_refresh_revokes_old_token(self, db: Session) -> None:
        """Refreshing token should revoke the old token (rotation)."""
        # Arrange
        user = UserFactory()
        old_refresh_token = refresh_token_service.create_sdk_refresh_token(db, user.id, "test_app")

        # Verify not revoked initially
        db_token = db.query(RefreshToken).filter(RefreshToken.id == old_refresh_token).first()
        assert db_token is not None
        assert db_token.revoked_at is None

        # Act
        result = refresh_token_service.refresh_token(db, old_refresh_token)

        # Assert - old token is revoked
        db.refresh(db_token)
        assert db_token.revoked_at is not None

        # Assert - new token exists and is not revoked
        new_db_token = db.query(RefreshToken).filter(RefreshToken.id == result.refresh_token).first()
        assert new_db_token is not None
        assert new_db_token.revoked_at is None

    def test_concurrent_sdk_refresh_allows_exactly_one_rotation(
        self,
        session_factory: sessionmaker[Session],
    ) -> None:
        """Two replays of one SDK refresh credential cannot both mint replacements."""
        user_id = uuid4()
        with session_factory() as setup:
            setup.add(
                User(
                    id=user_id,
                    first_name=None,
                    last_name=None,
                    email=None,
                    external_user_id=f"refresh-race-{user_id}",
                    health_evidence_generation=0,
                    health_write_state="active",
                    health_source_policy="legacy-mixed",
                )
            )
            setup.commit()
            original = refresh_token_service.create_sdk_refresh_token(
                setup,
                user_id,
                "refresh-race-proof",
            )

        start = Barrier(2)

        def rotate() -> tuple[int, str | None]:
            with session_factory() as worker:
                start.wait(timeout=5)
                try:
                    response = refresh_token_service.refresh_token(worker, original)
                except HTTPException as exc:
                    worker.rollback()
                    return exc.status_code, None
                return 200, response.refresh_token

        try:
            with ThreadPoolExecutor(max_workers=2) as executor:
                results = list(executor.map(lambda _index: rotate(), range(2)))

            assert sorted(status_code for status_code, _token in results) == [200, 401]
            replacement_ids = [token for status_code, token in results if status_code == 200]
            assert len(replacement_ids) == 1
            assert replacement_ids[0] is not None

            with session_factory() as verify:
                original_row = verify.query(RefreshToken).filter_by(id=original).one()
                assert original_row.revoked_at is not None
                active = (
                    verify.query(RefreshToken)
                    .filter(
                        RefreshToken.user_id == user_id,
                        RefreshToken.revoked_at.is_(None),
                    )
                    .all()
                )
                assert [row.id for row in active] == replacement_ids
        finally:
            with session_factory() as cleanup:
                cleanup.query(RefreshToken).filter(RefreshToken.user_id == user_id).delete()
                cleanup.query(User).filter(User.id == user_id).delete()
                cleanup.commit()


class TestRevokeToken:
    """Tests for revoke_token method."""

    def test_revoke_token_success(self, db: Session) -> None:
        """Revoking token should set revoked_at timestamp."""
        # Arrange
        user = UserFactory()
        refresh_token = refresh_token_service.create_sdk_refresh_token(db, user.id, "test_app")

        # Act
        result = refresh_token_service.revoke_token(db, refresh_token)

        # Assert
        assert result is True
        db_token = db.query(RefreshToken).filter(RefreshToken.id == refresh_token).first()
        assert db_token is not None
        assert db_token.revoked_at is not None

    def test_revoke_nonexistent_token_raises_404(self, db: Session) -> None:
        """Revoking non-existent token should raise 404."""
        # Arrange
        invalid_token = "rt-nonexistent123456789012345678"

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            refresh_token_service.revoke_token(db, invalid_token)

        assert exc_info.value.status_code == 404

    def test_revoke_already_revoked_token_raises_404(self, db: Session) -> None:
        """Revoking already revoked token should raise 404."""
        # Arrange
        user = UserFactory()
        refresh_token = refresh_token_service.create_sdk_refresh_token(db, user.id, "test_app")
        refresh_token_service.revoke_token(db, refresh_token)

        # Act & Assert
        with pytest.raises(HTTPException) as exc_info:
            refresh_token_service.revoke_token(db, refresh_token)

        assert exc_info.value.status_code == 404
