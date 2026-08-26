"""sdk client installations

Revision ID: e6f8a0b2c4d5
Revises: c4d6e8f0a2b3
"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

revision: str = "e6f8a0b2c4d5"
down_revision: str | None = "c4d6e8f0a2b3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.add_column(
        "api_key",
        sa.Column(
            "scopes",
            postgresql.JSONB(),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
    )
    op.add_column(
        "user",
        sa.Column("health_evidence_generation", sa.Integer(), server_default="0", nullable=False),
    )
    op.add_column(
        "user",
        sa.Column("health_write_state", sa.String(length=32), server_default="active", nullable=False),
    )
    op.add_column(
        "user",
        sa.Column("health_source_policy", sa.String(length=32), server_default="legacy-mixed", nullable=False),
    )
    op.add_column("user", sa.Column("health_reset_operation_id", sa.UUID(), nullable=True))
    op.add_column("user", sa.Column("health_reset_manifest_sha256", sa.String(length=64), nullable=True))
    op.add_column("user", sa.Column("health_reset_manifest_counts", postgresql.JSONB(), nullable=True))
    op.add_column("user", sa.Column("health_reset_deleted_counts", postgresql.JSONB(), nullable=True))
    op.add_column("user", sa.Column("health_reset_applied_at", sa.DateTime(timezone=True), nullable=True))
    op.create_check_constraint(
        "ck_user_health_write_state",
        "user",
        "health_write_state IN ('active', 'fenced', 'awaiting-v2-pairing', 'activating')",
    )
    op.create_check_constraint(
        "ck_user_health_source_policy",
        "user",
        "health_source_policy IN ('legacy-mixed', 'apple-mobile-v2-only')",
    )
    op.create_check_constraint(
        "ck_user_health_evidence_generation",
        "user",
        "health_evidence_generation >= 0",
    )
    op.add_column(
        "user_invitation_code",
        sa.Column("health_evidence_generation", sa.Integer(), server_default="0", nullable=False),
    )
    op.add_column(
        "refresh_token",
        sa.Column("health_evidence_generation", sa.Integer(), nullable=True),
    )

    op.create_table(
        "sdk_source_reset_seal",
        sa.Column("operation_id", sa.UUID(), nullable=False),
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("health_evidence_generation", sa.Integer(), nullable=False),
        sa.Column("inventory_digest_sha256", sa.String(length=64), nullable=False),
        sa.Column("configuration_digest_sha256", sa.String(length=64), nullable=False),
        sa.Column("resource_counts", postgresql.JSONB(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "health_evidence_generation >= 0",
            name="ck_sdk_source_reset_seal_generation",
        ),
        sa.CheckConstraint(
            "inventory_digest_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_sdk_source_reset_seal_digest",
        ),
        sa.CheckConstraint(
            "configuration_digest_sha256 ~ '^[0-9a-f]{64}$'",
            name="ck_sdk_source_reset_seal_configuration_digest",
        ),
        sa.CheckConstraint(
            "jsonb_typeof(resource_counts) = 'object'",
            name="ck_sdk_source_reset_seal_counts_object",
        ),
        sa.ForeignKeyConstraint(["user_id"], ["user.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("operation_id"),
    )
    op.create_index(
        "uq_sdk_source_reset_seal_user_generation",
        "sdk_source_reset_seal",
        ["user_id", "health_evidence_generation"],
        unique=True,
    )
    op.execute(
        """
        CREATE FUNCTION reject_sdk_source_reset_seal_update()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION 'sdk_source_reset_seal rows are immutable';
        END;
        $$
        """
    )
    op.execute(
        """
        CREATE TRIGGER sdk_source_reset_seal_immutable
        BEFORE UPDATE ON sdk_source_reset_seal
        FOR EACH ROW
        EXECUTE FUNCTION reject_sdk_source_reset_seal_update()
        """
    )

    op.create_table(
        "sdk_client_installation",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("app_id", sa.String(length=64), nullable=False),
        sa.Column("bundle_id", sa.String(length=100), nullable=False),
        sa.Column("app_version", sa.String(length=32), nullable=False),
        sa.Column("build_number", sa.String(length=32), nullable=False),
        sa.Column("protocol_version", sa.Integer(), nullable=False),
        sa.Column("activation_policy", postgresql.JSONB(), nullable=True),
        sa.Column("health_evidence_generation", sa.Integer(), nullable=False),
        sa.Column("generation", sa.Integer(), nullable=False),
        sa.Column("status", sa.String(length=32), nullable=False),
        sa.Column("connected_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_contact_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("last_terminal_receipt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "status IN ('active', 'revoked')",
            name="ck_sdk_client_installation_status",
        ),
        sa.CheckConstraint("generation > 0", name="ck_sdk_client_installation_generation"),
        sa.CheckConstraint("protocol_version > 0", name="ck_sdk_client_installation_protocol_version"),
        sa.ForeignKeyConstraint(["user_id"], ["user.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index(
        "uq_sdk_client_installation_app_id",
        "sdk_client_installation",
        ["app_id"],
        unique=True,
    )
    op.create_index(
        "uq_sdk_client_installation_user_generation",
        "sdk_client_installation",
        ["user_id", "generation"],
        unique=True,
    )
    op.create_index(
        "uq_sdk_client_installation_active_user",
        "sdk_client_installation",
        ["user_id"],
        unique=True,
        postgresql_where=sa.text("status = 'active'"),
    )

    op.drop_constraint("uq_sdk_sleep_inbox_identity", "sdk_sleep_inbox", type_="unique")
    op.drop_constraint("ck_sdk_sleep_inbox_status", "sdk_sleep_inbox", type_="check")
    op.add_column("sdk_sleep_inbox", sa.Column("installation_id", sa.UUID(), nullable=True))
    op.add_column("sdk_sleep_inbox", sa.Column("installation_generation", sa.Integer(), nullable=True))
    op.add_column("sdk_sleep_inbox", sa.Column("health_evidence_generation", sa.Integer(), nullable=True))
    op.create_foreign_key(
        "fk_sdk_sleep_inbox_installation_id",
        "sdk_sleep_inbox",
        "sdk_client_installation",
        ["installation_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_check_constraint(
        "ck_sdk_sleep_inbox_installation_scope",
        "sdk_sleep_inbox",
        "(installation_id IS NULL AND installation_generation IS NULL "
        "AND health_evidence_generation IS NULL) OR "
        "(installation_id IS NOT NULL AND installation_generation > 0 "
        "AND health_evidence_generation >= 0)",
    )
    op.create_check_constraint(
        "ck_sdk_sleep_inbox_status",
        "sdk_sleep_inbox",
        "status IN ('staged', 'projecting', 'projected', 'materialized', 'quarantined')",
    )
    op.create_index(
        "uq_sdk_sleep_inbox_identity_generation",
        "sdk_sleep_inbox",
        ["user_id", "provider", "external_id", "health_evidence_generation"],
        unique=True,
        postgresql_nulls_not_distinct=True,
    )

    op.add_column("sdk_batch_receipt", sa.Column("installation_id", sa.UUID(), nullable=True))
    op.add_column("sdk_batch_receipt", sa.Column("installation_generation", sa.Integer(), nullable=True))
    op.add_column(
        "sdk_batch_receipt",
        sa.Column("health_evidence_generation", sa.Integer(), nullable=True),
    )
    op.add_column(
        "sdk_batch_receipt",
        sa.Column("content_lower_bound_inclusive", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "sdk_batch_receipt",
        sa.Column("content_upper_bound_exclusive", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "sdk_batch_receipt",
        sa.Column(
            "covered_type_identifiers",
            postgresql.JSONB(),
            server_default=sa.text("'[]'::jsonb"),
            nullable=False,
        ),
    )
    op.create_foreign_key(
        "fk_sdk_batch_receipt_installation_id",
        "sdk_batch_receipt",
        "sdk_client_installation",
        ["installation_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_index(
        "ix_sdk_batch_receipt_installation_id",
        "sdk_batch_receipt",
        ["installation_id"],
    )
    op.create_check_constraint(
        "ck_sdk_batch_receipt_installation_scope",
        "sdk_batch_receipt",
        "(installation_id IS NULL AND installation_generation IS NULL "
        "AND health_evidence_generation IS NULL) OR "
        "(installation_id IS NOT NULL AND installation_generation > 0 "
        "AND health_evidence_generation >= 0)",
    )
    op.create_check_constraint(
        "ck_sdk_batch_receipt_content_bounds",
        "sdk_batch_receipt",
        "(content_lower_bound_inclusive IS NULL AND content_upper_bound_exclusive IS NULL) OR "
        "(content_lower_bound_inclusive IS NOT NULL AND content_upper_bound_exclusive IS NOT NULL "
        "AND content_lower_bound_inclusive <= content_upper_bound_exclusive)",
    )

    op.create_table(
        "sdk_upload_inbox",
        sa.Column("id", sa.UUID(), nullable=False),
        sa.Column("user_id", sa.UUID(), nullable=False),
        sa.Column("installation_id", sa.UUID(), nullable=True),
        sa.Column("installation_generation", sa.Integer(), nullable=True),
        sa.Column("health_evidence_generation", sa.Integer(), nullable=True),
        sa.Column("provider", sa.String(length=32), nullable=False),
        sa.Column("payload_sha256", sa.String(length=64), nullable=False),
        sa.Column("content_type", sa.String(length=100), nullable=False),
        sa.Column("content_size_bytes", sa.Integer(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.CheckConstraint(
            "(installation_id IS NULL AND installation_generation IS NULL "
            "AND health_evidence_generation IS NULL) OR "
            "(installation_id IS NOT NULL AND installation_generation > 0 "
            "AND health_evidence_generation >= 0)",
            name="ck_sdk_upload_inbox_installation_scope",
        ),
        sa.CheckConstraint("content_size_bytes > 0", name="ck_sdk_upload_inbox_content_size"),
        sa.ForeignKeyConstraint(["id"], ["sdk_batch_receipt.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["user_id"], ["user.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(
            ["installation_id"],
            ["sdk_client_installation.id"],
            ondelete="RESTRICT",
        ),
        sa.PrimaryKeyConstraint("id"),
    )
    op.create_index("ix_sdk_upload_inbox_user_id", "sdk_upload_inbox", ["user_id"])
    op.create_index("ix_sdk_upload_inbox_expires_at", "sdk_upload_inbox", ["expires_at"])
    op.create_index(
        "ix_sdk_upload_inbox_installation_id",
        "sdk_upload_inbox",
        ["installation_id"],
    )

    op.add_column("sdk_sync_window_receipt", sa.Column("installation_id", sa.UUID(), nullable=True))
    op.add_column("sdk_sync_window_receipt", sa.Column("installation_generation", sa.Integer(), nullable=True))
    op.add_column(
        "sdk_sync_window_receipt",
        sa.Column("health_evidence_generation", sa.Integer(), nullable=True),
    )
    op.create_foreign_key(
        "fk_sdk_sync_window_receipt_installation_id",
        "sdk_sync_window_receipt",
        "sdk_client_installation",
        ["installation_id"],
        ["id"],
        ondelete="RESTRICT",
    )
    op.create_index(
        "ix_sdk_sync_window_receipt_installation_id",
        "sdk_sync_window_receipt",
        ["installation_id"],
    )
    op.create_check_constraint(
        "ck_sdk_sync_window_receipt_installation_scope",
        "sdk_sync_window_receipt",
        "(installation_id IS NULL AND installation_generation IS NULL "
        "AND health_evidence_generation IS NULL) OR "
        "(installation_id IS NOT NULL AND installation_generation > 0 "
        "AND health_evidence_generation >= 0)",
    )


def downgrade() -> None:
    bind = op.get_bind()
    populated = bind.execute(
        sa.text(
            """
            SELECT
              EXISTS (SELECT 1 FROM sdk_client_installation) OR
              EXISTS (SELECT 1 FROM sdk_upload_inbox) OR
              EXISTS (SELECT 1 FROM sdk_source_reset_seal) OR
              EXISTS (
                SELECT 1 FROM sdk_batch_receipt
                WHERE installation_id IS NOT NULL
                   OR installation_generation IS NOT NULL
                   OR health_evidence_generation IS NOT NULL
                   OR content_lower_bound_inclusive IS NOT NULL
                   OR content_upper_bound_exclusive IS NOT NULL
                   OR jsonb_array_length(covered_type_identifiers) > 0
              ) OR
              EXISTS (
                SELECT 1 FROM sdk_sync_window_receipt
                WHERE installation_id IS NOT NULL
                   OR installation_generation IS NOT NULL
                   OR health_evidence_generation IS NOT NULL
              ) OR
              EXISTS (
                SELECT 1 FROM sdk_sleep_inbox
                WHERE installation_id IS NOT NULL
                   OR installation_generation IS NOT NULL
                   OR health_evidence_generation IS NOT NULL
                   OR status = 'quarantined'
              ) OR
              EXISTS (
                SELECT 1 FROM "user"
                WHERE health_evidence_generation <> 0
                   OR health_write_state <> 'active'
                   OR health_source_policy <> 'legacy-mixed'
                   OR health_reset_operation_id IS NOT NULL
                   OR health_reset_manifest_sha256 IS NOT NULL
                   OR health_reset_manifest_counts IS NOT NULL
                   OR health_reset_deleted_counts IS NOT NULL
                   OR health_reset_applied_at IS NOT NULL
              ) OR
              EXISTS (SELECT 1 FROM user_invitation_code WHERE health_evidence_generation <> 0) OR
              EXISTS (SELECT 1 FROM refresh_token WHERE health_evidence_generation IS NOT NULL) OR
              EXISTS (SELECT 1 FROM api_key WHERE jsonb_array_length(scopes) > 0)
            """
        )
    ).scalar_one()
    if populated:
        raise RuntimeError("e6f8a0b2c4d5 is forward-only after first-class mobile/reset state has been populated")

    op.execute("DROP TRIGGER sdk_source_reset_seal_immutable ON sdk_source_reset_seal")
    op.execute("DROP FUNCTION reject_sdk_source_reset_seal_update()")
    op.drop_index("uq_sdk_source_reset_seal_user_generation", table_name="sdk_source_reset_seal")
    op.drop_table("sdk_source_reset_seal")

    op.drop_index("ix_sdk_upload_inbox_expires_at", table_name="sdk_upload_inbox")
    op.drop_index("ix_sdk_upload_inbox_installation_id", table_name="sdk_upload_inbox")
    op.drop_index("ix_sdk_upload_inbox_user_id", table_name="sdk_upload_inbox")
    op.drop_table("sdk_upload_inbox")

    op.drop_constraint(
        "ck_sdk_sync_window_receipt_installation_scope",
        "sdk_sync_window_receipt",
        type_="check",
    )
    op.drop_index("ix_sdk_sync_window_receipt_installation_id", table_name="sdk_sync_window_receipt")
    op.drop_constraint(
        "fk_sdk_sync_window_receipt_installation_id",
        "sdk_sync_window_receipt",
        type_="foreignkey",
    )
    op.drop_column("sdk_sync_window_receipt", "installation_id")
    op.drop_column("sdk_sync_window_receipt", "installation_generation")
    op.drop_column("sdk_sync_window_receipt", "health_evidence_generation")

    op.drop_constraint(
        "ck_sdk_batch_receipt_content_bounds",
        "sdk_batch_receipt",
        type_="check",
    )
    op.drop_constraint(
        "ck_sdk_batch_receipt_installation_scope",
        "sdk_batch_receipt",
        type_="check",
    )
    op.drop_index("ix_sdk_batch_receipt_installation_id", table_name="sdk_batch_receipt")
    op.drop_constraint(
        "fk_sdk_batch_receipt_installation_id",
        "sdk_batch_receipt",
        type_="foreignkey",
    )
    op.drop_column("sdk_batch_receipt", "installation_id")
    op.drop_column("sdk_batch_receipt", "installation_generation")
    op.drop_column("sdk_batch_receipt", "health_evidence_generation")
    op.drop_column("sdk_batch_receipt", "content_lower_bound_inclusive")
    op.drop_column("sdk_batch_receipt", "content_upper_bound_exclusive")
    op.drop_column("sdk_batch_receipt", "covered_type_identifiers")

    op.drop_index("uq_sdk_sleep_inbox_identity_generation", table_name="sdk_sleep_inbox")
    op.drop_constraint("ck_sdk_sleep_inbox_status", "sdk_sleep_inbox", type_="check")
    op.drop_constraint("ck_sdk_sleep_inbox_installation_scope", "sdk_sleep_inbox", type_="check")
    op.drop_constraint("fk_sdk_sleep_inbox_installation_id", "sdk_sleep_inbox", type_="foreignkey")
    op.drop_column("sdk_sleep_inbox", "health_evidence_generation")
    op.drop_column("sdk_sleep_inbox", "installation_generation")
    op.drop_column("sdk_sleep_inbox", "installation_id")
    op.create_check_constraint(
        "ck_sdk_sleep_inbox_status",
        "sdk_sleep_inbox",
        "status IN ('staged', 'projecting', 'projected', 'materialized')",
    )
    op.create_unique_constraint(
        "uq_sdk_sleep_inbox_identity",
        "sdk_sleep_inbox",
        ["user_id", "provider", "external_id"],
    )

    op.drop_index("uq_sdk_client_installation_active_user", table_name="sdk_client_installation")
    op.drop_index("uq_sdk_client_installation_user_generation", table_name="sdk_client_installation")
    op.drop_index("uq_sdk_client_installation_app_id", table_name="sdk_client_installation")
    op.drop_table("sdk_client_installation")

    op.drop_column("refresh_token", "health_evidence_generation")
    op.drop_column("user_invitation_code", "health_evidence_generation")
    op.drop_constraint("ck_user_health_evidence_generation", "user", type_="check")
    op.drop_constraint("ck_user_health_source_policy", "user", type_="check")
    op.drop_constraint("ck_user_health_write_state", "user", type_="check")
    op.drop_column("user", "health_reset_applied_at")
    op.drop_column("user", "health_reset_deleted_counts")
    op.drop_column("user", "health_reset_manifest_counts")
    op.drop_column("user", "health_reset_manifest_sha256")
    op.drop_column("user", "health_reset_operation_id")
    op.drop_column("user", "health_source_policy")
    op.drop_column("user", "health_write_state")
    op.drop_column("user", "health_evidence_generation")
    op.drop_column("api_key", "scopes")
