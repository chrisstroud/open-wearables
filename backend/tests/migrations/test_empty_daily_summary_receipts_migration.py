import importlib.util
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import sqlalchemy as sa
from sqlalchemy.engine import Connection

MIGRATION_PATH = (
    Path(__file__).parents[2]
    / "migrations"
    / "versions"
    / "2026_09_01_2100-a3c5e7f9b1d2_allow_empty_daily_summary_receipts.py"
)


class FakeAlembicOp:
    def __init__(self, connection: Connection) -> None:
        self.connection = connection
        self.mutations: list[tuple[str, tuple[Any, ...]]] = []

    def get_bind(self) -> Connection:
        return self.connection

    def __getattr__(self, name: str) -> Any:
        def record(*args: Any, **_kwargs: Any) -> None:
            self.mutations.append((name, args))

        return record


def _load_migration() -> Any:
    spec = importlib.util.spec_from_file_location("empty_daily_summary_receipts_migration", MIGRATION_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def migration_connection() -> Iterator[Connection]:
    engine = sa.create_engine("sqlite://")
    with engine.begin() as connection:
        connection.execute(
            sa.text(
                """
                CREATE TABLE sdk_batch_receipt (
                    id INTEGER PRIMARY KEY,
                    daily_summaries_saved INTEGER NOT NULL DEFAULT 0,
                    revision_set_digest TEXT
                )
                """
            )
        )
        yield connection
    engine.dispose()


def test_migration_extends_daily_summary_head_and_replaces_constraint(migration_connection: Connection) -> None:
    migration = _load_migration()
    fake_op = FakeAlembicOp(migration_connection)
    migration.op = fake_op

    migration.upgrade()

    assert migration.down_revision == "f1a2b3c4d5e6"
    assert fake_op.mutations[0] == (
        "drop_constraint",
        ("ck_sdk_batch_receipt_revision_set_digest_state", "sdk_batch_receipt"),
    )
    assert fake_op.mutations[1][0] == "create_check_constraint"
    assert migration.EMPTY_REVISION_SET_DIGEST in fake_op.mutations[1][1][2]


def test_downgrade_refuses_an_accepted_empty_revision_receipt(migration_connection: Connection) -> None:
    migration = _load_migration()
    migration_connection.execute(
        sa.text(
            """
            INSERT INTO sdk_batch_receipt (id, daily_summaries_saved, revision_set_digest)
            VALUES (1, 0, :digest)
            """
        ),
        {"digest": migration.EMPTY_REVISION_SET_DIGEST},
    )
    fake_op = FakeAlembicOp(migration_connection)
    migration.op = fake_op

    with pytest.raises(RuntimeError, match="cannot downgrade after an empty daily-summary receipt"):
        migration.downgrade()

    assert fake_op.mutations == []


def test_downgrade_restores_previous_constraint_when_no_empty_receipt_exists(
    migration_connection: Connection,
) -> None:
    migration = _load_migration()
    fake_op = FakeAlembicOp(migration_connection)
    migration.op = fake_op

    migration.downgrade()

    assert [mutation[0] for mutation in fake_op.mutations] == ["drop_constraint", "create_check_constraint"]
