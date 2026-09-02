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
    / "2026_09_01_1800-f1a2b3c4d5e6_apple_health_daily_summaries.py"
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
    spec = importlib.util.spec_from_file_location("apple_health_daily_summaries_migration", MIGRATION_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_daily_summary_migration_extends_the_applied_multi_source_head() -> None:
    migration = _load_migration()

    assert migration.down_revision == "f7a9b1c3d5e7"


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
        connection.execute(sa.text("CREATE TABLE apple_health_daily_summary (id INTEGER PRIMARY KEY)"))
        yield connection
    engine.dispose()


@pytest.mark.parametrize(
    "seed_sql",
    [
        "INSERT INTO apple_health_daily_summary (id) VALUES (1)",
        "INSERT INTO sdk_batch_receipt (id, daily_summaries_saved) VALUES (1, 1)",
        "INSERT INTO sdk_batch_receipt (id, revision_set_digest) VALUES (1, 'acknowledged-digest')",
    ],
    ids=["summary-row", "acknowledged-count", "acknowledged-digest"],
)
def test_downgrade_refuses_populated_daily_summary_state(
    migration_connection: Connection,
    seed_sql: str,
) -> None:
    migration_connection.execute(sa.text(seed_sql))
    migration = _load_migration()
    fake_op = FakeAlembicOp(migration_connection)
    migration.op = fake_op

    with pytest.raises(RuntimeError, match="f1a2b3c4d5e6 is forward-only"):
        migration.downgrade()

    assert fake_op.mutations == []


def test_downgrade_allows_empty_daily_summary_state(migration_connection: Connection) -> None:
    migration = _load_migration()
    fake_op = FakeAlembicOp(migration_connection)
    migration.op = fake_op

    migration.downgrade()

    assert ("drop_table", ("apple_health_daily_summary",)) in fake_op.mutations
    assert ("drop_column", ("sdk_batch_receipt", "daily_summaries_saved")) in fake_op.mutations
