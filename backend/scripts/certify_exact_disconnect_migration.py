"""Verify upgrade/backfill/replay in an exclusively owned local database."""

import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import unquote, urlsplit
from uuid import uuid4

import psycopg
from psycopg import sql


def main() -> None:
    admin_url = os.environ.get("OW_LIFECYCLE_TEST_ADMIN_URL", "")
    parsed = urlsplit(admin_url)
    if (
        parsed.scheme not in {"postgres", "postgresql"}
        or parsed.hostname not in {"127.0.0.1", "::1"}
        or parsed.path != "/postgres"
        or parsed.query
        or parsed.fragment
        or not parsed.username
    ):
        raise RuntimeError("OW_LIFECYCLE_TEST_ADMIN_URL must identify a local postgres admin database")
    name = "ow_exact_disconnect_migration"
    backend = Path(__file__).resolve().parents[1]
    env = {
        **os.environ,
        "SECRET_KEY": "synthetic-migration-test-secret",
        "MASTER_KEY": "dGVzdC1tYXN0ZXIta2V5LWZvci10ZXN0aW5nLW9ubHk=",
        "DB_HOST": parsed.hostname,
        "DB_PORT": str(parsed.port or 5432),
        "DB_USER": unquote(parsed.username),
        "DB_PASSWORD": unquote(parsed.password or ""),
        "DB_NAME": name,
    }

    def migrate(revision: str) -> None:
        subprocess.run([sys.executable, "-m", "alembic", "upgrade", revision], cwd=backend, env=env, check=True)

    with psycopg.connect(admin_url, autocommit=True) as admin:
        if admin.execute("SELECT 1 FROM pg_database WHERE datname=%s", (name,)).fetchone():
            raise RuntimeError("Refuse to replace an existing migration database")
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(name)))
    try:
        migrate("f7a9b1c3d5e7")
        with psycopg.connect(admin_url, dbname=name) as db:
            for status in ["active", "revoked"]:
                owner = uuid4()
                db.execute('INSERT INTO "user" (id) VALUES (%s)', (owner,))
                db.execute(
                    "INSERT INTO user_connection (id,user_id,provider,status,access_token,updated_at) "
                    "VALUES (%s,%s,%s,%s,%s,NOW())",
                    (uuid4(), owner, "whoop", status, "synthetic-untouched" if status == "active" else None),
                )
        migrate("a8b0c2d4e6f8")
        migrate("a8b0c2d4e6f8")
        with psycopg.connect(admin_url, dbname=name) as db:
            rows = db.execute(
                "SELECT status,authorization_generation,disconnect_confirmed_generation,access_token "
                "FROM user_connection ORDER BY status"
            ).fetchall()
            assert rows == [("active", 1, None, "synthetic-untouched"), ("revoked", 1, None, None)]
            for statement in [
                "UPDATE user_connection SET authorization_generation=0",
                "UPDATE user_connection SET disconnect_confirmed_generation=2",
            ]:
                try:
                    with db.transaction():
                        db.execute(statement)
                except psycopg.errors.CheckViolation:
                    pass
                else:
                    raise AssertionError("Invalid authorization generation accepted")
            assert db.execute("SELECT version_num FROM alembic_version").fetchone() == ("a8b0c2d4e6f8",)
        print(
            "PASS: migration chain, active/revoked backfill, replay and constraints; no fabricated disconnect receipt"
        )
        migrate("b9c1d3e5f7a9")
        with psycopg.connect(admin_url, dbname=name) as db:
            assert db.execute("SELECT count(*) FROM whoop_history_dispatch").fetchone() == (0,)
            active_connection = db.execute("SELECT user_id,id FROM user_connection WHERE status='active'").fetchone()
            assert active_connection is not None
            owner, connection = active_connection
            dispatch, task = uuid4(), uuid4()
            insert = (
                "INSERT INTO whoop_history_dispatch "
                "(id,user_id,connection_id,authorization_generation,health_evidence_generation,task_id,"
                "requested_start_at,requested_end_at,status) "
                "VALUES (%s,%s,%s,1,0,%s,'2000-01-01Z','2026-01-01Z','queued')"
            )
            db.execute(insert, (dispatch, owner, connection, task))
            for statement in [
                "UPDATE whoop_history_dispatch SET authorization_generation=0",
                "UPDATE whoop_history_dispatch SET requested_end_at=requested_start_at",
                "UPDATE whoop_history_dispatch SET status='succeeded'",
                "UPDATE whoop_history_dispatch SET status='running'",
            ]:
                try:
                    with db.transaction():
                        db.execute(statement)
                except psycopg.errors.CheckViolation:
                    pass
                else:
                    raise AssertionError("Invalid history receipt accepted")
            try:
                with db.transaction():
                    db.execute(insert, (uuid4(), owner, connection, uuid4()))
            except psycopg.errors.UniqueViolation:
                pass
            else:
                raise AssertionError("Concurrent active histories accepted for one user")
        migrate("b9c1d3e5f7a9")
        with psycopg.connect(admin_url, dbname=name) as db:
            assert db.execute("SELECT id,task_id,status FROM whoop_history_dispatch").fetchall() == [
                (dispatch, task, "queued")
            ]
            assert db.execute("SELECT version_num FROM alembic_version").fetchone() == ("b9c1d3e5f7a9",)
        print("PASS: additive history receipt table, replay preservation, state/window/uniqueness constraints")
    finally:
        with psycopg.connect(admin_url, autocommit=True) as admin:
            admin.execute(sql.SQL("DROP DATABASE {}").format(sql.Identifier(name)))
        print("Owned migration database removed")


if __name__ == "__main__":
    main()
