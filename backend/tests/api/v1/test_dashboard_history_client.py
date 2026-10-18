"""Opt-in cross-repository contract check using the actual Dashboard TS client.

Set DASHBOARD_HISTORY_CLIENT_CHECKOUT to its checkout and put Node 22 on PATH.
The listener forwards HTTP to real FastAPI handlers and the test PostgreSQL DB.
WHOOP and Celery delivery are synthetic; this is not a live-provider certificate.
"""

import os
import subprocess
from contextlib import nullcontext
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from threading import Thread
from unittest.mock import patch
from uuid import UUID

import pytest
from fastapi.testclient import TestClient
from sqlalchemy.orm import Session

from app.integrations.celery.tasks.whoop_history_task import sync_whoop_history
from tests.factories import ApiKeyFactory, UserConnectionFactory


@pytest.mark.skipif(
    not os.environ.get("DASHBOARD_HISTORY_CLIENT_CHECKOUT"), reason="Optional Dashboard checkout required"
)
def test_actual_dashboard_history_client(client: TestClient, db: Session) -> None:
    checkout = Path(os.environ["DASHBOARD_HISTORY_CLIENT_CHECKOUT"]).resolve()
    script = checkout / "scripts/certify-provider-history-client.ts"
    assert script.is_file()
    connection = UserConnectionFactory(provider="whoop")
    key = ApiKeyFactory()
    queued: list[UUID] = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args: object) -> None:
            pass

        def forward(self) -> None:
            if self.command == "GET" and "/sync/full-history/" in self.path and queued:
                with (
                    patch(
                        "app.integrations.celery.tasks.whoop_history_task.SessionLocal", return_value=nullcontext(db)
                    ),
                    patch(
                        "app.services.providers.whoop.workouts.WhoopWorkouts._make_api_request",
                        return_value={"records": []},
                    ),
                    patch(
                        "app.services.providers.whoop.data_247.Whoop247Data._make_api_request",
                        return_value={"records": []},
                    ),
                ):
                    sync_whoop_history(str(queued.pop(0)))
            length = int(self.headers.get("Content-Length", 0))
            assert length <= 4096
            content = self.rfile.read(length) if length else None
            response = client.request(
                self.command,
                self.path,
                content=content,
                headers={
                    "X-Open-Wearables-API-Key": self.headers.get("X-Open-Wearables-API-Key", ""),
                    "Content-Type": "application/json",
                },
            )
            self.send_response(response.status_code)
            self.send_header("Content-Type", response.headers.get("Content-Type", "application/json"))
            self.send_header("Content-Length", str(len(response.content)))
            self.end_headers()
            self.wfile.write(response.content)

        do_GET = do_POST = do_DELETE = forward  # noqa: N815 - stdlib HTTP handler hooks

    with (
        HTTPServer(("127.0.0.1", 0), Handler) as server,
        patch(
            "app.services.whoop_history_service.publish_history_dispatch",
            side_effect=lambda dispatch, _: queued.append(dispatch),
        ),
        patch("app.services.providers.whoop.oauth.WhoopOAuth.deregister_user") as deregister,
    ):
        thread = Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            result = subprocess.run(
                ["node", "--import", "tsx", str(script)],
                cwd=checkout,
                capture_output=True,
                text=True,
                timeout=30,
                env={
                    **os.environ,
                    "OW_LOCAL_CONTRACT_FIXTURE": "synthetic-only",
                    "OW_LOCAL_CONTRACT_URL": f"http://127.0.0.1:{server.server_port}",
                    "OW_LOCAL_CONTRACT_USER": str(connection.user_id),
                    "OW_LOCAL_CONTRACT_CONNECTION": str(connection.id),
                    "OW_LOCAL_CONTRACT_KEY": str(key.id),
                    "NODE_OPTIONS": "--conditions=react-server",
                },
            )
            assert result.returncode == 0, result.stderr
            assert "PASS: actual Dashboard client" in result.stdout
            deregister.assert_called_once()
        finally:
            server.shutdown()
            thread.join(timeout=5)
