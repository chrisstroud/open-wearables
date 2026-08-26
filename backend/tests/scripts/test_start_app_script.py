"""Static contract tests for the backend startup script."""

from pathlib import Path

_SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "start" / "app.sh"


def test_fastapi_port_prefers_railway_then_api_port_then_default() -> None:
    script = _SCRIPT_PATH.read_text()

    assert script.count('--port "${PORT:-${API_PORT:-8000}}"') == 2
    assert '--port "${API_PORT:-8000}"' not in script
