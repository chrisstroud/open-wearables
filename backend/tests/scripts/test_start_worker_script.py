"""Preserve default startup while supporting a stable release-owned worker name."""

import os
import subprocess
from pathlib import Path

import pytest

_SCRIPT_PATH = Path(__file__).resolve().parents[2] / "scripts" / "start" / "worker.sh"


@pytest.mark.parametrize("worker_name", [None, "", "ow-worker-1@production"])
def test_worker_name_is_optional_and_passed_as_one_argument(tmp_path: Path, worker_name: str | None) -> None:
    uv = tmp_path / "uv"
    uv.write_text("#!/bin/sh\nprintf '%s\\n' \"$@\"\n")
    uv.chmod(0o755)
    environment = {**os.environ, "PATH": f"{tmp_path}:/usr/bin:/bin"}
    environment.pop("CELERY_WORKER_NAME", None)
    if worker_name is not None:
        environment["CELERY_WORKER_NAME"] = worker_name
    result = subprocess.run(
        ["/bin/bash", str(_SCRIPT_PATH)], capture_output=True, text=True, env=environment, check=True
    )
    arguments = result.stdout.splitlines()
    assert arguments[:5] == ["run", "celery", "-A", "app.main:celery_app", "worker"]
    assert arguments[5:9] == [
        "--loglevel=info",
        "--pool=threads",
        "-Q",
        "default,sdk_sync,garmin_sync,webhook_sync",
    ]
    assert arguments[9:] == ([f"--hostname={worker_name}"] if worker_name else [])
