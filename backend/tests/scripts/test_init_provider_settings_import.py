"""Cold-start regression tests for provider-settings initialization imports."""

import os
import subprocess
import sys
from pathlib import Path

_BACKEND_ROOT = Path(__file__).resolve().parents[2]
_TEST_MASTER_KEY = "dGVzdC1tYXN0ZXIta2V5LWZvci10ZXN0aW5nLW9ubHk="


def _run_cold_import(source: str) -> subprocess.CompletedProcess[str]:
    environment = os.environ.copy()
    environment["SECRET_KEY"] = "cold-start-import-test-secret"
    environment["MASTER_KEY"] = _TEST_MASTER_KEY
    return subprocess.run(
        [sys.executable, "-c", source],
        cwd=_BACKEND_ROOT,
        env=environment,
        capture_output=True,
        check=False,
        text=True,
    )


def test_provider_settings_initializer_imports_in_a_fresh_interpreter() -> None:
    result = _run_cold_import("import scripts.init_provider_settings")

    assert result.returncode == 0, result.stderr


def test_service_compatibility_exports_use_repository_authority() -> None:
    result = _run_cold_import(
        "from app.repositories.provider_identity_authority import provider_identity_fingerprint as canonical; "
        "from app.services.provider_identity_authority import provider_identity_fingerprint as compatibility; "
        "assert compatibility is canonical"
    )

    assert result.returncode == 0, result.stderr
