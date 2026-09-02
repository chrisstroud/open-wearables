#!/usr/bin/env python3
"""Operate the one-off founder shadow WHOOP cleanup without exposing identifiers.

The target and keeper UUIDs are accepted only through protected environment
variables so they are not copied into shell history or process arguments.
"""

import argparse
import json
import os
from collections.abc import Sequence
from uuid import UUID

from app.database import SessionLocal
from app.integrations.celery import create_celery
from app.services.founder_shadow_whoop_cleanup_service import (
    FounderShadowWhoopCleanupError,
    founder_shadow_whoop_cleanup_service,
)

TARGET_ENV = "FOUNDER_SHADOW_WHOOP_TARGET_USER_ID"
KEEPER_ENV = "FOUNDER_SHADOW_WHOOP_KEEPER_USER_ID"
PLAN_ENV = "FOUNDER_SHADOW_WHOOP_EXPECTED_PLAN_SHA256"
CONFIRM_ENV = "FOUNDER_SHADOW_WHOOP_CONFIRM"
CONFIRM_VALUE = "DELETE_LOCAL_WHOOP_ONLY"


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Operate the bounded founder shadow WHOOP repair")
    parser.add_argument("mode", choices=("plan", "execute", "verify"))
    return parser.parse_args(argv)


def _required_uuid(name: str) -> UUID:
    raw = str(os.environ.get(name) or "").strip()
    if not raw:
        raise ValueError(f"{name} is required")
    try:
        return UUID(raw)
    except ValueError as exc:
        raise ValueError(f"{name} must be a UUID") from exc


def _emit(payload: dict[str, object]) -> None:
    print(json.dumps(payload, sort_keys=True, separators=(",", ":")))


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    try:
        target_user_id = _required_uuid(TARGET_ENV)
        keeper_user_id = _required_uuid(KEEPER_ENV)
        # Standalone operators do not import app.main, which is otherwise the
        # only place the process-wide Celery app receives the Railway Redis
        # broker configuration.  Configure it explicitly before the external
        # inventory performs its mandatory worker inspection.
        create_celery()
        with SessionLocal() as db_session:
            if args.mode == "plan":
                plan = founder_shadow_whoop_cleanup_service.plan(
                    db_session,
                    target_user_id=target_user_id,
                    keeper_user_id=keeper_user_id,
                )
                _emit(plan.public_dict())
                return 0 if plan.executable else 2

            if args.mode == "execute":
                if os.environ.get(CONFIRM_ENV) != CONFIRM_VALUE:
                    raise FounderShadowWhoopCleanupError("founder-shadow.explicit-confirmation-required")
                expected_plan = str(os.environ.get(PLAN_ENV) or "").strip().lower()
                if len(expected_plan) != 64 or any(char not in "0123456789abcdef" for char in expected_plan):
                    raise FounderShadowWhoopCleanupError("founder-shadow.expected-plan-digest-required")
                result = founder_shadow_whoop_cleanup_service.execute(
                    db_session,
                    target_user_id=target_user_id,
                    keeper_user_id=keeper_user_id,
                    expected_plan_sha256=expected_plan,
                )
                _emit(result.public_dict())
                return 0 if result.verified else 2

            result = founder_shadow_whoop_cleanup_service.verify(
                db_session,
                target_user_id=target_user_id,
                keeper_user_id=keeper_user_id,
            )
            _emit(result.public_dict())
            return 0 if result.verified else 2
    except (FounderShadowWhoopCleanupError, ValueError) as exc:
        blockers = list(exc.blockers) if isinstance(exc, FounderShadowWhoopCleanupError) else [str(exc)]
        _emit({"verified": False, "blockers": blockers})
        return 2
    except Exception:
        # The operator surface must never echo database parameters, provider
        # identifiers, credentials, or payload values from an unexpected
        # infrastructure exception.
        _emit({"verified": False, "blockers": ["founder-shadow.unexpected-operator-failure"]})
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
