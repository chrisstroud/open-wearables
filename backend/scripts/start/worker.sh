#!/bin/bash
set -e -x

worker_args=()
if [[ -n "${CELERY_WORKER_NAME:-}" ]]; then
    worker_args+=(--hostname="$CELERY_WORKER_NAME")
fi

uv run celery -A app.main:celery_app worker --loglevel=info --pool=threads -Q default,sdk_sync,garmin_sync,webhook_sync "${worker_args[@]}"
