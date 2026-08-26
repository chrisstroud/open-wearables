"""Helper functions for Sentry error reporting."""

from logging import Logger

import sentry_sdk
from fastapi import HTTPException


def log_and_capture_error(
    exception: Exception,
    logger: Logger,
    message: str,
    *,
    level: str = "error",
    extra: dict | None = None,
    include_exc_info: bool = True,
) -> None:
    """
    Log an error and capture it in Sentry.

    Use this for exceptions that are caught and handled gracefully,
    but still need to be reported to Sentry for monitoring.

    Args:
        exception: The exception to log and capture
        logger: Logger instance to use for logging
        message: Log message (can include format placeholders)
        level: Log level ('error', 'warning', 'info')
        extra: Optional extra context to attach to Sentry event
        include_exc_info: Whether to attach the currently handled traceback to
            the log record. Disable this when the original exception contains
            sensitive input and ``exception`` is a sanitized replacement.

    Example:
        try:
            process_data(item)
        except ValidationError as e:
            log_and_capture_error(
                e,
                logger,
                f"Failed to process item {item.id}",
                extra={"item_id": item.id, "user_id": user.id}
            )
            # Continue processing other items
    """
    # 4xx HTTPExceptions are explicit, handled client errors (e.g. a provider
    # rejecting a token refresh) — log them but keep them out of Sentry. 5xx and
    # everything else stay alertable.
    if isinstance(exception, HTTPException) and 400 <= exception.status_code < 500:
        logger.warning(message, exc_info=include_exc_info)
        return

    # Log with standard logger
    log_func = getattr(logger, level, logger.error)
    log_func(message, exc_info=include_exc_info)

    # Capture in Sentry with optional extra context
    if extra:
        with sentry_sdk.push_scope() as scope:
            for key, value in extra.items():
                scope.set_context(key, {"value": value})
            sentry_sdk.capture_exception(exception)
    else:
        sentry_sdk.capture_exception(exception)
