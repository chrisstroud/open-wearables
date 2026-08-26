"""Device resolution utilities for HealthKit data."""

from app.schemas.providers.mobile_sdk import OSVersion, SourceInfo

_SOURCE_TEXT_LIMIT = 100


def _bounded(value: str | None, limit: int = _SOURCE_TEXT_LIMIT) -> str | None:
    """Normalize untrusted source labels before they reach bounded DB columns."""
    if value is None:
        return None
    normalized = " ".join(value.split()).strip()
    return normalized[:limit] or None


def _format_os_version(os_version: OSVersion | None) -> str | None:
    if not os_version:
        return None
    return f"{os_version.major_version}.{os_version.minor_version}.{os_version.patch_version}"


def _get_device_model(source: SourceInfo | None) -> str | None:
    if not source:
        return None
    if source.device_model:
        return _bounded(source.device_model)
    if source.product_type:
        return _bounded(source.product_type)
    if source.device_name:
        return _bounded(source.device_name)
    return _bounded(source.device_manufacturer)


def _get_original_source_name(source: SourceInfo | None) -> str | None:
    if not source:
        return None
    if source.name:
        return _bounded(source.name)
    if source.device_name:
        return _bounded(source.device_name)
    # Third-party Health Connect writers (Peloton, Strava, Zwift, etc.)
    # never set ``name`` or ``device_name`` — they only populate the
    # writer's package identifier via ``appId`` (HC SDK ``DataOrigin``)
    # or ``bundleIdentifier`` (HealthKit Source bundle id). Falling back
    # to those keeps the workout's original provenance instead of
    # collapsing it to "unknown".
    if source.app_id:
        return _bounded(source.app_id)
    if source.bundle_identifier:
        return _bounded(source.bundle_identifier)
    return None


def extract_source_identifier(source: SourceInfo | None) -> str | None:
    """Return the bounded originating app/bundle identity without guessing a brand."""
    if not source:
        return None
    return _bounded(source.bundle_identifier) or _bounded(source.app_id) or _get_original_source_name(source)


def extract_device_info(source: SourceInfo | None) -> tuple[str | None, str | None, str | None]:
    """Extract device information from SourceInfo.

    Returns:
        Tuple of (device_model, software_version, original_source_name).
    """
    if not source:
        return None, None, None

    device_model = _get_device_model(source)
    software_version = _format_os_version(source.operating_system_version)
    original_source_name = _get_original_source_name(source)  # e.g. "Apple Watch (Jan)" or "Zepp Life"

    return device_model, software_version, original_source_name
