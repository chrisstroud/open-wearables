import base64
import hashlib
import hmac
import json
from dataclasses import dataclass, field, replace
from typing import Any
from uuid import UUID, uuid4

import boto3
from celery import current_app as current_celery_app
from redis.exceptions import ResponseError, WatchError

from app.config import settings
from app.integrations.redis_client import get_redis_client
from app.services.provider_identity_authority import (
    provider_identity_fingerprint as _identity_fingerprint,
)

RAW_OBJECTS = "open-wearables.raw-payload-objects"
FIT_OBJECTS = "open-wearables.fit-objects"
QUEUED_TASKS = "open-wearables.queued-tasks"
RESULT_BACKEND = "open-wearables.result-backend"
REDIS_COORDINATION = "open-wearables.redis-coordination"
INTERNAL_LOCATOR_PROVIDER = "_open-wearables"
EXTERNAL_CONFIGURATION_SCHEMA_VERSION = 1
CELERY_INSPECTION_TIMEOUT_SECONDS = 1.0
CELERY_INSPECTION_COLLECTIONS = ("active", "reserved", "scheduled")
REDIS_UNACKED_KEY = "unacked"
REDIS_UNACKED_INDEX_KEY = "unacked_index"
REDIS_QUEUED_EXACT_KEYS = frozenset(
    {
        "default",
        "sdk_sync",
        "garmin_sync",
        "webhook_sync",
        REDIS_UNACKED_KEY,
        REDIS_UNACKED_INDEX_KEY,
    }
)
REDIS_QUEUED_PREFIXES = ("unacked",)
REDIS_PRIORITY_SEPARATOR = "\x06\x16"
REDIS_PRIORITY_SUFFIXES = ("3", "6", "9")
REDIS_SUPPORTED_TYPES = ("string", "list", "set", "zset", "hash", "stream")
REDIS_INVENTORY_KEY_RETRY_LIMIT = 3
REDIS_INVENTORY_CLIENT_BLOCKER = "open-wearables.redis-coordination.inventory-client-unavailable"
REDIS_INVENTORY_PING_BLOCKER = "open-wearables.redis-coordination.inventory-ping-unavailable"
REDIS_INVENTORY_SCAN_BLOCKER = "open-wearables.redis-coordination.inventory-scan-unavailable"
REDIS_INVENTORY_KEY_REVIEW_BLOCKER = "open-wearables.redis-coordination.inventory-key-review-unavailable"
REDIS_INVENTORY_KEY_UNSTABLE_BLOCKER = "open-wearables.redis-coordination.inventory-key-review-unstable"

# These providers accept asynchronous webhook payloads whose raw S3 key and
# Celery envelope can contain only the provider-side identity.  The internal
# Open Wearables UUID is not available until a worker resolves the connection.
WEBHOOK_IDENTITY_PROVIDERS = frozenset(
    {
        "garmin",
        "google",
        "oura",
        "polar",
        "strava",
        "suunto",
        "whoop",
    }
)


def _identity_key_marker() -> str:
    return hmac.new(
        settings.secret_key.encode(),
        b"sdk-source-reset:v1:key-marker",
        hashlib.sha256,
    ).hexdigest()


@dataclass(frozen=True)
class ProviderIdentity:
    provider: str
    values: tuple[str, ...]
    fingerprints: tuple[str, ...]


@dataclass(frozen=True)
class ProviderIdentityScope:
    """Provider identities used only to locate external reset state.

    ``values`` are present while the connection rows still exist.  Only the
    keyed fingerprints are persisted with the reset receipt, which is enough
    to match parsed webhook fields during later verification.
    """

    identities: tuple[ProviderIdentity, ...] = ()
    incomplete_providers: tuple[str, ...] = ()
    ambiguous_providers: tuple[str, ...] = ()
    proof_unavailable: bool = False

    @classmethod
    def from_values(
        cls,
        provider_values: dict[str, set[str]],
        *,
        incomplete_providers: set[str] | None = None,
        ambiguous_providers: set[str] | None = None,
    ) -> "ProviderIdentityScope":
        identities: list[ProviderIdentity] = []
        for raw_provider, raw_values in sorted(provider_values.items()):
            provider = raw_provider.strip().lower()
            values = tuple(sorted({str(value).strip() for value in raw_values if str(value).strip()}))
            if not provider or not values:
                continue
            identities.append(
                ProviderIdentity(
                    provider=provider,
                    values=values,
                    fingerprints=tuple(sorted(_identity_fingerprint(provider, value) for value in values)),
                )
            )
        return cls(
            identities=tuple(identities),
            incomplete_providers=tuple(
                sorted({provider.strip().lower() for provider in incomplete_providers or set() if provider.strip()})
            ),
            ambiguous_providers=tuple(
                sorted({provider.strip().lower() for provider in ambiguous_providers or set() if provider.strip()})
            ),
        )

    @classmethod
    def from_proof(cls, proof: object, *, required: bool) -> "ProviderIdentityScope":
        if proof is None:
            return cls(proof_unavailable=required)
        if not isinstance(proof, dict) or proof.get("version") != 1:
            return cls(proof_unavailable=True)
        if not hmac.compare_digest(str(proof.get("key_marker") or ""), _identity_key_marker()):
            return cls(proof_unavailable=True)
        providers = proof.get("providers")
        if not isinstance(providers, dict):
            return cls(proof_unavailable=True)
        raw_ambiguous_providers = proof.get("ambiguous_providers", [])
        if not isinstance(raw_ambiguous_providers, list):
            return cls(proof_unavailable=True)

        identities: list[ProviderIdentity] = []
        try:
            for raw_provider, raw_fingerprints in sorted(providers.items()):
                provider = str(raw_provider).strip().lower()
                if not provider or not isinstance(raw_fingerprints, list):
                    raise ValueError
                fingerprints = tuple(
                    sorted(
                        {
                            str(fingerprint)
                            for fingerprint in raw_fingerprints
                            if len(str(fingerprint)) == 64
                            and all(char in "0123456789abcdef" for char in str(fingerprint))
                        }
                    )
                )
                if len(fingerprints) != len(raw_fingerprints):
                    raise ValueError
                identities.append(ProviderIdentity(provider, (), fingerprints))
            ambiguous_providers = tuple(sorted({str(provider).strip().lower() for provider in raw_ambiguous_providers}))
            if any(not provider for provider in ambiguous_providers) or len(ambiguous_providers) != len(
                raw_ambiguous_providers
            ):
                raise ValueError
        except (TypeError, ValueError):
            return cls(proof_unavailable=True)
        return cls(identities=tuple(identities), ambiguous_providers=ambiguous_providers)

    def merge(self, other: "ProviderIdentityScope") -> "ProviderIdentityScope":
        by_provider: dict[str, tuple[set[str], set[str]]] = {}
        for identity in (*self.identities, *other.identities):
            values, fingerprints = by_provider.setdefault(identity.provider, (set(), set()))
            values.update(identity.values)
            fingerprints.update(identity.fingerprints)
        return ProviderIdentityScope(
            identities=tuple(
                ProviderIdentity(provider, tuple(sorted(values)), tuple(sorted(fingerprints)))
                for provider, (values, fingerprints) in sorted(by_provider.items())
            ),
            incomplete_providers=tuple(sorted(set((*self.incomplete_providers, *other.incomplete_providers)))),
            ambiguous_providers=tuple(sorted(set((*self.ambiguous_providers, *other.ambiguous_providers)))),
            proof_unavailable=self.proof_unavailable or other.proof_unavailable,
        )

    def to_proof(self) -> dict[str, object]:
        return {
            "version": 1,
            "key_marker": _identity_key_marker(),
            "providers": {identity.provider: list(identity.fingerprints) for identity in self.identities},
            "ambiguous_providers": list(self.ambiguous_providers),
        }

    def manifest_tokens(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                hashlib.sha256(f"{identity.provider}:{fingerprint}".encode()).hexdigest()
                for identity in self.identities
                for fingerprint in identity.fingerprints
            )
        )

    def for_provider(self, provider: str) -> ProviderIdentity | None:
        normalized = provider.strip().lower()
        if normalized in self.ambiguous_providers:
            return None
        return next((identity for identity in self.identities if identity.provider == normalized), None)


@dataclass(frozen=True)
class ObjectReference:
    resource_key: str
    bucket: str
    key: str
    endpoint_url: str | None


@dataclass(frozen=True)
class S3ResetTarget:
    bucket: str
    endpoint_url: str | None
    raw_prefix: str | None
    fit_prefix: str | None
    apple_xml_prefix: str | None = None


@dataclass(frozen=True)
class RedisReference:
    resource_key: str
    key: str
    value_type: str
    locator: str | None
    raw_value: str | None = field(repr=False)
    reviewed_key_type: str | None = None
    reviewed_state_digest_sha256: str | None = None


@dataclass(frozen=True)
class ExternalResetInventory:
    counts: dict[str, int]
    identity_tokens: dict[str, tuple[str, ...]]
    blockers: tuple[str, ...]
    objects: tuple[ObjectReference, ...]
    redis_references: tuple[RedisReference, ...]
    active_task_ids: tuple[str, ...]
    configuration_digest_sha256: str = ""


class _RedisKeyDisappearedDuringReviewError(RuntimeError):
    """A watched Redis key vanished before its exact state could be captured."""


class _RedisKeyReviewUnstableError(RuntimeError):
    """A Redis key changed throughout the bounded inventory retry window."""


class SDKSourceResetExternalPlanes:
    """Inventory and erase user-bound object, Redis, and Celery state without returning payloads."""

    @staticmethod
    def _s3_client(endpoint_url: str | None) -> Any:
        kwargs: dict[str, Any] = {"region_name": settings.aws_region}
        if settings.aws_access_key_id and settings.aws_secret_access_key:
            kwargs["aws_access_key_id"] = settings.aws_access_key_id
            kwargs["aws_secret_access_key"] = settings.aws_secret_access_key.get_secret_value()
        if endpoint_url:
            kwargs["endpoint_url"] = endpoint_url
        return boto3.client("s3", **kwargs)

    @staticmethod
    def _versioning_blocker(client: Any, *, bucket: str, resource_key: str) -> str | None:
        try:
            response = client.get_bucket_versioning(Bucket=bucket)
        except Exception:
            return f"{resource_key}.versioning-inspection-unavailable"
        if response.get("Status") in {"Enabled", "Suspended"}:
            # Deleting only the current key would leave non-current PHI
            # versions recoverable.  Until version-aware enumeration/deletion
            # is implemented, refuse to certify this plane as empty.
            return f"{resource_key}.version-history-unerasable"
        return None

    def _list_objects(
        self,
        *,
        resource_key: str,
        bucket: str,
        prefix: str,
        endpoint_url: str | None,
        user_segment: str | None = None,
    ) -> tuple[list[ObjectReference], str | None]:
        try:
            client = self._s3_client(endpoint_url)
            versioning_blocker = self._versioning_blocker(client, bucket=bucket, resource_key=resource_key)
            if versioning_blocker:
                return [], versioning_blocker
            token: str | None = None
            rows: list[ObjectReference] = []
            while True:
                kwargs: dict[str, Any] = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
                if token:
                    kwargs["ContinuationToken"] = token
                response = client.list_objects_v2(**kwargs)
                for item in response.get("Contents", []):
                    key = str(item["Key"])
                    if user_segment is not None and user_segment not in key:
                        continue
                    rows.append(ObjectReference(resource_key, bucket, key, endpoint_url))
                if not response.get("IsTruncated"):
                    break
                token = response.get("NextContinuationToken")
                if not token:
                    return [], f"{resource_key}.inventory-incomplete"
            return rows, None
        except Exception:
            return [], f"{resource_key}.inventory-unavailable"

    @staticmethod
    def _walk_text_values(raw: object) -> tuple[str, ...]:
        """Decode bounded JSON/base64 envelopes into scalar text values."""

        pending: list[tuple[object, int]] = [(raw, 0)]
        seen_text: set[str] = set()
        values: list[str] = []
        visited = 0
        while pending:
            value, depth = pending.pop()
            visited += 1
            if visited > 100_000:
                raise ValueError("External value traversal exceeded its safety bound")
            if isinstance(value, dict):
                if depth < 8:
                    pending.extend((item, depth + 1) for pair in value.items() for item in pair)
                continue
            if isinstance(value, (list, tuple, set)):
                if depth < 8:
                    pending.extend((item, depth + 1) for item in value)
                continue

            text = value.decode(errors="strict") if isinstance(value, bytes) else str(value)
            values.append(text)
            if depth >= 8 or text in seen_text:
                continue
            seen_text.add(text)
            try:
                parsed = json.loads(text)
            except (TypeError, ValueError):
                parsed = None
            if parsed is not None and parsed != text:
                pending.append((parsed, depth + 1))
            if len(text) >= 8:
                try:
                    decoded = base64.b64decode(text, validate=True).decode(errors="strict")
                except (UnicodeDecodeError, ValueError, TypeError):
                    decoded = ""
                if decoded and decoded != text:
                    pending.append((decoded, depth + 1))
        return tuple(values)

    @classmethod
    def _contains_identity(
        cls,
        raw: object,
        *,
        user_id: str,
        identity_scope: ProviderIdentityScope,
        provider_hint: str | None = None,
    ) -> bool:
        values = cls._walk_text_values(raw)
        if any(user_id in value for value in values):
            return True

        lowered_values = tuple(value.lower() for value in values)
        for identity in identity_scope.identities:
            if identity.provider in identity_scope.ambiguous_providers:
                continue
            provider_present = (
                identity.provider == INTERNAL_LOCATOR_PROVIDER
                or provider_hint == identity.provider
                or any(
                    value == identity.provider
                    or f".{identity.provider}." in value
                    or f"/{identity.provider}/" in value
                    or f":{identity.provider}:" in value
                    for value in lowered_values
                )
            )
            if not provider_present:
                continue
            if any(value in identity.values for value in values):
                return True
            if any(_identity_fingerprint(identity.provider, value) in identity.fingerprints for value in values):
                return True
        return False

    def _raw_object_matches(
        self,
        *,
        client: Any,
        bucket: str,
        key: str,
        provider: str,
        user_id: str,
        identity_scope: ProviderIdentityScope,
    ) -> bool:
        response = client.get_object(Bucket=bucket, Key=key)
        metadata = response.get("Metadata") or {}
        if self._contains_identity(
            metadata,
            user_id=user_id,
            identity_scope=identity_scope,
            provider_hint=provider,
        ):
            return True

        body = response.get("Body")
        if body is None or not hasattr(body, "read"):
            raise ValueError("Raw object has no readable body")
        limit = max(1, int(settings.raw_payload_max_size_bytes))
        try:
            payload = body.read(limit + 1)
        finally:
            close = getattr(body, "close", None)
            if callable(close):
                close()
        if not isinstance(payload, bytes) or len(payload) > limit:
            raise ValueError("Raw object exceeds the identity inspection bound")
        decoded = payload.decode("utf-8", errors="strict")
        try:
            parsed = json.loads(decoded)
        except (TypeError, ValueError) as exc:
            raise ValueError("Raw object is not valid JSON") from exc
        return self._contains_identity(
            parsed,
            user_id=user_id,
            identity_scope=identity_scope,
            provider_hint=provider,
        )

    def _list_raw_objects(
        self,
        *,
        bucket: str,
        prefix: str,
        endpoint_url: str | None,
        user_id: str,
        identity_scope: ProviderIdentityScope,
    ) -> tuple[list[ObjectReference], str | None]:
        try:
            client = self._s3_client(endpoint_url)
            versioning_blocker = self._versioning_blocker(client, bucket=bucket, resource_key=RAW_OBJECTS)
            if versioning_blocker:
                return [], versioning_blocker
            token: str | None = None
            rows: list[ObjectReference] = []
            stripped_prefix = prefix.strip("/")
            normalized_prefix = f"{stripped_prefix}/" if stripped_prefix else ""
            while True:
                kwargs: dict[str, Any] = {
                    "Bucket": bucket,
                    "Prefix": normalized_prefix,
                    "MaxKeys": 1000,
                }
                if token:
                    kwargs["ContinuationToken"] = token
                response = client.list_objects_v2(**kwargs)
                for item in response.get("Contents", []):
                    key = str(item["Key"])
                    if f"/{user_id}/" in key:
                        rows.append(ObjectReference(RAW_OBJECTS, bucket, key, endpoint_url))
                        continue
                    relative = key.removeprefix(normalized_prefix)
                    parts = relative.split("/")
                    if len(parts) < 5 or parts[-2] != "_unknown":
                        continue
                    provider = parts[0].strip().lower()
                    if identity_scope.for_provider(provider) is None:
                        continue
                    if self._raw_object_matches(
                        client=client,
                        bucket=bucket,
                        key=key,
                        provider=provider,
                        user_id=user_id,
                        identity_scope=identity_scope,
                    ):
                        rows.append(ObjectReference(RAW_OBJECTS, bucket, key, endpoint_url))
                if not response.get("IsTruncated"):
                    break
                token = response.get("NextContinuationToken")
                if not token:
                    return rows, f"{RAW_OBJECTS}.inventory-incomplete"
            return rows, None
        except Exception:
            return [], f"{RAW_OBJECTS}.identity-inspection-unavailable"

    @staticmethod
    def _sorted_storage_targets(targets: set[S3ResetTarget]) -> tuple[S3ResetTarget, ...]:
        return tuple(
            sorted(
                targets,
                key=lambda row: (
                    row.bucket,
                    row.endpoint_url or "",
                    row.raw_prefix or "",
                    row.fit_prefix or "",
                    row.apple_xml_prefix if row.apple_xml_prefix is not None else "\uffff",
                ),
            )
        )

    @classmethod
    def _current_storage_targets(cls) -> tuple[S3ResetTarget, ...]:
        targets: set[S3ResetTarget] = set()
        raw_bucket = str(settings.raw_payload_s3_bucket or "").strip() or None
        aws_bucket = str(settings.aws_bucket_name or "").strip() or None
        endpoint_url = str(settings.raw_payload_s3_endpoint_url or "").strip().rstrip("/") or None
        raw_prefix = settings.raw_payload_s3_prefix.strip("/")

        if raw_bucket:
            targets.add(
                S3ResetTarget(
                    raw_bucket,
                    endpoint_url,
                    raw_prefix,
                    "fit-files",
                    "" if raw_bucket == aws_bucket and endpoint_url is None else None,
                )
            )
        if aws_bucket:
            # Provider raw/FIT storage falls back to AWS_BUCKET_NAME. Scan the
            # current compatible endpoint and native AWS; Apple XML is native.
            targets.add(
                S3ResetTarget(
                    aws_bucket,
                    endpoint_url,
                    raw_prefix,
                    "fit-files",
                    "" if endpoint_url is None else None,
                )
            )
            targets.add(S3ResetTarget(aws_bucket, None, raw_prefix, "fit-files", ""))
        return cls._sorted_storage_targets(targets)

    @classmethod
    def _retired_storage_targets(cls) -> tuple[S3ResetTarget, ...]:
        return cls._sorted_storage_targets(
            {
                S3ResetTarget(
                    target.bucket,
                    target.endpoint_url,
                    target.raw_prefix,
                    target.fit_prefix,
                    target.apple_xml_prefix,
                )
                for target in settings.source_reset_retired_s3_targets
            }
        )

    @classmethod
    def _storage_targets(cls) -> tuple[S3ResetTarget, ...]:
        """Return every current and explicitly retired governed S3 target."""

        return cls._sorted_storage_targets(
            {
                *cls._current_storage_targets(),
                *cls._retired_storage_targets(),
            }
        )

    @staticmethod
    def _target_configuration(target: S3ResetTarget) -> dict[str, str | None]:
        return {
            "bucket": target.bucket,
            "endpoint_url": target.endpoint_url,
            "raw_prefix": target.raw_prefix,
            "fit_prefix": target.fit_prefix,
            "apple_xml_prefix": target.apple_xml_prefix,
        }

    @classmethod
    def _configuration_digest_sha256(cls) -> str:
        canonical = {
            "schema_version": EXTERNAL_CONFIGURATION_SCHEMA_VERSION,
            "s3": {
                "history_complete": bool(settings.source_reset_s3_target_history_complete),
                "configured_current": {
                    "aws_bucket": str(settings.aws_bucket_name or "").strip() or None,
                    "raw_bucket": str(settings.raw_payload_s3_bucket or "").strip() or None,
                    "endpoint_url": str(settings.raw_payload_s3_endpoint_url or "").strip().rstrip("/") or None,
                    "raw_prefix": settings.raw_payload_s3_prefix.strip("/"),
                    "fit_prefix": "fit-files",
                    "apple_xml_prefix": "",
                },
                "current_targets": [cls._target_configuration(target) for target in cls._current_storage_targets()],
                "retired_targets": [cls._target_configuration(target) for target in cls._retired_storage_targets()],
                "raw_payload_storage": settings.raw_payload_storage.strip().lower(),
                "raw_payload_max_size_bytes": int(settings.raw_payload_max_size_bytes),
                "store_fit_files": bool(settings.store_fit_files),
                "aws_region": settings.aws_region.strip().lower(),
            },
            "redis": {
                "host": settings.redis_host.strip().lower(),
                "port": int(settings.redis_port),
                "db": int(settings.redis_db),
                "ssl": bool(settings.redis_ssl),
                "username_configured": bool(settings.redis_username),
                "scan_match": "*",
                "supported_types": list(REDIS_SUPPORTED_TYPES),
                "queued_exact_keys": sorted(REDIS_QUEUED_EXACT_KEYS),
                "queued_prefixes": list(REDIS_QUEUED_PREFIXES),
                "priority_separator": REDIS_PRIORITY_SEPARATOR,
                "priority_suffixes": list(REDIS_PRIORITY_SUFFIXES),
                "result_key_prefix": "celery-task-meta-",
            },
            "celery": {
                "inspection_timeout_seconds": CELERY_INSPECTION_TIMEOUT_SECONDS,
                "inspection_collections": list(CELERY_INSPECTION_COLLECTIONS),
                "provider_identity_providers": sorted(WEBHOOK_IDENTITY_PROVIDERS),
            },
        }
        return hashlib.sha256(json.dumps(canonical, sort_keys=True, separators=(",", ":")).encode()).hexdigest()

    @staticmethod
    def _user_prefix(prefix: str, user_id: str) -> str:
        normalized = prefix.strip("/")
        return f"{normalized}/{user_id}/" if normalized else f"{user_id}/"

    def _object_inventory(
        self,
        user_id: UUID,
        identity_scope: ProviderIdentityScope | None = None,
    ) -> tuple[tuple[ObjectReference, ...], tuple[str, ...]]:
        user = str(user_id)
        scope = identity_scope or ProviderIdentityScope()
        rows: list[ObjectReference] = []
        blockers: list[str] = []

        if scope.proof_unavailable or scope.incomplete_providers:
            blockers.append("open-wearables.external-identity-scope-incomplete")
        if scope.ambiguous_providers:
            blockers.append("open-wearables.external-identity-scope-ambiguous")

        if not settings.source_reset_s3_target_history_complete:
            blockers.append("open-wearables.external-storage-target-history-unverifiable")

        if settings.raw_payload_storage == "log":
            blockers.append("open-wearables.raw-payload-objects.log-retention-unerasable")
        storage_targets = self._storage_targets()
        has_raw_target = any(target.raw_prefix is not None for target in storage_targets)
        has_fit_target = any(target.fit_prefix is not None for target in storage_targets)
        if not has_raw_target:
            if settings.raw_payload_storage == "s3":
                blockers.append("open-wearables.raw-payload-objects.bucket-unconfigured")
            elif not settings.source_reset_s3_target_history_complete:
                blockers.append("open-wearables.raw-payload-objects.storage-history-unverifiable")
        if not has_fit_target:
            if settings.store_fit_files:
                blockers.append("open-wearables.fit-objects.bucket-unconfigured")
            elif not settings.source_reset_s3_target_history_complete:
                blockers.append("open-wearables.fit-objects.storage-history-unverifiable")
        for target in storage_targets:
            if target.raw_prefix is not None:
                raw_rows, blocker = self._list_raw_objects(
                    bucket=target.bucket,
                    prefix=target.raw_prefix,
                    endpoint_url=target.endpoint_url,
                    user_id=user,
                    identity_scope=scope,
                )
                rows.extend(raw_rows)
                if blocker:
                    blockers.append(blocker)

            if target.fit_prefix is not None:
                fit_rows, blocker = self._list_objects(
                    resource_key=FIT_OBJECTS,
                    bucket=target.bucket,
                    prefix=f"{target.fit_prefix.strip('/')}/" if target.fit_prefix else "",
                    endpoint_url=target.endpoint_url,
                    user_segment=f"/{user}/",
                )
                rows.extend(fit_rows)
                if blocker:
                    blockers.append(blocker)

            if target.apple_xml_prefix is not None:
                apple_xml, blocker = self._list_objects(
                    resource_key=RAW_OBJECTS,
                    bucket=target.bucket,
                    prefix=self._user_prefix(target.apple_xml_prefix, user),
                    endpoint_url=target.endpoint_url,
                )
                rows.extend(apple_xml)
                if blocker:
                    blockers.append(blocker)

        deduplicated = {(row.resource_key, row.bucket, row.key, row.endpoint_url): row for row in rows}
        return tuple(deduplicated.values()), tuple(sorted(set(blockers)))

    @staticmethod
    def _contains_user(raw: object, user: str) -> bool:
        """Find a user UUID through Celery's nested JSON/base64 envelopes.

        Redis broker messages commonly carry the task body as a base64 string
        inside an outer JSON object. Missing that nested body would let queued
        work survive a reset. A traversal bound turns pathological state into
        an inventory blocker (the caller catches the exception) instead of a
        false zero.
        """
        return any(user in value for value in SDKSourceResetExternalPlanes._walk_text_values(raw))

    @staticmethod
    def _redis_resource_key(key: str) -> str:
        if key.startswith("celery-task-meta-"):
            return RESULT_BACKEND
        if key in REDIS_QUEUED_EXACT_KEYS:
            return QUEUED_TASKS
        base_key, separator, priority = key.partition(REDIS_PRIORITY_SEPARATOR)
        if separator and base_key in REDIS_QUEUED_EXACT_KEYS and priority in REDIS_PRIORITY_SUFFIXES:
            return QUEUED_TASKS
        if key.startswith(REDIS_QUEUED_PREFIXES):
            return QUEUED_TASKS
        return REDIS_COORDINATION

    @staticmethod
    def _redis_serialized_state(client: Any, key: str) -> tuple[str, bytes]:
        key_type = str(client.type(key))
        dumped = client.dump(key)
        if key_type == "none" or dumped is None:
            raise _RedisKeyDisappearedDuringReviewError("Redis key disappeared during inventory")
        if isinstance(dumped, memoryview):
            dumped = dumped.tobytes()
        if isinstance(dumped, bytearray):
            dumped = bytes(dumped)
        if not isinstance(dumped, bytes):
            raise RuntimeError("Redis DUMP did not return exact serialized bytes")
        return key_type, dumped

    @staticmethod
    def _redis_state_digest(key: str, key_type: str, dumped: bytes) -> str:
        encoded_key = key.encode()
        message = (
            b"sdk-source-reset:redis-state:v1\0"
            + len(encoded_key).to_bytes(8, "big")
            + encoded_key
            + b"\0"
            + key_type.encode()
            + b"\0"
            + dumped
        )
        return hmac.new(settings.secret_key.encode(), message, hashlib.sha256).hexdigest()

    @staticmethod
    def _redis_reference_identity_token(reference: RedisReference) -> str:
        """Return a keyed durable identity for only the selected Redis target.

        The full-key DUMP digest belongs to the short-lived erase CAS. Including
        it here would let unrelated traffic in a shared Celery key continuously
        invalidate an otherwise unchanged user's reset manifest.
        """

        encoded_parts = tuple(
            str(value or "").encode()
            for value in (
                reference.resource_key,
                reference.key,
                reference.value_type,
                # List positions are transient CAS locators. Unrelated head
                # traffic may shift them without changing the selected target.
                None if reference.value_type == "list" else reference.locator,
                reference.raw_value,
            )
        )
        message = b"sdk-source-reset:redis-reference:v1\0" + b"".join(
            len(part).to_bytes(8, "big") + part for part in encoded_parts
        )
        return hmac.new(settings.secret_key.encode(), message, hashlib.sha256).hexdigest()

    @staticmethod
    def _raw_stream_entries(client: Any, key: str) -> tuple[tuple[str, object], ...]:
        """Read XRANGE without redis-py's duplicate-field-collapsing callback."""

        if hasattr(client, "response_callbacks") and hasattr(client, "set_response_callback"):
            client.response_callbacks = dict(client.response_callbacks)
            client.set_response_callback("XRANGE", lambda response, **_options: response)
            raw_entries = client.execute_command("XRANGE", key, "-", "+") or []
        else:
            raw_entries = client.xrange(key) or []

        entries: list[tuple[str, object]] = []
        for raw_entry in raw_entries:
            if not isinstance(raw_entry, (list, tuple)) or len(raw_entry) != 2:
                raise RuntimeError("Redis stream response is malformed")
            entry_id, raw_fields = raw_entry
            if isinstance(raw_fields, dict):
                field_values: object = tuple(raw_fields.items())
            elif isinstance(raw_fields, (list, tuple)):
                if len(raw_fields) % 2:
                    raise RuntimeError("Redis stream field/value response is malformed")
                field_values = tuple(
                    (raw_fields[index], raw_fields[index + 1]) for index in range(0, len(raw_fields), 2)
                )
            else:
                raise RuntimeError("Redis stream field/value response is malformed")
            entries.append((str(entry_id), field_values))
        return tuple(entries)

    @staticmethod
    def _verify_watched_redis_absence(review_pipe: Any) -> None:
        """Prove a watched key stayed absent through a no-op transaction."""

        review_pipe.multi()
        review_pipe.ping()
        if review_pipe.execute() != [True]:
            raise RuntimeError("Redis key absence transaction was incomplete")

    def _redis_key_inventory_once(
        self,
        client: Any,
        redis_key: str,
        *,
        user_id: UUID,
        identity_scope: ProviderIdentityScope,
    ) -> tuple[RedisReference, ...]:
        user = str(user_id)
        review_pipe = client.pipeline(transaction=True)
        try:
            watched_keys = (redis_key, REDIS_UNACKED_INDEX_KEY) if redis_key == REDIS_UNACKED_KEY else (redis_key,)
            review_pipe.watch(*watched_keys)
            reviewed_key_type = str(review_pipe.type(redis_key))
            if reviewed_key_type == "none":
                self._verify_watched_redis_absence(review_pipe)
                return ()

            resource_key = self._redis_resource_key(redis_key)
            key_references: list[RedisReference] = []
            try:
                if user in redis_key:
                    key_references.append(RedisReference(resource_key, redis_key, "key", None, None))
                elif reviewed_key_type == "string":
                    value = review_pipe.get(redis_key)
                    if value is not None and self._contains_identity(
                        {"key": redis_key, "value": value},
                        user_id=user,
                        identity_scope=identity_scope,
                    ):
                        key_references.append(RedisReference(resource_key, redis_key, "key", None, str(value)))
                elif reviewed_key_type == "list":
                    values = tuple(str(value) for value in review_pipe.lrange(redis_key, 0, -1))
                    for position, value in enumerate(values):
                        if self._contains_identity(
                            {"key": redis_key, "value": value},
                            user_id=user,
                            identity_scope=identity_scope,
                        ):
                            key_references.append(
                                RedisReference(
                                    resource_key,
                                    redis_key,
                                    "list",
                                    str(position),
                                    value,
                                )
                            )
                elif reviewed_key_type == "set":
                    for value in review_pipe.sscan_iter(redis_key):
                        if self._contains_identity(
                            {"key": redis_key, "value": value},
                            user_id=user,
                            identity_scope=identity_scope,
                        ):
                            key_references.append(RedisReference(resource_key, redis_key, "set", None, str(value)))
                elif reviewed_key_type == "zset":
                    for value, _score in review_pipe.zscan_iter(redis_key):
                        if self._contains_identity(
                            {"key": redis_key, "value": value},
                            user_id=user,
                            identity_scope=identity_scope,
                        ):
                            key_references.append(RedisReference(resource_key, redis_key, "zset", None, str(value)))
                elif reviewed_key_type == "hash":
                    for hash_field, value in review_pipe.hscan_iter(redis_key):
                        if self._contains_identity(
                            {"key": redis_key, "field": hash_field, "value": value},
                            user_id=user,
                            identity_scope=identity_scope,
                        ):
                            key_references.append(
                                RedisReference(
                                    resource_key,
                                    redis_key,
                                    "hash",
                                    str(hash_field),
                                    str(value),
                                )
                            )
                elif reviewed_key_type == "stream":
                    for entry_id, field_values in self._raw_stream_entries(review_pipe, redis_key):
                        if self._contains_identity(
                            {"key": redis_key, "field_values": field_values},
                            user_id=user,
                            identity_scope=identity_scope,
                        ):
                            key_references.append(
                                RedisReference(
                                    resource_key,
                                    redis_key,
                                    "stream",
                                    entry_id,
                                    json.dumps(
                                        field_values,
                                        ensure_ascii=False,
                                        separators=(",", ":"),
                                        default=str,
                                    ),
                                )
                            )

                reviewed_digest: str | None = None
                if key_references:
                    final_key_type, final_dump = self._redis_serialized_state(review_pipe, redis_key)
                    if final_key_type != reviewed_key_type:
                        raise RuntimeError("Redis key type changed during inventory")
                    reviewed_digest = self._redis_state_digest(redis_key, reviewed_key_type, final_dump)

                paired_references: list[RedisReference] = []
                if redis_key == REDIS_UNACKED_KEY and key_references:
                    delivery_tags = tuple(
                        sorted(
                            {
                                row.locator
                                for row in key_references
                                if row.value_type == "hash" and row.locator is not None
                            }
                        )
                    )
                    if len(delivery_tags) != len(key_references):
                        raise RuntimeError("Redis unacked review is inconsistent")
                    index_type = str(review_pipe.type(REDIS_UNACKED_INDEX_KEY))
                    if index_type != "zset":
                        raise RuntimeError("Redis unacked index is unavailable")
                    for delivery_tag in delivery_tags:
                        if review_pipe.zscore(REDIS_UNACKED_INDEX_KEY, delivery_tag) is None:
                            raise RuntimeError("Redis unacked index member is unavailable")
                    final_index_type, final_index_dump = self._redis_serialized_state(
                        review_pipe,
                        REDIS_UNACKED_INDEX_KEY,
                    )
                    if final_index_type != index_type:
                        raise RuntimeError("Redis unacked index type changed during inventory")
                    index_digest = self._redis_state_digest(
                        REDIS_UNACKED_INDEX_KEY,
                        index_type,
                        final_index_dump,
                    )
                    paired_references.extend(
                        RedisReference(
                            QUEUED_TASKS,
                            REDIS_UNACKED_INDEX_KEY,
                            "zset",
                            None,
                            delivery_tag,
                            reviewed_key_type=index_type,
                            reviewed_state_digest_sha256=index_digest,
                        )
                        for delivery_tag in delivery_tags
                    )
            except _RedisKeyDisappearedDuringReviewError:
                self._verify_watched_redis_absence(review_pipe)
                return ()
            except (ResponseError, RuntimeError):
                # A WRONGTYPE or consistency error can be either stable
                # structural corruption or ordinary queue churn between the
                # watched TYPE and the type-specific read. A successful no-op
                # transaction proves the failure was stable and is re-raised;
                # WatchError proves concurrency and enters the bounded retry.
                review_pipe.multi()
                review_pipe.ping()
                if review_pipe.execute() != [True]:
                    raise RuntimeError("Redis key error-review transaction was incomplete")
                raise

            review_pipe.multi()
            review_pipe.ping()
            if review_pipe.execute() != [True]:
                raise RuntimeError("Redis key review transaction was incomplete")

            references = tuple(
                replace(
                    row,
                    reviewed_key_type=reviewed_key_type,
                    reviewed_state_digest_sha256=reviewed_digest,
                )
                for row in key_references
                if reviewed_digest is not None
            )
            return (*references, *paired_references)
        finally:
            review_pipe.reset()

    def _redis_key_inventory(
        self,
        client: Any,
        redis_key: str,
        *,
        user_id: UUID,
        identity_scope: ProviderIdentityScope,
    ) -> tuple[RedisReference, ...]:
        for attempt in range(REDIS_INVENTORY_KEY_RETRY_LIMIT):
            try:
                return self._redis_key_inventory_once(
                    client,
                    redis_key,
                    user_id=user_id,
                    identity_scope=identity_scope,
                )
            except WatchError as exc:
                if attempt + 1 == REDIS_INVENTORY_KEY_RETRY_LIMIT:
                    raise _RedisKeyReviewUnstableError("Redis key remained unstable during inventory") from exc
        raise AssertionError("Redis key inventory retry loop did not terminate")

    def _redis_inventory(
        self,
        user_id: UUID,
        identity_scope: ProviderIdentityScope | None = None,
    ) -> tuple[tuple[RedisReference, ...], tuple[str, ...]]:
        scope = identity_scope or ProviderIdentityScope()
        try:
            client = get_redis_client()
        except Exception:
            return (), (REDIS_INVENTORY_CLIENT_BLOCKER,)
        try:
            if client.ping() is not True:
                raise RuntimeError("Redis inventory ping did not confirm availability")
        except Exception:
            return (), (REDIS_INVENTORY_PING_BLOCKER,)

        try:
            key_iterator = iter(client.scan_iter(match="*", count=500))
        except Exception:
            return (), (REDIS_INVENTORY_SCAN_BLOCKER,)

        references: list[RedisReference] = []
        while True:
            try:
                raw_key = next(key_iterator)
            except StopIteration:
                break
            except Exception:
                return (), (REDIS_INVENTORY_SCAN_BLOCKER,)

            try:
                references.extend(
                    self._redis_key_inventory(
                        client,
                        str(raw_key),
                        user_id=user_id,
                        identity_scope=scope,
                    )
                )
            except _RedisKeyReviewUnstableError:
                return (), (REDIS_INVENTORY_KEY_UNSTABLE_BLOCKER,)
            except Exception:
                return (), (REDIS_INVENTORY_KEY_REVIEW_BLOCKER,)

        unique = {
            (
                row.resource_key,
                row.key,
                row.value_type,
                row.locator,
                row.raw_value,
                row.reviewed_key_type,
                row.reviewed_state_digest_sha256,
            ): row
            for row in references
        }
        return tuple(unique.values()), ()

    @classmethod
    def _task_inventory(
        cls,
        user_id: UUID,
        identity_scope: ProviderIdentityScope | None = None,
    ) -> tuple[tuple[str, ...], tuple[str, ...]]:
        user = str(user_id)
        scope = identity_scope or ProviderIdentityScope()
        try:
            inspector = current_celery_app.control.inspect(timeout=CELERY_INSPECTION_TIMEOUT_SECONDS)
            ping = inspector.ping()
            if not ping:
                return (), ("open-wearables.queued-tasks.worker-inspection-unavailable",)
            matches: set[str] = set()
            for collection_name in CELERY_INSPECTION_COLLECTIONS:
                collection = getattr(inspector, collection_name)()
                if collection is None:
                    return (), ("open-wearables.queued-tasks.worker-inspection-incomplete",)
                for tasks in collection.values():
                    for task in tasks:
                        if cls._contains_identity(
                            task,
                            user_id=user,
                            identity_scope=scope,
                        ):
                            task_id = task.get("id") or task.get("request", {}).get("id")
                            matches.add(str(task_id or hashlib.sha256(repr(task).encode()).hexdigest()))
            return tuple(sorted(matches)), ()
        except Exception:
            return (), ("open-wearables.queued-tasks.worker-inspection-unavailable",)

    def inventory(
        self,
        user_id: UUID,
        *,
        identity_scope: ProviderIdentityScope | None = None,
    ) -> ExternalResetInventory:
        scope = identity_scope or ProviderIdentityScope()
        configuration_digest = self._configuration_digest_sha256()
        objects, object_blockers = self._object_inventory(user_id, scope)
        redis_refs, redis_blockers = self._redis_inventory(user_id, scope)
        active_tasks, task_blockers = self._task_inventory(user_id, scope)
        configuration_blockers: tuple[str, ...] = ()
        if self._configuration_digest_sha256() != configuration_digest:
            configuration_blockers = ("open-wearables.external-configuration-changed-during-inventory",)

        scope_blockers: tuple[str, ...] = ()
        if scope.proof_unavailable or scope.incomplete_providers:
            scope_blockers = ("open-wearables.external-identity-scope-incomplete",)
        if scope.ambiguous_providers:
            scope_blockers = (*scope_blockers, "open-wearables.external-identity-scope-ambiguous")

        counts = {
            RAW_OBJECTS: sum(row.resource_key == RAW_OBJECTS for row in objects),
            FIT_OBJECTS: sum(row.resource_key == FIT_OBJECTS for row in objects),
            QUEUED_TASKS: sum(row.resource_key == QUEUED_TASKS for row in redis_refs) + len(active_tasks),
            RESULT_BACKEND: sum(row.resource_key == RESULT_BACKEND for row in redis_refs),
            REDIS_COORDINATION: sum(row.resource_key == REDIS_COORDINATION for row in redis_refs),
        }
        identity_tokens: dict[str, list[str]] = {key: [] for key in counts}
        for row in objects:
            identity_tokens[row.resource_key].append(
                hashlib.sha256(f"{row.endpoint_url or ''}:{row.bucket}/{row.key}".encode()).hexdigest()
            )
        for row in redis_refs:
            identity_tokens[row.resource_key].append(self._redis_reference_identity_token(row))
        identity_tokens[QUEUED_TASKS].extend(hashlib.sha256(task_id.encode()).hexdigest() for task_id in active_tasks)
        identity_tokens[REDIS_COORDINATION].append(
            hashlib.sha256(f"sdk-source-reset:external-configuration:v1:{configuration_digest}".encode()).hexdigest()
        )
        return ExternalResetInventory(
            counts=counts,
            identity_tokens={key: tuple(sorted(values)) for key, values in identity_tokens.items()},
            blockers=tuple(
                sorted(
                    set(
                        (
                            *object_blockers,
                            *redis_blockers,
                            *task_blockers,
                            *scope_blockers,
                            *configuration_blockers,
                        )
                    )
                )
            ),
            objects=objects,
            redis_references=redis_refs,
            active_task_ids=active_tasks,
            configuration_digest_sha256=configuration_digest,
        )

    def erase_objects(self, objects: tuple[ObjectReference, ...]) -> None:
        grouped: dict[tuple[str, str | None], list[str]] = {}
        for row in objects:
            grouped.setdefault((row.bucket, row.endpoint_url), []).append(row.key)
        try:
            for (bucket, endpoint_url), keys in grouped.items():
                client = self._s3_client(endpoint_url)
                for offset in range(0, len(keys), 1000):
                    chunk = keys[offset : offset + 1000]
                    response = client.delete_objects(
                        Bucket=bucket,
                        Delete={"Objects": [{"Key": key} for key in chunk], "Quiet": True},
                    )
                    if response.get("Errors"):
                        raise RuntimeError("object deletion incomplete")
        except Exception as exc:
            raise RuntimeError("Object deletion is unavailable") from exc

    @staticmethod
    def revoke_tasks(task_ids: tuple[str, ...]) -> None:
        try:
            for task_id in task_ids:
                # Non-terminating revoke prevents reserved/scheduled work from
                # starting.  Already-running work remains visible to the drain
                # inventory until it exits, avoiding unsafe process kills.
                current_celery_app.control.revoke(task_id, terminate=False)
        except Exception as exc:
            raise RuntimeError("Celery task revocation is unavailable") from exc

    @staticmethod
    def erase_redis(references: tuple[RedisReference, ...], *, include_results: bool) -> None:
        try:
            included = tuple(row for row in references if include_results or row.resource_key != RESULT_BACKEND)
            if not included:
                return

            key_groups: dict[str, list[RedisReference]] = {}
            for row in included:
                key_groups.setdefault(row.key, []).append(row)

            client = get_redis_client()
            pipe = client.pipeline(transaction=True)
            try:
                pipe.watch(*sorted(key_groups))

                list_snapshots: dict[str, tuple[str, ...]] = {}
                positions_by_key: dict[str, tuple[int, ...]] = {}
                for key, rows in sorted(key_groups.items()):
                    expected_types = {row.reviewed_key_type for row in rows}
                    expected_digests = {row.reviewed_state_digest_sha256 for row in rows}
                    if None in expected_types or len(expected_types) != 1:
                        raise RuntimeError("Redis key review type is incomplete")
                    if None in expected_digests or len(expected_digests) != 1:
                        raise RuntimeError("Redis key review identity is incomplete")
                    expected_type = next(value for value in expected_types if value is not None)
                    expected_digest = next(digest for digest in expected_digests if digest is not None)
                    current_type, current_dump = SDKSourceResetExternalPlanes._redis_serialized_state(pipe, key)
                    current_digest = SDKSourceResetExternalPlanes._redis_state_digest(
                        key,
                        current_type,
                        current_dump,
                    )
                    if current_type != expected_type or not hmac.compare_digest(current_digest, expected_digest):
                        raise RuntimeError("Redis key changed after inventory")

                    reviewed_value_types = {row.value_type for row in rows}
                    if "key" in reviewed_value_types:
                        if reviewed_value_types != {"key"} or len(rows) != 1:
                            raise RuntimeError("Redis whole-key review is inconsistent")
                        continue
                    if reviewed_value_types != {current_type}:
                        raise RuntimeError("Redis review value type is inconsistent")

                    list_rows = [row for row in rows if row.value_type == "list"]
                    if list_rows:
                        snapshot = tuple(str(value) for value in pipe.lrange(key, 0, -1))
                        list_snapshots[key] = snapshot
                        try:
                            positions = tuple(int(row.locator or "") for row in list_rows)
                        except ValueError as exc:
                            raise RuntimeError("Redis list review position is invalid") from exc
                        if len(set(positions)) != len(positions):
                            raise RuntimeError("Redis list review positions are not unique")
                        for row, position in zip(list_rows, positions, strict=True):
                            if (
                                position < 0
                                or position >= len(snapshot)
                                or row.raw_value is None
                                or snapshot[position] != row.raw_value
                            ):
                                raise RuntimeError("Redis list changed after inventory")
                        positions_by_key[key] = tuple(sorted(positions))
                    elif current_type in {"set", "zset"}:
                        members = tuple(row.raw_value for row in rows)
                        if None in members or len(set(members)) != len(members):
                            raise RuntimeError("Redis member review is incomplete")
                    elif current_type in {"hash", "stream"}:
                        locators = tuple(row.locator for row in rows)
                        if None in locators or len(set(locators)) != len(locators):
                            raise RuntimeError("Redis locator review is incomplete")
                    else:
                        raise RuntimeError("Redis review type is not deletable")

                pipe.multi()
                queued_operations = 0
                count_result_checks: list[tuple[int, int]] = []
                success_result_checks: list[int] = []
                for row in included:
                    if row.value_type == "list":
                        continue
                    if row.value_type == "key":
                        pipe.delete(row.key)
                        count_result_checks.append((queued_operations, 1))
                        queued_operations += 1
                    elif row.value_type == "set" and row.raw_value is not None:
                        pipe.srem(row.key, row.raw_value)
                        count_result_checks.append((queued_operations, 1))
                        queued_operations += 1
                    elif row.value_type == "zset" and row.raw_value is not None:
                        pipe.zrem(row.key, row.raw_value)
                        count_result_checks.append((queued_operations, 1))
                        queued_operations += 1
                    elif row.value_type == "hash" and row.locator is not None:
                        pipe.hdel(row.key, row.locator)
                        count_result_checks.append((queued_operations, 1))
                        queued_operations += 1
                    elif row.value_type == "stream" and row.locator is not None:
                        pipe.xdel(row.key, row.locator)
                        count_result_checks.append((queued_operations, 1))
                        queued_operations += 1
                    else:
                        raise RuntimeError("Redis review reference is not deletable")

                for key, positions in sorted(positions_by_key.items()):
                    snapshot = list_snapshots[key]
                    marker = f"__open_wearables_source_reset__:{uuid4()}"
                    while marker in snapshot:
                        marker = f"__open_wearables_source_reset__:{uuid4()}"
                    for position in positions:
                        pipe.lset(key, position, marker)
                        success_result_checks.append(queued_operations)
                        queued_operations += 1
                    pipe.lrem(key, 0, marker)
                    count_result_checks.append((queued_operations, len(positions)))
                    queued_operations += 1

                results = pipe.execute()
                if len(results) != queued_operations:
                    raise RuntimeError("Redis deletion result count is incomplete")
                for result_index, expected_count in count_result_checks:
                    result = results[result_index]
                    if type(result) is not int or result != expected_count:
                        raise RuntimeError("Redis deletion was incomplete")
                for result_index in success_result_checks:
                    if results[result_index] is not True:
                        raise RuntimeError("Redis list position deletion was incomplete")
            finally:
                pipe.reset()
        except Exception as exc:
            raise RuntimeError("Redis deletion is unavailable") from exc


sdk_source_reset_external_planes = SDKSourceResetExternalPlanes()
