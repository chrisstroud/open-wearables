import base64
import json
from collections.abc import Iterator
from dataclasses import replace
from io import BytesIO
from typing import Any
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest

from app.config import SourceResetS3Target, settings
from app.integrations.redis_client import get_redis_client
from app.services.sdk_source_reset_external import (
    FIT_OBJECTS,
    INTERNAL_LOCATOR_PROVIDER,
    QUEUED_TASKS,
    RAW_OBJECTS,
    REDIS_COORDINATION,
    REDIS_PRIORITY_SEPARATOR,
    ProviderIdentityScope,
    SDKSourceResetExternalPlanes,
)


class FakeS3Client:
    def __init__(
        self,
        objects: dict[str, bytes],
        *,
        unreadable: set[str] | None = None,
        versioning_status: str | None = None,
    ) -> None:
        self.objects = objects
        self.unreadable = unreadable or set()
        self.versioning_status = versioning_status
        self.list_requests: list[tuple[str, str]] = []

    def get_bucket_versioning(self, **_kwargs: object) -> dict[str, str]:
        return {"Status": self.versioning_status} if self.versioning_status else {}

    def list_objects_v2(self, **kwargs: object) -> dict[str, object]:
        self.list_requests.append((str(kwargs["Bucket"]), str(kwargs["Prefix"])))
        prefix = str(kwargs["Prefix"])
        return {
            "Contents": [{"Key": key} for key in sorted(self.objects) if key.startswith(prefix)],
            "IsTruncated": False,
        }

    def get_object(self, **kwargs: object) -> dict[str, object]:
        key = str(kwargs["Key"])
        if key in self.unreadable:
            raise RuntimeError("unreadable")
        return {"Body": BytesIO(self.objects[key]), "Metadata": {}}


class FakeRedisClient:
    def __init__(self, queue_values: list[str], *, count_result_override: object | None = None) -> None:
        self.queue_values = queue_values
        self.count_result_override = count_result_override

    def ping(self) -> bool:
        return True

    def scan_iter(self, *, match: str, count: int) -> list[str]:
        del match, count
        return ["webhook_sync"]

    def type(self, _key: str) -> str:
        return "list"

    def dump(self, _key: str) -> bytes:
        return json.dumps(["list", self.queue_values], separators=(",", ":")).encode()

    def lrange(self, _key: str, _start: int, _end: int) -> list[str]:
        return list(self.queue_values)

    def pipeline(self, *, transaction: bool) -> "FakeRedisPipeline":
        assert transaction is True
        return FakeRedisPipeline(self)


class FakeRedisPipeline:
    def __init__(self, client: FakeRedisClient) -> None:
        self.client = client
        self.commands: list[tuple[object, ...]] = []

    def watch(self, *_keys: str) -> None:
        pass

    def type(self, key: str) -> str:
        return self.client.type(key)

    def dump(self, key: str) -> bytes:
        return self.client.dump(key)

    def lrange(self, key: str, start: int, end: int) -> list[str]:
        return self.client.lrange(key, start, end)

    def multi(self) -> None:
        pass

    def ping(self) -> None:
        self.commands.append(("ping",))

    def lset(self, key: str, position: int, value: str) -> None:
        self.commands.append(("lset", key, position, value))

    def lrem(self, key: str, count: int, value: str) -> None:
        self.commands.append(("lrem", key, count, value))

    def execute(self) -> list[object]:
        results: list[object] = []
        for command in self.commands:
            if command[0] == "ping":
                results.append(True)
            elif command[0] == "lset":
                _, _key, position, value = command
                self.client.queue_values[int(position)] = str(value)
                results.append(True)
            elif command[0] == "lrem":
                _, _key, count, value = command
                assert count == 0
                before = len(self.client.queue_values)
                self.client.queue_values[:] = [item for item in self.client.queue_values if item != value]
                count_result = before - len(self.client.queue_values)
                results.append(
                    self.client.count_result_override if self.client.count_result_override is not None else count_result
                )
        return results

    def reset(self) -> None:
        pass


class FakeCeleryInspector:
    def __init__(self, active_tasks: list[dict[str, object]]) -> None:
        self.active_tasks = active_tasks

    def ping(self) -> dict[str, dict[str, str]]:
        return {"worker": {"ok": "pong"}}

    def active(self) -> dict[str, list[dict[str, object]]]:
        return {"worker": self.active_tasks}

    def reserved(self) -> dict[str, list[dict[str, object]]]:
        return {"worker": []}

    def scheduled(self) -> dict[str, list[dict[str, object]]]:
        return {"worker": []}


@pytest.fixture(autouse=True)
def _attested_source_reset_s3_history() -> Iterator[None]:
    with (
        patch.object(settings, "source_reset_s3_target_history_complete", True),
        patch.object(settings, "source_reset_retired_s3_targets", []),
    ):
        yield


@pytest.fixture
def redis_test_keys() -> Iterator[list[str]]:
    keys: list[str] = []
    try:
        yield keys
    finally:
        if keys:
            get_redis_client().delete(*keys)


def _seed_reviewed_redis_type(
    client: Any,
    redis_type: str,
    user_id: str,
    tracked_keys: list[str],
) -> tuple[str, str, str]:
    key = f"source-reset-review:{redis_type}:{uuid4()}"
    tracked_keys.append(key)
    target = json.dumps({"user_id": user_id, "payload": f"target-{redis_type}"})
    other = json.dumps({"user_id": "other-user", "payload": f"other-{redis_type}"})
    if redis_type == "string":
        client.set(key, target)
    elif redis_type == "list":
        client.rpush(key, target, other)
    elif redis_type == "set":
        client.sadd(key, target, other)
    elif redis_type == "zset":
        client.zadd(key, {target: 1.0, other: 2.0})
    elif redis_type == "hash":
        client.hset(key, mapping={"target": target, "other": other})
    elif redis_type == "stream":
        client.xadd(key, {"payload": target})
        client.xadd(key, {"payload": other})
    else:  # pragma: no cover - guarded by parametrization
        raise AssertionError(f"unsupported Redis test type: {redis_type}")
    return key, target, other


def test_nested_celery_base64_body_is_user_scoped() -> None:
    user_id = str(uuid4())
    body = base64.b64encode(json.dumps([[], {"user_id": user_id}, {"callbacks": None}]).encode()).decode()
    celery_envelope = json.dumps(
        {
            "body": body,
            "headers": {"task": "app.tasks.sync", "id": str(uuid4())},
            "properties": {"delivery_info": {"routing_key": "sdk_sync"}},
        }
    )

    assert SDKSourceResetExternalPlanes._contains_user(celery_envelope, user_id) is True
    assert SDKSourceResetExternalPlanes._contains_user(celery_envelope, str(uuid4())) is False


def test_provider_identity_proof_matches_nested_webhook_without_persisting_raw_identifier() -> None:
    provider_user_id = "whoop-user-4815162342"
    original = ProviderIdentityScope.from_values({"whoop": {provider_user_id}})
    proof = original.to_proof()
    restored = ProviderIdentityScope.from_proof(proof, required=True)
    body = base64.b64encode(
        json.dumps(
            [
                [
                    "whoop",
                    {"type": "sleep.updated", "user_id": provider_user_id},
                    "trace-id",
                ]
            ]
        ).encode()
    ).decode()
    envelope = {"body": body, "headers": {"task": "process_webhook_push"}}

    assert provider_user_id not in json.dumps(proof)
    assert restored.proof_unavailable is False
    assert SDKSourceResetExternalPlanes._contains_identity(
        envelope,
        user_id=str(uuid4()),
        identity_scope=restored,
    )


def test_internal_batch_locator_matches_queue_and_result_without_user_identifier() -> None:
    batch_id = str(uuid4())
    original = ProviderIdentityScope.from_values({INTERNAL_LOCATOR_PROVIDER: {batch_id}})
    restored = ProviderIdentityScope.from_proof(original.to_proof(), required=True)
    envelope = {
        "body": base64.b64encode(json.dumps([[], {"batch_id": batch_id}, {}]).encode()).decode(),
        "status": "SUCCESS",
    }

    assert batch_id not in json.dumps(original.to_proof())
    assert SDKSourceResetExternalPlanes._contains_identity(
        envelope,
        user_id=str(uuid4()),
        identity_scope=restored,
    )


def test_rotated_identity_fingerprint_key_fails_closed() -> None:
    proof = ProviderIdentityScope.from_values({"whoop": {"provider-user"}}).to_proof()

    with patch.object(settings, "secret_key", "rotated-test-secret"):
        restored = ProviderIdentityScope.from_proof(proof, required=True)

    assert restored.proof_unavailable is True


def test_retired_s3_target_contract_normalizes_prefixes_and_rejects_url_credentials() -> None:
    target = SourceResetS3Target(
        bucket=" historical-private-bucket ",
        endpoint_url="https://objects.example.test/",
        raw_prefix="/legacy/raw/",
        fit_prefix="/legacy/fit/",
        apple_xml_prefix="/legacy/xml/",
    )

    assert target.bucket == "historical-private-bucket"
    assert target.endpoint_url == "https://objects.example.test"
    assert target.raw_prefix == "legacy/raw"
    assert target.fit_prefix == "legacy/fit"
    assert target.apple_xml_prefix == "legacy/xml"
    with pytest.raises(ValueError, match="must not contain credentials"):
        SourceResetS3Target(bucket="private", endpoint_url="https://access:secret@objects.example.test")
    with pytest.raises(ValueError, match="must not contain a query or fragment"):
        SourceResetS3Target(bucket="private", endpoint_url="https://objects.example.test?token=secret")


def test_external_configuration_digest_is_equivalent_for_retired_target_reordering() -> None:
    service = SDKSourceResetExternalPlanes()
    first_target = SourceResetS3Target(
        bucket="retired-a",
        endpoint_url="https://a.objects.example.test",
        raw_prefix="legacy/raw-a",
        fit_prefix="legacy/fit-a",
    )
    second_target = SourceResetS3Target(
        bucket="retired-b",
        endpoint_url="https://b.objects.example.test",
        raw_prefix="legacy/raw-b",
        fit_prefix="legacy/fit-b",
        apple_xml_prefix="legacy/xml-b",
    )
    with (
        patch.object(settings, "aws_bucket_name", None),
        patch.object(settings, "raw_payload_s3_bucket", None),
        patch.object(settings, "source_reset_retired_s3_targets", [first_target, second_target]),
    ):
        ordered_digest = service._configuration_digest_sha256()
    with (
        patch.object(settings, "aws_bucket_name", None),
        patch.object(settings, "raw_payload_s3_bucket", None),
        patch.object(settings, "source_reset_retired_s3_targets", [second_target, first_target]),
    ):
        reversed_digest = service._configuration_digest_sha256()

    assert ordered_digest == reversed_digest


def test_external_configuration_digest_changes_with_target_attestation_and_redis_topology() -> None:
    service = SDKSourceResetExternalPlanes()
    with (
        patch.object(settings, "aws_bucket_name", None),
        patch.object(settings, "raw_payload_s3_bucket", "current-private"),
        patch.object(settings, "raw_payload_s3_endpoint_url", "https://objects.example.test"),
        patch.object(settings, "raw_payload_s3_prefix", "current/raw"),
    ):
        baseline = service._configuration_digest_sha256()
        with patch.object(settings, "raw_payload_s3_prefix", "changed/raw"):
            changed_target = service._configuration_digest_sha256()
        with patch.object(settings, "source_reset_s3_target_history_complete", False):
            changed_attestation = service._configuration_digest_sha256()
        with patch.object(settings, "redis_db", settings.redis_db + 1):
            changed_redis_topology = service._configuration_digest_sha256()

    assert len({baseline, changed_target, changed_attestation, changed_redis_topology}) == 4


def test_configuration_digest_is_bound_into_zero_count_external_identity_tokens() -> None:
    service = SDKSourceResetExternalPlanes()
    with (
        patch.object(service, "_object_inventory", return_value=((), ())),
        patch.object(service, "_redis_inventory", return_value=((), ())),
        patch.object(service, "_task_inventory", return_value=((), ())),
    ):
        baseline = service.inventory(uuid4())
        with patch.object(settings, "raw_payload_s3_prefix", "changed/raw"):
            changed = service.inventory(uuid4())

    assert all(count == 0 for count in baseline.counts.values())
    assert len(baseline.configuration_digest_sha256) == 64
    assert baseline.configuration_digest_sha256 != changed.configuration_digest_sha256
    assert baseline.identity_tokens[REDIS_COORDINATION] != changed.identity_tokens[REDIS_COORDINATION]


def test_disabled_toggles_still_inventory_historical_s3_fit_and_unknown_webhook_objects() -> None:
    service = SDKSourceResetExternalPlanes()
    user_id = uuid4()
    provider_user_id = "whoop-user-reset-target"
    scope = ProviderIdentityScope.from_values({"whoop": {provider_user_id}})
    direct_raw = f"raw-payloads/apple/sdk/2026-08-26/{user_id}/direct.json"
    matching_unknown = "raw-payloads/whoop/webhook/2026-08-26/_unknown/target.json"
    other_unknown = "raw-payloads/whoop/webhook/2026-08-26/_unknown/other.json"
    fit_object = f"fit-files/garmin/2026-08-26/{user_id}/activity.fit"
    fake_s3 = FakeS3Client(
        {
            direct_raw: b"not-read-because-the-key-is-authoritative",
            matching_unknown: json.dumps({"user_id": provider_user_id, "type": "sleep.updated"}).encode(),
            other_unknown: json.dumps({"user_id": "another-whoop-user"}).encode(),
            fit_object: b"fit-bytes",
        }
    )

    with (
        patch.object(settings, "aws_bucket_name", None),
        patch.object(settings, "raw_payload_s3_bucket", "historical-private-bucket"),
        patch.object(settings, "raw_payload_s3_endpoint_url", None),
        patch.object(settings, "raw_payload_storage", "disabled"),
        patch.object(settings, "store_fit_files", False),
        patch.object(service, "_s3_client", return_value=fake_s3),
    ):
        objects, blockers = service._object_inventory(user_id, scope)

    assert blockers == ()
    assert {(row.resource_key, row.key) for row in objects} == {
        (RAW_OBJECTS, direct_raw),
        (RAW_OBJECTS, matching_unknown),
        (FIT_OBJECTS, fit_object),
    }


def test_retired_bucket_prefix_and_endpoint_are_scanned_with_current_toggles_off() -> None:
    service = SDKSourceResetExternalPlanes()
    user_id = uuid4()
    retired_raw = f"legacy/raw/apple/sdk/2025-01-02/{user_id}/payload.json"
    retired_fit = f"legacy/fit/garmin/2025-01-02/{user_id}/activity.fit"
    retired_xml = f"legacy/xml/{user_id}/raw/export.xml"
    current_s3 = FakeS3Client({})
    retired_s3 = FakeS3Client(
        {
            retired_raw: b"direct-user-key-does-not-require-body-inspection",
            retired_fit: b"fit-bytes",
            retired_xml: b"xml-bytes",
        }
    )
    retired_target = SourceResetS3Target(
        bucket="retired-private-bucket",
        endpoint_url="https://retired-objects.example.test",
        raw_prefix="legacy/raw",
        fit_prefix="legacy/fit",
        apple_xml_prefix="legacy/xml",
    )

    def client_for_endpoint(endpoint_url: str | None) -> FakeS3Client:
        if endpoint_url == retired_target.endpoint_url:
            return retired_s3
        return current_s3

    with (
        patch.object(settings, "aws_bucket_name", None),
        patch.object(settings, "raw_payload_s3_bucket", "current-private-bucket"),
        patch.object(settings, "raw_payload_s3_endpoint_url", "https://current-objects.example.test"),
        patch.object(settings, "raw_payload_s3_prefix", "current/raw"),
        patch.object(settings, "raw_payload_storage", "disabled"),
        patch.object(settings, "store_fit_files", False),
        patch.object(settings, "source_reset_retired_s3_targets", [retired_target]),
        patch.object(service, "_s3_client", side_effect=client_for_endpoint),
    ):
        objects, blockers = service._object_inventory(user_id, ProviderIdentityScope())

    assert blockers == ()
    assert {(row.resource_key, row.bucket, row.key, row.endpoint_url) for row in objects} == {
        (RAW_OBJECTS, retired_target.bucket, retired_raw, retired_target.endpoint_url),
        (FIT_OBJECTS, retired_target.bucket, retired_fit, retired_target.endpoint_url),
        (RAW_OBJECTS, retired_target.bucket, retired_xml, retired_target.endpoint_url),
    }
    assert (retired_target.bucket, "legacy/raw/") in retired_s3.list_requests
    assert (retired_target.bucket, "legacy/fit/") in retired_s3.list_requests
    assert (retired_target.bucket, f"legacy/xml/{user_id}/") in retired_s3.list_requests
    assert ("current-private-bucket", "current/raw/") in current_s3.list_requests


def test_unattested_s3_target_history_blocks_even_when_current_target_is_scannable() -> None:
    service = SDKSourceResetExternalPlanes()
    with (
        patch.object(settings, "aws_bucket_name", None),
        patch.object(settings, "raw_payload_s3_bucket", "current-private-bucket"),
        patch.object(settings, "raw_payload_s3_endpoint_url", None),
        patch.object(settings, "raw_payload_storage", "disabled"),
        patch.object(settings, "store_fit_files", False),
        patch.object(settings, "source_reset_s3_target_history_complete", False),
        patch.object(service, "_s3_client", return_value=FakeS3Client({})),
    ):
        objects, blockers = service._object_inventory(uuid4(), ProviderIdentityScope())

    assert objects == ()
    assert blockers == ("open-wearables.external-storage-target-history-unverifiable",)


def test_unknown_raw_object_that_cannot_be_inspected_blocks_false_zero() -> None:
    service = SDKSourceResetExternalPlanes()
    provider_user_id = "whoop-user-reset-target"
    unknown_key = "raw-payloads/whoop/webhook/2026-08-26/_unknown/unreadable.json"
    fake_s3 = FakeS3Client(
        {unknown_key: json.dumps({"user_id": provider_user_id}).encode()},
        unreadable={unknown_key},
    )

    with (
        patch.object(settings, "aws_bucket_name", None),
        patch.object(settings, "raw_payload_s3_bucket", "historical-private-bucket"),
        patch.object(settings, "raw_payload_s3_endpoint_url", None),
        patch.object(settings, "raw_payload_storage", "disabled"),
        patch.object(settings, "store_fit_files", False),
        patch.object(service, "_s3_client", return_value=fake_s3),
    ):
        objects, blockers = service._object_inventory(
            uuid4(),
            ProviderIdentityScope.from_values({"whoop": {provider_user_id}}),
        )

    assert objects == ()
    assert blockers == ("open-wearables.raw-payload-objects.identity-inspection-unavailable",)


def test_versioned_s3_plane_blocks_current_key_false_zero() -> None:
    service = SDKSourceResetExternalPlanes()
    fake_s3 = FakeS3Client({}, versioning_status="Enabled")

    with (
        patch.object(settings, "aws_bucket_name", None),
        patch.object(settings, "raw_payload_s3_bucket", "versioned-private-bucket"),
        patch.object(settings, "raw_payload_s3_endpoint_url", None),
        patch.object(settings, "raw_payload_storage", "disabled"),
        patch.object(settings, "store_fit_files", False),
        patch.object(service, "_s3_client", return_value=fake_s3),
    ):
        objects, blockers = service._object_inventory(uuid4(), ProviderIdentityScope())

    assert objects == ()
    assert blockers == (
        "open-wearables.fit-objects.version-history-unerasable",
        "open-wearables.raw-payload-objects.version-history-unerasable",
    )


def test_provider_scoped_queued_webhook_is_inventoried_without_internal_user_id() -> None:
    service = SDKSourceResetExternalPlanes()
    provider_user_id = "whoop-user-reset-target"
    target = json.dumps(["whoop", {"user_id": provider_user_id}, "trace-target"])
    other = json.dumps(["whoop", {"user_id": "other-user"}, "trace-other"])
    with patch(
        "app.services.sdk_source_reset_external.get_redis_client",
        return_value=FakeRedisClient([target, other]),
    ):
        references, blockers = service._redis_inventory(
            uuid4(),
            ProviderIdentityScope.from_values({"whoop": {provider_user_id}}),
        )

    assert blockers == ()
    assert len(references) == 1
    assert references[0].resource_key == QUEUED_TASKS
    assert references[0].locator == "0"
    assert references[0].raw_value == target
    assert references[0].reviewed_key_type == "list"
    assert references[0].reviewed_state_digest_sha256 is not None


@pytest.mark.parametrize("redis_type", ["string", "list", "set", "zset", "hash", "stream"])
def test_exact_reviewed_redis_state_deletes_only_the_selected_reference(
    redis_type: str,
    redis_test_keys: list[str],
) -> None:
    service = SDKSourceResetExternalPlanes()
    client = get_redis_client()
    user_id = str(uuid4())
    key, _target, other = _seed_reviewed_redis_type(client, redis_type, user_id, redis_test_keys)

    references, blockers = service._redis_inventory(UUID(user_id))
    selected = tuple(row for row in references if row.key == key)

    assert blockers == ()
    assert len(selected) == 1
    assert selected[0].reviewed_key_type == redis_type
    assert selected[0].reviewed_state_digest_sha256 is not None
    service.erase_redis(selected, include_results=True)

    if redis_type == "string":
        assert client.exists(key) == 0
    elif redis_type == "list":
        assert client.lrange(key, 0, -1) == [other]
    elif redis_type == "set":
        assert client.smembers(key) == {other}
    elif redis_type == "zset":
        assert client.zrange(key, 0, -1) == [other]
    elif redis_type == "hash":
        assert client.hgetall(key) == {"other": other}
    else:
        assert [values["payload"] for _entry_id, values in client.xrange(key)] == [other]

    fresh_references, fresh_blockers = service._redis_inventory(UUID(user_id))
    assert fresh_blockers == ()
    assert all(row.key != key for row in fresh_references)
    service.erase_redis(tuple(row for row in fresh_references if row.key == key), include_results=True)


@pytest.mark.parametrize("redis_type", ["string", "list", "set", "zset", "hash", "stream"])
def test_post_review_redis_mutation_fails_closed_for_every_supported_type(
    redis_type: str,
    redis_test_keys: list[str],
) -> None:
    service = SDKSourceResetExternalPlanes()
    client = get_redis_client()
    user_id = str(uuid4())
    key, target, other = _seed_reviewed_redis_type(client, redis_type, user_id, redis_test_keys)
    references, blockers = service._redis_inventory(UUID(user_id))
    selected = tuple(row for row in references if row.key == key)
    replacement = json.dumps({"user_id": "replacement-user", "payload": redis_type})

    assert blockers == ()
    assert len(selected) == 1
    if redis_type == "string":
        client.set(key, replacement)
    elif redis_type == "list":
        client.rpush(key, target)
    elif redis_type == "set":
        client.srem(key, other)
        client.sadd(key, replacement)
    elif redis_type == "zset":
        client.zadd(key, {other: 99.0})
    elif redis_type == "hash":
        client.hset(key, "other", replacement)
    else:
        other_entry_id = next(entry_id for entry_id, values in client.xrange(key) if values["payload"] == other)
        client.xdel(key, other_entry_id)
        client.xadd(key, {"payload": replacement})
    changed_dump = client.dump(key)

    with pytest.raises(RuntimeError, match="Redis deletion is unavailable"):
        service.erase_redis(selected, include_results=True)

    assert client.dump(key) == changed_dump


def test_kombu_priority_queue_key_is_governed_as_queued_work(redis_test_keys: list[str]) -> None:
    service = SDKSourceResetExternalPlanes()
    client = get_redis_client()
    user_id = str(uuid4())
    key = f"webhook_sync{REDIS_PRIORITY_SEPARATOR}3"
    redis_test_keys.append(key)
    client.delete(key)
    client.rpush(
        key,
        json.dumps({"user_id": user_id, "payload": "target-priority"}),
        json.dumps({"user_id": "other-user", "payload": "other-priority"}),
    )

    references, blockers = service._redis_inventory(UUID(user_id))
    selected = tuple(row for row in references if row.key == key)

    assert blockers == ()
    assert len(selected) == 1
    assert selected[0].resource_key == QUEUED_TASKS


def test_unacked_hash_inventory_derives_and_deletes_paired_index_member(
    redis_test_keys: list[str],
) -> None:
    service = SDKSourceResetExternalPlanes()
    client = get_redis_client()
    user_id = str(uuid4())
    delivery_tag = f"target-delivery-{uuid4()}"
    other_tag = f"other-delivery-{uuid4()}"
    redis_test_keys.extend(["unacked", "unacked_index"])
    client.delete("unacked", "unacked_index")
    client.hset(
        "unacked",
        mapping={
            delivery_tag: json.dumps({"user_id": user_id, "payload": "target-unacked"}),
            other_tag: json.dumps({"user_id": "other-user", "payload": "other-unacked"}),
        },
    )
    client.zadd("unacked_index", {delivery_tag: 1.0, other_tag: 2.0})

    references, blockers = service._redis_inventory(UUID(user_id))
    selected = tuple(row for row in references if row.key in {"unacked", "unacked_index"})

    assert blockers == ()
    assert {(row.key, row.value_type, row.locator, row.raw_value) for row in selected} == {
        ("unacked", "hash", delivery_tag, json.dumps({"user_id": user_id, "payload": "target-unacked"})),
        ("unacked_index", "zset", None, delivery_tag),
    }
    service.erase_redis(selected, include_results=True)
    assert client.hget("unacked", delivery_tag) is None
    assert client.zscore("unacked_index", delivery_tag) is None
    assert client.hget("unacked", other_tag) is not None
    assert client.zscore("unacked_index", other_tag) == 2.0


def test_unacked_index_mutation_after_review_blocks_both_deletions(redis_test_keys: list[str]) -> None:
    service = SDKSourceResetExternalPlanes()
    client = get_redis_client()
    user_id = str(uuid4())
    delivery_tag = f"target-delivery-{uuid4()}"
    other_tag = f"other-delivery-{uuid4()}"
    redis_test_keys.extend(["unacked", "unacked_index"])
    client.delete("unacked", "unacked_index")
    target = json.dumps({"user_id": user_id, "payload": "target-unacked"})
    client.hset("unacked", mapping={delivery_tag: target, other_tag: "other"})
    client.zadd("unacked_index", {delivery_tag: 1.0, other_tag: 2.0})

    references, blockers = service._redis_inventory(UUID(user_id))
    selected = tuple(row for row in references if row.key in {"unacked", "unacked_index"})
    client.zadd("unacked_index", {other_tag: 3.0})

    assert blockers == ()
    with pytest.raises(RuntimeError, match="Redis deletion is unavailable"):
        service.erase_redis(selected, include_results=True)
    assert client.hget("unacked", delivery_tag) == target
    assert client.zscore("unacked_index", delivery_tag) == 1.0


def test_stream_inventory_preserves_duplicate_field_values(redis_test_keys: list[str]) -> None:
    service = SDKSourceResetExternalPlanes()
    client = get_redis_client()
    user_id = str(uuid4())
    key = f"source-reset-review:stream-duplicate-fields:{uuid4()}"
    redis_test_keys.append(key)
    target = json.dumps({"user_id": user_id, "payload": "target-stream"})
    client.execute_command("XADD", key, "*", "payload", target, "payload", "other-user")

    references, blockers = service._redis_inventory(UUID(user_id))
    selected = tuple(row for row in references if row.key == key)

    assert blockers == ()
    assert len(selected) == 1
    assert selected[0].value_type == "stream"


def test_durable_list_identity_ignores_unrelated_position_shift() -> None:
    service = SDKSourceResetExternalPlanes()
    provider_user_id = "whoop-user-reset-target"
    target = json.dumps(["whoop", {"user_id": provider_user_id}, "trace-target"])
    other = json.dumps(["whoop", {"user_id": "other-user"}, "trace-other"])
    inserted = json.dumps(["whoop", {"user_id": "inserted-user"}, "trace-inserted"])
    client = FakeRedisClient([other, target])
    scope = ProviderIdentityScope.from_values({"whoop": {provider_user_id}})
    with (
        patch.object(service, "_object_inventory", return_value=((), ())),
        patch.object(service, "_task_inventory", return_value=((), ())),
        patch("app.services.sdk_source_reset_external.get_redis_client", return_value=client),
    ):
        before = service.inventory(uuid4(), identity_scope=scope)
        client.queue_values.insert(0, inserted)
        after = service.inventory(uuid4(), identity_scope=scope)

    assert before.redis_references[0].locator == "1"
    assert after.redis_references[0].locator == "2"
    assert before.redis_references[0].reviewed_state_digest_sha256 != (
        after.redis_references[0].reviewed_state_digest_sha256
    )
    assert before.identity_tokens[QUEUED_TASKS] == after.identity_tokens[QUEUED_TASKS]


def test_non_integer_redis_delete_count_fails_closed_and_requires_fresh_inventory() -> None:
    service = SDKSourceResetExternalPlanes()
    provider_user_id = "whoop-user-reset-target"
    target = json.dumps(["whoop", {"user_id": provider_user_id}, "trace-target"])
    other = json.dumps(["whoop", {"user_id": "other-user"}, "trace-other"])
    client = FakeRedisClient([target, other], count_result_override="1")
    scope = ProviderIdentityScope.from_values({"whoop": {provider_user_id}})
    with patch("app.services.sdk_source_reset_external.get_redis_client", return_value=client):
        references, blockers = service._redis_inventory(uuid4(), scope)
        assert blockers == ()

        with pytest.raises(RuntimeError, match="Redis deletion is unavailable"):
            service.erase_redis(references, include_results=True)

        fresh_references, fresh_blockers = service._redis_inventory(uuid4(), scope)
        assert fresh_blockers == ()
        assert fresh_references == ()
        service.erase_redis(fresh_references, include_results=True)


def test_inconsistent_redis_reference_type_is_rejected_before_mutation() -> None:
    service = SDKSourceResetExternalPlanes()
    provider_user_id = "whoop-user-reset-target"
    target = json.dumps(["whoop", {"user_id": provider_user_id}, "trace-target"])
    other = json.dumps(["whoop", {"user_id": "other-user"}, "trace-other"])
    client = FakeRedisClient([target, other])
    scope = ProviderIdentityScope.from_values({"whoop": {provider_user_id}})
    with patch("app.services.sdk_source_reset_external.get_redis_client", return_value=client):
        references, blockers = service._redis_inventory(uuid4(), scope)
        forged = (replace(references[0], value_type="set"),)

        assert blockers == ()
        with pytest.raises(RuntimeError, match="Redis deletion is unavailable"):
            service.erase_redis(forged, include_results=True)

    assert client.queue_values == [target, other]


def test_duplicate_identical_queue_entries_preserve_multiplicity_and_delete_reviewed_positions() -> None:
    service = SDKSourceResetExternalPlanes()
    provider_user_id = "whoop-user-reset-target"
    target = json.dumps(["whoop", {"user_id": provider_user_id}, "trace-target"])
    other = json.dumps(["whoop", {"user_id": "other-user"}, "trace-other"])
    client = FakeRedisClient([target, target, other])
    with patch(
        "app.services.sdk_source_reset_external.get_redis_client",
        return_value=client,
    ):
        references, blockers = service._redis_inventory(
            uuid4(),
            ProviderIdentityScope.from_values({"whoop": {provider_user_id}}),
        )

        assert blockers == ()
        assert [row.locator for row in references] == ["0", "1"]
        assert len({row.reviewed_state_digest_sha256 for row in references}) == 1
        service.erase_redis(references, include_results=True)

    assert client.queue_values == [other]


def test_queue_duplicate_inserted_after_review_blocks_deletion() -> None:
    service = SDKSourceResetExternalPlanes()
    provider_user_id = "whoop-user-reset-target"
    target = json.dumps(["whoop", {"user_id": provider_user_id}, "trace-target"])
    other = json.dumps(["whoop", {"user_id": "other-user"}, "trace-other"])
    client = FakeRedisClient([target, other])
    with patch(
        "app.services.sdk_source_reset_external.get_redis_client",
        return_value=client,
    ):
        references, blockers = service._redis_inventory(
            uuid4(),
            ProviderIdentityScope.from_values({"whoop": {provider_user_id}}),
        )
        assert blockers == ()

        client.queue_values.append(target)
        reviewed_then_changed = list(client.queue_values)
        with pytest.raises(RuntimeError, match="Redis deletion is unavailable"):
            service.erase_redis(references, include_results=True)

    assert client.queue_values == reviewed_then_changed


def test_equal_count_queue_substitution_after_review_blocks_deletion() -> None:
    service = SDKSourceResetExternalPlanes()
    provider_user_id = "whoop-user-reset-target"
    target = json.dumps(["whoop", {"user_id": provider_user_id}, "trace-target"])
    other = json.dumps(["whoop", {"user_id": "other-user"}, "trace-other"])
    substituted = json.dumps(["whoop", {"user_id": "substituted-user"}, "trace-substitution"])
    client = FakeRedisClient([target, other])
    with patch(
        "app.services.sdk_source_reset_external.get_redis_client",
        return_value=client,
    ):
        references, blockers = service._redis_inventory(
            uuid4(),
            ProviderIdentityScope.from_values({"whoop": {provider_user_id}}),
        )
        assert blockers == ()

        client.queue_values[:] = [target, substituted]
        with pytest.raises(RuntimeError, match="Redis deletion is unavailable"):
            service.erase_redis(references, include_results=True)

    assert client.queue_values == [target, substituted]


def test_shared_provider_identity_blocks_provider_only_external_selection_for_user_a() -> None:
    service = SDKSourceResetExternalPlanes()
    user_a = uuid4()
    shared_provider_user_id = "shared-whoop-user-a-and-b"
    scope = ProviderIdentityScope.from_values(
        {"whoop": {shared_provider_user_id}},
        ambiguous_providers={"whoop"},
    )
    unknown_key = "raw-payloads/whoop/webhook/2026-08-26/_unknown/shared.json"
    shared_payload = json.dumps(["whoop", {"user_id": shared_provider_user_id}, "shared-trace"])
    fake_s3 = FakeS3Client({unknown_key: json.dumps({"user_id": shared_provider_user_id}).encode()})
    fake_redis = FakeRedisClient([shared_payload])
    inspector = FakeCeleryInspector(
        [
            {
                "id": "shared-provider-active-task",
                "name": "process_webhook_push",
                "args": ["whoop", {"user_id": shared_provider_user_id}, "shared-trace"],
            }
        ]
    )

    with (
        patch.object(settings, "aws_bucket_name", None),
        patch.object(settings, "raw_payload_s3_bucket", "current-private-bucket"),
        patch.object(settings, "raw_payload_s3_endpoint_url", None),
        patch.object(settings, "raw_payload_storage", "disabled"),
        patch.object(settings, "store_fit_files", False),
        patch.object(service, "_s3_client", return_value=fake_s3),
        patch(
            "app.services.sdk_source_reset_external.get_redis_client",
            return_value=fake_redis,
        ),
        patch(
            "app.services.sdk_source_reset_external.current_celery_app.control.inspect",
            return_value=inspector,
        ),
    ):
        inventory = service.inventory(user_a, identity_scope=scope)

    assert inventory.blockers == ("open-wearables.external-identity-scope-ambiguous",)
    assert inventory.objects == ()
    assert inventory.redis_references == ()
    assert inventory.active_task_ids == ()
    assert inventory.counts[RAW_OBJECTS] == 0
    assert inventory.counts[QUEUED_TASKS] == 0


def test_provider_scoped_active_webhook_is_inventoried_without_internal_user_id() -> None:
    provider_user_id = "whoop-user-active-reset-target"
    inspector = FakeCeleryInspector(
        [
            {
                "id": "active-webhook-task",
                "name": "app.integrations.celery.tasks.webhook_push_task.process_webhook_push",
                "args": ["whoop", {"user_id": provider_user_id}, "trace-target"],
            },
            {
                "id": "other-active-task",
                "name": "app.integrations.celery.tasks.webhook_push_task.process_webhook_push",
                "args": ["whoop", {"user_id": "other-user"}, "trace-other"],
            },
        ]
    )

    with patch(
        "app.services.sdk_source_reset_external.current_celery_app.control.inspect",
        return_value=inspector,
    ):
        task_ids, blockers = SDKSourceResetExternalPlanes._task_inventory(
            uuid4(),
            ProviderIdentityScope.from_values({"whoop": {provider_user_id}}),
        )

    assert blockers == ()
    assert task_ids == ("active-webhook-task",)


def test_matched_webhook_tasks_are_revoked_without_terminating_workers() -> None:
    with patch("app.services.sdk_source_reset_external.current_celery_app.control.revoke") as revoke:
        SDKSourceResetExternalPlanes.revoke_tasks(("active-webhook-task",))

    revoke.assert_called_once_with("active-webhook-task", terminate=False)


def test_missing_persisted_identity_proof_is_an_explicit_blocker() -> None:
    scope = ProviderIdentityScope.from_proof(None, required=True)
    service = SDKSourceResetExternalPlanes()
    with (
        patch.object(settings, "aws_bucket_name", None),
        patch.object(settings, "raw_payload_s3_bucket", None),
        patch.object(settings, "raw_payload_storage", "disabled"),
        patch.object(settings, "store_fit_files", False),
        patch.object(settings, "source_reset_s3_target_history_complete", False),
    ):
        objects, blockers = service._object_inventory(uuid4(), scope)

    assert objects == ()
    assert "open-wearables.external-identity-scope-incomplete" in blockers
    assert "open-wearables.external-storage-target-history-unverifiable" in blockers
    assert "open-wearables.raw-payload-objects.storage-history-unverifiable" in blockers
    assert "open-wearables.fit-objects.storage-history-unverifiable" in blockers


def test_log_raw_payload_mode_is_an_explicit_reset_blocker() -> None:
    service = SDKSourceResetExternalPlanes()
    with (
        patch.object(settings, "aws_bucket_name", None),
        patch.object(settings, "raw_payload_s3_bucket", None),
        patch.object(settings, "raw_payload_storage", "log"),
        patch.object(settings, "store_fit_files", False),
        patch.object(settings, "source_reset_s3_target_history_complete", False),
    ):
        objects, blockers = service._object_inventory(uuid4())

    assert objects == ()
    assert blockers == (
        "open-wearables.external-storage-target-history-unverifiable",
        "open-wearables.fit-objects.storage-history-unverifiable",
        "open-wearables.raw-payload-objects.log-retention-unerasable",
        "open-wearables.raw-payload-objects.storage-history-unverifiable",
    )


def test_unavailable_redis_inventory_fails_closed() -> None:
    service = SDKSourceResetExternalPlanes()
    with patch(
        "app.services.sdk_source_reset_external.get_redis_client",
        side_effect=RuntimeError("redis unavailable"),
    ):
        references, blockers = service._redis_inventory(uuid4())

    assert references == ()
    assert blockers == ("open-wearables.redis-coordination.inventory-unavailable",)
