"""Durable delivery receipts, ownership fencing and step-scoped resume identities.

Receipts store request fingerprints and small result identities, not event
payloads. A pending receipt is never evidence that a remote effect succeeded.
Consumer acknowledgement is separate from producer lease ownership because a
workflow can consume an event after the producer dies.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import Literal, Protocol, runtime_checkable
from uuid import uuid4

from custos_spl.errors import ImmutableViolation, LeaseBusy, LeaseExpired
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.dead_letters import (
    DeadLetterData,
    DeadLetterEntry,
    DispatchAttempt,
    json_value,
)
from custos_spl.interfaces.metadata_store import Run
from custos_spl.pagination import Cursor, Page

DELIVERY_SCHEMA_REVISION = 8
MAX_DELIVERY_RETENTION_SECONDS = 31_536_600
RESUME_CHANNEL_PREFIX = "custos.resume.v2."
DeliveryNamespace = Literal["trigger", "workflow-start", "workflow-resume"]


def delivery_instant(value: datetime) -> datetime:
    """Use aware UTC instants for provider-time comparisons, including DST folds."""
    if value.utcoffset() is None:
        raise ValueError("delivery timestamps must be timezone-aware")
    return value.astimezone(UTC)


def delivery_identity(*parts: str) -> str:
    """Hash a length-unambiguous identity without exposing user event names in runtime channels."""
    if any(not isinstance(part, str) or not part.strip() for part in parts):
        raise ValueError("delivery identity parts must be nonblank strings")
    return hashlib.sha256(json.dumps(parts, ensure_ascii=True).encode()).hexdigest()


def resume_channel(workspace_id: str, run_id: str, step_id: str, event_name: str) -> str:
    """Give each logical wait a distinct lowercase channel in Dapr's case-insensitive namespace."""
    return RESUME_CHANNEL_PREFIX + delivery_identity(workspace_id, run_id, step_id, event_name)


def delivery_fingerprint(value: Mapping[str, object]) -> str:
    """Fingerprint JSON inputs without treating an in-flight reservation as completion."""
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def resume_identity(
    workspace_id: str,
    run_id: str,
    step_id: str,
    event_name: str,
    idempotency_key: str,
    payload: Mapping[str, object],
) -> tuple[str, str]:
    """Bind a resume receipt to its exact scoped wait, original key and payload."""
    key = delivery_identity(workspace_id, run_id, step_id, event_name, idempotency_key)
    return key, delivery_fingerprint({"key": key, "payload": payload})


def resume_result(key: str) -> str:
    """Identify a durable consumer result, distinct from runtime inbox submission."""
    return delivery_result(json.dumps({"deliveryKey": key, "consumed": True}))


def encode_delivery_request(data: DeadLetterData) -> str:
    """Persist validated replay inputs without inventing dispatch-attempt history."""
    return json.dumps(
        {
            "schema_version": data.schema_version,
            "workspace_id": data.workspace_id,
            "entry_id": data.entry_id,
            "subscription_id": data.subscription_id,
            "idempotency_key": data.idempotency_key,
            "event": json_value(data.event),
            "target": json_value(data.target),
            "retention_seconds": data.retention_seconds,
            "max_replay_attempts": data.max_replay_attempts,
        },
        ensure_ascii=False,
        allow_nan=False,
    )


def decode_delivery_request(raw: str, now: datetime) -> DeadLetterData:
    """Materialize a capture template; actual failure history is supplied after an attempt."""
    value = json.loads(raw)
    if not isinstance(value, dict) or set(value) != {
        "schema_version",
        "workspace_id",
        "entry_id",
        "subscription_id",
        "idempotency_key",
        "event",
        "target",
        "retention_seconds",
        "max_replay_attempts",
    }:
        raise ValueError("invalid delivery request snapshot")
    return DeadLetterData(**value, history=(DispatchAttempt(now, "configuration"),))


def validate_delivery_key(workspace_id: WorkspaceId, namespace: str, key: str) -> None:
    """Reject invalid scopes before storage access."""
    if namespace not in ("trigger", "workflow-start", "workflow-resume"):
        raise ValueError("unsupported delivery namespace")
    for value in (workspace_id, key):
        if (
            not isinstance(value, str)
            or not value.strip()
            or "\0" in value
            or len(value.encode()) > 512
        ):
            raise ValueError("delivery scope and key must be nonblank and at most 512 bytes")


def delivery_result(value: str) -> str:
    """Canonicalize a bounded result object; payloads and transport errors do not belong here."""
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise ValueError("delivery result must be an object")
    encoded = json.dumps(parsed, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if len(encoded.encode()) > 16_384:
        raise ValueError("delivery result exceeds 16 KiB")
    return encoded


def validate_failure_capture(claim: DeliveryReceipt, data: DeadLetterData) -> None:
    """Bind captured failure to the original scoped producer claim and resolved target."""
    if (
        claim.namespace != "trigger"
        or claim.workspace_id != data.workspace_id
        or claim.key != data.idempotency_key
        or claim.request_hash != delivery_fingerprint({"target": json_value(data.target)})
    ):
        raise ImmutableViolation("failure capture does not match the delivery claim")


@dataclass(frozen=True, slots=True)
class DeliveryReceipt:
    """A provider-owned pending claim or immutable successful result for one request identity."""

    workspace_id: WorkspaceId
    namespace: DeliveryNamespace
    key: str
    request_hash: str
    retain_until: datetime
    epoch: int
    token: str | None
    lease_until: datetime | None
    result_json: str | None = None
    request_json: str | None = None
    submitted_at: datetime | None = None

    @property
    def completed(self) -> bool:
        """Only a stored result, never an in-flight reservation, represents completion."""
        return self.result_json is not None


def reserve_delivery(
    current: DeliveryReceipt | None,
    workspace_id: WorkspaceId,
    namespace: DeliveryNamespace,
    key: str,
    request_hash: str,
    retain_until: datetime,
    now: datetime,
    lease_seconds: int,
    request_json: str | None = None,
) -> DeliveryReceipt:
    """Reserve or recover a pending receipt without erasing fingerprints or completed outcomes."""
    validate_delivery_key(workspace_id, namespace, key)
    if re.fullmatch(r"[a-f0-9]{64}", request_hash) is None:
        raise ValueError("delivery request hash must be SHA-256")
    now, until = delivery_instant(now), delivery_instant(retain_until)
    if request_json is not None:
        if namespace != "trigger":
            raise ValueError("request snapshots belong to Trigger producer receipts")
        if current is not None and current.epoch > 0:
            request_hash = current.request_hash
            if not current.completed and current.request_json is None:
                raise ImmutableViolation("pending delivery has no original request available")
            request_json = current.request_json
        if request_json is not None:
            original = decode_delivery_request(request_json, now)
            if original.workspace_id != workspace_id or original.idempotency_key != key:
                raise ValueError("delivery request snapshot has a different scope or key")
            if delivery_fingerprint({"target": json_value(original.target)}) != request_hash:
                raise ImmutableViolation("delivery snapshot fingerprint does not match")
            until = max(until, now + timedelta(seconds=original.retention_seconds + 600))
            request_json = encode_delivery_request(original)
    if not now < until <= now + timedelta(seconds=MAX_DELIVERY_RETENTION_SECONDS):
        raise ValueError("delivery retention is expired or exceeds the supported horizon")
    if type(lease_seconds) is not int or not 1 <= lease_seconds <= 300:
        raise ValueError("delivery lease must be 1..300 seconds")
    if current is not None:
        if current.request_hash != request_hash:
            raise ImmutableViolation("delivery key is bound to another request")
        until = max(until, current.retain_until)
        if current.completed:
            return replace(current, retain_until=until)
        if current.lease_until is not None and current.lease_until > now:
            raise LeaseBusy("delivery is in flight")
    return DeliveryReceipt(
        workspace_id,
        namespace,
        key,
        request_hash,
        until,
        current.epoch + 1 if current else 1,
        uuid4().hex,
        min(until, now + timedelta(seconds=lease_seconds)),
        request_json=request_json
        if request_json is not None
        else current.request_json
        if current
        else None,
        submitted_at=current.submitted_at if current else None,
    )


def require_delivery(
    current: DeliveryReceipt | None, claim: DeliveryReceipt, now: datetime
) -> None:
    """Reject absent, released, expired and superseded producer authority."""
    if (
        current is None
        or current.completed
        or current.token is None
        or current.token != claim.token
        or current.epoch != claim.epoch
        or current.workspace_id != claim.workspace_id
        or current.namespace != claim.namespace
        or current.key != claim.key
        or current.request_hash != claim.request_hash
        or current.lease_until is None
        or current.lease_until <= delivery_instant(now)
    ):
        raise LeaseExpired("delivery claim expired or is no longer owned")


@dataclass(frozen=True, slots=True)
class WorkflowMirror:
    """Workspace-scoped durable mirror snapshot with immutable association identity."""

    workspace_id: WorkspaceId
    mirror_id: str
    run_id: str
    step_id: str
    identity_hash: str
    expires_at: datetime
    payload_json: str

    def __post_init__(self) -> None:
        """Validate bounded mirror serialization and identity before either provider accepts it."""
        for value in (self.mirror_id, self.run_id, self.step_id):
            validate_delivery_key(self.workspace_id, "workflow-resume", value)
        if re.fullmatch(r"[a-f0-9]{64}", self.identity_hash) is None:
            raise ValueError("mirror association hash must be SHA-256")
        delivery_instant(self.expires_at)
        if len(self.payload_json.encode("utf-8")) > 262_144:
            raise ValueError("workflow mirror exceeds 256 KiB")
        payload = json.loads(self.payload_json)
        if not isinstance(payload, dict):
            raise ValueError("workflow mirror must encode an object")
        json.dumps(payload, allow_nan=False)


@dataclass(frozen=True, slots=True)
class WorkflowStart:
    """Immutable versioned runtime input committed atomically with the initial Run row."""

    workspace_id: WorkspaceId
    run_id: str
    request_hash: str
    input_json: str

    def __post_init__(self) -> None:
        """Validate protocol/scope before persisting a replay-authoritative snapshot."""
        validate_delivery_key(self.workspace_id, "workflow-start", self.run_id)
        if re.fullmatch(r"[a-f0-9]{64}", self.request_hash) is None:
            raise ValueError("workflow start hash must be SHA-256")
        value = json.loads(self.input_json)
        if not isinstance(value, dict) or value.get("workspace_id") != self.workspace_id:
            raise ValueError("workflow input does not match its workspace")
        if type(value.get("delivery_protocol")) is not int or value["delivery_protocol"] != 2:
            raise ValueError("durable workflow input requires protocol 2")
        version = value.get("workflow_version_id")
        if not isinstance(version, str) or not isinstance(value.get("inputs"), dict):
            raise ValueError("workflow input requires a version and object inputs")
        validate_delivery_key(self.workspace_id, "workflow-start", version)
        if (
            delivery_fingerprint({"workflowVersionId": version, "inputs": value["inputs"]})
            != self.request_hash
        ):
            raise ValueError("workflow input does not match its request fingerprint")
        json.dumps(value, allow_nan=False)


@runtime_checkable
class DeliveryStateProvider(Protocol):
    """Optional capability used by Trigger and Workflow; mutations use provider time."""

    async def check_delivery_ready(self) -> None:
        """Require revision 8 without changing the general metadata minimum."""
        ...

    async def delivery_now(self) -> datetime:
        """Return authoritative provider time for retention planning."""
        ...

    async def put_workflow_start(self, run: Run, start: WorkflowStart) -> WorkflowStart:
        """Atomically persist Run plus immutable runtime input; reject a legacy run collision."""
        ...

    async def get_workflow_start(
        self, workspace_id: WorkspaceId, run_id: str
    ) -> WorkflowStart | None:
        """Read the persisted protocol and runtime input independently of receipt retention."""
        ...

    async def get_delivery(
        self,
        workspace_id: WorkspaceId,
        namespace: DeliveryNamespace,
        key: str,
    ) -> DeliveryReceipt | None:
        """Read only one workspace's receipt, including pending and completed outcomes."""
        ...

    async def reserve_delivery(
        self,
        workspace_id: WorkspaceId,
        namespace: DeliveryNamespace,
        key: str,
        request_hash: str,
        retain_until: datetime,
        *,
        lease_seconds: int = 300,
        request: DeadLetterData | None = None,
    ) -> DeliveryReceipt:
        """Claim/recover pending work, or return an equal-input completed result.

        Equal-input contention extends the minimum retention before raising
        LeaseBusy, without renewing or granting the existing worker's lease.
        """
        ...

    async def mark_delivery_submitted(self, claim: DeliveryReceipt) -> DeliveryReceipt:
        """Record runtime submission without claiming the wait consumed the event."""
        ...

    async def clear_expired_delivery_requests(
        self,
        workspace_id: WorkspaceId,
        *,
        limit: int | None = None,
    ) -> int:
        """Erase bounded expired unowned payloads while retaining identity and fencing metadata."""
        ...

    async def complete_delivery(self, claim: DeliveryReceipt, result_json: str) -> DeliveryReceipt:
        """Finish a live claim; an identical consumer-acknowledged result is an idempotent read."""
        ...

    async def release_delivery(self, claim: DeliveryReceipt) -> None:
        """Release only current live ownership; keep the fingerprint and recovery identity."""
        ...

    async def capture_failed_delivery(
        self,
        claim: DeliveryReceipt,
        data: DeadLetterData,
    ) -> DeadLetterEntry:
        """Atomically persist a failed dispatch and complete its current fenced producer receipt."""
        ...

    async def acknowledge_delivery(
        self,
        workspace_id: WorkspaceId,
        key: str,
        request_hash: str,
        result_json: str,
    ) -> DeliveryReceipt:
        """Commit a workflow-resume consumer acknowledgement independently of producer liveness."""
        ...

    async def put_workflow_mirror(self, mirror: WorkflowMirror) -> None:
        """Upsert retry metadata without rebinding a mirror's immutable association."""
        ...

    async def query_workflow_mirrors(
        self,
        workspace_id: WorkspaceId,
        *,
        run_id: str | None = None,
        step_id: str | None = None,
        expired_before: datetime | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[WorkflowMirror]:
        """Return bounded scoped keyset pages for reconciliation and cleanup."""
        ...

    async def delete_workflow_mirror(self, workspace_id: WorkspaceId, mirror_id: str) -> None:
        """Delete a scoped mirror idempotently after durable consumer acknowledgement."""
        ...
