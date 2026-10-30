"""Versioned, workspace-scoped failed-dispatch storage and fenced replay claims.

Only normalized event data and the resolved target enter storage. Transport
frames and arbitrary exception text are deliberately excluded. Provider time
governs retention and claims; an already-live claim may finish beyond retention.
Claims are bounded and non-renewable. Storage alone does not guarantee downstream idempotency.
"""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from types import MappingProxyType
from typing import Any, Literal, Protocol, TypeAlias, runtime_checkable
from uuid import uuid4

from custos_spl.errors import ImmutableViolation, LeaseBusy, LeaseExpired
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.metadata_store import AuditEvent
from custos_spl.pagination import Cursor, Page

DEAD_LETTER_SCHEMA_REVISION = 7
MAX_PAYLOAD_BYTES = 262_144
MAX_INITIAL_ATTEMPTS = 64
MAX_REPLAY_ATTEMPTS = 100
MAX_CLAIM_SECONDS = 300

JsonValue: TypeAlias = (
    str
    | int
    | float
    | bool
    | Mapping[str, "JsonValue"]
    | tuple["JsonValue", ...]
    | list["JsonValue"]
    | None
)
DeadLetterStatus: TypeAlias = Literal["pending", "replaying", "replayed", "discarded"]
FailureCode: TypeAlias = Literal["transient", "permanent", "configuration", "unsafe"]
ReplayOutcome: TypeAlias = Literal["replaying", "replayed", "failed", "interrupted"]
_STATUSES = ("pending", "replaying", "replayed", "discarded")
_FAILURES = ("transient", "permanent", "configuration", "unsafe")
_PRIVATE_KEYS = frozenset(
    {
        "raw",
        "headers",
        "authorization",
        "proxyauthorization",
        "cookie",
        "setcookie",
        "signature",
        "signatures",
        "signingkey",
        "signingmaterial",
        "privatekey",
        "secret",
        "clientsecret",
        "password",
        "credentials",
        "token",
        "accesstoken",
        "refreshtoken",
        "apikey",
        "xapikey",
        "xcustoscallcontext",
        "xcustoscallctx",
        "callcontext",
    }
)


def _instant(value: datetime) -> datetime:
    """Require an aware instant; local repeated-hour ordering must not govern a claim."""
    if value.utcoffset() is None:
        raise ValueError("dead-letter timestamps must be timezone-aware")
    return value.astimezone(UTC)


def validate_dispatch_identifier(value: str) -> None:
    """Bound identifiers and reject blank, non-Unicode and database-invalid strings."""
    if not isinstance(value, str) or not value.strip() or "\0" in value:
        raise ValueError("dead-letter identifiers must be nonblank strings without NUL")
    if len(value.encode("utf-8")) > 512:
        raise ValueError("dead-letter identifier exceeds 512 bytes")


def _freeze(value: object, depth: int = 0) -> JsonValue:
    """Snapshot JSON recursively and reject credential-bearing structures before persistence."""
    if depth > 32:
        raise ValueError("dead-letter JSON exceeds maximum depth")
    if isinstance(value, Mapping):
        result: dict[str, JsonValue] = {}
        for key, child in value.items():
            if not isinstance(key, str):
                raise ValueError("dead-letter JSON keys must be strings")
            normalized = re.sub(r"[^a-z0-9]", "", key.lower())
            if normalized in _PRIVATE_KEYS or any(
                marker in normalized for marker in ("signature", "authorization", "signingkey")
            ):
                raise ValueError("private transport or credential fields cannot be persisted")
            _freeze(key, depth + 1)
            result[key] = _freeze(child, depth + 1)
        return MappingProxyType(result)
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(child, depth + 1) for child in value)
    if isinstance(value, str):
        if "\0" in value:
            raise ValueError("dead-letter JSON must not contain NUL")
        value.encode("utf-8")
        return value
    if value is None or type(value) in (int, float, bool):
        if isinstance(value, (int, float, bool)):
            json.dumps(value, allow_nan=False)
            return value
        return None
    raise ValueError("dead-letter data must be finite JSON")


def json_value(value: JsonValue) -> object:
    """Convert frozen JSON to ordinary containers for serialization, without exposing aliases."""
    if isinstance(value, Mapping):
        return {key: json_value(child) for key, child in value.items()}
    if isinstance(value, (tuple, list)):
        return [json_value(child) for child in value]
    return value


def snapshot_dispatch_object(value: Mapping[str, object]) -> Mapping[str, JsonValue]:
    """Validate, size-bound and deeply freeze one JSON object."""
    frozen = _freeze(value)
    if not isinstance(frozen, Mapping):
        raise ValueError("dead-letter event and target must be JSON objects")
    if len(json.dumps(json_value(frozen), ensure_ascii=False).encode("utf-8")) > MAX_PAYLOAD_BYTES:
        raise ValueError("dead-letter object exceeds 256 KiB")
    return frozen


def validate_dispatch_target(
    value: Mapping[str, object], *, allow_unresolved_version: bool = False
) -> Mapping[str, JsonValue]:
    """Validate the complete wrapped RPC target, including size, depth and identifiers."""
    target = snapshot_dispatch_object(value)
    kind = target.get("kind")
    required = (
        {"kind", "workflowVersionId", "inputs"}
        if kind == "start"
        else {"kind", "runId", "stepId", "eventName", "payload"}
    )
    if kind not in ("start", "resume") or set(target) - {"deliveryProtocol"} != required:
        raise ValueError("resolved target must be an exact start or resume RPC shape")
    protocol = target.get("deliveryProtocol", 1)
    if type(protocol) is not int or protocol not in (1, 2):
        raise ValueError("unsupported target delivery protocol")
    for key in required - {"kind", "inputs", "payload"}:
        item = target[key]
        if key == "workflowVersionId" and item is None and allow_unresolved_version:
            continue
        if not isinstance(item, str):
            raise ValueError("resolved target identifiers must be strings")
        validate_dispatch_identifier(item)
    if not isinstance(target.get("inputs" if kind == "start" else "payload"), Mapping):
        raise ValueError("resolved RPC data must be an object")
    return target


@dataclass(frozen=True, slots=True)
class DispatchAttempt:
    """One failed original RPC attempt, with a safe classification rather than exception text."""

    at: datetime
    code: FailureCode
    http_status: int | None = None

    def __post_init__(self) -> None:
        """Validate safe error metadata at the shared boundary."""
        _instant(self.at)
        if self.code not in _FAILURES:
            raise ValueError("unknown dispatch failure classification")
        if self.http_status is not None and (
            type(self.http_status) is not int or not 100 <= self.http_status <= 599
        ):
            raise ValueError("invalid dispatch HTTP status")


@dataclass(frozen=True, slots=True, kw_only=True)
class DeadLetterData:
    """Immutable replay inputs, independent of subsequent subscription edits.

    ``event`` uses the normalized wire shape without ``raw``; ``target`` is the
    resolved start/resume RPC input, not a subscription to rematch. Application
    values must already be credential-free: arbitrary strings cannot be
    reliably classified as secrets. Known credential/transport keys fail closed.
    """

    workspace_id: WorkspaceId
    entry_id: str
    subscription_id: str
    idempotency_key: str
    event: Mapping[str, JsonValue]
    target: Mapping[str, JsonValue]
    history: tuple[DispatchAttempt, ...]
    retention_seconds: int = 2_592_000
    max_replay_attempts: int = 3
    schema_version: Literal[1] = 1

    def __post_init__(self) -> None:
        """Reject unknown versions, raw transport, malformed targets and unbounded capture."""
        for identifier in (
            self.workspace_id,
            self.entry_id,
            self.subscription_id,
            self.idempotency_key,
        ):
            validate_dispatch_identifier(identifier)
        if type(self.schema_version) is not int or self.schema_version != 1:
            raise ValueError("unsupported dead-letter schema version")
        if type(self.retention_seconds) is not int or not 0 < self.retention_seconds <= 31_536_000:
            raise ValueError("dead-letter retention must be 1..31536000 seconds")
        if (
            type(self.max_replay_attempts) is not int
            or not 0 < self.max_replay_attempts <= MAX_REPLAY_ATTEMPTS
        ):
            raise ValueError("replay attempt limit must be 1..100")
        if not 0 < len(self.history) <= MAX_INITIAL_ATTEMPTS:
            raise ValueError("capture must contain 1..64 dispatch attempts")
        if not all(isinstance(attempt, DispatchAttempt) for attempt in self.history):
            raise ValueError("invalid dispatch attempt history")
        object.__setattr__(self, "history", tuple(self.history))
        event = snapshot_dispatch_object(self.event)
        target = validate_dispatch_target(
            self.target, allow_unresolved_version=self.history[-1].code == "configuration"
        )
        if set(event) - {"schemaVersion", "eventId", "source", "kind", "subject", "data"}:
            raise ValueError("unsupported persisted event field")
        if event.get("schemaVersion") != "1":
            raise ValueError("unsupported normalized event version")
        for key in ("eventId", "kind"):
            value = event.get(key)
            if not isinstance(value, str):
                raise ValueError("event requires eventId and kind")
            validate_dispatch_identifier(value)
        source = event.get("source")
        if not isinstance(source, Mapping) or set(source) - {
            "type",
            "connectorInstanceId",
            "subscriptionId",
            "vendor",
            "occurredAt",
        }:
            raise ValueError("invalid normalized source")
        if source.get("type") not in (
            "manual",
            "scheduled",
            "webhook",
            "vendor-push",
            "pull",
            "internal",
        ):
            raise ValueError("invalid normalized source type")
        if not isinstance(source.get("occurredAt"), str) or not source["occurredAt"]:
            raise ValueError("normalized source requires occurredAt")
        for key in ("connectorInstanceId", "subscriptionId", "vendor"):
            if source.get(key) is not None and not isinstance(source[key], str):
                raise ValueError("normalized source identifiers must be strings")
        if not isinstance(event.get("data"), Mapping) or not isinstance(
            event.get("subject", ""), str
        ):
            raise ValueError("normalized event requires object data and string subject")
        object.__setattr__(self, "event", event)
        object.__setattr__(self, "target", target)


@dataclass(frozen=True, slots=True)
class ReplayAttempt:
    """A consumed operator attempt; interrupted claims remain visible and count toward the limit."""

    epoch: int
    worker_id: str
    started_at: datetime
    outcome: ReplayOutcome = "replaying"
    finished_at: datetime | None = None
    failure: DispatchAttempt | None = None


@dataclass(frozen=True, slots=True)
class ReplayClaim:
    """Opaque claim identity; callers cannot extend authority by editing its expiry."""

    workspace_id: WorkspaceId
    entry_id: str
    token: str
    epoch: int
    expires_at: datetime
    observed_at: datetime


@dataclass(frozen=True, slots=True)
class DeadLetterEntry:
    """Stored immutable inputs plus provider-owned status, retention and replay history."""

    data: DeadLetterData
    created_at: datetime
    expires_at: datetime
    status: DeadLetterStatus = "pending"
    replay_history: tuple[ReplayAttempt, ...] = ()
    claim: ReplayClaim | None = None
    result_id: str | None = None


@dataclass(frozen=True, slots=True)
class DeadLetterFilter:
    """Inclusive UTC creation-time filters; pagination binds every filter and workspace."""

    subscription_id: str | None = None
    status: DeadLetterStatus | None = None
    since: datetime | None = None
    until: datetime | None = None

    def scope(self, workspace_id: WorkspaceId) -> tuple[str | None, ...]:
        """Validate filters and produce a canonical query-cursor scope."""
        validate_dispatch_identifier(workspace_id)
        if self.subscription_id is not None:
            validate_dispatch_identifier(self.subscription_id)
        if self.status is not None and self.status not in _STATUSES:
            raise ValueError("invalid dead-letter status")
        since = _instant(self.since) if self.since is not None else None
        until = _instant(self.until) if self.until is not None else None
        if since is not None and until is not None and since > until:
            raise ValueError("since must not follow until")
        return (
            "dead-letters",
            workspace_id,
            self.subscription_id,
            self.status,
            since.isoformat() if since else None,
            until.isoformat() if until else None,
        )


def new_dead_letter(data: DeadLetterData, now: datetime) -> DeadLetterEntry:
    """Start retention at durable capture time, not the original event's untrusted timestamp."""
    now = _instant(now)
    return DeadLetterEntry(data, now, now + timedelta(seconds=data.retention_seconds))


def recover_replay(entry: DeadLetterEntry, now: datetime) -> DeadLetterEntry:
    """Recover an expired claim without erasing its consumed attempt or changing retention."""
    now = _instant(now)
    if entry.claim is None or entry.claim.expires_at > now:
        return entry
    last = replace(entry.replay_history[-1], outcome="interrupted", finished_at=now)
    status: DeadLetterStatus = (
        "discarded" if len(entry.replay_history) >= entry.data.max_replay_attempts else "pending"
    )
    return replace(
        entry, claim=None, status=status, replay_history=(*entry.replay_history[:-1], last)
    )


def claim_replay(
    entry: DeadLetterEntry, worker_id: str, ttl_seconds: int, now: datetime
) -> DeadLetterEntry:
    """Consume one attempt; active contenders raise LeaseBusy, ineligible rows conflict."""
    validate_dispatch_identifier(worker_id)
    now = _instant(now)
    if type(ttl_seconds) is not int or not 0 < ttl_seconds <= MAX_CLAIM_SECONDS:
        raise ValueError("replay claim duration must be 1..300 seconds")
    entry = recover_replay(entry, now)
    if entry.claim is not None:
        raise LeaseBusy("dead-letter entry is already being replayed")
    if entry.status != "pending" or entry.expires_at <= now:
        raise ImmutableViolation("dead-letter entry is terminal or beyond retention")
    epoch = len(entry.replay_history) + 1
    claim = ReplayClaim(
        entry.data.workspace_id,
        entry.data.entry_id,
        uuid4().hex,
        epoch,
        now + timedelta(seconds=ttl_seconds),
        now,
    )
    attempt = ReplayAttempt(epoch, worker_id, now)
    return replace(
        entry, status="replaying", claim=claim, replay_history=(*entry.replay_history, attempt)
    )


def require_replay(entry: DeadLetterEntry | None, claim: ReplayClaim, now: datetime) -> None:
    """Fence against provider state, including scope/token/epoch and post-lock provider time."""
    if (
        entry is None
        or entry.claim is None
        or entry.status != "replaying"
        or entry.data.workspace_id != claim.workspace_id
        or entry.data.entry_id != claim.entry_id
        or entry.claim.token != claim.token
        or entry.claim.epoch != claim.epoch
        or entry.claim.expires_at <= _instant(now)
    ):
        raise LeaseExpired("replay claim expired or is no longer owned")


def finish_replay(
    entry: DeadLetterEntry,
    claim: ReplayClaim,
    now: datetime,
    *,
    failure: DispatchAttempt | None = None,
    result_id: str | None = None,
) -> DeadLetterEntry:
    """Finish only a current claim, retaining the original target/key and bounded history."""
    require_replay(entry, claim, now)
    if failure is not None and result_id is not None:
        raise ValueError("failed replay cannot carry a successful result")
    if result_id is not None:
        validate_dispatch_identifier(result_id)
    status: DeadLetterStatus = "replayed"
    if failure is not None:
        status = (
            "discarded"
            if len(entry.replay_history) >= entry.data.max_replay_attempts
            else "pending"
        )
    last = replace(
        entry.replay_history[-1],
        outcome="failed" if failure else "replayed",
        finished_at=_instant(now),
        failure=failure,
    )
    return replace(
        entry,
        claim=None,
        status=status,
        result_id=result_id,
        replay_history=(*entry.replay_history[:-1], last),
    )


def discard_dead_letter(entry: DeadLetterEntry, now: datetime) -> DeadLetterEntry:
    """Reject active replay and successful outcomes; repeated discard is idempotent."""
    entry = recover_replay(entry, now)
    if entry.claim is not None:
        raise LeaseBusy("cannot discard an active replay")
    if entry.status == "replayed":
        raise ImmutableViolation("cannot discard a successful replay")
    return replace(entry, status="discarded")


def dead_letter_transition_audits(
    before: DeadLetterEntry | None,
    after: DeadLetterEntry | None,
    now: datetime,
    *,
    actor: str = "trigger-service",
    reason: str | None = None,
) -> tuple[AuditEvent, ...]:
    """Prepare payload-free lifecycle audits for the same transaction as the state change."""
    validate_dispatch_identifier(actor)
    if reason is not None and (
        not isinstance(reason, str) or not reason.strip() or len(reason) > 1024
    ):
        raise ValueError("discard reason must be nonblank and at most 1024 characters")
    entry = after if after is not None else before
    if entry is None:
        raise ValueError("a dead-letter transition requires an entry")
    source = entry.data.event["source"]
    assert isinstance(source, Mapping)
    events: list[tuple[str, Mapping[str, Any]]] = []
    if before is None:
        events.append(("stored", {}))
    elif after is None:
        events.append(("expired", {"previousStatus": before.status}))
    else:
        for index, attempt in enumerate(after.replay_history):
            previous = before.replay_history[index] if index < len(before.replay_history) else None
            if attempt != previous and attempt.outcome in ("failed", "interrupted", "replayed"):
                events.append(
                    (
                        "replayed" if attempt.outcome == "replayed" else "replay_failed",
                        {"attempt": attempt.epoch, "outcome": attempt.outcome},
                    )
                )
        if after.status == "discarded" and before.status != "discarded":
            events.append(("discarded", {"reason": reason}))
    return tuple(
        AuditEvent(
            workspace_id=entry.data.workspace_id,
            event_id=uuid4().hex,
            event_type="trigger.deadletter." + outcome,
            actor=actor,
            subject={"entryId": entry.data.entry_id, "subscriptionId": entry.data.subscription_id},
            payload={"schemaVersion": 1, "sourceType": source["type"], **payload},
            occurred_at=_instant(now),
        )
        for outcome, payload in events
    )


def encode_dead_letter(entry: DeadLetterEntry) -> str:
    """Serialize version-1 storage state, never arbitrary errors or private transport frames."""
    data = entry.data
    claim = entry.claim

    def failure(value: DispatchAttempt) -> dict[str, object]:
        """Encode the closed failure classification and its timestamp."""
        return {"at": value.at.isoformat(), "code": value.code, "http_status": value.http_status}

    return json.dumps(
        {
            "schema_version": data.schema_version,
            "workspace_id": data.workspace_id,
            "entry_id": data.entry_id,
            "subscription_id": data.subscription_id,
            "idempotency_key": data.idempotency_key,
            "event": json_value(data.event),
            "target": json_value(data.target),
            "history": [failure(value) for value in data.history],
            "retention_seconds": data.retention_seconds,
            "max_replay_attempts": data.max_replay_attempts,
            "created_at": entry.created_at.isoformat(),
            "expires_at": entry.expires_at.isoformat(),
            "status": entry.status,
            "result_id": entry.result_id,
            "claim": {
                "token": claim.token,
                "epoch": claim.epoch,
                "expires_at": claim.expires_at.isoformat(),
                "observed_at": claim.observed_at.isoformat(),
            }
            if claim
            else None,
            "replay_history": [
                {
                    "epoch": value.epoch,
                    "worker_id": value.worker_id,
                    "started_at": value.started_at.isoformat(),
                    "outcome": value.outcome,
                    "finished_at": value.finished_at.isoformat() if value.finished_at else None,
                    "failure": failure(value.failure) if value.failure else None,
                }
                for value in entry.replay_history
            ],
        },
        ensure_ascii=False,
        allow_nan=False,
    )


def decode_dead_letter(payload: str) -> DeadLetterEntry:
    """Decode trusted provider state, validating the version and replay-input boundary again."""
    value: dict[str, Any] = json.loads(payload)

    def failure(item: dict[str, Any]) -> DispatchAttempt:
        """Rehydrate safe failure metadata without exception text."""
        return DispatchAttempt(
            datetime.fromisoformat(item["at"]), item["code"], item["http_status"]
        )

    data = DeadLetterData(
        workspace_id=WorkspaceId(value["workspace_id"]),
        entry_id=value["entry_id"],
        subscription_id=value["subscription_id"],
        idempotency_key=value["idempotency_key"],
        event=value["event"],
        target=value["target"],
        history=tuple(map(failure, value["history"])),
        retention_seconds=value["retention_seconds"],
        max_replay_attempts=value["max_replay_attempts"],
        schema_version=value["schema_version"],
    )
    claim = value["claim"]
    return DeadLetterEntry(
        data=data,
        created_at=datetime.fromisoformat(value["created_at"]),
        expires_at=datetime.fromisoformat(value["expires_at"]),
        status=value["status"],
        result_id=value["result_id"],
        claim=ReplayClaim(
            data.workspace_id,
            data.entry_id,
            claim["token"],
            claim["epoch"],
            datetime.fromisoformat(claim["expires_at"]),
            datetime.fromisoformat(claim["observed_at"]),
        )
        if claim
        else None,
        replay_history=tuple(
            ReplayAttempt(
                item["epoch"],
                item["worker_id"],
                datetime.fromisoformat(item["started_at"]),
                item["outcome"],
                datetime.fromisoformat(item["finished_at"]) if item["finished_at"] else None,
                failure(item["failure"]) if item["failure"] else None,
            )
            for item in value["replay_history"]
        ),
    )


@runtime_checkable
class DeadLetterStoreProvider(Protocol):
    """Optional metadata capability; no worker, authorization or Workflow RPC is implied."""

    async def check_dead_letters_ready(self) -> None:
        """Require revision 7 without changing the general metadata revision-4 minimum."""
        ...

    async def put_dead_letter(self, data: DeadLetterData) -> DeadLetterEntry:
        """Atomically append capture/audit; duplicate identity raises ImmutableViolation."""
        ...

    async def get_dead_letter(
        self, workspace_id: WorkspaceId, entry_id: str
    ) -> DeadLetterEntry | None:
        """Read an entry in one workspace, including expired rows awaiting audited cleanup."""
        ...

    async def query_dead_letters(
        self,
        workspace_id: WorkspaceId,
        filters: DeadLetterFilter | None = None,
        *,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[DeadLetterEntry]:
        """Page by entry ID with indexed filters; size defaults to 50 and caps at 200."""
        ...

    async def claim_dead_letter(
        self,
        workspace_id: WorkspaceId,
        entry_id: str,
        worker_id: str,
        *,
        ttl_seconds: int = 60,
    ) -> DeadLetterEntry:
        """Claim pending/abandoned work atomically; consume an attempt before any remote effect."""
        ...

    async def finish_dead_letter(
        self,
        claim: ReplayClaim,
        *,
        failure: DispatchAttempt | None = None,
        result_id: str | None = None,
    ) -> DeadLetterEntry:
        """Atomically audit and finish a live claim; stale ownership raises LeaseExpired."""
        ...

    async def recover_dead_letter(
        self,
        workspace_id: WorkspaceId,
        entry_id: str,
    ) -> DeadLetterEntry:
        """Audit interrupted claims and final-attempt discard; active claims remain unchanged."""
        ...

    async def discard_dead_letter(
        self,
        workspace_id: WorkspaceId,
        entry_id: str,
        *,
        actor: str = "trigger-service",
        reason: str | None = None,
    ) -> DeadLetterEntry:
        """Atomically audit discard, rejecting live claims and preserving successful replay."""
        ...

    async def delete_expired_dead_letter(self, workspace_id: WorkspaceId, entry_id: str) -> bool:
        """Atomically audit and delete beyond retention with no live claim.

        Return False for absent, unexpired or actively claimed rows. This is an
        eligibility result, never a backend-failure fallback.
        """
        ...
