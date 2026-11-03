"""Durable activity-attempt ownership: pending effects never expire into permission to rerun."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Protocol, runtime_checkable

from custos_spl.interfaces.dead_letters import validate_execution_reference

ACTIVITY_EXECUTION_SCHEMA_REVISION = 15
MAX_ACTIVITY_SNAPSHOT_BYTES = 16 * 1024 * 1024


def activity_execution_key(run_id: str, step_id: str, attempt: int) -> str:
    """Hash opaque coordinates without delimiter ambiguities or oversized database index keys."""
    for value in (run_id, step_id):
        if not isinstance(value, str) or not value.strip() or "\0" in value or len(value) > 4096:
            raise ValueError("invalid activity execution coordinate")
    if type(attempt) is not int or not 1 <= attempt <= 2_147_483_647:
        raise ValueError("activity attempt must be a positive 32-bit integer")
    return hashlib.sha256(json.dumps([run_id, step_id, attempt]).encode()).hexdigest()


def validate_activity_claim(
    workspace_id: str, run_id: str, step_id: str, attempt: int, request_hash: str, owner: str
) -> str:
    """Validate all owner-controlled claim fields before storage access."""
    if not workspace_id.strip() or "\0" in workspace_id or len(workspace_id) > 120:
        raise ValueError("invalid activity workspace")
    if re.fullmatch(r"[a-f0-9]{64}", request_hash) is None:
        raise ValueError("activity request fingerprint must be SHA-256")
    if re.fullmatch(r"[a-f0-9]{32}", owner) is None:
        raise ValueError("activity owner must be an opaque generation identifier")
    return activity_execution_key(run_id, step_id, attempt)


def activity_snapshot(value: str) -> str:
    """Canonicalize bounded JSON without ever accepting a result-shaped scalar."""
    if len(value.encode()) > MAX_ACTIVITY_SNAPSHOT_BYTES:
        raise ValueError("activity snapshot exceeds 16 MiB")
    parsed = json.loads(value)
    if not isinstance(parsed, dict):
        raise ValueError("activity snapshot must be an object")
    result = json.dumps(parsed, sort_keys=True, separators=(",", ":"), allow_nan=False)
    if len(result.encode()) > MAX_ACTIVITY_SNAPSHOT_BYTES:
        raise ValueError("activity snapshot exceeds 16 MiB")
    return result


def validate_activity_snapshot(
    value: str,
    *,
    workspace_id: str,
    run_id: str,
    step_id: str,
    attempt: int,
    request_hash: str,
    owner: str,
    completed: bool,
    previous: str | None = None,
) -> str:
    """Bind the snapshot to its row and enforce immutable authority and forward-only transitions."""
    canonical = activity_snapshot(value)
    parsed = json.loads(canonical)
    identity = {
        "workspace_id": workspace_id,
        "run_id": run_id,
        "step_id": step_id,
        "attempt": attempt,
        "request_hash": request_hash,
        "owner_id": owner,
    }
    if any(parsed.get(key) != expected for key, expected in identity.items()):
        raise ValueError("activity snapshot does not match its durable identity")
    if type(parsed.get("attempt")) is not int or type(completed) is not bool:
        raise ValueError("invalid activity attempt or completion type")
    states = ("pending", "resolving", "materializing", "running", "finalizing")
    terminal = {
        "succeeded": "success",
        "failed": ("retryable", "permanent"),
        "cancelled": "cancelled",
    }
    state = parsed.get("state")
    if state not in (*states, *terminal) or completed != (state in terminal):
        raise ValueError("activity completion does not match its state")
    if not isinstance(parsed.get("activity_ref"), str) or not parsed["activity_ref"]:
        raise ValueError("activity snapshot requires its activity reference")
    for field in ("started_at", "deadline"):
        instant = parsed.get(field)
        if not isinstance(instant, str) or datetime.fromisoformat(instant).utcoffset() is None:
            raise ValueError("activity snapshot requires aware timestamps")
    reference = parsed.get("execution_identity")
    if reference is not None:
        validate_execution_reference(reference, workspace_id=workspace_id)
    if previous is None:
        if state != "pending" or completed or parsed.get("result") is not None:
            raise ValueError("activity ownership must begin pending without a result")
    else:
        old = json.loads(activity_snapshot(previous))
        for field in (
            *identity,
            "activity_ref",
            "started_at",
            "execution_identity",
            "credential_scopes",
            "credential_grant_hash",
        ):
            if old.get(field) != parsed.get(field):
                raise ValueError("activity authority is immutable")
        prior = old.get("state")
        if prior not in states:
            raise ValueError("terminal activity snapshot is immutable")
        allowed = {"failed", "cancelled"}
        allowed.add(states[states.index(prior) + 1] if prior != "finalizing" else "succeeded")
        if state not in allowed:
            raise ValueError("activity transition is not forward-only")
        if datetime.fromisoformat(parsed["deadline"]) > datetime.fromisoformat(old["deadline"]):
            raise ValueError("activity deadline cannot be extended")
    if completed:
        result = parsed.get("result")
        if not isinstance(result, dict) or result.get("attempt") != attempt:
            raise ValueError("terminal activity snapshot requires its exact result")
        result_class = result.get("class_", result.get("class"))
        expected = terminal[state]
        if result_class not in (expected if isinstance(expected, tuple) else (expected,)):
            raise ValueError("activity result does not match its terminal state")
        finished = parsed.get("finished_at")
        if not isinstance(finished, str) or datetime.fromisoformat(finished).utcoffset() is None:
            raise ValueError("terminal activity requires its completion time")
    elif parsed.get("result") is not None:
        raise ValueError("pending activity cannot contain a terminal result")
    return canonical


@dataclass(frozen=True, slots=True)
class ActivityExecutionRecord:
    """One immutable identity and fenced snapshot; completion stores the exact result."""

    workspace_id: str
    run_id: str
    step_id: str
    attempt: int
    request_hash: str
    owner: str
    snapshot_json: str
    completed: bool
    authority_revoked: bool = False


@runtime_checkable
class ActivityExecutionStoreProvider(Protocol):
    """Durable ownership: no lease timeout permits re-execution of an unknown effect."""

    async def check_activity_execution_ready(self) -> None:
        """Require the durable schema or raise an explicit migration/backend error."""
        ...

    async def claim_activity_execution(
        self,
        workspace_id: str,
        run_id: str,
        step_id: str,
        attempt: int,
        request_hash: str,
        owner: str,
        snapshot_json: str,
    ) -> ActivityExecutionRecord:
        """Atomically accept one owner, return an identical existing claim, or reject reuse."""
        ...

    async def read_activity_execution(
        self,
        workspace_id: str,
        run_id: str,
        step_id: str,
        attempt: int,
    ) -> ActivityExecutionRecord | None:
        """Read only the exact workspace/attempt; pending is never a successful receipt."""
        ...

    async def write_activity_execution(
        self,
        record: ActivityExecutionRecord,
        snapshot_json: str,
        *,
        completed: bool,
    ) -> ActivityExecutionRecord:
        """Update only a matching owner/fingerprint; terminal snapshots are immutable."""
        ...

    async def revoke_activity_authority(
        self, record: ActivityExecutionRecord
    ) -> ActivityExecutionRecord:
        """Close credential admission without asserting that the owned attempt terminated."""
        ...
