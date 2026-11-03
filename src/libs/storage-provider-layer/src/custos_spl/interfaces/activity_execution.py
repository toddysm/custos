"""Durable activity-attempt ownership: pending effects never expire into permission to rerun."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

ACTIVITY_EXECUTION_SCHEMA_REVISION = 15
MAX_ACTIVITY_SNAPSHOT_BYTES = 16 * 1024 * 1024


def activity_execution_key(run_id: str, step_id: str, attempt: int) -> str:
    """Hash opaque coordinates without delimiter ambiguities or oversized database index keys."""
    for value in (run_id, step_id):
        if not isinstance(value, str) or not value.strip() or "\0" in value or len(value) > 4096:
            raise ValueError("invalid activity execution coordinate")
    if type(attempt) is not int or attempt < 1:
        raise ValueError("activity attempt must be a positive integer")
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
