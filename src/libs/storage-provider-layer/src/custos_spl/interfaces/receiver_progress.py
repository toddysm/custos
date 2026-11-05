"""Versioned receiver scan positions and fenced ownership, never effect authority.

Providers use their own clock. Discovery refresh cannot change a progress
revision. A claim moves service order before any effect; checkpoint follows
acknowledged work. Failed/expired ownership retains the last committed cursor.
"""

from __future__ import annotations

import json
import math
import re
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import StrEnum
from typing import Protocol, runtime_checkable

from custos_spl.errors import LeaseExpired
from custos_spl.interfaces.dead_letters import validate_dispatch_identifier
from custos_spl.interfaces.receiver_workspaces import RECEIVER_WORKSPACE_SCOPE
from custos_spl.interfaces.scheduler import SchedulerLease, utc_instant
from custos_spl.pagination import Cursor, decode_query_cursor

RECEIVER_PROGRESS_SCHEMA_REVISION = 16
MAX_CONTINUATION_BYTES = 65536
DISCOVERY_PAGE_SIZE = 50
INACTIVE_PROGRESS_RETENTION = timedelta(days=30)
_TOKEN = re.compile(r"^[a-f0-9]{32}$")


class ScanOperation(StrEnum):
    """The closed set of independently scheduled receiver operations."""

    SCHEDULED = "scheduled"
    DELIVERY = "delivery"
    PULL_DEMAND = "pull-demand"
    RETENTION = "retention"


def scan_identity(value: str) -> None:
    """Use the existing bounded opaque dispatch identity contract for every scope."""
    validate_dispatch_identifier(value)


def scan_duration(seconds: int) -> timedelta:
    """Reject booleans, non-integral and out-of-policy lease lifetimes before storage."""
    if type(seconds) is not int or not 1 <= seconds <= 3600:
        raise ValueError("receiver scan lease must be in 1..3600 seconds")
    return timedelta(seconds=seconds)


def scan_delay(seconds: float) -> timedelta:
    """Require a finite, positive, bounded retry/interval delay."""
    if isinstance(seconds, bool) or not math.isfinite(seconds) or not 0 < seconds <= 3600:
        raise ValueError("receiver scan interval must be in (0,3600] seconds")
    return timedelta(seconds=seconds)


@dataclass(frozen=True, slots=True)
class ScanContinuation:
    """Closed public paging value; cursor is opaque, cutoff is preserved only for scheduled work."""

    operation: ScanOperation
    cursor: Cursor | None = None
    due_cutoff: datetime | None = None
    version: int = 1

    def __post_init__(self) -> None:
        """Reject unknown versions, cross-operation fields and unbounded serialized state."""
        if (
            not isinstance(self.operation, ScanOperation)
            or type(self.version) is not int
            or self.version != 1
        ):
            raise ValueError("unsupported receiver continuation version or operation")
        if self.cursor is not None and (
            not isinstance(self.cursor, Cursor)
            or not isinstance(self.cursor.token, str)
            or not self.cursor.token
        ):
            raise ValueError("receiver cursor must be nonempty opaque text")
        if self.operation is ScanOperation.SCHEDULED:
            if (self.cursor is None) != (self.due_cutoff is None):
                raise ValueError("scheduled cursor and cutoff must be present together")
        elif self.due_cutoff is not None:
            raise ValueError("only scheduled progress may have a cutoff")
        if self.operation is ScanOperation.PULL_DEMAND and self.cursor is not None:
            raise ValueError("pull demand has no scan continuation")
        if self.due_cutoff is not None:
            utc_instant(self.due_cutoff)
        if len(self.to_json().encode("utf-8")) > MAX_CONTINUATION_BYTES:
            raise ValueError("receiver continuation exceeds 64 KiB")

    def to_json(self) -> str:
        """Serialize only public continuation fields, never collaborator objects or claims."""
        return json.dumps(
            {
                "version": self.version,
                "operation": self.operation.value,
                "cursor": self.cursor.token if self.cursor else None,
                "dueCutoff": utc_instant(self.due_cutoff).isoformat() if self.due_cutoff else None,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            allow_nan=False,
        )

    @classmethod
    def from_json(cls, raw: str) -> ScanContinuation:
        """Decode a strict bounded object, rejecting unknown/duplicate fields before use."""
        if not isinstance(raw, str) or len(raw.encode("utf-8")) > MAX_CONTINUATION_BYTES:
            raise ValueError("receiver continuation exceeds 64 KiB")
        value = json.loads(raw, object_pairs_hook=_unique_fields)
        if not isinstance(value, dict) or set(value) != {
            "version",
            "operation",
            "cursor",
            "dueCutoff",
        }:
            raise ValueError("invalid receiver continuation fields")
        cursor, cutoff = value["cursor"], value["dueCutoff"]
        if cursor is not None and not isinstance(cursor, str):
            raise ValueError("invalid receiver continuation cursor")
        if cutoff is not None and not isinstance(cutoff, str):
            raise ValueError("invalid receiver continuation cutoff")
        return cls(
            ScanOperation(value["operation"]),
            Cursor(cursor) if cursor is not None else None,
            datetime.fromisoformat(cutoff) if cutoff is not None else None,
            value["version"],
        )


def _unique_fields(pairs: list[tuple[str, object]]) -> dict[str, object]:
    """Do not let a repeated field silently replace a persisted version or operation."""
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate receiver continuation field")
        result[key] = value
    return result


@dataclass(frozen=True, slots=True)
class ScanClaim:
    """Scope/incarnation/generation/token fencing; the serialized expiry is not authoritative."""

    deployment_id: str
    workspace_id: str | None
    operation: ScanOperation | None
    incarnation: str
    generation: int
    token: str
    holder: str
    expires_at: datetime
    revision: int

    def __post_init__(self) -> None:
        """Validate shape; the provider still compares authoritative stored ownership."""
        scan_identity(self.deployment_id)
        scan_identity(self.holder)
        if (self.workspace_id is None) != (self.operation is None):
            raise ValueError("discovery and operation claim scopes cannot be mixed")
        if self.workspace_id is not None:
            scan_identity(self.workspace_id)
            if not isinstance(self.operation, ScanOperation):
                raise ValueError("invalid receiver operation")
        if any(
            not isinstance(value, str) or not _TOKEN.fullmatch(value)
            for value in (self.incarnation, self.token)
        ):
            raise ValueError("invalid receiver claim token")
        if type(self.generation) is not int or self.generation < 1:
            raise ValueError("invalid receiver claim generation")
        if type(self.revision) is not int or self.revision < 0:
            raise ValueError("invalid receiver progress revision")
        utc_instant(self.expires_at)


def require_scan_claim(current: ScanClaim | None, claim: ScanClaim, now: datetime) -> None:
    """Fence scope/identity/revision using stored expiry, ignoring caller expiry edits."""
    if (
        current is None
        or (
            current.deployment_id,
            current.workspace_id,
            current.operation,
            current.incarnation,
            current.generation,
            current.token,
            current.holder,
            current.revision,
        )
        != (
            claim.deployment_id,
            claim.workspace_id,
            claim.operation,
            claim.incarnation,
            claim.generation,
            claim.token,
            claim.holder,
            claim.revision,
        )
        or utc_instant(current.expires_at) <= utc_instant(now)
    ):
        raise LeaseExpired("receiver scan claim expired or no longer owned")


@dataclass(frozen=True, slots=True)
class DiscoveryScan:
    """Claimed discovery position; sweep increments only after a committed terminal page."""

    claim: ScanClaim
    cursor: Cursor | None
    sweep: int
    completed_sweep: int


@dataclass(frozen=True, slots=True)
class ReceiverScan:
    """One claimed operation, including its exact committed continuation and service order."""

    claim: ScanClaim
    continuation: ScanContinuation
    service_ticket: int


def validate_discovery_page(
    workspaces: Sequence[str],
    previous: Cursor | None,
    following: Cursor | None,
) -> None:
    """Enforce bounded increasing IDs and exact keyset continuation, without enumerating Auth."""
    if len(workspaces) > DISCOVERY_PAGE_SIZE:
        raise ValueError("receiver discovery page exceeds 50 IDs")
    after = decode_query_cursor(previous, RECEIVER_WORKSPACE_SCOPE)
    for workspace in workspaces:
        scan_identity(workspace)
        if after is not None and workspace <= after:
            raise ValueError("receiver discovery IDs must be strictly increasing")
        after = workspace
    if following is not None:
        next_after = decode_query_cursor(following, RECEIVER_WORKSPACE_SCOPE)
        if not workspaces or next_after != workspaces[-1]:
            raise ValueError("receiver discovery continuation must match the final ID")


@runtime_checkable
class ReceiverScanProgressProvider(Protocol):
    """Optional durable fair scan capability; does not grant authorization for external effects."""

    async def check_receiver_scan_progress_ready(self) -> None:
        """Fail explicitly unless the schema and capability support version-1 progress."""
        ...

    async def claim_receiver_discovery(
        self,
        deployment_id: str,
        holder: str,
        *,
        lease_seconds: int = 30,
    ) -> DiscoveryScan | None:
        """Claim one deployment's bounded discovery, or return no claim if already owned."""
        ...

    async def commit_receiver_discovery(
        self,
        scan: DiscoveryScan,
        workspaces: Sequence[str],
        next_cursor: Cursor | None,
    ) -> None:
        """Atomically register/checkpoint a page; reread after an ambiguous acknowledgement."""
        ...

    async def claim_receiver_scan(
        self,
        deployment_id: str,
        operation: ScanOperation,
        holder: str,
        *,
        lease_seconds: int = 360,
        scheduler_lease: SchedulerLease | None = None,
    ) -> ReceiverScan | None:
        """Claim the oldest eligible ticket; scheduled claims require live scheduler authority."""
        ...

    async def checkpoint_receiver_scan(
        self,
        scan: ReceiverScan,
        continuation: ScanContinuation,
        *,
        interval_seconds: float = 1,
    ) -> None:
        """Persist exactly the acknowledged next position and release; stale writes fail."""
        ...

    async def release_receiver_scan(self, scan: ReceiverScan) -> None:
        """Retain prior progress and release with a five-second retry floor, only while owned."""
        ...

    async def prune_receiver_scans(
        self,
        deployment_id: str,
        completed_sweep: int,
        *,
        limit: int = 50,
    ) -> int:
        """Delete a bounded set of finished, absent, unleased rows idle for at least 30 days."""
        ...
