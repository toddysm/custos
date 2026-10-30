"""Optional durable Connector polling policy, demand and fenced cursor reservations."""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timedelta
from typing import Literal, Protocol, runtime_checkable

from custos_spl.errors import ImmutableViolation, LeaseBusy, LeaseExpired, WorkspaceMismatch
from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.dead_letters import validate_dispatch_identifier
from custos_spl.interfaces.metadata_store import AuditEvent, ConnectorCursor, LeaseHandle
from custos_spl.interfaces.scheduler import utc_instant
from custos_spl.interfaces.trigger_source import (
    MIN_POLL_INTERVAL_SECONDS as MIN_PULL_INTERVAL_SECONDS,
)
from custos_spl.interfaces.trigger_source import ResumeSource
from custos_spl.pagination import Cursor, Page

PULL_CONTROL_SCHEMA_REVISION = 11
MAX_PULL_LEASE_SECONDS = 300
MAX_PULL_CONTROL_KEYS = 200


def pull_control_keys(
    workspace_id: str, identifiers: Sequence[ConnectorInstanceId]
) -> tuple[str, ...]:
    """Validate a bounded unique key batch before either backend reads policy."""
    validate_dispatch_identifier(workspace_id)
    if isinstance(identifiers, (str, bytes)) or not 1 <= len(identifiers) <= MAX_PULL_CONTROL_KEYS:
        raise ValueError("pull-control reads require between 1 and 200 identifiers")
    for identifier in identifiers:
        validate_dispatch_identifier(identifier)
    if len(set(identifiers)) != len(identifiers):
        raise ValueError("pull-control identifiers must be unique")
    return tuple(sorted(identifiers))


def pull_interval(value: object) -> int:
    """Validate a whole-second polling interval without imposing an arbitrary scheduling ceiling."""
    if type(value) is not int or value < MIN_PULL_INTERVAL_SECONDS:
        raise ValueError("pull interval must be an integer of at least ten seconds")
    try:
        timedelta(seconds=value)
    except OverflowError as exc:
        raise ValueError("pull interval exceeds the supported duration") from exc
    return value


def pull_deadline(now: datetime, seconds: int) -> datetime:
    """Reject timestamp overflow before changing cursor ownership or interval reservations."""
    try:
        return utc_instant(now) + timedelta(seconds=seconds)
    except OverflowError as exc:
        raise ValueError("pull deadline exceeds the supported timestamp range") from exc


def effective_pull_interval(native: int | None, trigger: int | None) -> int | None:
    """Share one timer across explicit Connector listening and Trigger-owned demand."""
    intervals = [pull_interval(value) for value in (native, trigger) if value is not None]
    return min(intervals) if intervals else None


@dataclass(frozen=True, slots=True)
class ConnectorPullControl:
    """Version-one instance policy; absent legacy controls must never imply running.

    Demand is independent of operator pause. Reservations and failure counts
    survive lease release, registration changes and process restart.
    """

    workspace_id: WorkspaceId
    instance_id: ConnectorInstanceId
    operator_paused: bool
    revision: int
    next_eligible_at: datetime
    updated_at: datetime
    updated_by: str
    effective_interval_seconds: int | None = None
    consecutive_failures: int = 0
    schema_version: int = 1
    native_interval_seconds: int | None = None
    trigger_interval_seconds: int | None = None
    invocation_id: str | None = None
    invocation_owner: str | None = None

    def __post_init__(self) -> None:
        """Reject ambiguous versions, scopes, counters and timestamps at the provider boundary."""
        for value in (self.workspace_id, self.instance_id, self.updated_by):
            validate_dispatch_identifier(value)
        if (
            type(self.schema_version) is not int
            or self.schema_version != 1
            or type(self.operator_paused) is not bool
            or type(self.revision) is not int
            or self.revision < 1
            or type(self.consecutive_failures) is not int
            or not 0 <= self.consecutive_failures <= 30
        ):
            raise ValueError("invalid pull control version or policy")
        utc_instant(self.next_eligible_at)
        utc_instant(self.updated_at)
        if self.invocation_id is not None:
            validate_dispatch_identifier(self.invocation_id)
        if self.invocation_owner is not None:
            validate_dispatch_identifier(self.invocation_owner)
        if (self.invocation_id is None) != (self.invocation_owner is None):
            raise ValueError("invocation fence and owner must be paired")
        if self.effective_interval_seconds is not None:
            pull_interval(self.effective_interval_seconds)
        if self.effective_interval_seconds != effective_pull_interval(
            self.native_interval_seconds, self.trigger_interval_seconds
        ):
            raise ValueError("effective pull interval differs from registered demand")


@dataclass(frozen=True, slots=True)
class PullLease(LeaseHandle):
    """One cursor reservation with an opaque ownership token and explicit deadline."""

    workspace_id: WorkspaceId
    instance_id: ConnectorInstanceId
    holder_id: str
    token: str
    expires_at: datetime
    interval_seconds: int

    def __post_init__(self) -> None:
        """Validate opaque ownership data without claiming that the current lease is still live."""
        for value in (self.workspace_id, self.instance_id, self.holder_id, self.token):
            validate_dispatch_identifier(value)
        utc_instant(self.expires_at)
        pull_interval(self.interval_seconds)


class PullTickBlocked(LeaseBusy):
    """An explicit policy/due-state skip, never permission to fall back to a volatile timer."""

    def __init__(
        self,
        reason: Literal["unconfirmed", "paused", "not_due", "no_demand", "invocation_unconfirmed"],
    ) -> None:
        """Keep the reason closed and safe for operational logging."""
        super().__init__(f"pull tick blocked: {reason}")
        self.reason = reason


def require_pull_ready(control: ConnectorPullControl | None, now: datetime) -> int:
    """Return the authoritative effective interval only when policy and due time permit a tick."""
    if control is None:
        raise PullTickBlocked("unconfirmed")
    if control.invocation_id is not None:
        raise PullTickBlocked("invocation_unconfirmed")
    if control.operator_paused:
        raise PullTickBlocked("paused")
    if control.effective_interval_seconds is None:
        raise PullTickBlocked("no_demand")
    if control.next_eligible_at > utc_instant(now):
        raise PullTickBlocked("not_due")
    return control.effective_interval_seconds


def require_pull_lease(
    workspace_id: str,
    lease: PullLease,
    row: ConnectorCursor | None,
    now: datetime,
    *,
    allow_expired: bool = False,
) -> None:
    """Reject stale, foreign or expired cursor ownership before commit or shared backoff."""
    if (
        lease.workspace_id != workspace_id
        or row is None
        or row.workspace_id != workspace_id
        or row.instance_id != lease.instance_id
        or row.lease_holder != lease.token
        or row.lease_expires_at is None
        or (not allow_expired and row.lease_expires_at <= utc_instant(now))
    ):
        raise LeaseExpired("pull cursor ownership expired or changed")


def require_pull_invocation(control: ConnectorPullControl | None, lease: PullLease) -> None:
    """Only the invocation that installed the fence may confirm its termination."""
    if (
        control is None
        or control.invocation_id != lease.token
        or control.workspace_id != lease.workspace_id
        or control.instance_id != lease.instance_id
        or control.invocation_owner != lease.holder_id
    ):
        raise LeaseExpired("pull invocation fence changed or was recovered")


def recover_pull_invocation(
    control: ConnectorPullControl | None,
    cursor: ConnectorCursor | None,
    workspace_id: str,
    instance_id: str,
    invocation_id: str,
    audit: AuditEvent,
    now: datetime,
) -> ConnectorPullControl:
    """Validate explicit recovery and prepare its exact, auditable state transition."""
    validate_pause_audit(workspace_id, instance_id, False, audit)
    validate_dispatch_identifier(invocation_id)
    reason = audit.payload.get("reason")
    if (
        audit.payload.get("invocationId") != invocation_id
        or audit.payload.get("confirmedStopped") is not True
        or not isinstance(reason, str)
        or not reason.strip()
        or len(reason) > 1024
    ):
        raise ValueError("recovery requires matching confirmation and a bounded reason")
    if control is None or control.invocation_id != invocation_id:
        raise ImmutableViolation("invocation changed; inspect the current cursor before recovery")
    if control.workspace_id != workspace_id or control.instance_id != instance_id:
        raise WorkspaceMismatch("pull recovery control scope differs")
    if cursor is not None and (
        cursor.workspace_id != workspace_id or cursor.instance_id != instance_id
    ):
        raise WorkspaceMismatch("pull recovery cursor scope differs")
    if (
        cursor is not None
        and cursor.lease_holder is not None
        and cursor.lease_expires_at is not None
        and cursor.lease_expires_at > utc_instant(now)
    ):
        raise LeaseBusy("a live pull owner cannot be replaced by recovery")
    return replace(
        control,
        invocation_id=None,
        invocation_owner=None,
        operator_paused=False,
        consecutive_failures=0,
        revision=control.revision + 1,
        updated_at=utc_instant(now),
        updated_by=audit.actor,
    )


def validate_pause_audit(
    workspace_id: str,
    instance_id: str,
    paused: bool,
    audit: AuditEvent,
) -> None:
    """Bind the required atomic audit to the exact operator action and scope."""
    validate_dispatch_identifier(workspace_id)
    validate_dispatch_identifier(instance_id)
    validate_dispatch_identifier(audit.actor)
    validate_dispatch_identifier(audit.event_id)
    if audit.workspace_id != workspace_id:
        raise WorkspaceMismatch("pull-control audit workspace differs")
    expected = "connector.pull-loop.paused" if paused else "connector.pull-loop.resumed"
    if (
        type(paused) is not bool
        or audit.event_type != expected
        or audit.subject.get("instance_id") != instance_id
    ):
        raise ValueError("pull-control audit does not describe the operator action")
    utc_instant(audit.occurred_at)
    try:
        json.dumps(
            {"subject": dict(audit.subject), "payload": dict(audit.payload)}, allow_nan=False
        ).encode("utf-8")
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise ValueError("pull-control audit must be finite JSON") from exc


def collect_pull_demand(
    starts: Iterable[object],
    resumes: Iterable[object],
    *,
    default_interval_seconds: int,
    minimum_interval_seconds: int,
) -> dict[str, int]:
    """Aggregate projected active starts and unexpired pull waits, rejecting corrupt associations.

    Providers supply only active pull-start sourceConfig and unexpired pull-resume
    source objects from one consistent workspace snapshot. No business payload
    participates in connector routing or interval selection.
    """
    default = pull_interval(default_interval_seconds)
    floor = pull_interval(minimum_interval_seconds)
    demand: dict[str, int] = {}
    for is_resume, sources in ((False, starts), (True, resumes)):
        for source in sources:
            if not isinstance(source, Mapping):
                raise ValueError("pull demand source must be an object")
            if is_resume:
                association = ResumeSource.from_wire(source)
                if "pull" not in association.delivery_modes:
                    raise ValueError("pull demand contains a push-only wait")
            elif any(
                source.get(key) is not None for key in ("cron", "intervalSeconds", "timezone")
            ) or set(source) - {
                "connectorInstanceId",
                "pollIntervalSeconds",
                "cron",
                "intervalSeconds",
                "timezone",
            }:
                raise ValueError("pull demand contains incompatible source configuration")
            identifier = source.get("connectorInstanceId")
            if not isinstance(identifier, str):
                raise ValueError("pull demand has no connector association")
            validate_dispatch_identifier(identifier)
            configured = source.get("pollIntervalSeconds")
            interval = max(floor, default if configured is None else pull_interval(configured))
            demand[identifier] = min(demand.get(identifier, interval), interval)
    return demand


@runtime_checkable
class ConnectorPullControlProvider(Protocol):
    """Atomic policy/demand/lease capability; the shared metadata minimum remains revision four."""

    async def check_pull_control_ready(self) -> None:
        """Require the optional migration before admitting any durable polling."""
        ...

    async def pull_now(self) -> datetime:
        """Return authoritative storage time for scheduler eligibility."""
        ...

    async def get_pull_control(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
    ) -> ConnectorPullControl | None:
        """Read persisted policy; None means operator intent is unconfirmed."""
        ...

    async def read_pull_controls(
        self,
        workspace_id: WorkspaceId,
        instance_ids: Sequence[ConnectorInstanceId],
    ) -> tuple[ConnectorPullControl, ...]:
        """Read up to 200 scoped controls, including no-demand rows; omit missing keys."""
        ...

    async def initialize_pull_control(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
        *,
        actor: str,
    ) -> ConnectorPullControl:
        """Initialize a server-generated new instance without overwriting existing policy."""
        ...

    async def set_pull_paused(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
        *,
        paused: bool,
        audit: AuditEvent,
    ) -> ConnectorPullControl:
        """Atomically persist intent and audit before acknowledgment, even without demand."""
        ...

    async def reconcile_pull_demand(
        self,
        workspace_id: WorkspaceId,
        *,
        default_interval_seconds: int,
        minimum_interval_seconds: int,
    ) -> None:
        """Refresh demand from current stored starts/waits, preserving pause and reserved time.

        Serialize reconciliations before reading intent. Missing legacy controls
        are created paused, never inferred running from active subscriptions.
        """
        ...

    async def set_pull_native_demand(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
        *,
        interval_seconds: int | None,
        actor: str,
    ) -> ConnectorPullControl:
        """Persist explicit Connector listen demand without clearing Trigger demand or pause."""
        ...

    async def query_pull_controls(
        self,
        workspace_id: WorkspaceId,
        *,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[ConnectorPullControl]:
        """Page controls with active demand by instance ID, including operator-paused rows."""
        ...

    async def acquire_pull_tick(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
        holder_id: str,
        *,
        ttl_seconds: int,
    ) -> tuple[ConnectorCursor, PullLease]:
        """Atomically check policy/time, claim a fresh cursor token and reserve the interval."""
        ...

    async def commit_pull_tick(
        self,
        workspace_id: WorkspaceId,
        lease: PullLease,
        new_value: str,
        new_advanced_at: datetime,
    ) -> ConnectorCursor:
        """Commit after every publication acknowledges; reset failures without clearing due time."""
        ...

    async def fail_pull_tick(
        self,
        workspace_id: WorkspaceId,
        lease: PullLease,
        *,
        backoff_cap_seconds: int,
    ) -> None:
        """Confirm termination, preserve the cursor and clear only the matching invocation.

        The caller must have verified termination (or never started the hook).
        An expired matching owner may finalize; a replaced/recovered owner may not.
        """
        ...

    async def recover_pull_invocation(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
        *,
        invocation_id: str,
        audit: AuditEvent,
    ) -> ConnectorPullControl:
        """Atomically audit explicit recovery of the observed invocation after lease expiry."""
        ...
