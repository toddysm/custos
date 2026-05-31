"""Idempotency-Key ledger for the API Adapter Validator.

This module implements WF-IMPL-063 (issue #449) — the ledger that
backs the ``(workspaceId, idempotencyKey)`` dedup contract specified
in :ref:`design.md § Idempotency Model
<workflow-service-design-idempotency>` and exercised by the
:class:`custos_workflow.runs.controller.RunController` six-gate
``start_run`` algorithm.

The ledger sits **between** the public REST surface and the Run
Controller: it lets the API Adapter answer "is this StartRun a
replay?" without touching the Catalog Service or the persistent Run
store. The Run Controller already runs a second, byte-equal dedup
check against the persisted :class:`RunRecord`; the ledger short-
circuits the common replay path so retries from a flaky network
never spin up a fresh Catalog round-trip or Dapr instance.

Surface
=======

* :class:`LedgerEntry` — the value object the ledger returns. Its
  ``replayed`` flag tells the caller whether the entry was minted
  fresh on this call (``False``) or already present from a prior
  call within the TTL window (``True``).
* :class:`IdempotencyLedger` — the narrow runtime Protocol the
  :class:`~custos_workflow.validator.service.StartRunValidator`
  depends on. In-memory ``InMemoryIdempotencyLedger`` ships in this
  module; the Postgres-backed adapter is a separate follow-up
  filed post-tracker (see todos.md).
* :class:`InMemoryIdempotencyLedger` — single-process adapter that
  satisfies the Protocol. Trivially threadsafe under
  :class:`asyncio.Lock`. Entries older than the configured TTL are
  garbage-collected lazily on each ``record_or_replay`` call so
  the ledger does not retain state past the dedup window.
* :func:`compute_request_fingerprint` — the canonical-JSON SHA-256
  helper used to derive the dedup-equality fingerprint. Mirrors
  :func:`custos_workflow.runs.controller._fingerprint_inputs`'s
  byte-stable shape so the API-side and controller-side dedup
  windows agree on what counts as a "byte-equal" replay.
* :data:`DEFAULT_IDEMPOTENCY_KEY_TTL` /
  :func:`idempotency_ttl_from_env` — the ``WF_IDEMPOTENCY_KEY_TTL``
  configuration knob (design.md § Configuration, default ``PT24H``).

Failure mapping
===============

The ledger raises :class:`~custos_workflow.validator.errors.IdempotencyConflictError`
when the supplied ``(workspaceId, idempotencyKey)`` is live in the
window but its stored fingerprint differs from the call's
fingerprint. The API exception handler
(:mod:`custos_workflow.api.errors`) maps that error to RFC 7807
``409 Conflict`` with the ``idempotencyKey`` extension key, per
WF-IMPL-061.

See the issue: https://github.com/toddysm/custos/issues/449
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Final, Protocol, runtime_checkable

from custos_workflow.validator.errors import IdempotencyConflictError

if TYPE_CHECKING:
    from collections.abc import Mapping
    from typing import Any


#: Type alias for the ``now`` callable injected into
#: :class:`InMemoryIdempotencyLedger`. Returns a timezone-aware UTC
#: :class:`~datetime.datetime`. Tests pass a deterministic clock.
NowCallable = Callable[[], datetime]


__all__ = [
    "DEFAULT_IDEMPOTENCY_KEY_TTL",
    "IDEMPOTENCY_TTL_ENV_VAR",
    "IdempotencyLedger",
    "InMemoryIdempotencyLedger",
    "LedgerEntry",
    "NowCallable",
    "compute_request_fingerprint",
    "idempotency_ttl_from_env",
]


# ---------------------------------------------------------------------------
# Configuration: TTL window
# ---------------------------------------------------------------------------


#: Default TTL window for ``(workspaceId, StartRun idempotencyKey)`` dedup.
#: Matches design.md § Configuration (``WF_IDEMPOTENCY_KEY_TTL``
#: default ``PT24H``).
DEFAULT_IDEMPOTENCY_KEY_TTL: Final[timedelta] = timedelta(hours=24)

#: Environment variable that overrides :data:`DEFAULT_IDEMPOTENCY_KEY_TTL`
#: at process startup. Surface published in design.md § Configuration
#: and the workflow-service README.
IDEMPOTENCY_TTL_ENV_VAR: Final[str] = "WF_IDEMPOTENCY_KEY_TTL"


#: Strict ISO-8601 duration grammar used to parse
#: ``WF_IDEMPOTENCY_KEY_TTL``. Lifted from
#: :data:`custos_workflow.runs.wait._ISO8601_DURATION_PATTERN` so the
#: ledger has a self-contained parser and never imports the run-time
#: wait module from a publish-time concern. Pattern parity is asserted
#: in the unit tests.
_ISO8601_DURATION_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^P(?:"
    r"(?P<weeks>\d+)W"
    r"|"
    r"(?:(?P<days>\d+)D)?"
    r"(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?"
    r")$"
)


def _parse_iso8601_duration(raw: str) -> timedelta:
    """Parse an ISO-8601 duration string into a positive :class:`timedelta`.

    Accepts the same grammar
    :func:`custos_workflow.runs.wait.parse_wait_duration` accepts:
    ``PnW`` weeks form OR ``P[nD][T[nH][nM][nS]]`` with at least
    one positive component. Months / years are rejected; they would
    translate to a calendar-dependent window which the ledger
    contract explicitly disallows.

    Args:
        raw: The raw ISO-8601 duration string, typically the value
            of ``WF_IDEMPOTENCY_KEY_TTL``.

    Returns:
        The parsed positive :class:`~datetime.timedelta`.

    Raises:
        ValueError: ``raw`` does not parse as an ISO-8601 duration,
            specifies months/years, or evaluates to a non-positive
            duration.
    """
    match = _ISO8601_DURATION_PATTERN.match(raw)
    if match is None:
        raise ValueError(
            f"WF_IDEMPOTENCY_KEY_TTL: {raw!r} is not a recognised ISO-8601 duration",
        )
    weeks = int(match.group("weeks") or 0)
    days = int(match.group("days") or 0)
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes") or 0)
    seconds = float(match.group("seconds") or 0.0)
    if weeks == 0 and days == 0 and hours == 0 and minutes == 0 and seconds == 0.0:
        raise ValueError(
            f"WF_IDEMPOTENCY_KEY_TTL: {raw!r} must be greater than zero",
        )
    return timedelta(
        weeks=weeks,
        days=days,
        hours=hours,
        minutes=minutes,
        seconds=seconds,
    )


def idempotency_ttl_from_env(
    environ: Mapping[str, str] | None = None,
) -> timedelta:
    """Resolve the ledger TTL from ``WF_IDEMPOTENCY_KEY_TTL``.

    Reads :data:`IDEMPOTENCY_TTL_ENV_VAR` from ``environ`` (defaults
    to :data:`os.environ`). An unset or blank value falls back to
    :data:`DEFAULT_IDEMPOTENCY_KEY_TTL`. A malformed value raises
    :class:`ValueError` so process startup surfaces the
    misconfiguration loudly rather than silently dedup'ing forever.

    Args:
        environ: Optional mapping to read the env var from. Tests
            inject a dict so the resolver stays hermetic.

    Returns:
        The parsed positive :class:`~datetime.timedelta` window.
    """
    source = environ if environ is not None else os.environ
    raw = source.get(IDEMPOTENCY_TTL_ENV_VAR)
    if raw is None or raw.strip() == "":
        return DEFAULT_IDEMPOTENCY_KEY_TTL
    return _parse_iso8601_duration(raw.strip())


# ---------------------------------------------------------------------------
# Fingerprint helper
# ---------------------------------------------------------------------------


def compute_request_fingerprint(
    workflow_version_id: str,
    inputs: Mapping[str, Any] | None,
) -> str:
    """Hex-encoded SHA-256 of the dedup-equality payload.

    Uses ``json.dumps(..., sort_keys=True, separators=(",", ":"))``
    so dict-key ordering and whitespace differences collapse to the
    same digest. The shape mirrors
    :func:`custos_workflow.runs.controller._fingerprint_inputs` so
    the API-side ledger and the Run Controller's six-gate dedup
    agree byte-for-byte on what counts as the "same" request.

    Args:
        workflow_version_id: The Catalog ``WorkflowVersion`` UUID
            the caller is targeting. Passed in to keep the ledger
            entry self-describing — two different versions with
            identical inputs hash to different fingerprints.
        inputs: The caller-supplied ``inputs`` mapping, or ``None``
            (treated as an empty mapping, matching the controller).

    Returns:
        The 64-character lowercase hex digest.
    """
    canonical = json.dumps(
        {
            "workflow_version_id": workflow_version_id,
            "inputs": dict(inputs or {}),
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Ledger value object + Protocol
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LedgerEntry:
    """One ``(workspaceId, idempotencyKey)`` dedup record.

    The :attr:`replayed` flag is the surface contract the Validator
    keys off: ``False`` on the first ``record_or_replay`` call (the
    Validator goes on to fetch Catalog + hand a fresh
    :class:`~custos_workflow.validator.service.ValidatedStartRun` to
    the controller); ``True`` on every subsequent matching call
    inside the TTL window (the Validator short-circuits and the
    controller's six-gate algorithm collapses the second
    ``start_run`` to the original :class:`RunRef`).

    Attributes:
        workspace_id: The owning workspace.
        idempotency_key: The caller-supplied opaque key.
        request_fingerprint: Hex-encoded SHA-256 from
            :func:`compute_request_fingerprint` for the original
            ``(workflow_version_id, inputs)`` that minted the entry.
        recorded_at: Timezone-aware UTC instant the entry was
            first minted. Used as the TTL anchor.
        replayed: ``True`` if the current call replayed a stored
            entry; ``False`` if the current call minted it.
    """

    workspace_id: str
    idempotency_key: str
    request_fingerprint: str
    recorded_at: datetime
    replayed: bool


@runtime_checkable
class IdempotencyLedger(Protocol):
    """Narrow runtime Protocol the Validator depends on.

    Production deployments swap the in-memory implementation for a
    :class:`MetadataStoreProvider`-resident adapter (separate
    follow-up issue). Tests and single-process integration runs use
    :class:`InMemoryIdempotencyLedger` directly.
    """

    async def record_or_replay(
        self,
        *,
        workspace_id: str,
        idempotency_key: str,
        request_fingerprint: str,
    ) -> LedgerEntry:
        """Record a new dedup entry or replay the stored one.

        Args:
            workspace_id: The owning workspace. Must be non-empty.
            idempotency_key: The caller-supplied opaque key. Must
                be non-empty — callers without a key skip the
                ledger entirely.
            request_fingerprint: The dedup-equality fingerprint
                computed by :func:`compute_request_fingerprint`.

        Returns:
            A :class:`LedgerEntry`. ``replayed=False`` on the first
            call; ``replayed=True`` on every subsequent matching
            call within the TTL window.

        Raises:
            IdempotencyConflictError: A live entry exists for the
                key inside the TTL window but its stored
                fingerprint differs from the supplied one.
        """
        ...


# ---------------------------------------------------------------------------
# In-memory adapter
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _LedgerRow:
    """Internal storage row for :class:`InMemoryIdempotencyLedger`.

    Mirrors :class:`LedgerEntry` minus the ``replayed`` flag — the
    flag is per-call, not per-row. Mutable on purpose: the ledger
    never rewrites a row's fingerprint, but the wrapper dataclass
    stays mutable to keep ``_purge_expired`` allocation-free.
    """

    workspace_id: str
    idempotency_key: str
    request_fingerprint: str
    recorded_at: datetime


def _now_utc() -> datetime:
    """Default clock returning a timezone-aware UTC instant.

    Injected as the default ``now`` callable of
    :class:`InMemoryIdempotencyLedger`; tests pass a deterministic
    clock to pin TTL boundaries.
    """
    return datetime.now(UTC)


class InMemoryIdempotencyLedger:
    """Single-process :class:`IdempotencyLedger` implementation.

    Backed by an ``asyncio.Lock``-guarded dict keyed on
    ``(workspaceId, idempotencyKey)``. Lazily purges entries whose
    age exceeds :attr:`ttl` on every ``record_or_replay`` call so
    the ledger never retains state past the dedup window.

    Args:
        ttl: The dedup window. Defaults to
            :data:`DEFAULT_IDEMPOTENCY_KEY_TTL`.
        now: Optional zero-arg callable returning a timezone-aware
            :class:`~datetime.datetime`. Defaults to
            :func:`_now_utc`. Tests inject a controllable clock to
            exercise TTL boundaries.

    Thread / asyncio safety:
        ``record_or_replay`` takes an :class:`asyncio.Lock` for the
        full critical section so concurrent callers observe a
        consistent record-or-replay decision. The ledger is
        single-process by construction; multi-replica deployments
        use the Postgres-backed adapter (separate follow-up).
    """

    def __init__(
        self,
        *,
        ttl: timedelta | None = None,
        now: NowCallable | None = None,
    ) -> None:
        if ttl is None:
            ttl = DEFAULT_IDEMPOTENCY_KEY_TTL
        if ttl <= timedelta(0):
            raise ValueError("InMemoryIdempotencyLedger.ttl must be positive")
        self._ttl: timedelta = ttl
        self._now: NowCallable = now if now is not None else _now_utc
        self._rows: dict[tuple[str, str], _LedgerRow] = {}
        self._lock: asyncio.Lock = asyncio.Lock()

    @property
    def ttl(self) -> timedelta:
        """The configured dedup window."""
        return self._ttl

    async def record_or_replay(
        self,
        *,
        workspace_id: str,
        idempotency_key: str,
        request_fingerprint: str,
    ) -> LedgerEntry:
        if not workspace_id:
            raise ValueError("workspace_id must be non-empty")
        if not idempotency_key:
            raise ValueError("idempotency_key must be non-empty")
        if not request_fingerprint:
            raise ValueError("request_fingerprint must be non-empty")
        async with self._lock:
            now = self._now()
            self._purge_expired(now)
            key = (workspace_id, idempotency_key)
            existing = self._rows.get(key)
            if existing is not None:
                if existing.request_fingerprint != request_fingerprint:
                    raise IdempotencyConflictError(
                        (
                            "idempotency key already maps to a different "
                            "request fingerprint within the dedup window"
                        ),
                        workspace_id=workspace_id,
                        idempotency_key=idempotency_key,
                    )
                return LedgerEntry(
                    workspace_id=workspace_id,
                    idempotency_key=idempotency_key,
                    request_fingerprint=existing.request_fingerprint,
                    recorded_at=existing.recorded_at,
                    replayed=True,
                )
            row = _LedgerRow(
                workspace_id=workspace_id,
                idempotency_key=idempotency_key,
                request_fingerprint=request_fingerprint,
                recorded_at=now,
            )
            self._rows[key] = row
            return LedgerEntry(
                workspace_id=workspace_id,
                idempotency_key=idempotency_key,
                request_fingerprint=request_fingerprint,
                recorded_at=now,
                replayed=False,
            )

    def _purge_expired(self, now: datetime) -> None:
        """Drop rows whose age has exceeded :attr:`ttl`.

        Called under :attr:`_lock`. Builds a small ``stale`` list so
        the dict mutation does not collide with the iteration.
        """
        cutoff = now - self._ttl
        stale: list[tuple[str, str]] = [
            key for key, row in self._rows.items() if row.recorded_at <= cutoff
        ]
        for key in stale:
            del self._rows[key]

    # Test-only introspection ----------------------------------------------

    def _snapshot(self) -> dict[tuple[str, str], LedgerEntry]:
        """Return a defensive copy of the live ledger contents.

        Used by unit tests to assert TTL eviction without poking at
        the private storage dict. Each entry is materialised with
        ``replayed=False`` since the snapshot itself is not a
        record-or-replay decision.
        """
        return {
            key: LedgerEntry(
                workspace_id=row.workspace_id,
                idempotency_key=row.idempotency_key,
                request_fingerprint=row.request_fingerprint,
                recorded_at=row.recorded_at,
                replayed=False,
            )
            for key, row in self._rows.items()
        }
