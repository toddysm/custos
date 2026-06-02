"""``ResumeSubscriptionMirror`` entity + repository (WF-IMPL-102).

The ``ResumeSubscriptionMirror`` is what makes the Workflow Service
the **source of truth** for resume subscriptions (REQ-081,
``design.md`` § *Data Models*). The Step Coordinator persists one
mirror row **before** it calls the Trigger Service to register a
``waitFor:`` step's resume subscription, so a crash between the
mirror write and the Trigger Service call leaves the Workflow
Service aware that the registration is still pending. On Dapr
Workflow replay the coordinator re-derives the open subscription
set from the mirror and idempotently re-registers each one.

This module ships:

* :class:`ResumeSubscriptionMirror` — the frozen value object,
  with byte-stable :meth:`~ResumeSubscriptionMirror.to_dict` /
  :meth:`~ResumeSubscriptionMirror.to_json` serialization and the
  matching :meth:`~ResumeSubscriptionMirror.from_dict` /
  :meth:`~ResumeSubscriptionMirror.from_json` reconstructors.
* :class:`ResumeSubscriptionMirrorRepository` — the narrow
  ``runtime_checkable`` Protocol the Resume Subscription Manager
  depends on (``put`` / ``list_open`` / ``list_open_for_step`` /
  ``delete`` / ``list_expired``).
* :class:`InMemoryResumeSubscriptionMirrorRepository` — the
  single-process adapter unit tests use. The production
  ``MetadataStoreProvider``-resident adapter (REQ-048) is a
  separate follow-up — same staging the idempotency ledger uses
  (in-memory now, store-backed adapter later).

Design references:

* ``design.md`` § *Data Models* — pins the
  ``ResumeSubscriptionMirror`` columns
  (``mirrorId`` / ``runId`` / ``stepId`` / ``eventKey`` /
  ``selector`` / ``tsSubscriptionId`` / ``registeredAt`` /
  ``expiresAt``).
* ``design.md`` § *Resume Subscription Replay Protocol*, mirror
  sequencing rule — *persist the mirror before the Trigger
  Service call; update the mirror's ``tsSubscriptionId`` if a
  replay re-registration returns a different id*.

Acceptance criteria (mirrored from #541):

* Byte-stable serialization round-trip of the mirror row.
* A mirror written before any Trigger Service call is observable
  via :meth:`~ResumeSubscriptionMirrorRepository.list_open`.
* :meth:`~ResumeSubscriptionMirrorRepository.list_expired` honors
  ``expiresAt``.
"""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from collections.abc import Mapping

__all__ = [
    "InMemoryResumeSubscriptionMirrorRepository",
    "ResumeSubscriptionMirror",
    "ResumeSubscriptionMirrorRepository",
]


# ---------------------------------------------------------------------------
# Entity
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ResumeSubscriptionMirror:
    """One persisted resume-subscription registration the WF owns.

    A frozen, hashable value object mirroring the
    ``ResumeSubscriptionMirror`` table in ``design.md`` § *Data
    Models*. :attr:`mirror_id` is the primary key the repository
    upserts on — re-``put``-ting a mirror with the same
    :attr:`mirror_id` replaces the row (this is how a replay
    updates :attr:`ts_subscription_id` after the Trigger Service
    re-issues an id post-TTL).

    :attr:`selector` is ``None`` when the subscription matches on
    :attr:`event_key` alone; an empty string is rejected because it
    almost always means an unresolved optional leaked through.

    :attr:`registered_at` / :attr:`expires_at` are timezone-aware
    UTC instants (the repository's ``list_expired`` sweep compares
    :attr:`expires_at` against a caller-supplied cutoff).

    :raises ValueError: If :attr:`mirror_id`, :attr:`run_id`,
        :attr:`step_id`, :attr:`event_key`, or
        :attr:`ts_subscription_id` is empty; if :attr:`selector` is
        an empty string; or if :attr:`registered_at` /
        :attr:`expires_at` is naive (no tzinfo).
    """

    mirror_id: str
    run_id: str
    step_id: str
    event_key: str
    ts_subscription_id: str
    registered_at: datetime
    expires_at: datetime
    selector: str | None = None

    def __post_init__(self) -> None:
        if not self.mirror_id:
            raise ValueError("ResumeSubscriptionMirror.mirror_id must be a non-empty string")
        if not self.run_id:
            raise ValueError("ResumeSubscriptionMirror.run_id must be a non-empty string")
        if not self.step_id:
            raise ValueError("ResumeSubscriptionMirror.step_id must be a non-empty string")
        if not self.event_key:
            raise ValueError("ResumeSubscriptionMirror.event_key must be a non-empty string")
        if not self.ts_subscription_id:
            raise ValueError(
                "ResumeSubscriptionMirror.ts_subscription_id must be a non-empty string"
            )
        if self.selector is not None and not self.selector:
            raise ValueError(
                "ResumeSubscriptionMirror.selector must be None or a non-empty string "
                "(an empty string usually means an unresolved optional leaked through)"
            )
        if self.registered_at.tzinfo is None:
            raise ValueError(
                "ResumeSubscriptionMirror.registered_at must be timezone-aware (use datetime.UTC)"
            )
        if self.expires_at.tzinfo is None:
            raise ValueError(
                "ResumeSubscriptionMirror.expires_at must be timezone-aware (use datetime.UTC)"
            )

    def to_dict(self) -> dict[str, Any]:
        """Render the mirror to its camelCase, JSON-safe wire form.

        Datetimes are emitted via :meth:`datetime.isoformat` so the
        round-trip through :meth:`from_dict` is exact. The key set
        and casing match the ``design.md`` § *Data Models* column
        names so a future ``MetadataStoreProvider`` adapter can
        persist the dict unchanged.
        """
        return {
            "mirrorId": self.mirror_id,
            "runId": self.run_id,
            "stepId": self.step_id,
            "eventKey": self.event_key,
            "selector": self.selector,
            "tsSubscriptionId": self.ts_subscription_id,
            "registeredAt": self.registered_at.isoformat(),
            "expiresAt": self.expires_at.isoformat(),
        }

    def to_json(self) -> str:
        """Serialize to a byte-stable canonical JSON string.

        Keys are sorted and separators are tight so two equal
        mirrors always produce byte-identical output — the property
        the acceptance criteria pin for the round-trip.
        """
        return json.dumps(self.to_dict(), sort_keys=True, separators=(",", ":"))

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> ResumeSubscriptionMirror:
        """Reconstruct a mirror from its :meth:`to_dict` wire form.

        :raises KeyError: If a required field is missing.
        :raises ValueError: If a datetime field is not a valid
            ISO-8601 string, or any invariant from
            :meth:`__post_init__` is violated.
        """
        return cls(
            mirror_id=data["mirrorId"],
            run_id=data["runId"],
            step_id=data["stepId"],
            event_key=data["eventKey"],
            ts_subscription_id=data["tsSubscriptionId"],
            registered_at=datetime.fromisoformat(data["registeredAt"]),
            expires_at=datetime.fromisoformat(data["expiresAt"]),
            selector=data.get("selector"),
        )

    @classmethod
    def from_json(cls, raw: str) -> ResumeSubscriptionMirror:
        """Reconstruct a mirror from a :meth:`to_json` string."""
        return cls.from_dict(json.loads(raw))


# ---------------------------------------------------------------------------
# Repository Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class ResumeSubscriptionMirrorRepository(Protocol):
    """Narrow runtime Protocol the Resume Subscription Manager depends on.

    Production deployments swap the in-memory implementation for a
    :class:`MetadataStoreProvider`-resident adapter (separate
    follow-up); tests and single-process integration runs use
    :class:`InMemoryResumeSubscriptionMirrorRepository` directly.
    """

    async def put(self, mirror: ResumeSubscriptionMirror) -> ResumeSubscriptionMirror:
        """Insert or replace the mirror keyed on its ``mirror_id``.

        Upsert semantics: re-``put``-ting the same ``mirror_id``
        overwrites the row, which is how a replay updates
        ``ts_subscription_id`` after a Trigger Service
        re-registration returns a fresh id.
        """
        ...

    async def list_open(self, run_id: str) -> tuple[ResumeSubscriptionMirror, ...]:
        """Return every mirror for ``run_id`` (the run's open subscriptions)."""
        ...

    async def list_open_for_step(
        self, run_id: str, step_id: str
    ) -> tuple[ResumeSubscriptionMirror, ...]:
        """Return every mirror for one ``(run_id, step_id)`` pair."""
        ...

    async def delete(self, mirror_id: str) -> None:
        """Delete the mirror keyed on ``mirror_id``; a no-op if absent."""
        ...

    async def list_expired(self, before: datetime) -> tuple[ResumeSubscriptionMirror, ...]:
        """Return every mirror whose ``expires_at`` is at or before ``before``.

        Drives the periodic TTL garbage-collection sweep
        (``design.md`` § *Data Models*).
        """
        ...


# ---------------------------------------------------------------------------
# In-memory adapter
# ---------------------------------------------------------------------------


class InMemoryResumeSubscriptionMirrorRepository:
    """Single-process :class:`ResumeSubscriptionMirrorRepository`.

    Backed by an :class:`asyncio.Lock`-guarded dict keyed on
    ``mirror_id``. Every query returns a tuple sorted by
    ``mirror_id`` so call sites and test assertions observe a
    deterministic order regardless of insertion order.

    The repository is single-process by construction; multi-replica
    deployments use the ``MetadataStoreProvider``-backed adapter
    (separate follow-up).
    """

    def __init__(self) -> None:
        self._rows: dict[str, ResumeSubscriptionMirror] = {}
        self._lock: asyncio.Lock = asyncio.Lock()

    async def put(self, mirror: ResumeSubscriptionMirror) -> ResumeSubscriptionMirror:
        async with self._lock:
            self._rows[mirror.mirror_id] = mirror
        return mirror

    async def list_open(self, run_id: str) -> tuple[ResumeSubscriptionMirror, ...]:
        async with self._lock:
            return tuple(
                sorted(
                    (row for row in self._rows.values() if row.run_id == run_id),
                    key=lambda row: row.mirror_id,
                )
            )

    async def list_open_for_step(
        self, run_id: str, step_id: str
    ) -> tuple[ResumeSubscriptionMirror, ...]:
        async with self._lock:
            return tuple(
                sorted(
                    (
                        row
                        for row in self._rows.values()
                        if row.run_id == run_id and row.step_id == step_id
                    ),
                    key=lambda row: row.mirror_id,
                )
            )

    async def delete(self, mirror_id: str) -> None:
        async with self._lock:
            # Idempotent: deleting an unknown id is a no-op so the
            # cancellation path can fire blindly per open mirror.
            self._rows.pop(mirror_id, None)

    async def list_expired(self, before: datetime) -> tuple[ResumeSubscriptionMirror, ...]:
        async with self._lock:
            return tuple(
                sorted(
                    (row for row in self._rows.values() if row.expires_at <= before),
                    key=lambda row: row.mirror_id,
                )
            )
