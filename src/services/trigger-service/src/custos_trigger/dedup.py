"""Reserve-before-dispatch dedup / idempotency (TS-IMPL-009).

The Trigger Service deduplicates inbound events on
``hash(subscriptionId, source.eventId)`` so a replayed event — Dapr's
at-least-once redelivery, a connector cursor rewind, a vendor double-push —
never starts a duplicate run (design ``§ Pipeline``, ``§ Failure Modes``).

The dedup window is backed by the SPL ``put_dedup_key`` reserve-or-read
primitive: the first event for a key **reserves** it (returns
:data:`DedupDecision.UNSEEN`) and any replay within the retention window hits
the existing row (returns :data:`DedupDecision.DUPLICATE`). The
:meth:`Deduplicator.guard` async context manager wraps a dispatch so the
reservation is **rolled back when the dispatch fails** — honoring the design's
"dedup key not committed" failure-mode contract (row 1) so the redelivery can
re-attempt instead of being suppressed as a false duplicate.

SPL exposes no selective dedup-clear in v1 (design ``§ TODO-007`` defers the
admin API), so the rollback is best-effort: it deletes the key when the
backing store advertises a ``release_dedup_key`` hook (the in-process backend
used in dev/tests) and otherwise relies on TTL expiry. The hard idempotency
guarantee remains the Workflow Service ``StartRun`` ``idempotencyKey`` (design
``§ Internal RPCs``); this store is the fast-path duplicate suppressor.
"""

from __future__ import annotations

import hashlib
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from dataclasses import dataclass
from enum import StrEnum
from typing import Protocol, runtime_checkable

from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.metadata_store import DedupDuplicate, DedupReserved

from custos_trigger.settings import DEFAULT_DEDUP_TTL_SECONDS
from custos_trigger.stores.base import TriggerMetadataStore

__all__ = [
    "DEDUP_KEY_PREFIX",
    "DedupDecision",
    "DedupReservation",
    "Deduplicator",
    "compute_dedup_key",
]

#: Namespacing prefix stamped onto every dedup key so the rows are
#: self-describing in the store and never collide with other key families.
DEDUP_KEY_PREFIX: str = "trigger.dedup.v1"


def compute_dedup_key(subscription_id: str, event_id: str) -> str:
    """Return the stable dedup key for ``(subscription_id, event_id)``.

    The two parts are length-prefixed before hashing so distinct
    ``(subscription_id, event_id)`` pairs can never alias through a shared
    delimiter (e.g. ``("a", "b:c")`` vs ``("a:b", "c")``). The digest is
    SHA-256 — deterministic across replicas and restarts, which is what makes
    the dedup window correct under at-least-once redelivery.
    """
    digest = hashlib.sha256()
    for part in (subscription_id, event_id):
        encoded = part.encode("utf-8")
        digest.update(f"{len(encoded)}:".encode("ascii"))
        digest.update(encoded)
    return f"{DEDUP_KEY_PREFIX}:{digest.hexdigest()}"


class DedupDecision(StrEnum):
    """Outcome of a dedup reservation."""

    #: The key was reserved for the first time — caller should dispatch.
    UNSEEN = "unseen"
    #: An un-expired row already existed — caller must suppress the dispatch.
    DUPLICATE = "duplicate"


@dataclass(frozen=True, slots=True)
class DedupReservation:
    """The result of reserving a dedup key."""

    key: str
    decision: DedupDecision

    @property
    def is_unseen(self) -> bool:
        """True when the event is new and the caller should dispatch."""
        return self.decision is DedupDecision.UNSEEN

    @property
    def is_duplicate(self) -> bool:
        """True when the event is a replay and dispatch must be suppressed."""
        return self.decision is DedupDecision.DUPLICATE


@runtime_checkable
class _DedupReleasable(Protocol):
    """Optional store capability: drop a previously reserved dedup key.

    The in-process backend implements this so :meth:`Deduplicator.guard` can
    roll back a reservation when the guarded dispatch fails. Stores without it
    (the v1 Postgres adapter) fall back to TTL expiry.
    """

    async def release_dedup_key(self, workspace_id: WorkspaceId, key: str) -> None: ...


class Deduplicator:
    """Reserve-before-dispatch dedup over a :class:`TriggerMetadataStore`."""

    def __init__(
        self,
        store: TriggerMetadataStore,
        *,
        default_ttl_seconds: int = DEFAULT_DEDUP_TTL_SECONDS,
    ) -> None:
        self._store = store
        self._default_ttl_seconds = default_ttl_seconds

    async def reserve(
        self,
        *,
        workspace_id: str,
        subscription_id: str,
        event_id: str,
        ttl_seconds: int | None = None,
    ) -> DedupReservation:
        """Reserve the dedup key for ``(subscription_id, event_id)``.

        Returns :data:`DedupDecision.UNSEEN` on first sight (the key is now
        reserved) or :data:`DedupDecision.DUPLICATE` when an un-expired row
        already exists. ``ttl_seconds`` overrides the configured default
        (``TRIGGER_DEDUP_TTL_SECONDS``).
        """
        key = compute_dedup_key(subscription_id, event_id)
        ttl = self._default_ttl_seconds if ttl_seconds is None else ttl_seconds
        result = await self._store.put_dedup_key(WorkspaceId(workspace_id), key, ttl)
        if isinstance(result, DedupReserved):
            decision = DedupDecision.UNSEEN
        elif isinstance(result, DedupDuplicate):
            decision = DedupDecision.DUPLICATE
        else:  # pragma: no cover - defensive guard against store contract drift
            raise TypeError(
                f"put_dedup_key returned unexpected type {type(result).__name__!r}; "
                "expected DedupReserved or DedupDuplicate"
            )
        return DedupReservation(key=key, decision=decision)

    async def release(self, *, workspace_id: str, key: str) -> None:
        """Best-effort drop of a reserved key (no-op when unsupported)."""
        store = self._store
        if isinstance(store, _DedupReleasable):
            await store.release_dedup_key(WorkspaceId(workspace_id), key)

    @asynccontextmanager
    async def guard(
        self,
        *,
        workspace_id: str,
        subscription_id: str,
        event_id: str,
        ttl_seconds: int | None = None,
    ) -> AsyncIterator[DedupReservation]:
        """Wrap a dispatch in a reserve-before-dispatch dedup guard.

        Reserves the key up front and yields the :class:`DedupReservation`.
        When it is :attr:`~DedupReservation.is_duplicate` the caller must skip
        the dispatch. On the unseen path the reservation stands once the body
        completes; if the body raises (dispatch failed) the reservation is
        rolled back so the redelivery can re-attempt — the dedup key is *not*
        committed on dispatch failure (design ``§ Failure Modes`` row 1).
        """
        reservation = await self.reserve(
            workspace_id=workspace_id,
            subscription_id=subscription_id,
            event_id=event_id,
            ttl_seconds=ttl_seconds,
        )
        if reservation.is_duplicate:
            yield reservation
            return
        try:
            yield reservation
        except Exception:
            # Best-effort rollback: never let a release failure mask the real
            # dispatch exception the caller is propagating.
            with suppress(Exception):
                await self.release(workspace_id=workspace_id, key=reservation.key)
            raise
