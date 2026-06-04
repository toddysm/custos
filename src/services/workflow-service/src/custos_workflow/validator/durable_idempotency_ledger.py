"""Durable :class:`IdempotencyLedger` backed by a ``MetadataStoreProvider``.

This module lands WF-IMPL-117 (issue #620) — the production
``(workspaceId, StartRun idempotencyKey)`` dedup ledger that survives
process restarts and HA failover. It complements the process-local
:class:`~custos_workflow.validator.idempotency_ledger.InMemoryIdempotencyLedger`
(WF-IMPL-063) shipped for the sidecar-free dev / test path.

Shape mapping
=============

The workflow-service ledger contract
(:class:`~custos_workflow.validator.idempotency_ledger.IdempotencyLedger`)
dedups purely on ``(workspaceId, idempotencyKey)`` with a request
fingerprint. The Storage-Provider-Layer
(:class:`custos_spl.interfaces.metadata_store.MetadataStoreProvider`)
gateway idempotency surface keys on the wider
``(workspace_id, principal_id, route, idempotency_key)`` tuple plus a
``request_hash``. This adapter pins ``route`` to :data:`LEDGER_ROUTE`
and ``principal_id`` to :data:`LEDGER_PRINCIPAL` so the SPL row collapses
back onto the workflow-service ``(workspace, key)`` contract, and maps
the WF ``request_fingerprint`` onto the SPL ``request_hash``.

Result mapping (:meth:`DurableIdempotencyLedger.record_or_replay`):

* ``IdemReserved`` — fresh reservation → :class:`LedgerEntry`
  ``replayed=False``. The caller proceeds with a real ``StartRun``.
* ``ExistingInFlight`` / ``ExistingCompleted`` — the key is live inside
  the TTL window with the *same* fingerprint → ``replayed=True``. The
  caller collapses to the original run.
* ``KeyReuse`` — the key is live with a *different* fingerprint →
  :class:`~custos_workflow.validator.errors.IdempotencyConflictError`.

The adapter never calls ``complete_idempotency_record``: the ledger has
no response snapshot to persist at reservation time (the run has not
started yet), and both the in-flight and completed reservation states
map to the same ``replayed=True`` replay signal. Rows are reaped by
:meth:`purge_expired` (the lifespan sweep) or reclaimed in place by the
SPL ``reserve`` CAS once their ``expires_at`` lapses.

See the issue: https://github.com/toddysm/custos/issues/620
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Final, cast

from custos_spl.ids import PrincipalId, WorkspaceId
from custos_spl.interfaces.metadata_store import (
    ExistingCompleted,
    ExistingInFlight,
    IdemReserved,
    KeyReuse,
)

from custos_workflow.validator.errors import IdempotencyConflictError
from custos_workflow.validator.idempotency_ledger import (
    DEFAULT_IDEMPOTENCY_KEY_TTL,
    LedgerEntry,
    NowCallable,
)

if TYPE_CHECKING:
    from custos_spl.interfaces.metadata_store import MetadataStoreProvider


__all__ = [
    "LEDGER_PRINCIPAL",
    "LEDGER_ROUTE",
    "DurableIdempotencyLedger",
]


#: Gateway ``route`` the workflow-service ledger reserves under. The
#: SPL idempotency key includes ``route`` so distinct routes never
#: collide; the workflow-service ledger only ever dedups ``StartRun``.
LEDGER_ROUTE: Final[str] = "StartRun"

#: Fixed ``principal_id`` the ledger reserves under. The workflow-service
#: ledger contract dedups on ``(workspaceId, idempotencyKey)`` only — it
#: carries no principal — so a constant sentinel collapses the wider SPL
#: ``(workspace, principal, route, key)`` key back onto the WF contract.
LEDGER_PRINCIPAL: Final[str] = "wf-start-run"


def _now_utc() -> datetime:
    """Default clock returning a timezone-aware UTC instant."""
    return datetime.now(UTC)


class DurableIdempotencyLedger:
    """:class:`IdempotencyLedger` over a :class:`MetadataStoreProvider`.

    Delegates the record-or-replay decision to the provider's atomic
    ``reserve_idempotency_record`` CAS so concurrent and post-restart
    ``StartRun`` retries dedup against the same durable row. The
    production lifespan injects the pooled ``custos_pg`` adapter; the
    sidecar-free dev / test path injects the in-process provider, which
    implements the same reserve / reap subset in memory.

    Args:
        provider: The metadata-store provider whose
            ``reserve_idempotency_record`` /
            ``delete_expired_idempotency_records`` methods back the
            ledger.
        ttl: The dedup window. Each reservation's ``expires_at`` is set
            ``ttl`` ahead of reservation time. Defaults to
            :data:`DEFAULT_IDEMPOTENCY_KEY_TTL`. Must be positive.
        now: Optional zero-arg UTC clock injected for
            :meth:`purge_expired`. Tests pass a controllable clock to
            pin the reap cutoff; production leaves it at the default.
    """

    __slots__ = ("_now", "_provider", "_ttl")

    def __init__(
        self,
        provider: MetadataStoreProvider,
        *,
        ttl: timedelta | None = None,
        now: NowCallable | None = None,
    ) -> None:
        if ttl is None:
            ttl = DEFAULT_IDEMPOTENCY_KEY_TTL
        if ttl <= timedelta(0):
            raise ValueError("DurableIdempotencyLedger.ttl must be positive")
        self._provider = provider
        self._ttl: timedelta = ttl
        self._now: NowCallable = now if now is not None else _now_utc

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
        """Reserve a fresh dedup row or replay the stored one.

        Args:
            workspace_id: The owning workspace. Must be non-empty.
            idempotency_key: The caller-supplied opaque key. Must be
                non-empty — keyless callers skip the ledger entirely.
            request_fingerprint: The dedup-equality fingerprint from
                :func:`~custos_workflow.validator.idempotency_ledger.compute_request_fingerprint`.

        Returns:
            A :class:`LedgerEntry`. ``replayed=False`` on the first
            reservation; ``replayed=True`` on every subsequent matching
            call inside the TTL window.

        Raises:
            IdempotencyConflictError: A live row exists for the key
                inside the TTL window but its stored fingerprint differs
                from the supplied one.
        """
        if not workspace_id:
            raise ValueError("workspace_id must be non-empty")
        if not idempotency_key:
            raise ValueError("idempotency_key must be non-empty")
        if not request_fingerprint:
            raise ValueError("request_fingerprint must be non-empty")

        ttl_seconds = max(1, int(self._ttl.total_seconds()))
        result = await self._provider.reserve_idempotency_record(
            cast(WorkspaceId, workspace_id),
            cast(PrincipalId, LEDGER_PRINCIPAL),
            LEDGER_ROUTE,
            idempotency_key,
            request_fingerprint,
            ttl_seconds,
        )

        if isinstance(result, IdemReserved):
            record = result.record
            return LedgerEntry(
                workspace_id=workspace_id,
                idempotency_key=idempotency_key,
                request_fingerprint=record.request_hash,
                recorded_at=record.reserved_at,
                replayed=False,
            )
        if isinstance(result, (ExistingInFlight, ExistingCompleted)):
            record = result.record
            return LedgerEntry(
                workspace_id=workspace_id,
                idempotency_key=idempotency_key,
                request_fingerprint=record.request_hash,
                recorded_at=record.reserved_at,
                replayed=True,
            )
        # ``KeyReuse`` — same key, different fingerprint inside the window.
        if isinstance(result, KeyReuse):
            raise IdempotencyConflictError(
                (
                    "idempotency key already maps to a different "
                    "request fingerprint within the dedup window"
                ),
                workspace_id=workspace_id,
                idempotency_key=idempotency_key,
            )
        # ``ReserveIdempotencyResult`` is a closed union; an unrecognised
        # variant means the SPL contract grew a case this adapter has not
        # been taught yet. Fail loudly rather than silently mis-deduping.
        raise RuntimeError(  # pragma: no cover - defensive on a closed union
            f"unexpected reserve_idempotency_record result: {type(result).__name__}"
        )

    async def purge_expired(self, before: datetime | None = None) -> int:
        """Reap every dedup row whose ``expires_at`` is at or before ``before``.

        Delegates to the provider's
        ``delete_expired_idempotency_records`` sweeper. The lifespan runs
        this on a wall-clock interval aligned with ``WF_IDEMPOTENCY_KEY_TTL``
        so abandoned reservations (keys reserved once and never re-sent)
        do not grow the table without bound.

        Args:
            before: Reap rows expiring at or before this instant.
                Defaults to the ledger clock's current UTC time.

        Returns:
            The number of rows deleted.
        """
        cutoff = before if before is not None else self._now()
        return await self._provider.delete_expired_idempotency_records(cutoff)
