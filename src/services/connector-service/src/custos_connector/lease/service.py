"""Lease Manager (CONN-IMPL-017 + CONN-IMPL-018 / Phase G/2 + G/3).

The Lease Manager mints, refreshes, releases, expires, and revokes
activity-token leases used by the secret-bridge sidecar. It enforces:

* Stable, unique ``lease_id`` per issuance \u2014 a 26-char Crockford
  base32 ULID with a ``lease_`` prefix.
* The four-level TTL precedence rule
  ``min(requested_or_default, type_max, instance_ttl, step_deadline -
  safety_buffer)``.
* The concurrent-lease cap (default 16) per
  ``(workspace_id, run_id, step_id, attempt)``.
* Audit emission of the full lease-event taxonomy
  (``lease.issued`` / ``lease.refreshed`` / ``lease.released`` /
  ``lease.expired`` / ``lease.revoke-requested`` / ``lease.revoked``
  / ``lease.denied``) after each state change or rejection.

The actual lease persistence sits behind the SPL
:class:`LeaseStoreProvider`; this service is a thin domain layer on
top.
"""

from __future__ import annotations

import secrets
import time
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Final

from custos_spl.ids import ConnectorInstanceId, RunId, StepId, WorkspaceId
from custos_spl.interfaces.lease_store import Lease, LeaseStoreProvider
from custos_spl.interfaces.metadata_store import MetadataStoreProvider

from custos_connector.audit import (
    audit_lease_denied,
    audit_lease_expired,
    audit_lease_issued,
    audit_lease_refreshed,
    audit_lease_released,
    audit_lease_revoke_requested,
    audit_lease_revoked,
)
from custos_connector.lease.errors import LeaseError, LeaseErrorCode, http_status_for

#: Seconds shaved off the step deadline when it dominates the TTL
#: precedence ladder. The buffer absorbs lease-refresh round-trip
#: jitter so a lease never expires *after* the step it backs.
_STEP_DEADLINE_SAFETY_BUFFER_SEC: Final[int] = 5

#: ULID alphabet (Crockford base32). Excludes I, L, O, U to dodge
#: visual ambiguity. Mirrors the canonical ULID spec.
_CROCKFORD: Final[str] = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def _ulid(now_ms: int | None = None) -> str:
    """Generate a 26-char Crockford-base32 ULID.

    48 bits of millisecond timestamp followed by 80 bits of randomness.
    Lexicographically sortable by issue time, which matches how the
    SPL store indexes by ``(issued_at DESC, lease_id ASC)``.
    """
    ts = now_ms if now_ms is not None else int(time.time() * 1000)
    # 48-bit timestamp + 80-bit randomness = 128 bits = 16 bytes.
    rnd = secrets.token_bytes(10)
    value = (ts & ((1 << 48) - 1)) << 80
    value |= int.from_bytes(rnd, "big")
    chars: list[str] = []
    for _ in range(26):
        chars.append(_CROCKFORD[value & 0x1F])
        value >>= 5
    return "".join(reversed(chars))


@dataclass(frozen=True, slots=True)
class TtlInputs:
    """Inputs to :meth:`LeaseManager._resolve_ttl_seconds`.

    Bundles the four caps the precedence ladder considers. ``None``
    means "no opinion at this level" and that level is skipped.
    """

    requested_ttl_sec: int | None
    type_max_ttl_sec: int | None
    instance_ttl_sec: int | None
    step_deadline: datetime | None


class LeaseManager:
    """Issues, refreshes, releases, expires, and revokes activity-token leases.

    Construction is bag-of-callables: the SPL stores, the metadata
    store (for audit emission), the cap-and-default settings, and the
    clock seam all arrive as named kwargs so unit tests can wire
    in-memory fakes and pin wall time.

    The manager is stateless beyond its constructor wiring; every
    public method derives its outcome from the supplied args plus a
    live read of the lease store.
    """

    def __init__(
        self,
        *,
        lease_store: LeaseStoreProvider,
        metadata_store: MetadataStoreProvider,
        default_ttl_sec: int,
        max_concurrent_leases: int,
        clock: Callable[[], datetime] | None = None,
        actor: str = "connector-service",
    ) -> None:
        if default_ttl_sec <= 0:
            raise ValueError(f"default_ttl_sec must be positive; got {default_ttl_sec}")
        if max_concurrent_leases <= 0:
            raise ValueError(f"max_concurrent_leases must be positive; got {max_concurrent_leases}")
        self._lease_store = lease_store
        self._metadata_store = metadata_store
        self._default_ttl_sec = default_ttl_sec
        self._max_concurrent_leases = max_concurrent_leases
        self._clock: Callable[[], datetime] = (
            clock if clock is not None else (lambda: datetime.now(UTC))
        )
        self._actor = actor

    # ----- TTL precedence -----

    def _resolve_ttl_seconds(self, *, now: datetime, inputs: TtlInputs) -> int:
        """Apply the four-level precedence ladder.

        Returns the resolved TTL in whole seconds. Raises
        :class:`LeaseError` (``INVALID_REQUEST``) when every input is
        non-positive \u2014 there must be at least one valid cap so
        the lease has a finite, future expiry.
        """
        candidates: list[int] = []
        base = (
            inputs.requested_ttl_sec
            if inputs.requested_ttl_sec is not None
            else self._default_ttl_sec
        )
        if base > 0:
            candidates.append(base)
        if inputs.type_max_ttl_sec is not None and inputs.type_max_ttl_sec > 0:
            candidates.append(inputs.type_max_ttl_sec)
        if inputs.instance_ttl_sec is not None and inputs.instance_ttl_sec > 0:
            candidates.append(inputs.instance_ttl_sec)
        if inputs.step_deadline is not None:
            remaining = (
                int((inputs.step_deadline - now).total_seconds()) - _STEP_DEADLINE_SAFETY_BUFFER_SEC
            )
            if remaining > 0:
                candidates.append(remaining)
            else:
                # Step deadline has already passed (or is within the
                # safety buffer): no extension is safe, refuse.
                raise LeaseError(
                    LeaseErrorCode.INVALID_REQUEST,
                    "step_deadline is in the past or within the safety buffer; "
                    "refusing to issue a lease that would outlive its step",
                )
        if not candidates:
            raise LeaseError(
                LeaseErrorCode.INVALID_REQUEST,
                "no positive TTL candidate (requested_ttl_sec, type_max_ttl_sec, "
                "instance_ttl_sec, and step_deadline all unusable)",
            )
        return min(candidates)

    # ----- Audit helpers (denial wiring) -----

    async def _record_denied_for(
        self,
        *,
        workspace_id: WorkspaceId,
        lease_id: str | None,
        connector_instance_id: str | None,
        op: str,
        exc: LeaseError,
    ) -> None:
        """Emit ``lease.denied`` for a Lease-Manager-raised :class:`LeaseError`.

        Internal wrapper around :meth:`record_denied`. Keeps the
        emit-then-raise pattern compact in :meth:`issue` /
        :meth:`refresh`. The mapping from
        :class:`LeaseErrorCode` to HTTP status lives on
        :func:`~custos_connector.lease.errors.http_status_for` so
        direct-call clients (unit tests) see the same status the
        wire-level handler will return.
        """
        await self.record_denied(
            workspace_id=workspace_id,
            lease_id=lease_id,
            connector_instance_id=connector_instance_id,
            op=op,
            reason_code=exc.code.value,
            reason_detail=exc.detail,
            http_status=http_status_for(exc.code),
        )

    # ----- Public surface -----

    async def issue(
        self,
        *,
        workspace_id: WorkspaceId,
        run_id: RunId,
        step_id: StepId,
        attempt: int,
        slot: str,
        capability: str,
        connector_instance_id: ConnectorInstanceId,
        token_type: str,
        requested_ttl_sec: int | None = None,
        type_max_ttl_sec: int | None = None,
        instance_ttl_sec: int | None = None,
        step_deadline: datetime | None = None,
    ) -> Lease:
        """Mint a new lease for an activity-token issuance.

        Order of operations: cap check \u2192 TTL precedence \u2192 ULID
        mint \u2192 SPL ``put_lease`` \u2192 audit emission. The cap
        check uses :meth:`LeaseStoreProvider.count_active_for_step_attempt`
        against the current clock so released or expired leases free
        up their slot.

        Raises :class:`LeaseError(CAPACITY_EXCEEDED)` when the
        ``(workspace_id, run_id, step_id, attempt)`` tuple is at the
        configured cap, and :class:`LeaseError(INVALID_REQUEST)` when
        the TTL precedence ladder cannot produce a positive value
        (e.g. step deadline already in the past). Every raise is
        preceded by a ``lease.denied`` audit emission.
        """
        instance_id_str = str(connector_instance_id)
        try:
            if attempt <= 0:
                raise LeaseError(
                    LeaseErrorCode.INVALID_REQUEST,
                    f"attempt must be positive; got {attempt}",
                )
            now = self._clock()
            active = await self._lease_store.count_active_for_step_attempt(
                workspace_id, run_id, step_id, attempt, now
            )
            if active >= self._max_concurrent_leases:
                raise LeaseError(
                    LeaseErrorCode.CAPACITY_EXCEEDED,
                    f"concurrent-lease cap reached "
                    f"({active}/{self._max_concurrent_leases}) for "
                    f"(run_id={run_id}, step_id={step_id}, attempt={attempt})",
                )
            ttl_sec = self._resolve_ttl_seconds(
                now=now,
                inputs=TtlInputs(
                    requested_ttl_sec=requested_ttl_sec,
                    type_max_ttl_sec=type_max_ttl_sec,
                    instance_ttl_sec=instance_ttl_sec,
                    step_deadline=step_deadline,
                ),
            )
        except LeaseError as exc:
            # No lease_id yet at this stage; the cap and TTL
            # validations fire before mint. Emit with lease_id=None.
            await self._record_denied_for(
                workspace_id=workspace_id,
                lease_id=None,
                connector_instance_id=instance_id_str,
                op="issue",
                exc=exc,
            )
            raise
        expires_at = now + timedelta(seconds=ttl_sec)
        lease_id = f"lease_{_ulid(int(now.timestamp() * 1000))}"
        lease = Lease(
            workspace_id=workspace_id,
            lease_id=lease_id,
            run_id=run_id,
            step_id=step_id,
            attempt=attempt,
            slot=slot,
            capability=capability,
            connector_instance_id=connector_instance_id,
            token_type=token_type,
            issued_at=now,
            expires_at=expires_at,
            released_at=None,
            revoked_at=None,
            revoke_reason=None,
            created_at=now,
            updated_at=now,
        )
        stored = await self._lease_store.put_lease(workspace_id, lease)
        await audit_lease_issued(
            self._metadata_store,
            workspace_id=str(workspace_id),
            actor=self._actor,
            lease_id=stored.lease_id,
            run_id=str(run_id),
            step_id=str(step_id),
            attempt=attempt,
            slot=slot,
            capability=capability,
            connector_instance_id=str(connector_instance_id),
            token_type=token_type,
            issued_at=stored.issued_at,
            expires_at=stored.expires_at,
        )
        return stored

    async def refresh(
        self,
        *,
        workspace_id: WorkspaceId,
        lease_id: str,
        requested_ttl_sec: int | None = None,
        type_max_ttl_sec: int | None = None,
        instance_ttl_sec: int | None = None,
        step_deadline: datetime | None = None,
    ) -> Lease:
        """Extend the expiry of an existing lease without changing its id.

        Looks up the current row, validates it is still active, runs
        the TTL precedence ladder against the current clock, and asks
        the SPL store to set the new ``expires_at``. The ``lease_id``
        is preserved so downstream correlation (audit, sidecar
        revocation) keeps working across refreshes.

        Raises :class:`LeaseError(NOT_FOUND)` when no row exists for
        ``(workspace_id, lease_id)`` and
        :class:`LeaseError(ALREADY_RELEASED)` when the row exists but
        ``released_at`` is set. Every raise is preceded by a
        ``lease.denied`` audit emission.
        """
        instance_id_str: str | None = None
        try:
            current = await self._lease_store.get_lease(workspace_id, lease_id)
            if current is None:
                raise LeaseError(
                    LeaseErrorCode.NOT_FOUND,
                    f"no lease {lease_id} in workspace {workspace_id}",
                )
            instance_id_str = str(current.connector_instance_id)
            if current.released_at is not None:
                raise LeaseError(
                    LeaseErrorCode.ALREADY_RELEASED,
                    f"lease {lease_id} was released at {current.released_at.isoformat()}",
                )
            now = self._clock()
            ttl_sec = self._resolve_ttl_seconds(
                now=now,
                inputs=TtlInputs(
                    requested_ttl_sec=requested_ttl_sec,
                    type_max_ttl_sec=type_max_ttl_sec,
                    instance_ttl_sec=instance_ttl_sec,
                    step_deadline=step_deadline,
                ),
            )
            new_expires_at = now + timedelta(seconds=ttl_sec)
            refreshed = await self._lease_store.refresh_lease(
                workspace_id, lease_id, new_expires_at
            )
            if refreshed is None:
                # Race: lease was released between get and refresh.
                raise LeaseError(
                    LeaseErrorCode.ALREADY_RELEASED,
                    f"lease {lease_id} was released concurrently with refresh",
                )
        except LeaseError as exc:
            await self._record_denied_for(
                workspace_id=workspace_id,
                lease_id=lease_id,
                connector_instance_id=instance_id_str,
                op="refresh",
                exc=exc,
            )
            raise
        await audit_lease_refreshed(
            self._metadata_store,
            workspace_id=str(workspace_id),
            actor=self._actor,
            lease_id=refreshed.lease_id,
            run_id=str(refreshed.run_id),
            step_id=str(refreshed.step_id),
            attempt=refreshed.attempt,
            slot=refreshed.slot,
            capability=refreshed.capability,
            connector_instance_id=str(refreshed.connector_instance_id),
            token_type=refreshed.token_type,
            previous_expires_at=current.expires_at,
            new_expires_at=refreshed.expires_at,
        )
        return refreshed

    async def release(
        self,
        *,
        workspace_id: WorkspaceId,
        lease_id: str,
    ) -> Lease | None:
        """Mark a lease as released.

        Idempotent: a second release call is a no-op at the storage
        layer and still emits an audit event so the caller's intent
        is recorded. Returns ``None`` when no row exists for
        ``(workspace_id, lease_id)`` \u2014 callers may treat that as
        a successful no-op or surface ``NOT_FOUND`` to the user.
        """
        now = self._clock()
        released = await self._lease_store.release_lease(workspace_id, lease_id, now)
        if released is None:
            return None
        await audit_lease_released(
            self._metadata_store,
            workspace_id=str(workspace_id),
            actor=self._actor,
            lease_id=released.lease_id,
            run_id=str(released.run_id),
            step_id=str(released.step_id),
            attempt=released.attempt,
            slot=released.slot,
            capability=released.capability,
            connector_instance_id=str(released.connector_instance_id),
            token_type=released.token_type,
            released_at=released.released_at or now,
        )
        return released

    async def expire(
        self,
        *,
        workspace_id: WorkspaceId,
        lease_id: str,
        reason: str = "ttl-reached",
    ) -> Lease | None:
        """Mark a lease as expired and emit ``lease.expired``.

        The SPL has no separate ``expire_lease`` method because the
        cap-check primitive treats released and revoked leases the
        same way as expired ones (``released_at IS NULL`` is the
        predicate). We therefore reuse :meth:`release_lease` to
        close the slot but emit ``lease.expired`` rather than
        ``lease.released`` so the audit consumer can distinguish
        voluntary release from infrastructure-driven cleanup.

        Callers are the future TTL sweeper (``reason="ttl-reached"``)
        and the per-step sidecar shutdown hook
        (``reason="sidecar-shutdown"``). Returns ``None`` when no row
        exists for ``(workspace_id, lease_id)``; idempotent against
        a previous release / expire / revoke.
        """
        now = self._clock()
        released = await self._lease_store.release_lease(workspace_id, lease_id, now)
        if released is None:
            return None
        await audit_lease_expired(
            self._metadata_store,
            workspace_id=str(workspace_id),
            actor=self._actor,
            lease_id=released.lease_id,
            run_id=str(released.run_id),
            step_id=str(released.step_id),
            attempt=released.attempt,
            slot=released.slot,
            capability=released.capability,
            connector_instance_id=str(released.connector_instance_id),
            token_type=released.token_type,
            expires_at=released.expires_at,
            expired_at=released.released_at or now,
            reason=reason,
        )
        return released

    async def revoke(
        self,
        *,
        workspace_id: WorkspaceId,
        lease_id: str,
        reason: str,
    ) -> Lease | None:
        """Revoke a lease and emit ``lease.revoked``.

        Calls :meth:`LeaseStoreProvider.revoke_lease` which sets
        ``revoked_at`` + ``revoke_reason`` and also closes
        ``released_at`` so the cap-check primitive frees the slot.
        Idempotent at the adapter level; emission fires on every
        successful call so the audit trail records each request.

        Returns the post-revoke row or ``None`` when no row exists
        for ``(workspace_id, lease_id)``.
        """
        now = self._clock()
        revoked = await self._lease_store.revoke_lease(workspace_id, lease_id, reason, now)
        if revoked is None:
            return None
        await audit_lease_revoked(
            self._metadata_store,
            workspace_id=str(workspace_id),
            actor=self._actor,
            lease_id=revoked.lease_id,
            run_id=str(revoked.run_id),
            step_id=str(revoked.step_id),
            attempt=revoked.attempt,
            slot=revoked.slot,
            capability=revoked.capability,
            connector_instance_id=str(revoked.connector_instance_id),
            token_type=revoked.token_type,
            revoked_at=revoked.revoked_at or now,
            revoke_reason=revoked.revoke_reason or reason,
        )
        return revoked

    async def record_revoke_requested(
        self,
        *,
        workspace_id: WorkspaceId,
        selector_type: str,
        selector_value: str,
        lease_ids: Sequence[str],
        reason: str,
        operator: str,
    ) -> None:
        """Emit ``lease.revoke-requested`` for an operator-initiated revoke.

        Pure emission helper; performs no state mutation. The
        operator REST endpoint in CONN-IMPL-028 calls this once
        before fanning out to per-lease :meth:`revoke` calls so the
        audit trail records both the high-level intent (one
        ``lease.revoke-requested``) and the per-lease outcome (N
        ``lease.revoked`` events).
        """
        await audit_lease_revoke_requested(
            self._metadata_store,
            workspace_id=str(workspace_id),
            actor=self._actor,
            selector_type=selector_type,
            selector_value=selector_value,
            lease_ids=lease_ids,
            reason=reason,
            operator=operator,
        )

    async def record_denied(
        self,
        *,
        workspace_id: WorkspaceId,
        lease_id: str | None,
        connector_instance_id: str | None,
        op: str,
        reason_code: str,
        reason_detail: str,
        http_status: int,
    ) -> None:
        """Emit ``lease.denied`` for a rejected lease request.

        Pure emission helper; performs no state mutation. Used by
        the future REST handler to record authorization or
        capability rejections that bounce before reaching
        :meth:`issue` / :meth:`refresh` / :meth:`release`, and
        called internally by those methods (via
        :meth:`_record_denied_for`) to auto-emit on every
        :class:`LeaseError` raise.
        """
        await audit_lease_denied(
            self._metadata_store,
            workspace_id=str(workspace_id),
            actor=self._actor,
            lease_id=lease_id,
            connector_instance_id=connector_instance_id,
            op=op,
            reason_code=reason_code,
            reason_detail=reason_detail,
            http_status=http_status,
        )


__all__ = ["LeaseManager", "TtlInputs"]
