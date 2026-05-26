"""Unit tests for :class:`LeaseManager` (CONN-IMPL-017 / Phase G/2).

Drives the manager against :class:`FakeLeaseAdapter` + the existing
:class:`FakeMetadataAdapter` so wall time, the cap, and the TTL
precedence ladder are all observable. No Postgres dependency \u2014
the conformance of the Pg adapter is covered by the integration
suite in ``src/libs/custos-postgres/tests/``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from custos_spl.errors import ImmutableViolation
from custos_spl.ids import ConnectorInstanceId, RunId, StepId, WorkspaceId
from custos_spl.interfaces.lease_store import LeaseStoreProvider
from custos_spl.interfaces.metadata_store import MetadataStoreProvider

from custos_connector.audit import (
    EVENT_LEASE_ISSUED,
    EVENT_LEASE_REFRESHED,
    EVENT_LEASE_RELEASED,
)
from custos_connector.lease import LeaseError, LeaseErrorCode, LeaseManager

from ._fakes import FakeLeaseAdapter, FakeMetadataAdapter

pytestmark = pytest.mark.asyncio


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

_T0 = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)
_WS = WorkspaceId("ws-1")
_RUN = RunId("run-1")
_STEP = StepId("step-1")
_INSTANCE = ConnectorInstanceId("ci-1")


def _make_manager(
    *,
    default_ttl_sec: int = 600,
    max_concurrent: int = 16,
    clock: datetime | None = None,
    lease_store: FakeLeaseAdapter | None = None,
    metadata_store: FakeMetadataAdapter | None = None,
) -> tuple[LeaseManager, FakeLeaseAdapter, FakeMetadataAdapter]:
    """Build a LeaseManager with a fixed-clock seam.

    Returns the manager plus the two fakes so tests can assert against
    audit emission and the lease store contents directly.
    """
    ls = lease_store if lease_store is not None else FakeLeaseAdapter()
    md = metadata_store if metadata_store is not None else FakeMetadataAdapter()
    now = clock or _T0
    mgr = LeaseManager(
        lease_store=cast("LeaseStoreProvider", ls),
        metadata_store=cast("MetadataStoreProvider", md),
        default_ttl_sec=default_ttl_sec,
        max_concurrent_leases=max_concurrent,
        clock=lambda: now,
    )
    return mgr, ls, md


async def _issue(
    mgr: LeaseManager,
    *,
    attempt: int = 1,
    slot: str = "primary",
    requested_ttl_sec: int | None = None,
    type_max_ttl_sec: int | None = None,
    instance_ttl_sec: int | None = None,
    step_deadline: datetime | None = None,
) -> str:
    """Issue a lease with sensible defaults and return its id."""
    lease = await mgr.issue(
        workspace_id=_WS,
        run_id=_RUN,
        step_id=_STEP,
        attempt=attempt,
        slot=slot,
        capability="read",
        connector_instance_id=_INSTANCE,
        token_type="bearer",
        requested_ttl_sec=requested_ttl_sec,
        type_max_ttl_sec=type_max_ttl_sec,
        instance_ttl_sec=instance_ttl_sec,
        step_deadline=step_deadline,
    )
    return lease.lease_id


# ---------------------------------------------------------------------------
# ULID + lease id shape
# ---------------------------------------------------------------------------


async def test_issue_returns_lease_with_lease_prefix_and_26_char_ulid() -> None:
    """Lease ids must be ``lease_<26-char Crockford base32 ULID>``."""
    mgr, _, _ = _make_manager()
    lease_id = await _issue(mgr)
    assert lease_id.startswith("lease_")
    payload = lease_id.removeprefix("lease_")
    assert len(payload) == 26
    allowed = set("0123456789ABCDEFGHJKMNPQRSTVWXYZ")
    assert set(payload) <= allowed


async def test_two_issues_mint_distinct_ids_under_same_clock() -> None:
    """Even when the clock is pinned, two issues must mint distinct ULIDs.

    The 80 random bits dominate \u2014 a collision under the fake clock
    would indicate the randomness was wired wrong.
    """
    mgr, _, _ = _make_manager()
    id1 = await _issue(mgr)
    id2 = await _issue(mgr, attempt=2)
    assert id1 != id2


# ---------------------------------------------------------------------------
# TTL precedence
# ---------------------------------------------------------------------------


async def test_ttl_uses_default_when_no_caps_supplied() -> None:
    mgr, ls, _ = _make_manager(default_ttl_sec=600)
    lease_id = await _issue(mgr)
    stored = await ls.get_lease(_WS, lease_id)
    assert stored is not None
    assert stored.expires_at == _T0 + timedelta(seconds=600)


async def test_ttl_type_max_wins_when_below_default() -> None:
    mgr, ls, _ = _make_manager(default_ttl_sec=600)
    lease_id = await _issue(mgr, type_max_ttl_sec=120)
    stored = await ls.get_lease(_WS, lease_id)
    assert stored is not None
    assert stored.expires_at == _T0 + timedelta(seconds=120)


async def test_ttl_instance_ttl_wins_when_lowest() -> None:
    mgr, ls, _ = _make_manager(default_ttl_sec=600)
    lease_id = await _issue(mgr, type_max_ttl_sec=300, instance_ttl_sec=90)
    stored = await ls.get_lease(_WS, lease_id)
    assert stored is not None
    assert stored.expires_at == _T0 + timedelta(seconds=90)


async def test_ttl_step_deadline_clamps_minus_safety_buffer() -> None:
    """Step deadline imposes a hard ceiling minus the 5-second buffer."""
    mgr, ls, _ = _make_manager(default_ttl_sec=600)
    deadline = _T0 + timedelta(seconds=60)
    lease_id = await _issue(mgr, requested_ttl_sec=600, step_deadline=deadline)
    stored = await ls.get_lease(_WS, lease_id)
    assert stored is not None
    # 60 - 5 (safety buffer) = 55s.
    assert stored.expires_at == _T0 + timedelta(seconds=55)


async def test_ttl_requested_value_overrides_default_when_lower() -> None:
    mgr, ls, _ = _make_manager(default_ttl_sec=600)
    lease_id = await _issue(mgr, requested_ttl_sec=45)
    stored = await ls.get_lease(_WS, lease_id)
    assert stored is not None
    assert stored.expires_at == _T0 + timedelta(seconds=45)


async def test_step_deadline_in_past_raises_invalid_request() -> None:
    mgr, _, _ = _make_manager()
    past = _T0 - timedelta(seconds=1)
    with pytest.raises(LeaseError) as exc:
        await _issue(mgr, step_deadline=past)
    assert exc.value.code is LeaseErrorCode.INVALID_REQUEST


# ---------------------------------------------------------------------------
# Concurrent-lease cap
# ---------------------------------------------------------------------------


async def test_cap_is_enforced_at_default_of_16() -> None:
    """The 17th lease on the same (run, step, attempt) is refused."""
    mgr, _, _ = _make_manager(max_concurrent=16)
    issued: list[str] = []
    for _ in range(16):
        issued.append(await _issue(mgr))
    assert len({lease_id for lease_id in issued}) == 16
    with pytest.raises(LeaseError) as exc:
        await _issue(mgr)
    assert exc.value.code is LeaseErrorCode.CAPACITY_EXCEEDED


async def test_cap_frees_slot_after_release() -> None:
    """Releasing a lease must free a slot for the next issue call."""
    mgr, _, _ = _make_manager(max_concurrent=2)
    a = await _issue(mgr)
    await _issue(mgr)
    with pytest.raises(LeaseError):
        await _issue(mgr)
    await mgr.release(workspace_id=_WS, lease_id=a)
    # Next issue must succeed.
    await _issue(mgr)


async def test_cap_is_scoped_per_step_attempt() -> None:
    """A second attempt on the same step gets a fresh cap."""
    mgr, _, _ = _make_manager(max_concurrent=2)
    await _issue(mgr, attempt=1)
    await _issue(mgr, attempt=1)
    # attempt=2 has its own bucket.
    await _issue(mgr, attempt=2)
    await _issue(mgr, attempt=2)


# ---------------------------------------------------------------------------
# Refresh
# ---------------------------------------------------------------------------


async def test_refresh_preserves_lease_id_and_only_updates_expires_at() -> None:
    mgr, ls, _ = _make_manager(default_ttl_sec=60)
    lease_id = await _issue(mgr)
    original = await ls.get_lease(_WS, lease_id)
    assert original is not None
    refreshed = await mgr.refresh(workspace_id=_WS, lease_id=lease_id, requested_ttl_sec=300)
    assert refreshed.lease_id == lease_id
    assert refreshed.issued_at == original.issued_at
    assert refreshed.expires_at == _T0 + timedelta(seconds=300)
    # Static fields unchanged.
    assert refreshed.run_id == original.run_id
    assert refreshed.step_id == original.step_id
    assert refreshed.attempt == original.attempt
    assert refreshed.slot == original.slot


async def test_refresh_raises_not_found_when_lease_absent() -> None:
    mgr, _, _ = _make_manager()
    with pytest.raises(LeaseError) as exc:
        await mgr.refresh(workspace_id=_WS, lease_id="lease_DOESNOTEXIST")
    assert exc.value.code is LeaseErrorCode.NOT_FOUND


async def test_refresh_raises_already_released_after_release() -> None:
    mgr, _, _ = _make_manager()
    lease_id = await _issue(mgr)
    await mgr.release(workspace_id=_WS, lease_id=lease_id)
    with pytest.raises(LeaseError) as exc:
        await mgr.refresh(workspace_id=_WS, lease_id=lease_id)
    assert exc.value.code is LeaseErrorCode.ALREADY_RELEASED


# ---------------------------------------------------------------------------
# Release
# ---------------------------------------------------------------------------


async def test_release_is_idempotent() -> None:
    """Two release calls succeed; ``released_at`` stays at the first call.

    The fake (and the Pg adapter) keep ``released_at`` pinned to the
    first release so audit replay can answer "when was this lease
    actually returned" deterministically.
    """
    mgr, ls, _ = _make_manager()
    lease_id = await _issue(mgr)
    first = await mgr.release(workspace_id=_WS, lease_id=lease_id)
    assert first is not None
    assert first.released_at == _T0
    second = await mgr.release(workspace_id=_WS, lease_id=lease_id)
    assert second is not None
    assert second.released_at == _T0  # unchanged
    stored = await ls.get_lease(_WS, lease_id)
    assert stored is not None
    assert stored.released_at == _T0


async def test_release_returns_none_when_lease_absent() -> None:
    mgr, _, _ = _make_manager()
    result = await mgr.release(workspace_id=_WS, lease_id="lease_NOPE")
    assert result is None


# ---------------------------------------------------------------------------
# Audit emission
# ---------------------------------------------------------------------------


async def test_issue_emits_lease_issued_audit_event() -> None:
    mgr, _, md = _make_manager()
    lease_id = await _issue(mgr)
    types = [ev.event_type for _, ev in md.append_audit_calls]
    assert types == [EVENT_LEASE_ISSUED]
    _, issued_evt = md.append_audit_calls[0]
    assert issued_evt.subject["lease_id"] == lease_id
    assert issued_evt.subject["connector_instance_id"] == str(_INSTANCE)
    assert issued_evt.payload["run_id"] == str(_RUN)
    assert issued_evt.payload["step_id"] == str(_STEP)
    assert issued_evt.payload["attempt"] == 1
    assert issued_evt.payload["token_type"] == "bearer"


async def test_refresh_emits_lease_refreshed_audit_event() -> None:
    mgr, _, md = _make_manager(default_ttl_sec=60)
    lease_id = await _issue(mgr)
    md.append_audit_calls.clear()
    await mgr.refresh(workspace_id=_WS, lease_id=lease_id, requested_ttl_sec=120)
    assert [ev.event_type for _, ev in md.append_audit_calls] == [EVENT_LEASE_REFRESHED]
    _, refreshed_evt = md.append_audit_calls[0]
    assert refreshed_evt.subject["lease_id"] == lease_id
    assert refreshed_evt.payload["new_expires_at"] == (_T0 + timedelta(seconds=120)).isoformat()


async def test_release_emits_lease_released_audit_event() -> None:
    mgr, _, md = _make_manager()
    lease_id = await _issue(mgr)
    md.append_audit_calls.clear()
    await mgr.release(workspace_id=_WS, lease_id=lease_id)
    assert [ev.event_type for _, ev in md.append_audit_calls] == [EVENT_LEASE_RELEASED]
    _, released_evt = md.append_audit_calls[0]
    assert released_evt.subject["lease_id"] == lease_id
    assert released_evt.payload["released_at"] == _T0.isoformat()


# ---------------------------------------------------------------------------
# SPL adapter contract guards
# ---------------------------------------------------------------------------


async def test_put_lease_is_create_only_via_fake() -> None:
    """Belt-and-braces: the fake mirrors the PgLeaseAdapter contract.

    The Lease Manager never re-puts the same id (it mints a fresh ULID
    every issue), so this guards against a future refactor that
    bypasses the manager.
    """
    mgr, ls, _ = _make_manager()
    lease_id = await _issue(mgr)
    stored = await ls.get_lease(_WS, lease_id)
    assert stored is not None
    with pytest.raises(ImmutableViolation):
        await ls.put_lease(_WS, stored)
