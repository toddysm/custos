"""Unit tests for :class:`LeaseManager` (CONN-IMPL-017 + CONN-IMPL-018 / Phase G/2 + G/3).

Drives the manager against :class:`FakeLeaseAdapter` + the existing
:class:`FakeMetadataAdapter` so wall time, the cap, and the TTL
precedence ladder are all observable. No Postgres dependency \u2014
the conformance of the Pg adapter is covered by the integration
suite in ``src/libs/custos-postgres/tests/``.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from custos_spl.errors import ImmutableViolation
from custos_spl.ids import ConnectorInstanceId, RunId, StepId, WorkspaceId
from custos_spl.interfaces.lease_store import LeaseStoreProvider
from custos_spl.interfaces.metadata_store import MetadataStoreProvider

from custos_connector.audit import (
    EVENT_LEASE_DENIED,
    EVENT_LEASE_EXPIRED,
    EVENT_LEASE_ISSUED,
    EVENT_LEASE_REFRESHED,
    EVENT_LEASE_RELEASED,
    EVENT_LEASE_REVOKE_REQUESTED,
    EVENT_LEASE_REVOKED,
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
    assert refreshed_evt.subject["connector_instance_id"] == str(_INSTANCE)
    # CONN-IMPL-018: every lease event carries the full identifier tuple.
    assert refreshed_evt.payload["run_id"] == str(_RUN)
    assert refreshed_evt.payload["step_id"] == str(_STEP)
    assert refreshed_evt.payload["attempt"] == 1
    assert refreshed_evt.payload["slot"] == "primary"
    assert refreshed_evt.payload["capability"] == "read"
    assert refreshed_evt.payload["token_type"] == "bearer"
    assert refreshed_evt.payload["previous_expires_at"] == (_T0 + timedelta(seconds=60)).isoformat()
    assert refreshed_evt.payload["new_expires_at"] == (_T0 + timedelta(seconds=120)).isoformat()


async def test_release_emits_lease_released_audit_event() -> None:
    mgr, _, md = _make_manager()
    lease_id = await _issue(mgr)
    md.append_audit_calls.clear()
    await mgr.release(workspace_id=_WS, lease_id=lease_id)
    assert [ev.event_type for _, ev in md.append_audit_calls] == [EVENT_LEASE_RELEASED]
    _, released_evt = md.append_audit_calls[0]
    assert released_evt.subject["lease_id"] == lease_id
    assert released_evt.subject["connector_instance_id"] == str(_INSTANCE)
    # CONN-IMPL-018: every lease event carries the full identifier tuple.
    assert released_evt.payload["run_id"] == str(_RUN)
    assert released_evt.payload["step_id"] == str(_STEP)
    assert released_evt.payload["attempt"] == 1
    assert released_evt.payload["slot"] == "primary"
    assert released_evt.payload["capability"] == "read"
    assert released_evt.payload["token_type"] == "bearer"
    assert released_evt.payload["released_at"] == _T0.isoformat()


# ---------------------------------------------------------------------------
# Expire (CONN-IMPL-018)
# ---------------------------------------------------------------------------


async def test_expire_releases_storage_and_emits_lease_expired() -> None:
    """``expire`` reuses ``release_lease`` (the cap predicate is
    ``released_at IS NULL``) but emits ``lease.expired`` so the audit
    consumer can distinguish voluntary release from involuntary
    expiry. The ``reason`` defaults to the sweeper tag.
    """
    mgr, ls, md = _make_manager(default_ttl_sec=60)
    lease_id = await _issue(mgr)
    md.append_audit_calls.clear()

    expired = await mgr.expire(workspace_id=_WS, lease_id=lease_id)

    assert expired is not None
    assert expired.released_at == _T0
    stored = await ls.get_lease(_WS, lease_id)
    assert stored is not None
    assert stored.released_at == _T0

    assert [ev.event_type for _, ev in md.append_audit_calls] == [EVENT_LEASE_EXPIRED]
    _, expired_evt = md.append_audit_calls[0]
    assert expired_evt.subject["lease_id"] == lease_id
    assert expired_evt.subject["connector_instance_id"] == str(_INSTANCE)
    assert expired_evt.payload["run_id"] == str(_RUN)
    assert expired_evt.payload["step_id"] == str(_STEP)
    assert expired_evt.payload["attempt"] == 1
    assert expired_evt.payload["slot"] == "primary"
    assert expired_evt.payload["capability"] == "read"
    assert expired_evt.payload["token_type"] == "bearer"
    assert expired_evt.payload["expires_at"] == (_T0 + timedelta(seconds=60)).isoformat()
    assert expired_evt.payload["expired_at"] == _T0.isoformat()
    assert expired_evt.payload["reason"] == "ttl-reached"


async def test_expire_accepts_custom_reason_for_sidecar_shutdown() -> None:
    mgr, _, md = _make_manager()
    lease_id = await _issue(mgr)
    md.append_audit_calls.clear()
    await mgr.expire(workspace_id=_WS, lease_id=lease_id, reason="sidecar-shutdown")
    _, evt = md.append_audit_calls[0]
    assert evt.payload["reason"] == "sidecar-shutdown"


async def test_expire_returns_none_when_lease_absent() -> None:
    mgr, _, md = _make_manager()
    result = await mgr.expire(workspace_id=_WS, lease_id="lease_NOPE")
    assert result is None
    # No emission when there is no lease to expire.
    assert md.append_audit_calls == []


# ---------------------------------------------------------------------------
# Revoke (CONN-IMPL-018)
# ---------------------------------------------------------------------------


async def test_revoke_sets_revoke_columns_and_closes_release_slot() -> None:
    """Revoke must set ``revoked_at`` + ``revoke_reason`` *and* close
    ``released_at`` so the cap-check primitive frees the slot.
    """
    mgr, ls, _ = _make_manager(max_concurrent=2)
    lease_id = await _issue(mgr)
    revoked = await mgr.revoke(workspace_id=_WS, lease_id=lease_id, reason="operator-rotation")

    assert revoked is not None
    assert revoked.revoked_at == _T0
    assert revoked.revoke_reason == "operator-rotation"
    assert revoked.released_at == _T0  # cap predicate now frees the slot

    stored = await ls.get_lease(_WS, lease_id)
    assert stored is not None
    assert stored.revoked_at == _T0
    assert stored.revoke_reason == "operator-rotation"
    assert stored.released_at == _T0


async def test_revoke_emits_lease_revoked_with_full_tuple() -> None:
    mgr, _, md = _make_manager()
    lease_id = await _issue(mgr)
    md.append_audit_calls.clear()
    await mgr.revoke(workspace_id=_WS, lease_id=lease_id, reason="suspected-leak")
    assert [ev.event_type for _, ev in md.append_audit_calls] == [EVENT_LEASE_REVOKED]
    _, evt = md.append_audit_calls[0]
    assert evt.subject["lease_id"] == lease_id
    assert evt.subject["connector_instance_id"] == str(_INSTANCE)
    assert evt.payload["run_id"] == str(_RUN)
    assert evt.payload["step_id"] == str(_STEP)
    assert evt.payload["attempt"] == 1
    assert evt.payload["slot"] == "primary"
    assert evt.payload["capability"] == "read"
    assert evt.payload["token_type"] == "bearer"
    assert evt.payload["revoked_at"] == _T0.isoformat()
    assert evt.payload["revoke_reason"] == "suspected-leak"


async def test_revoke_is_idempotent_preserves_first_reason() -> None:
    """A second revoke must not overwrite the original ``revoked_at``
    or ``revoke_reason`` \u2014 forensics depend on the first cause
    being stable.
    """
    mgr, _, _ = _make_manager()
    lease_id = await _issue(mgr)
    first = await mgr.revoke(workspace_id=_WS, lease_id=lease_id, reason="reason-1")
    assert first is not None
    second = await mgr.revoke(workspace_id=_WS, lease_id=lease_id, reason="reason-2")
    assert second is not None
    assert second.revoke_reason == "reason-1"  # unchanged
    assert second.revoked_at == _T0  # unchanged


async def test_revoke_returns_none_when_lease_absent() -> None:
    mgr, _, md = _make_manager()
    result = await mgr.revoke(workspace_id=_WS, lease_id="lease_NOPE", reason="x")
    assert result is None
    assert md.append_audit_calls == []


async def test_revoke_frees_the_cap_slot() -> None:
    """A revoked lease must not count toward the per-step concurrent cap."""
    mgr, _, _ = _make_manager(max_concurrent=1)
    a = await _issue(mgr)
    with pytest.raises(LeaseError) as exc:
        await _issue(mgr)
    assert exc.value.code is LeaseErrorCode.CAPACITY_EXCEEDED
    await mgr.revoke(workspace_id=_WS, lease_id=a, reason="rotate")
    # Cap free again.
    await _issue(mgr)


# ---------------------------------------------------------------------------
# Revoke-requested (CONN-IMPL-018)
# ---------------------------------------------------------------------------


async def test_record_revoke_requested_emits_event_with_selector_and_lease_ids() -> None:
    mgr, _, md = _make_manager()
    a = await _issue(mgr)
    b = await _issue(mgr, attempt=2)
    md.append_audit_calls.clear()
    await mgr.record_revoke_requested(
        workspace_id=_WS,
        selector_type="instance",
        selector_value=str(_INSTANCE),
        lease_ids=[a, b],
        reason="suspected-leak",
        operator="alice@example.com",
    )
    assert [ev.event_type for _, ev in md.append_audit_calls] == [EVENT_LEASE_REVOKE_REQUESTED]
    _, evt = md.append_audit_calls[0]
    assert evt.subject == {"selector_type": "instance", "selector_value": str(_INSTANCE)}
    assert evt.payload == {
        "lease_ids": [a, b],
        "reason": "suspected-leak",
        "operator": "alice@example.com",
    }


# ---------------------------------------------------------------------------
# Denial auto-emission (CONN-IMPL-018)
# ---------------------------------------------------------------------------


async def test_issue_capacity_exceeded_auto_emits_lease_denied_429() -> None:
    mgr, _, md = _make_manager(max_concurrent=1)
    await _issue(mgr)  # uses the only slot
    md.append_audit_calls.clear()
    with pytest.raises(LeaseError):
        await _issue(mgr)
    assert [ev.event_type for _, ev in md.append_audit_calls] == [EVENT_LEASE_DENIED]
    _, evt = md.append_audit_calls[0]
    assert evt.subject["lease_id"] is None  # no lease was minted
    assert evt.subject["connector_instance_id"] == str(_INSTANCE)
    assert evt.payload["op"] == "issue"
    assert evt.payload["reason_code"] == LeaseErrorCode.CAPACITY_EXCEEDED.value
    assert evt.payload["http_status"] == 429


async def test_issue_invalid_attempt_auto_emits_lease_denied_400() -> None:
    mgr, _, md = _make_manager()
    with pytest.raises(LeaseError):
        await _issue(mgr, attempt=0)
    assert [ev.event_type for _, ev in md.append_audit_calls] == [EVENT_LEASE_DENIED]
    _, evt = md.append_audit_calls[0]
    assert evt.payload["op"] == "issue"
    assert evt.payload["reason_code"] == LeaseErrorCode.INVALID_REQUEST.value
    assert evt.payload["http_status"] == 400


async def test_refresh_not_found_auto_emits_lease_denied_404() -> None:
    mgr, _, md = _make_manager()
    with pytest.raises(LeaseError):
        await mgr.refresh(workspace_id=_WS, lease_id="lease_NOPE")
    assert [ev.event_type for _, ev in md.append_audit_calls] == [EVENT_LEASE_DENIED]
    _, evt = md.append_audit_calls[0]
    assert evt.subject["lease_id"] == "lease_NOPE"
    assert evt.subject["connector_instance_id"] is None  # never resolved
    assert evt.payload["op"] == "refresh"
    assert evt.payload["reason_code"] == LeaseErrorCode.NOT_FOUND.value
    assert evt.payload["http_status"] == 404


async def test_refresh_already_released_auto_emits_lease_denied_410() -> None:
    mgr, _, md = _make_manager()
    lease_id = await _issue(mgr)
    await mgr.release(workspace_id=_WS, lease_id=lease_id)
    md.append_audit_calls.clear()
    with pytest.raises(LeaseError):
        await mgr.refresh(workspace_id=_WS, lease_id=lease_id)
    assert [ev.event_type for _, ev in md.append_audit_calls] == [EVENT_LEASE_DENIED]
    _, evt = md.append_audit_calls[0]
    assert evt.subject["lease_id"] == lease_id
    # After get_lease succeeded, we know the instance id.
    assert evt.subject["connector_instance_id"] == str(_INSTANCE)
    assert evt.payload["op"] == "refresh"
    assert evt.payload["reason_code"] == LeaseErrorCode.ALREADY_RELEASED.value
    assert evt.payload["http_status"] == 410


async def test_record_denied_direct_call_for_external_rejection() -> None:
    """Direct ``record_denied`` is used by the future REST handler
    when authorization or capability checks bounce a request before
    it reaches :meth:`issue` / :meth:`refresh` / :meth:`release`.
    """
    mgr, _, md = _make_manager()
    await mgr.record_denied(
        workspace_id=_WS,
        lease_id=None,
        connector_instance_id=str(_INSTANCE),
        op="issue",
        reason_code="AUTHZ_DENIED",
        reason_detail="caller missing leases.issue permission",
        http_status=403,
    )
    assert [ev.event_type for _, ev in md.append_audit_calls] == [EVENT_LEASE_DENIED]
    _, evt = md.append_audit_calls[0]
    assert evt.payload["http_status"] == 403
    assert evt.payload["reason_code"] == "AUTHZ_DENIED"


# ---------------------------------------------------------------------------
# Token redaction property (CONN-IMPL-018 acceptance criterion)
# ---------------------------------------------------------------------------


_REDACTED_SUBSTRINGS = ("token_bytes", "secret", "material", "credential", "password")


async def test_no_lease_event_leaks_token_bytes_across_full_lifecycle() -> None:
    """End-to-end redaction guard: cycle through every lease code path
    and assert no emitted event contains a field name or value that
    smells like raw token material.

    The manager never receives raw token bytes \u2014 the ``token_type``
    field is the only token-adjacent value it knows about, and it is
    a category tag (``"bearer"`` / ``"oauth"`` / ...), not a secret.
    This test pins that contract so a future refactor that smuggles
    secrets into the payload trips immediately.

    ``token_type`` is allowed because it is a category tag; the screen
    looks for the more specific ``token_bytes`` / ``secret`` /
    ``material`` / ``credential`` / ``password`` substrings instead.
    """
    mgr, _, md = _make_manager(default_ttl_sec=60, max_concurrent=1)

    # 1) issue success.
    lease_id = await _issue(mgr)
    # 2) refresh success.
    await mgr.refresh(workspace_id=_WS, lease_id=lease_id, requested_ttl_sec=120)
    # 3) issue denial (cap exhausted).
    with pytest.raises(LeaseError):
        await _issue(mgr)
    # 4) revoke success.
    await mgr.revoke(workspace_id=_WS, lease_id=lease_id, reason="operator-rotation")
    # 5) refresh denial (already released by the revoke).
    with pytest.raises(LeaseError):
        await mgr.refresh(workspace_id=_WS, lease_id=lease_id)
    # 6) release on the already-revoked lease (idempotent at storage).
    await mgr.release(workspace_id=_WS, lease_id=lease_id)
    # 7) issue another, then expire it.
    lease_id2 = await _issue(mgr)
    await mgr.expire(workspace_id=_WS, lease_id=lease_id2, reason="ttl-reached")
    # 8) revoke-requested envelope.
    await mgr.record_revoke_requested(
        workspace_id=_WS,
        selector_type="lease",
        selector_value=lease_id2,
        lease_ids=[lease_id2],
        reason="audit-trail-probe",
        operator="alice@example.com",
    )
    # 9) direct denied for an external rejection.
    await mgr.record_denied(
        workspace_id=_WS,
        lease_id=None,
        connector_instance_id=str(_INSTANCE),
        op="issue",
        reason_code="AUTHZ_DENIED",
        reason_detail="caller missing leases.issue permission",
        http_status=403,
    )

    # All seven event types must have fired at least once.
    seen_types = {ev.event_type for _, ev in md.append_audit_calls}
    expected = {
        EVENT_LEASE_ISSUED,
        EVENT_LEASE_REFRESHED,
        EVENT_LEASE_DENIED,
        EVENT_LEASE_REVOKED,
        EVENT_LEASE_RELEASED,
        EVENT_LEASE_EXPIRED,
        EVENT_LEASE_REVOKE_REQUESTED,
    }
    assert expected.issubset(seen_types)

    # Screen every emitted subject + payload for token-bytes substrings.
    for _, evt in md.append_audit_calls:
        serialized = json.dumps(
            {"subject": evt.subject, "payload": evt.payload},
            default=str,
        ).lower()
        for needle in _REDACTED_SUBSTRINGS:
            assert needle not in serialized, (
                f"event {evt.event_type} leaked a suspected secret "
                f"substring {needle!r}: {serialized}"
            )


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
