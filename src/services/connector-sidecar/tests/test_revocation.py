"""Tests for :class:`custos_sidecar.revocation.RevocationRegistry`."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime

from custos_sidecar.revocation import (
    RevocationRecord,
    RevocationRegistry,
    RevokeMarkStatus,
)


def test_new_registry_is_empty() -> None:
    reg = RevocationRegistry()
    assert len(reg) == 0
    assert not reg.is_revoked("lease_anything")
    assert reg.reason_for("lease_anything") is None
    assert reg.record("lease_anything") is None
    assert "lease_anything" not in reg


async def test_mark_revoked_returns_revoked_on_first_call() -> None:
    reg = RevocationRegistry()
    status = await reg.mark_revoked("lease_001", "rotate")
    assert status is RevokeMarkStatus.REVOKED
    assert reg.is_revoked("lease_001")
    assert reg.reason_for("lease_001") == "rotate"
    assert "lease_001" in reg
    assert len(reg) == 1


async def test_mark_revoked_returns_already_revoked_on_repeat() -> None:
    reg = RevocationRegistry()
    assert await reg.mark_revoked("lease_001", "first") is RevokeMarkStatus.REVOKED
    assert await reg.mark_revoked("lease_001", "second") is RevokeMarkStatus.ALREADY_REVOKED
    # First reason preserved.
    assert reg.reason_for("lease_001") == "first"


async def test_mark_revoked_stores_supplied_timestamp() -> None:
    reg = RevocationRegistry()
    stamp = datetime(2026, 5, 27, 12, 0, 0, tzinfo=UTC)
    await reg.mark_revoked("lease_001", "rotate", now=stamp)
    record = reg.record("lease_001")
    assert isinstance(record, RevocationRecord)
    assert record.revoked_at == stamp


async def test_mark_revoked_uses_wallclock_when_now_omitted() -> None:
    reg = RevocationRegistry()
    before = datetime.now(UTC)
    await reg.mark_revoked("lease_001", "rotate")
    after = datetime.now(UTC)
    record = reg.record("lease_001")
    assert record is not None
    assert before <= record.revoked_at <= after


async def test_concurrent_mark_revoked_first_writer_wins() -> None:
    """Two coroutines racing on the same lease id: exactly one wins.

    The other observes :attr:`RevokeMarkStatus.ALREADY_REVOKED`. The
    stored reason matches the winner.
    """
    reg = RevocationRegistry()

    async def race(reason: str) -> RevokeMarkStatus:
        return await reg.mark_revoked("lease_race", reason)

    statuses = await asyncio.gather(race("a"), race("b"))
    # Exactly one REVOKED, one ALREADY_REVOKED — order undefined.
    assert sorted(s.value for s in statuses) == [
        RevokeMarkStatus.ALREADY_REVOKED.value,
        RevokeMarkStatus.REVOKED.value,
    ]
    assert reg.reason_for("lease_race") in {"a", "b"}


async def test_independent_lease_ids_do_not_collide() -> None:
    reg = RevocationRegistry()
    await reg.mark_revoked("lease_A", "a")
    await reg.mark_revoked("lease_B", "b")
    assert reg.is_revoked("lease_A")
    assert reg.is_revoked("lease_B")
    assert reg.reason_for("lease_A") == "a"
    assert reg.reason_for("lease_B") == "b"
    assert len(reg) == 2


def test_contains_rejects_non_string_keys() -> None:
    reg = RevocationRegistry()
    assert 42 not in reg
    assert None not in reg
