"""Tests for :mod:`custos_auth.callctx_keyring` (AS-IMPL-018)."""

from __future__ import annotations

import asyncio
import base64
from collections.abc import Awaitable
from datetime import UTC, datetime, timedelta

import pytest

from custos_auth.callctx_keyring import (
    DEFAULT_ROTATION_PERIOD_SECONDS,
    JWKS_CACHE_FRACTION,
    OVERLAP_FACTOR,
    JwksEntry,
    KeyRing,
    install_key_age_metric,
    run_rotation_loop,
)
from custos_auth.callctx_signer import (
    SigningKey,
    StaticSigningKeyResolver,
)


def _fixed_key(*, created_at: datetime | None = None) -> SigningKey:
    """Generate a deterministic-creation-time SigningKey."""
    return SigningKey.generate(
        created_at=created_at if created_at is not None else datetime.now(UTC),
    )


# ---------------------------------------------------------------------------
# JwksEntry.to_jwk
# ---------------------------------------------------------------------------


def test_to_jwk_emits_rfc_8037_okp_shape() -> None:
    key = _fixed_key()
    entry = JwksEntry(
        kid=key.kid,
        public_key=key.public_key,
        created_at=key.created_at,
    )
    jwk = entry.to_jwk()
    assert jwk["kty"] == "OKP"
    assert jwk["crv"] == "Ed25519"
    assert jwk["alg"] == "EdDSA"
    assert jwk["use"] == "sig"
    assert jwk["kid"] == key.kid
    # Base64url-no-pad: no '=' suffix, only urlsafe alphabet.
    assert "=" not in jwk["x"]
    assert "+" not in jwk["x"]
    assert "/" not in jwk["x"]
    # Round-trip the raw bytes — must match the 32-byte Ed25519 pubkey.
    padding = "=" * (-len(jwk["x"]) % 4)
    decoded = base64.urlsafe_b64decode(jwk["x"] + padding)
    assert len(decoded) == 32
    assert decoded == entry.public_raw_bytes()


# ---------------------------------------------------------------------------
# KeyRing basics
# ---------------------------------------------------------------------------


def test_keyring_rejects_non_positive_rotation_period() -> None:
    with pytest.raises(ValueError, match="positive integer"):
        KeyRing(_fixed_key(), rotation_period_seconds=0)
    with pytest.raises(ValueError, match="positive integer"):
        KeyRing(_fixed_key(), rotation_period_seconds=-1)


def test_keyring_overlap_window_matches_design() -> None:
    ring = KeyRing(_fixed_key(), rotation_period_seconds=120)
    assert ring.rotation_period_seconds == 120
    assert ring.overlap_window_seconds == OVERLAP_FACTOR * 120


def test_keyring_initial_state_has_no_retired_entries() -> None:
    key = _fixed_key()
    ring = KeyRing(key)
    assert ring.active is key
    assert ring.retired_entries() == []
    entries = ring.all_public_entries()
    assert len(entries) == 1
    assert entries[0].kid == key.kid


def test_keyring_current_key_age_seconds_uses_injected_clock() -> None:
    created = datetime(2026, 5, 24, 12, 0, 0, tzinfo=UTC)
    key = _fixed_key(created_at=created)
    later = created + timedelta(seconds=42)
    ring = KeyRing(key, clock=lambda: later.timestamp())
    assert ring.current_key_age_seconds() == pytest.approx(42.0)


def test_keyring_current_key_age_is_clamped_at_zero() -> None:
    created = datetime(2026, 5, 24, 12, 0, 0, tzinfo=UTC)
    key = _fixed_key(created_at=created)
    # Clock returning a moment *before* creation should not produce
    # a negative age — useful when wall clocks skew slightly.
    earlier = created - timedelta(seconds=5)
    ring = KeyRing(key, clock=lambda: earlier.timestamp())
    assert ring.current_key_age_seconds() == 0.0


# ---------------------------------------------------------------------------
# KeyRing.rotate
# ---------------------------------------------------------------------------


def test_rotate_promotes_new_key_and_retires_previous() -> None:
    first = _fixed_key()
    ring = KeyRing(first, rotation_period_seconds=60)
    second = _fixed_key()
    ring.rotate(second, now=datetime(2026, 5, 24, 12, 0, 0, tzinfo=UTC))
    assert ring.active is second
    retired = ring.retired_entries()
    assert len(retired) == 1
    assert retired[0].kid == first.kid
    assert retired[0].retired_at == datetime(2026, 5, 24, 12, 0, 0, tzinfo=UTC)


def test_rotate_rejects_same_kid_swap() -> None:
    key = _fixed_key()
    ring = KeyRing(key)
    with pytest.raises(ValueError, match="currently active"):
        ring.rotate(key)


def test_rotate_prunes_entries_past_overlap_window() -> None:
    rotation = 10
    initial = _fixed_key()
    ring = KeyRing(initial, rotation_period_seconds=rotation)
    now = datetime(2026, 5, 24, 12, 0, 0, tzinfo=UTC)
    # Rotate three times. The first retirement (initial -> retired at
    # ``now``) is pushed past the 2x rotation overlap window when the
    # third rotation happens at ``now + 3*rotation``.
    second = _fixed_key()
    ring.rotate(second, now=now)
    third = _fixed_key()
    ring.rotate(third, now=now + timedelta(seconds=rotation))
    # Jump forward enough that the very first retired entry exceeds the
    # overlap window. Cutoff after the third rotation is
    # ``(now+3*rotation) - 2*rotation = now+rotation``, so any entry
    # retired strictly before ``now+rotation`` is dropped.
    fourth = _fixed_key()
    ring.rotate(fourth, now=now + timedelta(seconds=rotation * 3))
    retired_kids = [e.kid for e in ring.retired_entries()]
    # ``initial`` was retired at ``now`` — past the cutoff — and is gone.
    # ``second`` (retired at now+rotation) is exactly on the cutoff and
    # remains. ``third`` (retired at now+3*rotation) remains.
    assert initial.kid not in retired_kids
    assert second.kid in retired_kids
    assert third.kid in retired_kids


def test_all_public_entries_lists_active_first_then_retired_newest_first() -> None:
    rotation = 60
    ring = KeyRing(_fixed_key(), rotation_period_seconds=rotation)
    now = datetime(2026, 5, 24, 12, 0, 0, tzinfo=UTC)
    second = _fixed_key()
    ring.rotate(second, now=now)
    third = _fixed_key()
    ring.rotate(third, now=now + timedelta(seconds=10))
    entries = ring.all_public_entries()
    assert entries[0].kid == third.kid  # active
    # Newest retired first.
    assert entries[1].kid == second.kid
    # Plus the original first key.
    assert len(entries) == 3


# ---------------------------------------------------------------------------
# install_key_age_metric
# ---------------------------------------------------------------------------


def test_install_key_age_metric_is_callable() -> None:
    ring = KeyRing(_fixed_key())
    # The OTel meter de-dupes by name, so calling twice is safe.
    install_key_age_metric(ring)
    install_key_age_metric(ring)


# ---------------------------------------------------------------------------
# run_rotation_loop
# ---------------------------------------------------------------------------


async def test_run_rotation_loop_zero_disables_loop() -> None:
    ring = KeyRing(_fixed_key())
    resolver = StaticSigningKeyResolver(key=ring.active)
    # Returns immediately when rotation_period_seconds == 0.
    await run_rotation_loop(
        key_ring=ring,
        resolver=resolver,
        rotation_period_seconds=0,
    )
    assert ring.active.kid == resolver.key.kid


async def test_run_rotation_loop_rejects_negative_period() -> None:
    ring = KeyRing(_fixed_key())
    resolver = StaticSigningKeyResolver(key=ring.active)
    with pytest.raises(ValueError, match="non-negative"):
        await run_rotation_loop(
            key_ring=ring,
            resolver=resolver,
            rotation_period_seconds=-1,
        )


async def test_run_rotation_loop_rotates_on_each_iteration() -> None:
    initial = _fixed_key()
    ring = KeyRing(initial, rotation_period_seconds=60)
    resolver = StaticSigningKeyResolver(key=initial)
    iterations = 0

    async def fake_sleep(_delay: float) -> None:
        nonlocal iterations
        iterations += 1
        if iterations >= 2:
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await run_rotation_loop(
            key_ring=ring,
            resolver=resolver,
            rotation_period_seconds=60,
            jitter_ratio=0.0,
            sleep=fake_sleep,
        )
    # After 1 successful iteration + 1 cancellation, the active key
    # was rotated exactly once and the resolver was updated.
    assert ring.active.kid != initial.kid
    assert resolver.key.kid == ring.active.kid
    assert any(entry.kid == initial.kid for entry in ring.retired_entries())


async def test_run_rotation_loop_propagates_cancellation_during_sleep() -> None:
    ring = KeyRing(_fixed_key())
    resolver = StaticSigningKeyResolver(key=ring.active)

    async def cancel_immediately(_delay: float) -> None:
        raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await run_rotation_loop(
            key_ring=ring,
            resolver=resolver,
            rotation_period_seconds=60,
            sleep=cancel_immediately,
        )


# ---------------------------------------------------------------------------
# AS-IMPL-018 acceptance criterion: a token minted just before rotation
# remains verifiable via the retired-key JWKS entry while it sits inside
# the overlap window.
# ---------------------------------------------------------------------------


def test_retired_key_remains_publishable_inside_overlap_window() -> None:
    rotation = 300
    initial = _fixed_key(created_at=datetime(2026, 5, 24, 0, 0, 0, tzinfo=UTC))
    ring = KeyRing(initial, rotation_period_seconds=rotation)
    new_key = _fixed_key(created_at=datetime(2026, 5, 24, 0, 5, 0, tzinfo=UTC))
    rotation_instant = datetime(2026, 5, 24, 0, 5, 0, tzinfo=UTC)
    ring.rotate(new_key, now=rotation_instant)
    kids_published = {entry.kid for entry in ring.all_public_entries()}
    # Both keys must appear in the JWKS body so an in-flight JWT
    # minted under ``initial`` still verifies after rotation.
    assert initial.kid in kids_published
    assert new_key.kid in kids_published


# ---------------------------------------------------------------------------
# JWKS cache fraction sanity
# ---------------------------------------------------------------------------


def test_jwks_cache_fraction_default_is_half() -> None:
    assert JWKS_CACHE_FRACTION == 0.5


def test_default_rotation_period_is_seven_days() -> None:
    assert DEFAULT_ROTATION_PERIOD_SECONDS == 7 * 24 * 60 * 60


# Awaitable typing sanity check — keeps unused import happy if any IDE
# folds the import block.
_typing_check: Awaitable[None] | None = None
