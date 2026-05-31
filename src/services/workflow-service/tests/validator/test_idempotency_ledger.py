"""Tests for the in-memory idempotency ledger (WF-IMPL-063).

Pins:

* Same ``(workspace_id, idempotency_key, fingerprint)`` →
  ``replayed=True`` on the second call.
* Different fingerprint within TTL → :class:`IdempotencyConflictError`.
* Entry whose age strictly exceeds the TTL is purged on the next
  ``record_or_replay`` call (lazy GC).
* Constructor rejects non-positive ``ttl``.
* ``record_or_replay`` rejects empty ``workspace_id`` /
  ``idempotency_key`` / ``request_fingerprint``.
* :func:`compute_request_fingerprint` is deterministic under key
  reordering and unaffected by whitespace differences in the input
  values (Hypothesis 200 examples).
* ``WF_IDEMPOTENCY_KEY_TTL`` env-var parser accepts the design.md
  default ``PT24H``, hours / minutes / seconds combos, the weeks
  form, and rejects months / years / negatives / nonsense.
"""

from __future__ import annotations

import asyncio
import re
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from custos_workflow.validator.errors import IdempotencyConflictError
from custos_workflow.validator.idempotency_ledger import (
    DEFAULT_IDEMPOTENCY_KEY_TTL,
    IDEMPOTENCY_TTL_ENV_VAR,
    IdempotencyLedger,
    InMemoryIdempotencyLedger,
    LedgerEntry,
    compute_request_fingerprint,
    idempotency_ttl_from_env,
)

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------


def test_default_ttl_is_pt24h() -> None:
    """design.md § Configuration pins the default at PT24H (24 hours)."""
    assert timedelta(hours=24) == DEFAULT_IDEMPOTENCY_KEY_TTL


def test_env_var_name_is_wf_idempotency_key_ttl() -> None:
    """README + design.md publish this env-var name; do not rename it."""
    assert IDEMPOTENCY_TTL_ENV_VAR == "WF_IDEMPOTENCY_KEY_TTL"


def test_iso8601_duration_pattern_byte_equal_to_runs_wait() -> None:
    """The ledger's ISO-8601 grammar must stay in lockstep with
    :data:`custos_workflow.runs.wait._ISO8601_DURATION_PATTERN`.

    The header comment in
    :mod:`custos_workflow.validator.idempotency_ledger` promises this
    parity; pin it with byte-equality on the pattern source so a
    silent drift in either module fails CI rather than diverging at
    runtime.
    """
    from custos_workflow.runs.wait import (
        _ISO8601_DURATION_PATTERN as _WAIT_PATTERN,
    )
    from custos_workflow.validator.idempotency_ledger import (
        _ISO8601_DURATION_PATTERN as _LEDGER_PATTERN,
    )

    assert _LEDGER_PATTERN.pattern == _WAIT_PATTERN.pattern
    assert _LEDGER_PATTERN.flags == _WAIT_PATTERN.flags


# ---------------------------------------------------------------------------
# request_fingerprint
# ---------------------------------------------------------------------------


def test_fingerprint_is_deterministic_under_key_reordering() -> None:
    """Canonical-JSON sort_keys collapses key order to the same digest."""
    a = compute_request_fingerprint("wfv-1", {"b": 2, "a": 1})
    b = compute_request_fingerprint("wfv-1", {"a": 1, "b": 2})
    assert a == b


def test_fingerprint_changes_with_workflow_version_id() -> None:
    """Two version IDs with identical inputs must hash to distinct digests."""
    a = compute_request_fingerprint("wfv-1", {"a": 1})
    b = compute_request_fingerprint("wfv-2", {"a": 1})
    assert a != b


def test_fingerprint_changes_with_inputs() -> None:
    """Two payloads with identical version IDs must hash to distinct digests."""
    a = compute_request_fingerprint("wfv-1", {"a": 1})
    b = compute_request_fingerprint("wfv-1", {"a": 2})
    assert a != b


def test_fingerprint_treats_none_inputs_as_empty_mapping() -> None:
    """``None`` inputs match ``{}`` so callers can pass either."""
    assert compute_request_fingerprint("wfv-1", None) == compute_request_fingerprint("wfv-1", {})


def test_fingerprint_is_lowercase_hex_64_chars() -> None:
    """SHA-256 hex digest shape, important for audit log expectations."""
    digest = compute_request_fingerprint("wfv-1", {"a": 1})
    assert len(digest) == 64
    assert re.match(r"^[0-9a-f]{64}$", digest)


_JSON_PRIMITIVES = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-(2**31), max_value=2**31 - 1),
    st.floats(allow_nan=False, allow_infinity=False, width=32),
    st.text(min_size=0, max_size=16),
)


@st.composite
def _json_objects(draw: st.DrawFn) -> dict[str, Any]:
    """Small ``str -> JSON-primitive`` dicts for fingerprint tests."""
    keys = draw(st.lists(st.text(min_size=1, max_size=6), min_size=0, max_size=6, unique=True))
    return {k: draw(_JSON_PRIMITIVES) for k in keys}


@settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(workflow_version_id=st.text(min_size=1, max_size=8), inputs=_json_objects())
def test_fingerprint_is_stable_under_key_permutation_property(
    workflow_version_id: str, inputs: dict[str, Any]
) -> None:
    """Hypothesis: any permutation of the same dict hashes identically."""
    permuted = dict(reversed(list(inputs.items())))
    assert compute_request_fingerprint(workflow_version_id, inputs) == compute_request_fingerprint(
        workflow_version_id, permuted
    )


# ---------------------------------------------------------------------------
# InMemoryIdempotencyLedger — protocol conformance
# ---------------------------------------------------------------------------


def test_in_memory_ledger_satisfies_protocol() -> None:
    """The runtime-checkable Protocol matches our adapter."""
    assert isinstance(InMemoryIdempotencyLedger(), IdempotencyLedger)


def test_in_memory_ledger_default_ttl() -> None:
    """Constructor default mirrors ``DEFAULT_IDEMPOTENCY_KEY_TTL``."""
    assert InMemoryIdempotencyLedger().ttl == DEFAULT_IDEMPOTENCY_KEY_TTL


def test_in_memory_ledger_rejects_non_positive_ttl() -> None:
    """A zero or negative TTL would dedup forever; reject loudly."""
    with pytest.raises(ValueError, match="positive"):
        InMemoryIdempotencyLedger(ttl=timedelta(0))
    with pytest.raises(ValueError, match="positive"):
        InMemoryIdempotencyLedger(ttl=timedelta(seconds=-1))


# ---------------------------------------------------------------------------
# InMemoryIdempotencyLedger — record_or_replay semantics
# ---------------------------------------------------------------------------


class _Clock:
    """Deterministic monotonic clock used by the ledger tests."""

    def __init__(self, start: datetime) -> None:
        self.value: datetime = start

    def __call__(self) -> datetime:
        return self.value

    def advance(self, delta: timedelta) -> None:
        self.value = self.value + delta


async def test_first_call_minted_second_call_replayed() -> None:
    """Two calls with the same key + fingerprint → replayed=True the second time."""
    clock = _Clock(datetime(2026, 5, 31, 12, 0, 0, tzinfo=UTC))
    ledger = InMemoryIdempotencyLedger(ttl=timedelta(hours=1), now=clock)
    fp = compute_request_fingerprint("wfv-1", {"a": 1})

    first = await ledger.record_or_replay(
        workspace_id="ws-1", idempotency_key="k-1", request_fingerprint=fp
    )
    assert first.replayed is False
    assert first.workspace_id == "ws-1"
    assert first.idempotency_key == "k-1"
    assert first.request_fingerprint == fp
    assert first.recorded_at == clock.value

    second = await ledger.record_or_replay(
        workspace_id="ws-1", idempotency_key="k-1", request_fingerprint=fp
    )
    assert second.replayed is True
    assert second.recorded_at == first.recorded_at  # original anchor preserved


async def test_different_fingerprint_same_key_raises_conflict() -> None:
    """Two distinct payloads under the same key inside TTL → conflict."""
    ledger = InMemoryIdempotencyLedger(ttl=timedelta(hours=1))
    fp_a = compute_request_fingerprint("wfv-1", {"a": 1})
    fp_b = compute_request_fingerprint("wfv-1", {"a": 2})
    await ledger.record_or_replay(
        workspace_id="ws-1", idempotency_key="k-1", request_fingerprint=fp_a
    )
    with pytest.raises(IdempotencyConflictError) as info:
        await ledger.record_or_replay(
            workspace_id="ws-1", idempotency_key="k-1", request_fingerprint=fp_b
        )
    assert info.value.workspace_id == "ws-1"
    assert info.value.idempotency_key == "k-1"


async def test_distinct_workspaces_do_not_collide() -> None:
    """``(workspaceId, key)`` is the dedup key; workspaces are isolated."""
    ledger = InMemoryIdempotencyLedger(ttl=timedelta(hours=1))
    fp = compute_request_fingerprint("wfv-1", {"a": 1})
    a = await ledger.record_or_replay(
        workspace_id="ws-A", idempotency_key="k", request_fingerprint=fp
    )
    b = await ledger.record_or_replay(
        workspace_id="ws-B", idempotency_key="k", request_fingerprint=fp
    )
    assert a.replayed is False
    assert b.replayed is False


async def test_entry_evicted_strictly_after_ttl() -> None:
    """Once age > TTL the entry is purged on the next ``record_or_replay``."""
    clock = _Clock(datetime(2026, 5, 31, 0, 0, 0, tzinfo=UTC))
    ledger = InMemoryIdempotencyLedger(ttl=timedelta(hours=1), now=clock)
    fp = compute_request_fingerprint("wfv-1", {"a": 1})
    first = await ledger.record_or_replay(
        workspace_id="ws-1", idempotency_key="k-1", request_fingerprint=fp
    )
    assert first.replayed is False

    # Advance past TTL — the next call observes a purged entry and
    # mints a fresh one. A divergent fingerprint must then succeed
    # because the original window has elapsed.
    clock.advance(timedelta(hours=1, seconds=1))
    fp_b = compute_request_fingerprint("wfv-1", {"a": 2})
    new = await ledger.record_or_replay(
        workspace_id="ws-1", idempotency_key="k-1", request_fingerprint=fp_b
    )
    assert new.replayed is False
    assert new.request_fingerprint == fp_b
    assert len(ledger._snapshot()) == 1


async def test_entry_at_exact_ttl_boundary_is_purged() -> None:
    """``recorded_at <= now - ttl`` is the inclusive cutoff (entry evicted)."""
    clock = _Clock(datetime(2026, 5, 31, 0, 0, 0, tzinfo=UTC))
    ledger = InMemoryIdempotencyLedger(ttl=timedelta(hours=1), now=clock)
    fp = compute_request_fingerprint("wfv-1", {"a": 1})
    await ledger.record_or_replay(
        workspace_id="ws-1", idempotency_key="k-1", request_fingerprint=fp
    )
    clock.advance(timedelta(hours=1))  # exactly TTL
    new = await ledger.record_or_replay(
        workspace_id="ws-2", idempotency_key="k-other", request_fingerprint=fp
    )
    # The boundary call triggers _purge_expired; the original row
    # must be gone now.
    snap = ledger._snapshot()
    assert ("ws-1", "k-1") not in snap
    assert ("ws-2", "k-other") in snap
    assert new.replayed is False


async def test_record_or_replay_rejects_empty_inputs() -> None:
    """Empty identifiers would corrupt the dedup key; reject loudly."""
    ledger = InMemoryIdempotencyLedger()
    fp = "0" * 64
    with pytest.raises(ValueError, match="workspace_id"):
        await ledger.record_or_replay(workspace_id="", idempotency_key="k", request_fingerprint=fp)
    with pytest.raises(ValueError, match="idempotency_key"):
        await ledger.record_or_replay(
            workspace_id="ws-1", idempotency_key="", request_fingerprint=fp
        )
    with pytest.raises(ValueError, match="request_fingerprint"):
        await ledger.record_or_replay(
            workspace_id="ws-1", idempotency_key="k", request_fingerprint=""
        )


async def test_record_or_replay_is_serialised_under_lock() -> None:
    """Concurrent callers see a consistent record-or-replay decision."""
    ledger = InMemoryIdempotencyLedger(ttl=timedelta(hours=1))
    fp = compute_request_fingerprint("wfv-1", {"a": 1})
    coros = [
        ledger.record_or_replay(workspace_id="ws-1", idempotency_key="k", request_fingerprint=fp)
        for _ in range(50)
    ]
    results: list[LedgerEntry] = await asyncio.gather(*coros)
    # Exactly one mint, the rest replay.
    mints = [r for r in results if not r.replayed]
    replays = [r for r in results if r.replayed]
    assert len(mints) == 1
    assert len(replays) == 49


# ---------------------------------------------------------------------------
# idempotency_ttl_from_env
# ---------------------------------------------------------------------------


def test_ttl_from_env_unset_returns_default() -> None:
    """No env var → default."""
    assert idempotency_ttl_from_env(environ={}) == DEFAULT_IDEMPOTENCY_KEY_TTL


def test_ttl_from_env_blank_returns_default() -> None:
    """Whitespace / empty string → default (defensive against shell wrappers)."""
    assert (
        idempotency_ttl_from_env(environ={IDEMPOTENCY_TTL_ENV_VAR: "   "})
        == DEFAULT_IDEMPOTENCY_KEY_TTL
    )


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("PT24H", timedelta(hours=24)),
        ("PT1H30M", timedelta(hours=1, minutes=30)),
        ("PT45S", timedelta(seconds=45)),
        ("P1W", timedelta(weeks=1)),
        ("P2D", timedelta(days=2)),
        ("P1DT2H3M4S", timedelta(days=1, hours=2, minutes=3, seconds=4)),
    ],
)
def test_ttl_from_env_parses_iso8601_durations(raw: str, expected: timedelta) -> None:
    """Round-trip a representative sample of design.md grammar."""
    assert idempotency_ttl_from_env(environ={IDEMPOTENCY_TTL_ENV_VAR: raw}) == expected


@pytest.mark.parametrize(
    "raw",
    [
        "24h",  # not ISO-8601
        "P1M",  # months not supported
        "P1Y",  # years not supported
        "PT0H",  # non-positive
        "P",  # no components
        "PT",  # no components
        "PT-1H",  # negative
        "garbage",  # nonsense
    ],
)
def test_ttl_from_env_rejects_invalid(raw: str) -> None:
    """Anything outside the design.md grammar fails loudly at startup."""
    with pytest.raises(ValueError, match="WF_IDEMPOTENCY_KEY_TTL"):
        idempotency_ttl_from_env(environ={IDEMPOTENCY_TTL_ENV_VAR: raw})
