"""Tests for the Clock protocol and its FixedClock / DaprWorkflowClock adapters."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest

from custos_cel import Clock, DaprWorkflowClock, FixedClock

# ---------------------------------------------------------------------------
# Clock protocol
# ---------------------------------------------------------------------------


def test_fixed_clock_satisfies_clock_protocol() -> None:
    clock = FixedClock(datetime(2026, 1, 1, tzinfo=UTC))
    assert isinstance(clock, Clock)


def test_dapr_workflow_clock_satisfies_clock_protocol() -> None:
    class _Ctx:
        current_utc_datetime = datetime(2026, 1, 1, tzinfo=UTC)

    clock = DaprWorkflowClock(_Ctx())
    assert isinstance(clock, Clock)


def test_arbitrary_callable_does_not_satisfy_protocol() -> None:
    assert not isinstance(lambda: datetime.now(tz=UTC), Clock)


# ---------------------------------------------------------------------------
# FixedClock
# ---------------------------------------------------------------------------


def test_fixed_clock_returns_constructor_value() -> None:
    dt = datetime(2026, 5, 22, 14, 30, 0, tzinfo=UTC)
    assert FixedClock(dt).now() == dt


def test_fixed_clock_is_byte_deterministic_across_calls() -> None:
    clock = FixedClock(datetime(2026, 5, 22, tzinfo=UTC))
    samples = [clock.now() for _ in range(100)]
    assert all(s == samples[0] for s in samples)
    # And the exact same object reference, since the dataclass is frozen.
    assert all(s is samples[0] for s in samples)


def test_fixed_clock_rejects_naive_datetime() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        FixedClock(datetime(2026, 5, 22, 12, 0, 0))


def test_fixed_clock_rejects_non_datetime() -> None:
    with pytest.raises(TypeError, match="datetime"):
        FixedClock("2026-05-22T12:00:00Z")  # type: ignore[arg-type]


def test_fixed_clock_accepts_non_utc_tzinfo() -> None:
    # Tz-aware is the rule; UTC is the *convention*. Accept any
    # tz-aware datetime so callers passing offset-aware timestamps
    # (e.g. from upstream JSON parsing) aren't artificially rejected.
    eastern = timezone(timedelta(hours=-5))
    dt = datetime(2026, 5, 22, 9, 30, 0, tzinfo=eastern)
    assert FixedClock(dt).now() == dt


def test_fixed_clock_equality_is_value_typed() -> None:
    dt = datetime(2026, 5, 22, tzinfo=UTC)
    assert FixedClock(dt) == FixedClock(dt)


# ---------------------------------------------------------------------------
# DaprWorkflowClock
# ---------------------------------------------------------------------------


class _DaprCtx:
    """Minimal duck-type stand-in for a Dapr workflow context."""

    def __init__(self, value: Any) -> None:
        self.current_utc_datetime = value


def test_dapr_workflow_clock_returns_context_value() -> None:
    dt = datetime(2026, 5, 22, 12, 0, 0, tzinfo=UTC)
    clock = DaprWorkflowClock(_DaprCtx(dt))
    assert clock.now() == dt


def test_dapr_workflow_clock_attaches_utc_to_naive_value() -> None:
    # Some test doubles surface naive datetimes — the adapter attaches
    # UTC explicitly so downstream comparisons stay tz-aware. This is
    # cosmetic safety; production Dapr ctx values are always tz-aware.
    naive = datetime(2026, 5, 22, 12, 0, 0)
    clock = DaprWorkflowClock(_DaprCtx(naive))
    result = clock.now()
    assert result.tzinfo is UTC
    assert result.replace(tzinfo=None) == naive


def test_dapr_workflow_clock_rejects_ctx_without_attribute() -> None:
    class _Bad:
        pass

    with pytest.raises(TypeError, match="current_utc_datetime"):
        DaprWorkflowClock(_Bad())


def test_dapr_workflow_clock_rejects_non_datetime_value() -> None:
    clock = DaprWorkflowClock(_DaprCtx("not-a-datetime"))
    with pytest.raises(TypeError, match="datetime"):
        clock.now()


def test_dapr_workflow_clock_observes_ctx_updates() -> None:
    # The adapter reads the context attribute on every call (Dapr
    # mutates ``current_utc_datetime`` as the workflow progresses
    # through replays). Caching the value would be a bug.
    ctx = _DaprCtx(datetime(2026, 1, 1, tzinfo=UTC))
    clock = DaprWorkflowClock(ctx)
    assert clock.now() == datetime(2026, 1, 1, tzinfo=UTC)
    ctx.current_utc_datetime = datetime(2026, 6, 1, tzinfo=UTC)
    assert clock.now() == datetime(2026, 6, 1, tzinfo=UTC)
