"""Replay-deterministic clock interface for the Custos CEL evaluator.

The Workflow Service's expression evaluator must observe wall-clock time
**only** through this :class:`Clock` protocol. The Dapr Workflow runtime
guarantees that a workflow's ``current_utc_datetime`` is replayed
identically across re-executions of the same instance, which is what
makes ``now()`` safe to use inside an orchestration. Any other clock
source (``time.time()``, ``datetime.datetime.utcnow()``, OS facilities)
would introduce non-determinism and is forbidden by the sandbox.

This module ships two adapters:

* :class:`DaprWorkflowClock` — production adapter. Wraps a Dapr workflow
  context (anything exposing a ``current_utc_datetime`` datetime
  attribute). The ``dapr`` package itself is **not** imported here — the
  adapter is structurally typed so the ``custos-cel`` library remains a
  pure-Python dependency.
* :class:`FixedClock` — test adapter. Returns a constructor-supplied
  ``datetime`` on every call. Two evaluations against the same
  ``FixedClock`` produce byte-equal output, which is what the
  determinism acceptance criterion requires.

See the issue: https://github.com/toddysm/custos/issues/181
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Protocol, runtime_checkable

__all__ = ["Clock", "DaprWorkflowClock", "FixedClock"]


@runtime_checkable
class Clock(Protocol):
    """The clock the evaluator calls for ``now()``.

    Implementations must return a timezone-aware :class:`datetime`
    (UTC by convention). The protocol is :func:`runtime_checkable` so
    callers can write defensive ``isinstance(x, Clock)`` guards in
    scope construction.
    """

    def now(self) -> datetime:  # pragma: no cover - protocol
        ...


@dataclass(frozen=True, slots=True)
class FixedClock:
    """Deterministic clock for tests.

    Returns the same constructor-supplied :class:`datetime` on every
    :meth:`now` call. The datetime must be timezone-aware (UTC by
    convention) so callers cannot accidentally rely on the host's local
    timezone.
    """

    fixed: datetime

    def __post_init__(self) -> None:
        if not isinstance(self.fixed, datetime):
            raise TypeError(
                f"FixedClock requires a datetime, got {type(self.fixed).__name__}",
            )
        if self.fixed.tzinfo is None:
            raise ValueError(
                "FixedClock requires a timezone-aware datetime; got a naive datetime. "
                "Use datetime(..., tzinfo=UTC) or .replace(tzinfo=UTC)."
            )

    def now(self) -> datetime:
        return self.fixed


class DaprWorkflowClock:
    """Replay-deterministic clock backed by a Dapr workflow context.

    Wraps any object that exposes a ``current_utc_datetime`` attribute
    yielding a :class:`datetime`. In production, that object is the
    Dapr Workflow runtime's per-instance context; the
    ``current_utc_datetime`` value is replay-stable, so the same
    workflow instance observes the same ``now()`` across re-executions.

    The Dapr SDK is **not** imported here — this adapter is
    structurally typed, which keeps ``custos-cel`` free of a runtime
    dependency on ``dapr`` and lets the Workflow Service inject any
    duck-typed replacement (most importantly, a no-op fake in unit
    tests of the Step Coordinator).

    The constructor performs a single attribute existence check so a
    mis-wired context fails loudly at scope-construction time rather
    than at the first ``now()`` evaluation.
    """

    __slots__ = ("_ctx",)

    def __init__(self, ctx: Any) -> None:
        if not hasattr(ctx, "current_utc_datetime"):
            raise TypeError(
                "DaprWorkflowClock requires an object exposing "
                "'current_utc_datetime'; got "
                f"{type(ctx).__name__} with no such attribute",
            )
        self._ctx = ctx

    def now(self) -> datetime:
        value = self._ctx.current_utc_datetime
        if not isinstance(value, datetime):
            raise TypeError(
                "Dapr workflow context 'current_utc_datetime' returned "
                f"{type(value).__name__}; expected datetime",
            )
        # Dapr emits UTC; if the underlying context yields a naive
        # datetime (some test doubles do), attach UTC explicitly so
        # downstream comparisons remain tz-aware.
        if value.tzinfo is None:
            return value.replace(tzinfo=UTC)
        return value
