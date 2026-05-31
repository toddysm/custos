"""Retry decision driver — ``on_error`` route walk + effective delay (WF-IMPL-053).

The Step Coordinator's retry brain. Given a failed activity attempt
and the compiled :class:`~custos_workflow.graph.ExecutionNode`, this
module produces a frozen :class:`RetryDecision` that the
:class:`ActivityStepHandler` (WF-IMPL-054) dispatches on:

* :class:`RetryNow` — schedule the next attempt after ``delay_seconds``.
* :class:`Skip` — the operator wants the step skipped on this match.
* :class:`FailNow` — the step terminates with the carried envelope.

The decision is a pure function of the inputs (modulo the supplied
:class:`random.Random` for jitter). No I/O, no event publication — the
caller emits the ``step.retry_scheduled`` lifecycle event via
:func:`emit_retry_scheduled` once it accepts the decision.

Pinned to ``design.md`` § *Retry Policy* → § *Runtime behavior*:

* Routes are walked in declaration order; first match wins. The
  compiler (:mod:`custos_workflow.on_error.compile`) **always**
  prepends a ``cls=cancelled → FAIL`` short-circuit route, so a
  ``do: retry`` arm can never resurrect an operator-initiated
  cancellation.
* A ``do: retry`` arm enforces ``attempt + 1 <= max_attempts``;
  otherwise the decision is
  :class:`FailNow` carrying a :class:`step.retry_budget_exhausted`
  envelope built from
  :class:`~custos_workflow.steps.errors.RetryBudgetExhaustedError`.
* The effective delay is computed per the design's
  § *Backoff formulas* table, jittered per § *Jitter strategies*,
  then clamped against the envelope's optional ``retryAfter`` hint
  when the prevailing policy's ``respect_retry_after`` is true.

The locked ``"step.retry_budget_exhausted"`` ``kind`` string lives in
:data:`~custos_workflow.steps.errors.LOCKED_STEP_KINDS` — the OTel
counter (WF-IMPL-058) pins its label set against that frozenset.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final

from custos_workflow.graph import (
    BackoffStrategyTag,
    JitterStrategyTag,
    OnErrorActionTag,
)
from custos_workflow.steps.errors import RetryBudgetExhaustedError

if TYPE_CHECKING:
    import random
    from collections.abc import Mapping

    from custos_workflow.graph import (
        ExecutionNode,
        OnErrorRoute,
        ResolvedRetryPolicy,
    )
    from custos_workflow.runs.controller import (
        LifecycleEvent,
        LifecycleEventPublisher,
    )
    from custos_workflow.runs.ids import RunId


__all__ = [
    "LIFECYCLE_KIND_STEP_RETRY_SCHEDULED",
    "FailNow",
    "RetryDecision",
    "RetryNow",
    "Skip",
    "build_retry_scheduled_event",
    "decide",
    "emit_retry_scheduled",
]


# ---------------------------------------------------------------------------
# Lifecycle kind tag
# ---------------------------------------------------------------------------


#: Wire-stable lifecycle kind for the ``step.retry_scheduled`` event.
#:
#: WF-IMPL-056 lands the full ``step.*`` lifecycle taxonomy and will
#: pin this constant alongside its other ``step.*`` kinds. Until then
#: the retry driver owns the constant locally — the wire string is
#: still part of the public contract (downstream consumers route on
#: it), so it lives here as a :class:`typing.Final` rather than a
#: stringy literal.
LIFECYCLE_KIND_STEP_RETRY_SCHEDULED: Final[str] = "step.retry_scheduled"


# ---------------------------------------------------------------------------
# RetryDecision union
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RetryNow:
    """The Step Coordinator should schedule the next attempt.

    Attributes:
        delay_seconds: Effective delay before the next attempt, in
            seconds. Always non-negative. Combines the chosen
            backoff strategy, jitter strategy, and any ``retryAfter``
            envelope hint (clamped at the prevailing
            :attr:`~ResolvedBackoffPolicy.max_delay_ms`).
        next_attempt: 1-indexed attempt number that the orchestrator
            should schedule next. The caller has already failed
            attempt ``next_attempt - 1``.
    """

    delay_seconds: float
    next_attempt: int


@dataclass(frozen=True, slots=True)
class Skip:
    """The matched ``do: skip`` arm fired — the step is reported as skipped.

    Attributes:
        reason: Short, log-safe summary explaining the skip
            decision (mirrors the
            :class:`~custos_workflow.runs.step_handler.StepSkipped`
            ``reason`` field so the audit emitter can forward it
            verbatim).
    """

    reason: str


@dataclass(frozen=True, slots=True)
class FailNow:
    """The step terminates now with :attr:`envelope`.

    Two production paths produce this decision:

    * A ``do: fail`` arm matched — :attr:`envelope` is the original
      activity envelope, wrapped immutable so the dispatch site
      cannot mutate the shared shape.
    * A ``do: retry`` arm matched but the next attempt would exceed
      the prevailing ``max_attempts`` — :attr:`envelope` is the
      :class:`~custos_workflow.steps.errors.RetryBudgetExhaustedError`
      ``to_dict()`` shape, carrying the last underlying
      ``code`` / ``codePrefix`` / ``class`` for audit correlation.
    """

    envelope: Mapping[str, Any]


#: The three retry-decision shapes the driver returns.
RetryDecision = RetryNow | Skip | FailNow


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def decide(
    node: ExecutionNode,
    envelope: Mapping[str, Any],
    attempt: int,
    prev_delay_seconds: float | None,
    rng: random.Random,
) -> RetryDecision:
    """Resolve one failed activity attempt into a :class:`RetryDecision`.

    The function is pure: same inputs (including the same
    ``rng`` state) produce byte-equal outputs, so it is safe to
    invoke from Dapr Workflow replay paths.

    Args:
        node: The compiled :class:`~custos_workflow.graph.ExecutionNode`
            whose attempt has just failed. The driver reads
            :attr:`~custos_workflow.graph.ExecutionNode.on_error_routes`
            and :attr:`~custos_workflow.graph.ExecutionNode.retry_policy`
            from it; no other fields are touched.
        envelope: The ARM error envelope returned by the failed
            attempt. The driver routes on ``envelope["class"]``,
            ``envelope["code"]``, and ``envelope["codePrefix"]``
            (any of which may be absent); the optional
            ``envelope["retryAfter"]`` ISO-8601 duration is used
            for the lower-bound clamp on the effective delay.
        attempt: 1-indexed attempt number that just failed. Used
            both for the backoff formulas (``n`` in design.md) and
            for the budget check (next attempt would be
            ``attempt + 1``).
        prev_delay_seconds: The effective delay produced for the
            *previous* retry decision on this step, or ``None``
            on the first retry. Only the ``decorrelated`` jitter
            strategy consults this value.
        rng: Seeded :class:`random.Random` instance. Tests pass
            ``random.Random(0)`` for determinism; production wires
            in a per-run RNG seeded off
            ``(run_id, step_id, attempt)``.

    Returns:
        The resolved :class:`RetryDecision`.

    Raises:
        ValueError: ``attempt`` is not at least ``1``.
        RuntimeError: No route matched the envelope. Defence in
            depth — the compiler always appends an implicit
            ``retryable`` / ``permanent`` fallback so this is a
            programmer error (e.g. the caller hand-built an
            ``ExecutionNode`` without going through the compiler).
    """
    if attempt < 1:
        raise ValueError(f"attempt must be >= 1, got {attempt}")

    route = _match_route(node.on_error_routes, envelope)
    if route is None:
        raise RuntimeError(
            f"retry_driver: no on_error route matched envelope for step "
            f"{node.step_id!r} (the compiler should always append a "
            "fallback — this graph was not produced by the compiler)",
        )

    if route.action is OnErrorActionTag.SKIP:
        return Skip(reason=_skip_reason(route))

    if route.action is OnErrorActionTag.FAIL:
        return FailNow(envelope=MappingProxyType(dict(envelope)))

    # ``OnErrorActionTag.RETRY`` is the only remaining variant. The
    # ``do: retry`` arm carries its own resolved policy when it
    # overrides the step-level one; otherwise the step's prevailing
    # policy is used.
    policy = route.retry if route.retry is not None else node.retry_policy
    if policy is None:
        # An activity step without a resolved retry policy cannot
        # honour a ``do: retry`` action — surface as defence in
        # depth (the compiler attaches the policy for every
        # activity step, but a hand-built node could lack one).
        raise RuntimeError(
            f"retry_driver: step {node.step_id!r} matched a do: retry "
            "arm but carries no resolved retry policy",
        )

    next_attempt = attempt + 1
    if next_attempt > policy.max_attempts:
        return FailNow(envelope=_budget_exhausted_envelope(node, envelope, attempt, policy))

    effective = _effective_delay_seconds(policy, envelope, attempt, prev_delay_seconds, rng)
    return RetryNow(delay_seconds=effective, next_attempt=next_attempt)


def build_retry_scheduled_event(
    *,
    workspace_id: str,
    run_id: RunId,
    workflow_version_id: str,
    step_id: str,
    decision: RetryNow,
    envelope: Mapping[str, Any],
    occurred_at: datetime,
) -> LifecycleEvent:
    """Build the :class:`LifecycleEvent` payload for ``step.retry_scheduled``.

    Split out from :func:`emit_retry_scheduled` so callers that need
    to inspect or batch the event (tests, the
    :class:`~custos_workflow.runs.controller.LifecycleEventPublisher`
    Dapr adapter) can do so without involving the publisher.

    Args:
        workspace_id: Owning workspace.
        run_id: The run instance id.
        workflow_version_id: The Catalog Workflow Version id this
            run was started against.
        step_id: The step whose retry was scheduled.
        decision: The :class:`RetryNow` that
            :func:`decide` produced.
        envelope: The ARM error envelope from the previous attempt.
            Surfaced as ``previous_code`` / ``previous_class`` in
            :attr:`LifecycleEvent.extra` for audit correlation.
        occurred_at: Replay-deterministic timestamp from
            :meth:`Clock.now`. Production wires
            :class:`custos_cel.DaprWorkflowClock` (snapshot of
            :class:`dapr.ext.workflow.DaprWorkflowContext.current_utc_datetime`);
            tests pass :class:`custos_cel.FixedClock`.

    Returns:
        A :class:`LifecycleEvent` with
        :attr:`~LifecycleEvent.kind` equal to
        :data:`LIFECYCLE_KIND_STEP_RETRY_SCHEDULED` and
        :attr:`~LifecycleEvent.extra` populated per design.md §
        *Runtime behavior*:

        ``{step_id, previous_attempt, previous_code, previous_code_prefix,
        previous_class, action, effective_delay_seconds, next_attempt}``.
    """
    # Local import: ``runs.controller`` imports from
    # ``steps`` transitively, so importing it at module top would
    # be a circular import. The import is a one-liner at call time.
    from custos_workflow.runs.controller import LifecycleEvent

    return LifecycleEvent(
        kind=LIFECYCLE_KIND_STEP_RETRY_SCHEDULED,
        workspace_id=workspace_id,
        run_id=run_id,
        workflow_version_id=workflow_version_id,
        occurred_at=occurred_at,
        extra={
            "step_id": step_id,
            "previous_attempt": decision.next_attempt - 1,
            "previous_code": envelope.get("code"),
            "previous_code_prefix": envelope.get("codePrefix"),
            "previous_class": envelope.get("class"),
            "action": OnErrorActionTag.RETRY.value,
            "effective_delay_seconds": decision.delay_seconds,
            "next_attempt": decision.next_attempt,
        },
    )


async def emit_retry_scheduled(
    *,
    workspace_id: str,
    run_id: RunId,
    workflow_version_id: str,
    step_id: str,
    decision: RetryNow,
    envelope: Mapping[str, Any],
    occurred_at: datetime,
    publisher: LifecycleEventPublisher,
) -> None:
    """Publish the ``step.retry_scheduled`` lifecycle event.

    Convenience wrapper that builds the event via
    :func:`build_retry_scheduled_event` and delegates to the
    publisher. Kept separate from :func:`decide` because the
    decision is a pure synchronous function — the event emission
    is the async side effect, owned by the caller's await
    boundary.

    Args:
        workspace_id: Owning workspace.
        run_id: The run instance id.
        workflow_version_id: The Catalog Workflow Version id.
        step_id: The step whose retry was scheduled.
        decision: The :class:`RetryNow` that
            :func:`decide` produced.
        envelope: The ARM error envelope from the previous attempt.
        occurred_at: Replay-deterministic timestamp.
        publisher: Sink for the lifecycle event. Must satisfy the
            :class:`~custos_workflow.runs.controller.LifecycleEventPublisher`
            Protocol.
    """
    event = build_retry_scheduled_event(
        workspace_id=workspace_id,
        run_id=run_id,
        workflow_version_id=workflow_version_id,
        step_id=step_id,
        decision=decision,
        envelope=envelope,
        occurred_at=occurred_at,
    )
    await publisher.publish(event)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _match_route(
    routes: tuple[OnErrorRoute, ...],
    envelope: Mapping[str, Any],
) -> OnErrorRoute | None:
    """Walk *routes* in declaration order; return the first match.

    Match semantics (mirroring
    :class:`~custos_workflow.document.OnErrorMatch` validation —
    exactly one of ``code`` / ``code_prefix`` / ``cls`` is set):

    * ``cls`` matches when ``envelope["class"] == route.cls``.
    * ``code`` matches when ``envelope["code"] == route.code``.
    * ``code_prefix`` matches when ``envelope["code"]`` starts with
      the configured prefix (a missing envelope code never matches).

    Returns ``None`` if no route matches — the compiler always
    appends a fallback arm so the runtime should never see this in
    production. Surfaced for defence-in-depth.
    """
    env_class = envelope.get("class")
    env_code = envelope.get("code")
    for route in routes:
        if route.cls is not None and route.cls == env_class:
            return route
        if route.code is not None and route.code == env_code:
            return route
        if (
            route.code_prefix is not None
            and isinstance(env_code, str)
            and env_code.startswith(route.code_prefix)
        ):
            return route
    return None


def _skip_reason(route: OnErrorRoute) -> str:
    """Format a short, log-safe reason string for a ``do: skip`` arm."""
    if route.cls is not None:
        return f"on_error[class={route.cls}]: skip"
    if route.code is not None:
        return f"on_error[code={route.code}]: skip"
    # ``code_prefix`` is the only remaining variant (the document
    # model enforces exactly one of the three).
    return f"on_error[codePrefix={route.code_prefix}]: skip"


def _budget_exhausted_envelope(
    node: ExecutionNode,
    envelope: Mapping[str, Any],
    attempt: int,
    policy: ResolvedRetryPolicy,
) -> Mapping[str, Any]:
    """Wrap the activity envelope in a ``step.retry_budget_exhausted`` envelope.

    The resulting mapping is immutable (
    :class:`~types.MappingProxyType`) so the dispatcher cannot
    mutate the audit shape after the fact.
    """
    err = RetryBudgetExhaustedError(
        message=(
            f"step {node.step_id!r}: retry budget exhausted after "
            f"{attempt} attempt(s) (max_attempts={policy.max_attempts})"
        ),
        step_id=node.step_id,
        attempt=attempt,
        max_attempts=policy.max_attempts,
        last_code=_str_or_none(envelope.get("code")),
        last_code_prefix=_str_or_none(envelope.get("codePrefix")),
        last_class=_str_or_none(envelope.get("class")),
    )
    return MappingProxyType(err.to_dict())


def _str_or_none(value: Any) -> str | None:
    """Coerce *value* to ``str`` when present, otherwise ``None``.

    Activity envelopes are user data; defensive coercion prevents
    a non-string slipping into the audit envelope (whose schema
    pins these three fields to ``str | None``).
    """
    if value is None:
        return None
    return str(value)


# ---------------------------------------------------------------------------
# Effective-delay computation
# ---------------------------------------------------------------------------


def _effective_delay_seconds(
    policy: ResolvedRetryPolicy,
    envelope: Mapping[str, Any],
    attempt: int,
    prev_delay_seconds: float | None,
    rng: random.Random,
) -> float:
    """Compute the effective delay before the next attempt, in seconds.

    Pipeline:

    1. Pre-jitter backoff per :attr:`policy.backoff.strategy` and
       :attr:`attempt` (``n`` in design.md).
    2. Clamp to ``[0, max_delay]``.
    3. Apply jitter per :attr:`policy.jitter`.
    4. If :attr:`policy.respect_retry_after` is ``True`` and the
       envelope carries a parseable ``retryAfter`` ISO-8601
       duration, take ``max(jittered, min(retry_after, max_delay))``
       so the hint acts as a lower bound on the wait without
       exceeding the prevailing :attr:`max_delay` ceiling.
    """
    initial = policy.backoff.initial_delay_ms / 1000.0
    max_delay = policy.backoff.max_delay_ms / 1000.0
    multiplier = policy.backoff.multiplier

    if policy.backoff.strategy is BackoffStrategyTag.CONSTANT:
        base = initial
    elif policy.backoff.strategy is BackoffStrategyTag.LINEAR:
        base = initial * attempt
    else:
        # ``BackoffStrategyTag.EXPONENTIAL`` is the only remaining
        # variant (the enum is closed at compile time).
        base = initial * (multiplier ** (attempt - 1))

    base = min(base, max_delay)

    if policy.jitter is JitterStrategyTag.NONE:
        jittered = base
    elif policy.jitter is JitterStrategyTag.FULL:
        # ``random.uniform`` is closed-closed; design.md specifies
        # ``random(a, b)`` as half-open ``[a, b)``. Using
        # ``random.random()`` keeps the upper bound strictly
        # exclusive — matters for the table-driven bound assertions
        # in the test suite.
        jittered = base * rng.random()
    elif policy.jitter is JitterStrategyTag.EQUAL:
        half = base / 2.0
        jittered = half + half * rng.random()
    else:
        # ``JitterStrategyTag.DECORRELATED`` -- design.md formula:
        # ``min(Dmax, random(D0, prevDelay * 3))`` with prevDelay
        # defaulting to D0 on the first retry.
        prev = prev_delay_seconds if prev_delay_seconds is not None else initial
        upper = prev * 3.0
        # The half-open interval ``[initial, upper)`` collapses to
        # ``initial`` when ``upper <= initial`` — guard so we never
        # call ``rng.uniform`` with a degenerate range.
        sample = initial if upper <= initial else initial + (upper - initial) * rng.random()
        jittered = min(max_delay, sample)

    if policy.respect_retry_after:
        retry_after_seconds = _parse_retry_after(envelope.get("retryAfter"))
        if retry_after_seconds is not None:
            # Per design.md: "A retryAfter hint older than the
            # prevailing maxDelay is clamped to maxDelay (the hint
            # is a lower bound on the delay, not on the upper
            # bound)."
            clamped = min(retry_after_seconds, max_delay)
            jittered = max(jittered, clamped)

    return jittered


# ---------------------------------------------------------------------------
# ``retryAfter`` ISO-8601 parser
# ---------------------------------------------------------------------------


#: Mirror of
#: :data:`~custos_workflow.runs.wait._ISO8601_DURATION_PATTERN`.
#:
#: Owned independently here because the runtime semantics differ —
#: a malformed ``retryAfter`` in an activity envelope is best-effort
#: ignored, not fatal (the activity is third-party data; the
#: ``wait:`` duration is operator-authored and validated at
#: publish time).
_ISO8601_DURATION_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^P(?:"
    r"(?P<weeks>\d+)W"
    r"|"
    r"(?:(?P<days>\d+)D)?"
    r"(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?"
    r")$"
)


def _parse_retry_after(raw: Any) -> float | None:
    """Best-effort parse of the envelope's ``retryAfter`` field.

    Returns the duration in seconds (a positive ``float``) on
    success, or ``None`` when *raw* is missing, not a string, fails
    the ISO-8601 grammar, or parses to a non-positive value. The
    driver intentionally swallows malformed hints rather than
    raising — the field is third-party data from the activity, and
    a malformed hint should degrade to "ignore the hint" rather
    than fail the retry decision.
    """
    if not isinstance(raw, str):
        return None
    match = _ISO8601_DURATION_PATTERN.match(raw)
    if match is None:
        return None
    weeks = int(match.group("weeks") or 0)
    days = int(match.group("days") or 0)
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes") or 0)
    seconds = float(match.group("seconds") or 0.0)
    total = weeks * 7 * 86400.0 + days * 86400.0 + hours * 3600.0 + minutes * 60.0 + seconds
    if total <= 0.0:
        return None
    return total
