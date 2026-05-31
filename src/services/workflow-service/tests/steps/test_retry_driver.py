"""Tests for the WF-IMPL-053 retry decision driver."""

from __future__ import annotations

import asyncio
import random
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from custos_workflow.document import ActivityStep
from custos_workflow.graph import (
    BackoffStrategyTag,
    ExecutionNode,
    JitterStrategyTag,
    OnErrorActionTag,
    OnErrorRoute,
    PrimitiveHandler,
    ResolvedBackoffPolicy,
    ResolvedRetryPolicy,
    StepKind,
)
from custos_workflow.runs.controller import (
    InMemoryLifecycleEventPublisher,
    LifecycleEvent,
)
from custos_workflow.runs.ids import RunId
from custos_workflow.steps import (
    LIFECYCLE_KIND_STEP_RETRY_SCHEDULED,
    FailNow,
    RetryBudgetExhaustedError,
    RetryNow,
    Skip,
    build_retry_scheduled_event,
    decide,
    emit_retry_scheduled,
)

# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


def _backoff(
    *,
    strategy: BackoffStrategyTag = BackoffStrategyTag.EXPONENTIAL,
    initial_delay_ms: int = 1_000,
    max_delay_ms: int = 60_000,
    multiplier: float = 2.0,
) -> ResolvedBackoffPolicy:
    return ResolvedBackoffPolicy(
        strategy=strategy,
        initial_delay_ms=initial_delay_ms,
        max_delay_ms=max_delay_ms,
        multiplier=multiplier,
    )


def _policy(
    *,
    max_attempts: int = 3,
    backoff: ResolvedBackoffPolicy | None = None,
    jitter: JitterStrategyTag = JitterStrategyTag.NONE,
    respect_retry_after: bool = True,
) -> ResolvedRetryPolicy:
    return ResolvedRetryPolicy(
        max_attempts=max_attempts,
        backoff=backoff if backoff is not None else _backoff(),
        jitter=jitter,
        respect_retry_after=respect_retry_after,
    )


def _default_routes(policy: ResolvedRetryPolicy) -> tuple[OnErrorRoute, ...]:
    """Mirror the compiler's implicit-policy route table."""
    return (
        OnErrorRoute(action=OnErrorActionTag.FAIL, cls="cancelled"),
        OnErrorRoute(action=OnErrorActionTag.RETRY, cls="retryable", retry=policy),
        OnErrorRoute(action=OnErrorActionTag.FAIL, cls="permanent"),
    )


_DEFAULT_POLICY = object()


def _node(
    *,
    step_id: str = "step-a",
    retry_policy: Any = _DEFAULT_POLICY,
    on_error_routes: tuple[OnErrorRoute, ...] | None = None,
) -> ExecutionNode:
    policy = _policy() if retry_policy is _DEFAULT_POLICY else retry_policy
    return ExecutionNode(
        step_id=step_id,
        kind=StepKind.ACTIVITY,
        primitive_handler=PrimitiveHandler.ACTIVITY_RUNTIME,
        retry_policy=policy,
        on_error_routes=on_error_routes
        if on_error_routes is not None
        else _default_routes(policy if policy is not None else _policy()),
        call_sites={},
        step_source=ActivityStep.model_validate(
            {"id": step_id, "activity": "x/y@1", "connector": "primary"},
        ),
    )


def _envelope(
    *,
    cls: str = "retryable",
    code: str | None = None,
    code_prefix: str | None = None,
    retry_after: str | None = None,
    extra: dict[str, Any] | None = None,
) -> dict[str, Any]:
    env: dict[str, Any] = {"class": cls}
    if code is not None:
        env["code"] = code
    if code_prefix is not None:
        env["codePrefix"] = code_prefix
    if retry_after is not None:
        env["retryAfter"] = retry_after
    if extra:
        env.update(extra)
    return env


# ---------------------------------------------------------------------------
# Decision-shape tests
# ---------------------------------------------------------------------------


class TestRetryDecisionShape:
    def test_decisions_are_frozen_dataclasses(self) -> None:
        rn = RetryNow(delay_seconds=1.5, next_attempt=2)
        sk = Skip(reason="r")
        fn = FailNow(envelope=MappingProxyType({"kind": "x"}))
        for obj, attr, value in (
            (rn, "delay_seconds", 9.0),
            (sk, "reason", "x"),
            (fn, "envelope", {}),
        ):
            with pytest.raises(Exception):  # noqa: B017 — frozen dataclass
                setattr(obj, attr, value)


class TestArgumentValidation:
    @pytest.mark.parametrize("attempt", [0, -1, -42])
    def test_attempt_below_one_raises_value_error(self, attempt: int) -> None:
        with pytest.raises(ValueError, match="attempt must be >= 1"):
            decide(
                _node(),
                _envelope(),
                attempt=attempt,
                prev_delay_seconds=None,
                rng=random.Random(0),
            )

    def test_no_route_match_raises_runtime_error(self) -> None:
        # Hand-build a node with only a cls=retryable arm, then feed
        # a permanent envelope so nothing matches. The compiler
        # would never produce this shape (it always appends a
        # fallback), so the runtime surfaces it as a programmer
        # error.
        policy = _policy()
        node = _node(
            on_error_routes=(
                OnErrorRoute(action=OnErrorActionTag.RETRY, cls="retryable", retry=policy),
            ),
        )
        with pytest.raises(RuntimeError, match="no on_error route matched envelope"):
            decide(
                node,
                _envelope(cls="permanent"),
                attempt=1,
                prev_delay_seconds=None,
                rng=random.Random(0),
            )

    def test_retry_arm_without_policy_raises_runtime_error(self) -> None:
        # Strip the retry policy from both the route and the node so
        # the do: retry arm has no policy to honour.
        node = _node(
            retry_policy=None,
            on_error_routes=(
                OnErrorRoute(action=OnErrorActionTag.FAIL, cls="cancelled"),
                OnErrorRoute(action=OnErrorActionTag.RETRY, cls="retryable", retry=None),
            ),
        )
        with pytest.raises(RuntimeError, match="carries no resolved retry policy"):
            decide(
                node,
                _envelope(cls="retryable"),
                attempt=1,
                prev_delay_seconds=None,
                rng=random.Random(0),
            )


# ---------------------------------------------------------------------------
# Class-routing tests (implicit + explicit on_error)
# ---------------------------------------------------------------------------


class TestImplicitRouting:
    def test_cancelled_short_circuits_to_fail(self) -> None:
        node = _node()
        env = _envelope(cls="cancelled", code="user.cancelled")
        result = decide(
            node,
            env,
            attempt=1,
            prev_delay_seconds=None,
            rng=random.Random(0),
        )
        assert isinstance(result, FailNow)
        assert result.envelope["class"] == "cancelled"

    def test_permanent_fails_immediately(self) -> None:
        node = _node()
        env = _envelope(cls="permanent", code="auth.forbidden")
        result = decide(
            node,
            env,
            attempt=1,
            prev_delay_seconds=None,
            rng=random.Random(0),
        )
        assert isinstance(result, FailNow)
        assert result.envelope["class"] == "permanent"
        assert result.envelope["code"] == "auth.forbidden"

    def test_retryable_retries_then_exhausts(self) -> None:
        node = _node(retry_policy=_policy(max_attempts=3))
        env = _envelope(cls="retryable", code="net.timeout")
        # attempt=1 -> retry now (next_attempt=2)
        d1 = decide(node, env, attempt=1, prev_delay_seconds=None, rng=random.Random(0))
        assert isinstance(d1, RetryNow)
        assert d1.next_attempt == 2
        # attempt=2 -> retry now (next_attempt=3)
        d2 = decide(node, env, attempt=2, prev_delay_seconds=d1.delay_seconds, rng=random.Random(0))
        assert isinstance(d2, RetryNow)
        assert d2.next_attempt == 3
        # attempt=3 -> next would be 4 > max_attempts=3 -> exhausted
        d3 = decide(node, env, attempt=3, prev_delay_seconds=d2.delay_seconds, rng=random.Random(0))
        assert isinstance(d3, FailNow)
        assert d3.envelope["kind"] == RetryBudgetExhaustedError.KIND
        assert d3.envelope["last_class"] == "retryable"
        assert d3.envelope["last_code"] == "net.timeout"
        assert d3.envelope["max_attempts"] == 3
        assert d3.envelope["attempt"] == 3


class TestExplicitOnError:
    def test_cancelled_short_circuit_wins_over_explicit_retry_arm(self) -> None:
        # User has declared a do:retry arm matching cancelled (which
        # would be rejected at compile time, but the compiler always
        # prepends the cancelled→FAIL guard so even a hand-built
        # node with the bad arm is overridden). Verify the prepended
        # guard wins.
        policy = _policy(max_attempts=5)
        routes = (
            OnErrorRoute(action=OnErrorActionTag.FAIL, cls="cancelled"),
            OnErrorRoute(action=OnErrorActionTag.RETRY, cls="cancelled", retry=policy),
        )
        node = _node(on_error_routes=routes)
        result = decide(
            node,
            _envelope(cls="cancelled"),
            attempt=1,
            prev_delay_seconds=None,
            rng=random.Random(0),
        )
        assert isinstance(result, FailNow)

    def test_explicit_code_arm_with_do_skip(self) -> None:
        policy = _policy()
        routes = (
            OnErrorRoute(action=OnErrorActionTag.FAIL, cls="cancelled"),
            OnErrorRoute(action=OnErrorActionTag.SKIP, code="vendor.notFound"),
            OnErrorRoute(action=OnErrorActionTag.RETRY, cls="retryable", retry=policy),
        )
        node = _node(on_error_routes=routes)
        result = decide(
            node,
            _envelope(cls="permanent", code="vendor.notFound"),
            attempt=1,
            prev_delay_seconds=None,
            rng=random.Random(0),
        )
        assert isinstance(result, Skip)
        assert "code=vendor.notFound" in result.reason

    def test_explicit_code_prefix_match_with_do_fail(self) -> None:
        policy = _policy()
        routes = (
            OnErrorRoute(action=OnErrorActionTag.FAIL, cls="cancelled"),
            OnErrorRoute(action=OnErrorActionTag.FAIL, code_prefix="auth."),
            OnErrorRoute(action=OnErrorActionTag.RETRY, cls="retryable", retry=policy),
        )
        node = _node(on_error_routes=routes)
        result = decide(
            node,
            _envelope(cls="permanent", code="auth.token.expired"),
            attempt=1,
            prev_delay_seconds=None,
            rng=random.Random(0),
        )
        assert isinstance(result, FailNow)
        assert result.envelope["code"] == "auth.token.expired"

    def test_skip_reason_format_for_cls_and_code_prefix(self) -> None:
        policy = _policy()
        cls_route = OnErrorRoute(action=OnErrorActionTag.SKIP, cls="retryable")
        prefix_route = OnErrorRoute(action=OnErrorActionTag.SKIP, code_prefix="vendor.")
        cls_node = _node(on_error_routes=(cls_route,))
        prefix_node = _node(on_error_routes=(prefix_route,), retry_policy=policy)
        cls_result = decide(
            cls_node,
            _envelope(cls="retryable"),
            attempt=1,
            prev_delay_seconds=None,
            rng=random.Random(0),
        )
        prefix_result = decide(
            prefix_node,
            _envelope(cls="permanent", code="vendor.x"),
            attempt=1,
            prev_delay_seconds=None,
            rng=random.Random(0),
        )
        assert isinstance(cls_result, Skip)
        assert cls_result.reason == "on_error[class=retryable]: skip"
        assert isinstance(prefix_result, Skip)
        assert prefix_result.reason == "on_error[codePrefix=vendor.]: skip"


# ---------------------------------------------------------------------------
# Effective-delay bounds — table-driven assertions from design.md
# ---------------------------------------------------------------------------


class TestEffectiveDelayBounds:
    """Pin the design.md backoff-formulas-and-jitter-strategies table.

    For every (strategy, jitter) combination, 50 Hypothesis examples
    drawn with rng=Random(0) all fall inside the documented interval.
    """

    @settings(
        max_examples=50,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(attempt=st.integers(min_value=1, max_value=10))
    def test_constant_none(self, attempt: int) -> None:
        policy = _policy(
            backoff=_backoff(strategy=BackoffStrategyTag.CONSTANT, initial_delay_ms=2_000),
            jitter=JitterStrategyTag.NONE,
            max_attempts=999,
        )
        node = _node(retry_policy=policy)
        d = decide(
            node, _envelope(), attempt=attempt, prev_delay_seconds=None, rng=random.Random(0)
        )
        assert isinstance(d, RetryNow)
        assert d.delay_seconds == 2.0

    @settings(
        max_examples=50,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(attempt=st.integers(min_value=1, max_value=5))
    def test_linear_full(self, attempt: int) -> None:
        policy = _policy(
            backoff=_backoff(
                strategy=BackoffStrategyTag.LINEAR,
                initial_delay_ms=1_000,
                max_delay_ms=30_000,
            ),
            jitter=JitterStrategyTag.FULL,
            max_attempts=999,
        )
        node = _node(retry_policy=policy)
        # pre-jitter base = min(1 * attempt, 30)
        upper = min(1.0 * attempt, 30.0)
        d = decide(
            node, _envelope(), attempt=attempt, prev_delay_seconds=None, rng=random.Random(0)
        )
        assert isinstance(d, RetryNow)
        assert 0.0 <= d.delay_seconds < upper or upper == 0.0

    @settings(
        max_examples=50,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(attempt=st.integers(min_value=1, max_value=5))
    def test_exponential_equal(self, attempt: int) -> None:
        policy = _policy(
            backoff=_backoff(
                strategy=BackoffStrategyTag.EXPONENTIAL,
                initial_delay_ms=1_000,
                max_delay_ms=60_000,
                multiplier=2.0,
            ),
            jitter=JitterStrategyTag.EQUAL,
            max_attempts=999,
        )
        node = _node(retry_policy=policy)
        base = min(1.0 * (2.0 ** (attempt - 1)), 60.0)
        d = decide(
            node, _envelope(), attempt=attempt, prev_delay_seconds=None, rng=random.Random(0)
        )
        assert isinstance(d, RetryNow)
        # equal jitter -> [base/2, base)
        assert base / 2.0 <= d.delay_seconds < base or base == 0.0

    @settings(
        max_examples=50,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(attempt=st.integers(min_value=1, max_value=5))
    def test_exponential_decorrelated_first_retry(self, attempt: int) -> None:
        policy = _policy(
            backoff=_backoff(
                strategy=BackoffStrategyTag.EXPONENTIAL,
                initial_delay_ms=1_000,
                max_delay_ms=120_000,
                multiplier=2.0,
            ),
            jitter=JitterStrategyTag.DECORRELATED,
            max_attempts=999,
        )
        node = _node(retry_policy=policy)
        # First retry: prev=D0=1.0 → upper = 3.0; sample in [1.0, 3.0)
        d = decide(
            node, _envelope(), attempt=attempt, prev_delay_seconds=None, rng=random.Random(0)
        )
        assert isinstance(d, RetryNow)
        assert 1.0 <= d.delay_seconds < 3.0

    def test_decorrelated_subsequent_uses_prev_delay(self) -> None:
        policy = _policy(
            backoff=_backoff(
                strategy=BackoffStrategyTag.EXPONENTIAL,
                initial_delay_ms=1_000,
                max_delay_ms=120_000,
                multiplier=2.0,
            ),
            jitter=JitterStrategyTag.DECORRELATED,
            max_attempts=999,
        )
        node = _node(retry_policy=policy)
        d = decide(node, _envelope(), attempt=2, prev_delay_seconds=4.0, rng=random.Random(0))
        assert isinstance(d, RetryNow)
        # upper = prev * 3 = 12; sample in [1, 12)
        assert 1.0 <= d.delay_seconds < 12.0

    def test_decorrelated_clamps_to_max_delay(self) -> None:
        policy = _policy(
            backoff=_backoff(
                strategy=BackoffStrategyTag.EXPONENTIAL,
                initial_delay_ms=1_000,
                max_delay_ms=2_000,  # 2s ceiling
                multiplier=2.0,
            ),
            jitter=JitterStrategyTag.DECORRELATED,
            max_attempts=999,
        )
        node = _node(retry_policy=policy)
        # prev*3 = 30s, far above max_delay=2s → must clamp at 2s
        d = decide(node, _envelope(), attempt=2, prev_delay_seconds=10.0, rng=random.Random(0))
        assert isinstance(d, RetryNow)
        assert d.delay_seconds <= 2.0

    def test_decorrelated_degenerate_range_returns_initial(self) -> None:
        # prev=0 ⇒ upper=0 ⇒ upper <= initial(=1.0) ⇒ sample collapses to initial.
        policy = _policy(
            backoff=_backoff(
                strategy=BackoffStrategyTag.EXPONENTIAL,
                initial_delay_ms=1_000,
                max_delay_ms=60_000,
                multiplier=2.0,
            ),
            jitter=JitterStrategyTag.DECORRELATED,
            max_attempts=999,
        )
        node = _node(retry_policy=policy)
        d = decide(node, _envelope(), attempt=2, prev_delay_seconds=0.0, rng=random.Random(0))
        assert isinstance(d, RetryNow)
        assert d.delay_seconds == 1.0

    def test_constant_clamps_to_max_delay(self) -> None:
        policy = _policy(
            backoff=_backoff(
                strategy=BackoffStrategyTag.CONSTANT,
                initial_delay_ms=10_000,
                max_delay_ms=3_000,  # ceiling smaller than initial
            ),
            jitter=JitterStrategyTag.NONE,
            max_attempts=999,
        )
        node = _node(retry_policy=policy)
        d = decide(node, _envelope(), attempt=1, prev_delay_seconds=None, rng=random.Random(0))
        assert isinstance(d, RetryNow)
        assert d.delay_seconds == 3.0


# ---------------------------------------------------------------------------
# retryAfter interaction
# ---------------------------------------------------------------------------


class TestRetryAfter:
    def test_retry_after_clamps_lower_bound(self) -> None:
        policy = _policy(
            backoff=_backoff(
                strategy=BackoffStrategyTag.CONSTANT,
                initial_delay_ms=1_000,
                max_delay_ms=60_000,
            ),
            jitter=JitterStrategyTag.NONE,
        )
        node = _node(retry_policy=policy)
        # retryAfter=PT5S; jittered backoff=1s → max(1, 5) = 5s
        env = _envelope(retry_after="PT5S")
        d = decide(node, env, attempt=1, prev_delay_seconds=None, rng=random.Random(0))
        assert isinstance(d, RetryNow)
        assert d.delay_seconds == 5.0

    def test_retry_after_clamped_to_max_delay(self) -> None:
        policy = _policy(
            backoff=_backoff(
                strategy=BackoffStrategyTag.CONSTANT,
                initial_delay_ms=1_000,
                max_delay_ms=10_000,
            ),
            jitter=JitterStrategyTag.NONE,
        )
        node = _node(retry_policy=policy)
        # retryAfter=PT1M (60s); ceiling is 10s → clamp retryAfter at 10
        env = _envelope(retry_after="PT1M")
        d = decide(node, env, attempt=1, prev_delay_seconds=None, rng=random.Random(0))
        assert isinstance(d, RetryNow)
        assert d.delay_seconds == 10.0

    def test_retry_after_ignored_when_respect_false(self) -> None:
        policy = _policy(
            backoff=_backoff(
                strategy=BackoffStrategyTag.CONSTANT,
                initial_delay_ms=1_000,
                max_delay_ms=60_000,
            ),
            jitter=JitterStrategyTag.NONE,
            respect_retry_after=False,
        )
        node = _node(retry_policy=policy)
        env = _envelope(retry_after="PT30S")
        d = decide(node, env, attempt=1, prev_delay_seconds=None, rng=random.Random(0))
        assert isinstance(d, RetryNow)
        # No clamp — straight 1s
        assert d.delay_seconds == 1.0

    @pytest.mark.parametrize(
        "raw",
        [
            None,
            123,  # not a string
            "not-iso",
            "P",  # structurally empty
            "PT0S",  # parses to zero
            "P1Y",  # months/years not supported by the grammar
        ],
    )
    def test_malformed_retry_after_is_ignored(self, raw: Any) -> None:
        policy = _policy(
            backoff=_backoff(
                strategy=BackoffStrategyTag.CONSTANT,
                initial_delay_ms=1_000,
                max_delay_ms=60_000,
            ),
            jitter=JitterStrategyTag.NONE,
        )
        node = _node(retry_policy=policy)
        env = _envelope()
        if raw is not None:
            env["retryAfter"] = raw
        d = decide(node, env, attempt=1, prev_delay_seconds=None, rng=random.Random(0))
        assert isinstance(d, RetryNow)
        assert d.delay_seconds == 1.0


# ---------------------------------------------------------------------------
# Budget exhaustion envelope shape
# ---------------------------------------------------------------------------


class TestRetryBudgetExhaustedEnvelope:
    def test_envelope_carries_last_underlying_metadata(self) -> None:
        policy = _policy(max_attempts=1)  # exhausted on first failure
        node = _node(step_id="probe", retry_policy=policy)
        env = _envelope(
            cls="retryable",
            code="net.timeout",
            extra={"codePrefix": "net."},
        )
        d = decide(node, env, attempt=1, prev_delay_seconds=None, rng=random.Random(0))
        assert isinstance(d, FailNow)
        assert d.envelope["kind"] == "step.retry_budget_exhausted"
        assert d.envelope["step_id"] == "probe"
        assert d.envelope["attempt"] == 1
        assert d.envelope["max_attempts"] == 1
        assert d.envelope["last_code"] == "net.timeout"
        assert d.envelope["last_code_prefix"] == "net."
        assert d.envelope["last_class"] == "retryable"
        assert d.envelope["kind"] == RetryBudgetExhaustedError.KIND

    def test_envelope_is_immutable(self) -> None:
        policy = _policy(max_attempts=1)
        node = _node(retry_policy=policy)
        env = _envelope(cls="retryable", code="x")
        d = decide(node, env, attempt=1, prev_delay_seconds=None, rng=random.Random(0))
        assert isinstance(d, FailNow)
        with pytest.raises(TypeError):
            d.envelope["mutated"] = 1  # type: ignore[index]

    def test_failnow_envelope_for_fail_arm_is_immutable_copy(self) -> None:
        node = _node()
        env = _envelope(cls="permanent", code="auth.fail")
        d = decide(node, env, attempt=1, prev_delay_seconds=None, rng=random.Random(0))
        assert isinstance(d, FailNow)
        with pytest.raises(TypeError):
            d.envelope["mutated"] = 1  # type: ignore[index]
        # The wrapped dict is a copy — mutating the original
        # envelope does not bleed through.
        env["code"] = "TAMPERED"
        assert d.envelope["code"] == "auth.fail"

    def test_envelope_coerces_non_string_underlying_metadata(self) -> None:
        policy = _policy(max_attempts=1)
        node = _node(retry_policy=policy)
        env = _envelope(cls="retryable")
        env["code"] = 42  # numeric code — defensive coercion
        d = decide(node, env, attempt=1, prev_delay_seconds=None, rng=random.Random(0))
        assert isinstance(d, FailNow)
        assert d.envelope["last_code"] == "42"


# ---------------------------------------------------------------------------
# Replay determinism
# ---------------------------------------------------------------------------


class TestReplayDeterminism:
    def test_two_calls_with_same_rng_produce_byte_equal_decisions(self) -> None:
        policy = _policy(
            backoff=_backoff(
                strategy=BackoffStrategyTag.EXPONENTIAL,
                initial_delay_ms=1_000,
                max_delay_ms=60_000,
                multiplier=2.0,
            ),
            jitter=JitterStrategyTag.DECORRELATED,
        )
        node = _node(retry_policy=policy)
        env = _envelope(retry_after="PT2S")
        d1 = decide(node, env, attempt=2, prev_delay_seconds=4.0, rng=random.Random(0))
        d2 = decide(node, env, attempt=2, prev_delay_seconds=4.0, rng=random.Random(0))
        assert isinstance(d1, RetryNow)
        assert isinstance(d2, RetryNow)
        assert d1.delay_seconds == d2.delay_seconds
        assert d1.next_attempt == d2.next_attempt


# ---------------------------------------------------------------------------
# Lifecycle event emission
# ---------------------------------------------------------------------------


class TestEmitRetryScheduled:
    _OCC = datetime(2026, 1, 1, 12, 0, 0, tzinfo=UTC)

    def test_build_event_shape(self) -> None:
        decision = RetryNow(delay_seconds=4.5, next_attempt=3)
        envelope = _envelope(cls="retryable", code="net.timeout", code_prefix="net.")
        event = build_retry_scheduled_event(
            workspace_id="ws-1",
            run_id=RunId("run-1"),
            workflow_version_id="wf-version-1",
            step_id="probe",
            decision=decision,
            envelope=envelope,
            occurred_at=self._OCC,
        )
        assert isinstance(event, LifecycleEvent)
        assert event.kind == LIFECYCLE_KIND_STEP_RETRY_SCHEDULED
        assert event.workspace_id == "ws-1"
        assert event.workflow_version_id == "wf-version-1"
        assert event.occurred_at == self._OCC
        assert event.extra["step_id"] == "probe"
        assert event.extra["previous_attempt"] == 2
        assert event.extra["previous_code"] == "net.timeout"
        assert event.extra["previous_code_prefix"] == "net."
        assert event.extra["previous_class"] == "retryable"
        assert event.extra["action"] == "retry"
        assert event.extra["effective_delay_seconds"] == 4.5
        assert event.extra["next_attempt"] == 3

    def test_build_event_tolerates_envelope_missing_fields(self) -> None:
        decision = RetryNow(delay_seconds=1.0, next_attempt=2)
        event = build_retry_scheduled_event(
            workspace_id="ws-1",
            run_id=RunId("run-1"),
            workflow_version_id="wf-version-1",
            step_id="probe",
            decision=decision,
            envelope={"class": "retryable"},  # no code / codePrefix
            occurred_at=self._OCC,
        )
        assert event.extra["previous_code"] is None
        assert event.extra["previous_code_prefix"] is None

    def test_emit_publishes_via_publisher(self) -> None:
        publisher = InMemoryLifecycleEventPublisher()
        decision = RetryNow(delay_seconds=2.0, next_attempt=2)
        envelope = _envelope(cls="retryable", code="vendor.busy")
        asyncio.run(
            emit_retry_scheduled(
                workspace_id="ws-1",
                run_id=RunId("run-1"),
                workflow_version_id="wf-version-1",
                step_id="probe",
                decision=decision,
                envelope=envelope,
                occurred_at=self._OCC,
                publisher=publisher,
            ),
        )
        assert len(publisher.events) == 1
        emitted = publisher.events[0]
        assert emitted.kind == LIFECYCLE_KIND_STEP_RETRY_SCHEDULED
        assert emitted.extra["next_attempt"] == 2
        assert emitted.extra["previous_code"] == "vendor.busy"
