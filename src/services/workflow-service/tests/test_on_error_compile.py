"""Tests for the on-error route compiler (WF-IMPL-023).

Each test reproduces one of the rules in
``design/components/workflow-service/design.md`` § Retry Policy →
§ Implicit ``on_error`` policy, § Where ``retry:`` may appear,
and § Publish-time validation. The compiler must enforce the
publish-time rules as defence-in-depth even though the Catalog
rejects first.
"""

from __future__ import annotations

import pytest

from custos_workflow.compiler import RetryPolicyCompileError
from custos_workflow.document import (
    ActivityStep,
    BackoffPolicy,
    LetStep,
    OnErrorAction,
    OnErrorArm,
    OnErrorMatch,
    RetryPolicy,
    WorkflowStep,
)
from custos_workflow.graph import (
    BackoffStrategyTag,
    JitterStrategyTag,
    OnErrorActionTag,
    ResolvedBackoffPolicy,
    ResolvedRetryPolicy,
)
from custos_workflow.on_error import compile_on_error
from custos_workflow.retry import resolve_step_retry


def _step_retry() -> ResolvedRetryPolicy:
    """The platform-default :class:`ResolvedRetryPolicy`.

    Mirrors what :func:`resolve_step_retry` produces for a step
    with no overrides and no ``spec.defaults.retry``.
    """
    return resolve_step_retry(None, None)


def _activity(
    *,
    step_id: str = "scan",
    on_error: list[OnErrorArm] | None = None,
    retry: RetryPolicy | None = None,
) -> ActivityStep:
    return ActivityStep(
        id=step_id,
        activity="security/scan@1",
        connector="primary",
        retry=retry,
        on_error=on_error,
    )


def _arm(
    *,
    cls: str | None = None,
    code: str | None = None,
    code_prefix: str | None = None,
    do: OnErrorAction,
    retry: RetryPolicy | None = None,
    max_attempts: int | None = None,
) -> OnErrorArm:
    return OnErrorArm(
        match=OnErrorMatch(code=code, codePrefix=code_prefix, **{"class": cls}),
        do=do,
        retry=retry,
        maxAttempts=max_attempts,
    )


# ---------------------------------------------------------------------------
# Implicit policy synthesis
# ---------------------------------------------------------------------------


class TestImplicitPolicy:
    def test_empty_on_error_produces_three_implicit_routes(self) -> None:
        # design.md § Implicit on_error policy: with no on_error
        # block, the synthesised table is exactly three arms.
        step = _activity(on_error=None)
        routes = compile_on_error(step, _step_retry())
        assert len(routes) == 3
        # Cancelled short-circuit always first.
        assert routes[0].cls == "cancelled"
        assert routes[0].action is OnErrorActionTag.FAIL
        assert routes[0].retry is None
        # Retryable → retry with the step-level policy.
        assert routes[1].cls == "retryable"
        assert routes[1].action is OnErrorActionTag.RETRY
        assert routes[1].retry == _step_retry()
        # Permanent → fail.
        assert routes[2].cls == "permanent"
        assert routes[2].action is OnErrorActionTag.FAIL
        assert routes[2].retry is None

    def test_cancelled_short_circuit_always_first_with_user_arms(self) -> None:
        # Even when the user declares arms, the cancelled
        # short-circuit is prepended so a misconfigured workflow
        # can never convert a cancellation into a retry loop.
        step = _activity(
            on_error=[
                _arm(code="E_TIMEOUT", do=OnErrorAction.RETRY),
            ],
        )
        routes = compile_on_error(step, _step_retry())
        assert routes[0].cls == "cancelled"
        assert routes[0].action is OnErrorActionTag.FAIL
        assert routes[0].retry is None

    def test_implicit_fallback_appended_after_user_arms(self) -> None:
        # "If no arm matches, the implicit policy above is the
        # fallback." Encode that fallback as appended routes so
        # the runtime walks one flat list.
        step = _activity(
            on_error=[
                _arm(code="E_TIMEOUT", do=OnErrorAction.SKIP),
            ],
        )
        routes = compile_on_error(step, _step_retry())
        # 1 (cancelled) + 1 (user) + 2 (fallback) = 4
        assert len(routes) == 4
        assert routes[-2].cls == "retryable"
        assert routes[-2].action is OnErrorActionTag.RETRY
        assert routes[-1].cls == "permanent"
        assert routes[-1].action is OnErrorActionTag.FAIL


# ---------------------------------------------------------------------------
# Arm projection
# ---------------------------------------------------------------------------


class TestArmProjection:
    def test_skip_arm_carries_no_retry(self) -> None:
        step = _activity(
            on_error=[_arm(code_prefix="E_RATE_", do=OnErrorAction.SKIP)],
        )
        routes = compile_on_error(step, _step_retry())
        skip_route = routes[1]  # after cancelled short-circuit
        assert skip_route.action is OnErrorActionTag.SKIP
        assert skip_route.code_prefix == "E_RATE_"
        assert skip_route.retry is None

    def test_fail_arm_carries_no_retry(self) -> None:
        step = _activity(
            on_error=[_arm(code="E_BAD_INPUT", do=OnErrorAction.FAIL)],
        )
        routes = compile_on_error(step, _step_retry())
        fail_route = routes[1]
        assert fail_route.action is OnErrorActionTag.FAIL
        assert fail_route.code == "E_BAD_INPUT"
        assert fail_route.retry is None

    def test_retry_arm_folds_inline_shorthand(self) -> None:
        # Inline ``maxAttempts: 5`` → ``retry: { maxAttempts: 5 }``
        # before resolution.
        step = _activity(
            on_error=[_arm(code="E_TIMEOUT", do=OnErrorAction.RETRY, max_attempts=5)],
        )
        routes = compile_on_error(step, _step_retry())
        retry_route = routes[1]
        assert retry_route.action is OnErrorActionTag.RETRY
        assert retry_route.retry is not None
        assert retry_route.retry.max_attempts == 5

    def test_retry_arm_overlays_backoff_field_by_field(self) -> None:
        # Per-arm backoff override overlays on top of the step
        # policy field-by-field — design.md § Retry Policy → §
        # Precedence.
        step = _activity(
            on_error=[
                _arm(
                    code="E_TIMEOUT",
                    do=OnErrorAction.RETRY,
                    retry=RetryPolicy(backoff=BackoffPolicy(initialDelay="PT10S")),
                ),
            ],
        )
        routes = compile_on_error(step, _step_retry())
        retry_route = routes[1]
        assert retry_route.retry is not None
        assert retry_route.retry.backoff.initial_delay_ms == 10_000
        # maxDelay falls through from the step policy (platform default).
        assert retry_route.retry.backoff.max_delay_ms == 300_000


# ---------------------------------------------------------------------------
# Publish-time rejections (defence-in-depth)
# ---------------------------------------------------------------------------


class TestRejections:
    def test_retry_on_permanent_arm_rejected(self) -> None:
        # design.md § Publish-time validation: do:retry on a
        # class:permanent arm is rejected.
        step = _activity(
            on_error=[_arm(cls="permanent", do=OnErrorAction.RETRY)],
        )
        with pytest.raises(RetryPolicyCompileError, match="class: permanent"):
            compile_on_error(step, _step_retry())

    def test_retry_on_cancelled_arm_rejected(self) -> None:
        step = _activity(
            on_error=[_arm(cls="cancelled", do=OnErrorAction.RETRY)],
        )
        with pytest.raises(RetryPolicyCompileError, match="class: cancelled"):
            compile_on_error(step, _step_retry())

    def test_retry_block_on_skip_arm_rejected(self) -> None:
        step = _activity(
            on_error=[
                _arm(
                    code="E_TIMEOUT",
                    do=OnErrorAction.SKIP,
                    retry=RetryPolicy(maxAttempts=3),
                ),
            ],
        )
        with pytest.raises(RetryPolicyCompileError, match="'do: skip'"):
            compile_on_error(step, _step_retry())

    def test_retry_block_on_fail_arm_rejected(self) -> None:
        step = _activity(
            on_error=[
                _arm(
                    code="E_BAD_INPUT",
                    do=OnErrorAction.FAIL,
                    retry=RetryPolicy(maxAttempts=3),
                ),
            ],
        )
        with pytest.raises(RetryPolicyCompileError, match="'do: fail'"):
            compile_on_error(step, _step_retry())

    def test_retry_on_let_step_rejected(self) -> None:
        # ``let:`` is on the disallowed-kinds table.
        step = LetStep(
            id="compute",
            let={"verdict": "fail"},
            retry=RetryPolicy(maxAttempts=3),
        )
        with pytest.raises(RetryPolicyCompileError, match="'retry:'"):
            compile_on_error(step, None)

    def test_on_error_on_let_step_rejected(self) -> None:
        step = LetStep(
            id="compute",
            let={"verdict": "fail"},
            on_error=[_arm(code="E_X", do=OnErrorAction.FAIL)],
        )
        with pytest.raises(RetryPolicyCompileError, match="'on_error:'"):
            compile_on_error(step, None)

    def test_retry_on_workflow_step_rejected(self) -> None:
        # Sub-workflow invocations own their own retry policy.
        step = WorkflowStep(
            id="invoke",
            workflow="acme/child@1.0.0",
            retry=RetryPolicy(maxAttempts=2),
        )
        with pytest.raises(RetryPolicyCompileError, match="'retry:'"):
            compile_on_error(step, None)


# ---------------------------------------------------------------------------
# Non-activity happy paths
# ---------------------------------------------------------------------------


class TestNonActivitySteps:
    def test_let_step_without_retry_yields_empty_routes(self) -> None:
        # A clean ``let:`` step has no compiled on-error routes.
        step = LetStep(id="compute", let={"verdict": "fail"})
        assert compile_on_error(step, None) == ()

    def test_workflow_step_without_retry_yields_empty_routes(self) -> None:
        step = WorkflowStep(id="invoke", workflow="acme/child@1.0.0")
        assert compile_on_error(step, None) == ()


# ---------------------------------------------------------------------------
# Cross-check: the synthesised step retry matches the platform defaults
# ---------------------------------------------------------------------------


class TestStepRetryShape:
    def test_step_retry_uses_platform_defaults(self) -> None:
        # Belt-and-braces guard: WF-IMPL-022's resolver produces the
        # exact resolved policy that compile_on_error then layers.
        # If this drifts, the implicit retryable arm's policy shape
        # silently shifts.
        assert _step_retry() == ResolvedRetryPolicy(
            max_attempts=3,
            backoff=ResolvedBackoffPolicy(
                strategy=BackoffStrategyTag.EXPONENTIAL,
                initial_delay_ms=1_000,
                max_delay_ms=300_000,
                multiplier=2.0,
            ),
            jitter=JitterStrategyTag.FULL,
            respect_retry_after=True,
        )
