"""Tests for the effective retry-policy resolver (WF-IMPL-022).

Each test reproduces one of the precedence-rule examples in
``design/components/workflow-service/design.md`` § Retry Policy →
§ Precedence. The acceptance criterion for WF-IMPL-022 is "every
example in design.md § Retry Policy → § Schema is reproduced by a
unit test" — the tests below cover:

* Empty step → platform defaults.
* Step with partial override → mixed result.
* ``spec.defaults`` partial + step partial → three-layer overlay.
* Per-match override → four-layer overlay.
* Each field overlays independently (no whole-block replacement).
* Shorthand ``maxAttempts:`` on an ``on_error`` arm.
* Conflicting shorthand vs structured maxAttempts → error.
* Malformed ISO-8601 duration → :exc:`RetryResolutionError`.
* ``maxDelay < initialDelay`` after overlay → error.
* ``do: skip`` / ``do: fail`` arms passed to ``resolve_arm_retry``
  → error (programmer mistake).
"""

from __future__ import annotations

import pytest

from custos_workflow.document import (
    BackoffPolicy,
    BackoffStrategy,
    Defaults,
    JitterStrategy,
    OnErrorAction,
    OnErrorArm,
    OnErrorMatch,
    RetryPolicy,
)
from custos_workflow.graph import (
    BackoffStrategyTag,
    JitterStrategyTag,
    ResolvedBackoffPolicy,
    ResolvedRetryPolicy,
)
from custos_workflow.retry import (
    PLATFORM_RETRY_DEFAULTS,
    RetryResolutionError,
    resolve_arm_retry,
    resolve_step_retry,
)

# ---------------------------------------------------------------------------
# Step-level resolver
# ---------------------------------------------------------------------------


class TestResolveStepRetry:
    def test_no_step_retry_no_defaults_yields_platform_defaults(self) -> None:
        # Layer 4 (platform defaults) wins by default.
        resolved = resolve_step_retry(None, None)
        assert resolved == ResolvedRetryPolicy(
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

    def test_partial_step_overrides_platform_defaults_per_field(self) -> None:
        # Step pins ``maxAttempts``; every other field falls through.
        step = RetryPolicy(maxAttempts=10)
        resolved = resolve_step_retry(step, None)
        assert resolved.max_attempts == 10
        # Backoff curve untouched.
        assert resolved.backoff.strategy is BackoffStrategyTag.EXPONENTIAL
        assert resolved.backoff.initial_delay_ms == 1_000
        assert resolved.backoff.max_delay_ms == 300_000
        # Jitter and retryAfter still platform defaults.
        assert resolved.jitter is JitterStrategyTag.FULL
        assert resolved.respect_retry_after is True

    def test_partial_backoff_overrides_field_by_field(self) -> None:
        # Step only pins ``initialDelay``; ``maxDelay`` /
        # ``multiplier`` / ``strategy`` keep platform values.
        step = RetryPolicy(backoff=BackoffPolicy(initialDelay="PT5S"))
        resolved = resolve_step_retry(step, None)
        assert resolved.backoff.initial_delay_ms == 5_000
        assert resolved.backoff.max_delay_ms == 300_000
        assert resolved.backoff.strategy is BackoffStrategyTag.EXPONENTIAL
        assert resolved.backoff.multiplier == 2.0

    def test_spec_defaults_fill_in_gaps_below_step(self) -> None:
        # design.md § Retry Policy → § Schema example: workflow-level
        # ``spec.defaults.retry`` is mostly populated; the step adds
        # ``maxAttempts``. Expected three-layer overlay:
        #   step.maxAttempts (5) → wins
        #   defaults.backoff / jitter → wins over platform
        #   defaults missing respectRetryAfter → platform fills in
        defaults = Defaults(
            retry=RetryPolicy(
                maxAttempts=4,
                backoff=BackoffPolicy(
                    strategy=BackoffStrategy.EXPONENTIAL,
                    initialDelay="PT2S",
                    maxDelay="PT2M",
                ),
                jitter=JitterStrategy.EQUAL,
            ),
        )
        step = RetryPolicy(maxAttempts=5)
        resolved = resolve_step_retry(step, defaults)
        assert resolved.max_attempts == 5  # step wins
        assert resolved.backoff.initial_delay_ms == 2_000  # defaults
        assert resolved.backoff.max_delay_ms == 120_000  # defaults
        assert resolved.backoff.strategy is BackoffStrategyTag.EXPONENTIAL
        assert resolved.backoff.multiplier == 2.0  # platform (defaults left it unset)
        assert resolved.jitter is JitterStrategyTag.EQUAL  # defaults
        assert resolved.respect_retry_after is True  # platform

    def test_defaults_only_when_step_is_none(self) -> None:
        defaults = Defaults(retry=RetryPolicy(maxAttempts=7, jitter=JitterStrategy.NONE))
        resolved = resolve_step_retry(None, defaults)
        assert resolved.max_attempts == 7
        assert resolved.jitter is JitterStrategyTag.NONE
        # Backoff fully from platform.
        assert resolved.backoff.initial_delay_ms == 1_000

    def test_malformed_duration_raises(self) -> None:
        step = RetryPolicy(backoff=BackoffPolicy(initialDelay="100ms"))
        with pytest.raises(RetryResolutionError, match="malformed ISO-8601"):
            resolve_step_retry(step, None)

    def test_max_delay_below_initial_delay_raises(self) -> None:
        # Step lowers maxDelay below the inherited platform initialDelay.
        step = RetryPolicy(backoff=BackoffPolicy(maxDelay="PT500MS"))
        with pytest.raises(RetryResolutionError):
            resolve_step_retry(step, None)

    def test_constant_strategy_keeps_multiplier_value(self) -> None:
        # ``multiplier`` is only meaningful for exponential, but the
        # resolver still emits the layered value so the wire envelope
        # is stable for downstream consumers.
        step = RetryPolicy(
            backoff=BackoffPolicy(strategy=BackoffStrategy.CONSTANT, initialDelay="PT2S"),
        )
        resolved = resolve_step_retry(step, None)
        assert resolved.backoff.strategy is BackoffStrategyTag.CONSTANT
        assert resolved.backoff.multiplier == 2.0  # from platform default


# ---------------------------------------------------------------------------
# Arm-level resolver
# ---------------------------------------------------------------------------


def _arm_retry(retry: RetryPolicy | None = None, max_attempts: int | None = None) -> OnErrorArm:
    return OnErrorArm(
        match=OnErrorMatch(code="E_TEST"),
        do=OnErrorAction.RETRY,
        retry=retry,
        maxAttempts=max_attempts,
    )


class TestResolveArmRetry:
    def test_arm_with_no_override_inherits_step_resolved(self) -> None:
        step_resolved = resolve_step_retry(None, None)
        arm = _arm_retry()
        resolved = resolve_arm_retry(arm, step_resolved)
        assert resolved == step_resolved

    def test_per_match_max_attempts_wins_over_step(self) -> None:
        # design.md § Retry Policy → § Schema example: per-match arm
        # only overrides ``maxAttempts`` and the backoff curve; jitter
        # / respectRetryAfter inherit the step resolution.
        step_resolved = ResolvedRetryPolicy(
            max_attempts=5,
            backoff=ResolvedBackoffPolicy(
                strategy=BackoffStrategyTag.EXPONENTIAL,
                initial_delay_ms=1_000,
                max_delay_ms=300_000,
                multiplier=2.0,
            ),
            jitter=JitterStrategyTag.FULL,
            respect_retry_after=True,
        )
        arm = _arm_retry(
            retry=RetryPolicy(
                maxAttempts=10,
                backoff=BackoffPolicy(
                    strategy=BackoffStrategy.EXPONENTIAL,
                    initialDelay="PT5S",
                    maxDelay="PT10M",
                ),
            ),
        )
        resolved = resolve_arm_retry(arm, step_resolved)
        assert resolved.max_attempts == 10  # arm wins
        assert resolved.backoff.initial_delay_ms == 5_000
        assert resolved.backoff.max_delay_ms == 600_000  # PT10M
        assert resolved.jitter is JitterStrategyTag.FULL  # inherited
        assert resolved.respect_retry_after is True  # inherited

    def test_arm_partial_backoff_overrides_field_by_field(self) -> None:
        # Arm only changes ``initialDelay`` — ``maxDelay`` /
        # ``strategy`` / ``multiplier`` survive from step_resolved.
        step_resolved = resolve_step_retry(None, None)
        arm = _arm_retry(retry=RetryPolicy(backoff=BackoffPolicy(initialDelay="PT3S")))
        resolved = resolve_arm_retry(arm, step_resolved)
        assert resolved.backoff.initial_delay_ms == 3_000
        assert resolved.backoff.max_delay_ms == 300_000  # from step_resolved
        assert resolved.backoff.strategy is BackoffStrategyTag.EXPONENTIAL

    def test_inline_max_attempts_shorthand_alone(self) -> None:
        # ``maxAttempts: N`` shorthand on the arm without a structured
        # ``retry:`` block.
        step_resolved = resolve_step_retry(None, None)
        arm = _arm_retry(max_attempts=2)
        resolved = resolve_arm_retry(arm, step_resolved)
        assert resolved.max_attempts == 2
        # Everything else inherits step_resolved.
        assert resolved.backoff == step_resolved.backoff

    def test_shorthand_and_structured_agree_is_fine(self) -> None:
        step_resolved = resolve_step_retry(None, None)
        arm = _arm_retry(retry=RetryPolicy(maxAttempts=4), max_attempts=4)
        resolved = resolve_arm_retry(arm, step_resolved)
        assert resolved.max_attempts == 4

    def test_shorthand_and_structured_disagree_raises(self) -> None:
        step_resolved = resolve_step_retry(None, None)
        arm = _arm_retry(retry=RetryPolicy(maxAttempts=7), max_attempts=2)
        with pytest.raises(RetryResolutionError, match="conflicts"):
            resolve_arm_retry(arm, step_resolved)

    def test_non_retry_arm_rejected(self) -> None:
        # ``resolve_arm_retry`` is only meaningful for ``do: retry``.
        step_resolved = resolve_step_retry(None, None)
        skip_arm = OnErrorArm(match=OnErrorMatch(code="E"), do=OnErrorAction.SKIP)
        with pytest.raises(RetryResolutionError, match="only do=retry"):
            resolve_arm_retry(skip_arm, step_resolved)

    def test_arm_backoff_inverted_after_overlay_raises(self) -> None:
        # Arm only sets ``initialDelay`` above the inherited
        # ``maxDelay`` → cross-field consistency check must fire.
        step_resolved = ResolvedRetryPolicy(
            max_attempts=3,
            backoff=ResolvedBackoffPolicy(
                strategy=BackoffStrategyTag.EXPONENTIAL,
                initial_delay_ms=1_000,
                max_delay_ms=2_000,
                multiplier=2.0,
            ),
            jitter=JitterStrategyTag.FULL,
            respect_retry_after=True,
        )
        arm = _arm_retry(retry=RetryPolicy(backoff=BackoffPolicy(initialDelay="PT1H")))
        with pytest.raises(RetryResolutionError):
            resolve_arm_retry(arm, step_resolved)

    def test_arm_jitter_and_respect_overrides(self) -> None:
        step_resolved = resolve_step_retry(None, None)
        arm = _arm_retry(
            retry=RetryPolicy(jitter=JitterStrategy.DECORRELATED, respectRetryAfter=False),
        )
        resolved = resolve_arm_retry(arm, step_resolved)
        assert resolved.jitter is JitterStrategyTag.DECORRELATED
        assert resolved.respect_retry_after is False


# ---------------------------------------------------------------------------
# Platform-defaults sanity
# ---------------------------------------------------------------------------


class TestPlatformDefaults:
    def test_platform_defaults_match_design_md(self) -> None:
        # design.md § Retry Policy → § Precedence (Platform defaults).
        assert PLATFORM_RETRY_DEFAULTS.max_attempts == 3
        assert PLATFORM_RETRY_DEFAULTS.respect_retry_after is True
        assert PLATFORM_RETRY_DEFAULTS.jitter is JitterStrategy.FULL
        backoff = PLATFORM_RETRY_DEFAULTS.backoff
        assert backoff is not None
        assert backoff.strategy is BackoffStrategy.EXPONENTIAL
        assert backoff.initial_delay == "PT1S"
        assert backoff.max_delay == "PT5M"
        assert backoff.multiplier == 2.0


# ---------------------------------------------------------------------------
# ISO-8601 duration parsing
# ---------------------------------------------------------------------------


class TestDurationParsing:
    @pytest.mark.parametrize(
        ("token", "expected_ms"),
        [
            ("PT1S", 1_000),
            ("PT5M", 300_000),
            ("PT1H", 3_600_000),
            ("P1D", 86_400_000),
            ("PT1H30M", 5_400_000),
            ("PT0.5S", 500),
        ],
    )
    def test_valid_durations(self, token: str, expected_ms: int) -> None:
        step = RetryPolicy(backoff=BackoffPolicy(initialDelay=token, maxDelay="P1D"))
        resolved = resolve_step_retry(step, None)
        assert resolved.backoff.initial_delay_ms == expected_ms

    @pytest.mark.parametrize(
        "token",
        [
            "100ms",  # not ISO-8601
            "1s",  # missing P / T prefix
            "P1W",  # weeks deliberately rejected
            "P1Y",  # years deliberately rejected
            "PT",  # empty
            "P",  # empty
            "PT-1S",  # negative
        ],
    )
    def test_invalid_durations_raise(self, token: str) -> None:
        step = RetryPolicy(backoff=BackoffPolicy(initialDelay=token))
        with pytest.raises(RetryResolutionError):
            resolve_step_retry(step, None)
