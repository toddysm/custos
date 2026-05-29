"""Effective retry-policy resolver — implements the precedence overlay.

The overlay rules (design.md § Retry Policy → § Precedence) are
"most-specific wins, **field-by-field** — partial overrides are
supported". Concretely, for any ``do: retry`` decision the
effective policy is computed in two stages:

1. :func:`resolve_step_retry` folds the step-level layers:
   ``step.retry`` → ``spec.defaults.retry`` → platform defaults.
   The returned :class:`~custos_workflow.graph.ResolvedRetryPolicy`
   is the default for *any* ``do: retry`` on that step that does
   not carry a per-match override.

2. :func:`resolve_arm_retry` folds a single ``on_error[]`` arm's
   ``retry:`` block (with the inline ``maxAttempts:`` shorthand
   merged in) on top of the step-resolved policy. The result is
   the policy the Step Coordinator applies when this specific arm
   wins the routing decision.

The two-stage split is deliberate. The Compiler can compute the
step-level resolution once and cache it on the
:class:`~custos_workflow.graph.ExecutionNode`; per-arm resolution
then only needs the cached step-level policy plus the arm itself.

ISO-8601 duration parsing is deliberately tight: only the subset
the Catalog publish-time validator accepts
(``P[nD]T[nH][nM][nS]``, no fractional weeks/years) is supported.
Anything else raises :exc:`RetryResolutionError` — runtime should
have been caught at publish time, so an error here is a Catalog
bug and we want a loud failure.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING

from custos_workflow.document import (
    BackoffPolicy,
    BackoffStrategy,
    JitterStrategy,
    OnErrorAction,
    OnErrorArm,
    RetryPolicy,
)
from custos_workflow.graph import (
    BackoffStrategyTag,
    JitterStrategyTag,
    ResolvedBackoffPolicy,
    ResolvedRetryPolicy,
)
from custos_workflow.retry.defaults import PLATFORM_RETRY_DEFAULTS

if TYPE_CHECKING:
    from custos_workflow.document import Defaults


__all__ = [
    "RetryResolutionError",
    "resolve_arm_retry",
    "resolve_step_retry",
]


class RetryResolutionError(ValueError):
    """The overlay produced an inconsistent or unparseable policy.

    Raised for two failure modes:

    * Malformed ISO-8601 duration in any layer's ``initialDelay`` /
      ``maxDelay`` field. The Catalog publish-time validator should
      have rejected the document, so seeing one at compile time
      means the document slipped past validation.
    * An ``on_error[]`` arm carries both an inline ``maxAttempts:``
      shorthand and an inner ``retry: { maxAttempts: ... }`` with
      **conflicting** values. Matching values are allowed (the
      shorthand is just redundant); conflicting values are a hard
      error per design.md § Retry Policy → § Precedence.
    """


# ---------------------------------------------------------------------------
# ISO-8601 duration parsing
# ---------------------------------------------------------------------------

# ``P[nD]T[nH][nM][nS]`` — the subset the Catalog validator accepts.
# Weeks (``P1W``) and calendar units (``P1Y``, ``P1M`` at the date
# position) are deliberately rejected: a retry backoff measured in
# years would always overflow ``maxDelay`` clamping in practice and
# weeks lack a precise milliseconds conversion in the absence of a
# calendar context.
_DURATION_RE = re.compile(
    r"^P"
    r"(?:(?P<days>\d+(?:\.\d+)?)D)?"
    r"(?:T"
    r"(?:(?P<hours>\d+(?:\.\d+)?)H)?"
    r"(?:(?P<minutes>\d+(?:\.\d+)?)M)?"
    r"(?:(?P<seconds>\d+(?:\.\d+)?)S)?"
    r")?$"
)


def _parse_iso_duration_ms(token: str) -> int:
    """Convert ISO-8601 duration token (e.g. ``PT5M``) to milliseconds.

    Only the ``P[nD]T[nH][nM][nS]`` subset is accepted. The
    Catalog publish-time validator rejects everything else, so a
    failure here means the document slipped past validation —
    raises :exc:`RetryResolutionError` to surface the bug loudly.
    """
    match = _DURATION_RE.match(token)
    if match is None:
        raise RetryResolutionError(
            f"retry: malformed ISO-8601 duration {token!r} — expected P[nD]T[nH][nM][nS] subset",
        )
    groups = match.groupdict()
    # An all-empty match (``"P"`` or ``"PT"``) is invalid — there
    # must be at least one non-zero quantity.
    if not any(groups.values()):
        raise RetryResolutionError(
            f"retry: empty ISO-8601 duration {token!r}",
        )
    days = float(groups["days"] or 0)
    hours = float(groups["hours"] or 0)
    minutes = float(groups["minutes"] or 0)
    seconds = float(groups["seconds"] or 0)
    total_ms = round(
        ((((days * 24) + hours) * 60 + minutes) * 60 + seconds) * 1000,
    )
    if total_ms <= 0:
        # ``PT0S`` is a real ISO-8601 duration but a zero-millisecond
        # backoff would degenerate the retry curve into a tight loop.
        # Catalog rejects it; reject here too for defence-in-depth.
        raise RetryResolutionError(
            f"retry: ISO-8601 duration {token!r} must be > 0",
        )
    return int(total_ms)


# ---------------------------------------------------------------------------
# Document → wire-tag conversions
# ---------------------------------------------------------------------------

_BACKOFF_STRATEGY_MAP: dict[BackoffStrategy, BackoffStrategyTag] = {
    BackoffStrategy.CONSTANT: BackoffStrategyTag.CONSTANT,
    BackoffStrategy.LINEAR: BackoffStrategyTag.LINEAR,
    BackoffStrategy.EXPONENTIAL: BackoffStrategyTag.EXPONENTIAL,
}

_JITTER_MAP: dict[JitterStrategy, JitterStrategyTag] = {
    JitterStrategy.NONE: JitterStrategyTag.NONE,
    JitterStrategy.FULL: JitterStrategyTag.FULL,
    JitterStrategy.EQUAL: JitterStrategyTag.EQUAL,
    JitterStrategy.DECORRELATED: JitterStrategyTag.DECORRELATED,
}


# ---------------------------------------------------------------------------
# Field-by-field overlay
# ---------------------------------------------------------------------------


def _overlay_backoff(
    high: BackoffPolicy | None,
    low: BackoffPolicy | None,
) -> BackoffPolicy | None:
    """Overlay ``high`` over ``low`` field-by-field, ``high`` wins."""
    if high is None:
        return low
    if low is None:
        return high
    return BackoffPolicy(
        strategy=high.strategy if high.strategy is not None else low.strategy,
        initialDelay=(high.initial_delay if high.initial_delay is not None else low.initial_delay),
        maxDelay=high.max_delay if high.max_delay is not None else low.max_delay,
        multiplier=high.multiplier if high.multiplier is not None else low.multiplier,
    )


def _overlay_retry(
    high: RetryPolicy | None,
    low: RetryPolicy | None,
) -> RetryPolicy | None:
    """Overlay ``high`` over ``low`` field-by-field, ``high`` wins.

    Each scalar field overlays independently — partial overrides
    are the design's whole point: a step can override
    ``maxAttempts`` while inheriting the default ``backoff`` curve.

    ``backoff`` recurses into :func:`_overlay_backoff` so a
    partial backoff block (e.g. only ``initialDelay``) merges with
    the lower-priority layer's missing fields.
    """
    if high is None:
        return low
    if low is None:
        return high
    return RetryPolicy(
        maxAttempts=(high.max_attempts if high.max_attempts is not None else low.max_attempts),
        backoff=_overlay_backoff(high.backoff, low.backoff),
        jitter=high.jitter if high.jitter is not None else low.jitter,
        respectRetryAfter=(
            high.respect_retry_after
            if high.respect_retry_after is not None
            else low.respect_retry_after
        ),
    )


def _to_resolved(policy: RetryPolicy) -> ResolvedRetryPolicy:
    """Convert a fully-populated document policy to the resolved wire shape.

    Every field on ``policy`` MUST be non-``None`` — the platform
    defaults guarantee that after the full overlay chain. A
    ``None`` field here is a bug in the overlay logic, not a user
    error, so we assert rather than raise.
    """
    backoff = policy.backoff
    assert backoff is not None, "overlay invariant: backoff is set by platform defaults"
    assert backoff.strategy is not None, "overlay invariant: backoff.strategy set"
    assert backoff.initial_delay is not None, "overlay invariant: initial_delay set"
    assert backoff.max_delay is not None, "overlay invariant: max_delay set"
    assert policy.max_attempts is not None, "overlay invariant: max_attempts set"
    assert policy.jitter is not None, "overlay invariant: jitter set"
    assert policy.respect_retry_after is not None, "overlay invariant: respect_retry_after set"

    initial_ms = _parse_iso_duration_ms(backoff.initial_delay)
    max_ms = _parse_iso_duration_ms(backoff.max_delay)
    if max_ms < initial_ms:
        # Catalog rejects this at publish time; defence-in-depth
        # mirror so the compiler never produces a malformed
        # ResolvedBackoffPolicy.
        raise RetryResolutionError(
            f"retry: backoff.maxDelay ({backoff.max_delay!r} = {max_ms}ms) "
            f"< backoff.initialDelay ({backoff.initial_delay!r} = {initial_ms}ms)",
        )

    # Multiplier is only meaningful for ``exponential``. For
    # ``constant`` / ``linear`` we still emit the layered value so
    # the wire envelope is stable; the Step Coordinator ignores it
    # for non-exponential strategies.
    multiplier = backoff.multiplier if backoff.multiplier is not None else 2.0

    return ResolvedRetryPolicy(
        max_attempts=policy.max_attempts,
        backoff=ResolvedBackoffPolicy(
            strategy=_BACKOFF_STRATEGY_MAP[backoff.strategy],
            initial_delay_ms=initial_ms,
            max_delay_ms=max_ms,
            multiplier=multiplier,
        ),
        jitter=_JITTER_MAP[policy.jitter],
        respect_retry_after=policy.respect_retry_after,
    )


# ---------------------------------------------------------------------------
# Public resolvers
# ---------------------------------------------------------------------------


def resolve_step_retry(
    step_retry: RetryPolicy | None,
    spec_defaults: Defaults | None,
) -> ResolvedRetryPolicy:
    """Resolve the step-level effective retry policy.

    Overlay chain (most-specific first):

    1. ``step_retry`` — the ``retry:`` block on the step.
    2. ``spec_defaults.retry`` — ``spec.defaults.retry`` (if any).
    3. :data:`PLATFORM_RETRY_DEFAULTS`.

    Parameters
    ----------
    step_retry
        The :class:`~custos_workflow.document.RetryPolicy` parsed
        from the step's ``retry:`` block, or ``None`` if the step
        does not declare one.
    spec_defaults
        The workflow-level :class:`~custos_workflow.document.Defaults`
        block (``spec.defaults``) — only its ``retry`` attribute is
        consulted. ``None`` means the workflow declares no defaults.

    Returns
    -------
    A fully-populated :class:`ResolvedRetryPolicy` with every field
    filled from the appropriate layer.

    Raises
    ------
    RetryResolutionError
        If any layer carries a malformed ISO-8601 duration or the
        composed backoff has ``maxDelay < initialDelay``.
    """
    defaults_retry = spec_defaults.retry if spec_defaults is not None else None
    # Layer 1 over layer 2 over layer 4. ``spec.defaults.retry`` may
    # be absent — the overlay function handles ``None`` gracefully.
    layered = _overlay_retry(
        step_retry,
        _overlay_retry(defaults_retry, PLATFORM_RETRY_DEFAULTS),
    )
    # PLATFORM_RETRY_DEFAULTS is a fully-populated RetryPolicy so the
    # overlay result is also fully populated — assertion documented
    # in _to_resolved.
    assert layered is not None, "overlay invariant: platform defaults always present"
    return _to_resolved(layered)


def resolve_arm_retry(
    arm: OnErrorArm,
    step_resolved: ResolvedRetryPolicy,
) -> ResolvedRetryPolicy:
    """Resolve the effective policy for a single ``on_error[]`` arm.

    The arm's ``retry:`` block (and inline ``maxAttempts:``
    shorthand) overrides ``step_resolved`` field-by-field. The
    shorthand and the structured block can coexist as long as they
    agree on ``maxAttempts``; conflicting values raise
    :exc:`RetryResolutionError`.

    Parameters
    ----------
    arm
        The :class:`~custos_workflow.document.OnErrorArm`. Must
        have ``do == retry`` — passing a ``skip`` / ``fail`` arm is
        a programmer error (those arms never consume a retry
        policy). The check stays at the call-site so the resolver
        does not need to be aware of routing semantics.
    step_resolved
        The :class:`ResolvedRetryPolicy` returned by
        :func:`resolve_step_retry` for the same step.

    Returns
    -------
    A fully-populated :class:`ResolvedRetryPolicy` — every field
    either comes from the arm's override or from
    ``step_resolved``.

    Raises
    ------
    RetryResolutionError
        If the inline ``maxAttempts:`` shorthand and the structured
        ``retry: { maxAttempts: ... }`` block on the same arm carry
        conflicting values, or if any duration in the arm's
        ``retry:`` block is malformed.
    """
    if arm.do is not OnErrorAction.RETRY:
        # Programmer error — the routing layer should have filtered
        # these out before the mechanics layer is asked to resolve
        # anything. Catch it loudly so the bug surfaces at the call
        # site rather than producing a meaningless ResolvedRetryPolicy.
        raise RetryResolutionError(
            f"resolve_arm_retry called on arm with do={arm.do.value!r} "
            "(only do=retry consumes a retry policy)",
        )

    arm_retry = _fold_shorthand(arm)

    # The arm's override is layer 1; step_resolved is layers 2+3+4
    # already collapsed. We do NOT round-trip step_resolved back
    # through _overlay_retry — that would lose the millisecond
    # representation. Instead we build a synthetic RetryPolicy from
    # arm_retry and selectively replace only the fields the arm
    # specifies.
    if arm_retry is None:
        # ``do: retry`` with no override — the arm inherits the
        # step's resolved policy verbatim.
        return step_resolved

    merged_max = (
        arm_retry.max_attempts if arm_retry.max_attempts is not None else step_resolved.max_attempts
    )
    merged_jitter = (
        _JITTER_MAP[arm_retry.jitter] if arm_retry.jitter is not None else step_resolved.jitter
    )
    merged_respect = (
        arm_retry.respect_retry_after
        if arm_retry.respect_retry_after is not None
        else step_resolved.respect_retry_after
    )
    merged_backoff = _merge_backoff_over_resolved(arm_retry.backoff, step_resolved.backoff)

    return ResolvedRetryPolicy(
        max_attempts=merged_max,
        backoff=merged_backoff,
        jitter=merged_jitter,
        respect_retry_after=merged_respect,
    )


def _fold_shorthand(arm: OnErrorArm) -> RetryPolicy | None:
    """Fold ``arm.max_attempts`` shorthand into ``arm.retry``.

    Returns the combined policy or ``None`` if the arm declares
    neither shorthand nor structured override.

    Raises :exc:`RetryResolutionError` when shorthand and
    structured value disagree (design.md § Retry Policy →
    § Precedence: "conflict promotes to error").
    """
    if arm.max_attempts is None:
        return arm.retry
    if arm.retry is None:
        return RetryPolicy(maxAttempts=arm.max_attempts)
    if arm.retry.max_attempts is not None and arm.retry.max_attempts != arm.max_attempts:
        raise RetryResolutionError(
            f"on_error arm: inline maxAttempts={arm.max_attempts} conflicts with "
            f"retry.maxAttempts={arm.retry.max_attempts}",
        )
    # Either ``arm.retry.max_attempts`` is None (shorthand fills
    # it in) or it equals the shorthand (no-op). Either way the
    # structured block with the shorthand value wins.
    return RetryPolicy(
        maxAttempts=arm.max_attempts,
        backoff=arm.retry.backoff,
        jitter=arm.retry.jitter,
        respectRetryAfter=arm.retry.respect_retry_after,
    )


def _merge_backoff_over_resolved(
    arm_backoff: BackoffPolicy | None,
    resolved: ResolvedBackoffPolicy,
) -> ResolvedBackoffPolicy:
    """Overlay an arm-level (possibly partial) backoff over a resolved one.

    Field-by-field: each scalar on ``arm_backoff`` overrides the
    corresponding field on ``resolved``. Duration strings are
    parsed into milliseconds at this layer so the returned
    :class:`ResolvedBackoffPolicy` is wire-ready.

    Cross-field consistency (``max_delay >= initial_delay``) is
    re-checked because the arm may move ``initial_delay`` above
    the inherited ``max_delay`` (or vice versa) and produce an
    invalid combination that neither input had on its own.
    """
    if arm_backoff is None:
        return resolved

    initial_ms = (
        _parse_iso_duration_ms(arm_backoff.initial_delay)
        if arm_backoff.initial_delay is not None
        else resolved.initial_delay_ms
    )
    max_ms = (
        _parse_iso_duration_ms(arm_backoff.max_delay)
        if arm_backoff.max_delay is not None
        else resolved.max_delay_ms
    )
    if max_ms < initial_ms:
        raise RetryResolutionError(
            f"retry: arm backoff maxDelay ({max_ms}ms) "
            f"< initialDelay ({initial_ms}ms) after overlay",
        )
    strategy_tag = (
        _BACKOFF_STRATEGY_MAP[arm_backoff.strategy]
        if arm_backoff.strategy is not None
        else resolved.strategy
    )
    multiplier = (
        arm_backoff.multiplier if arm_backoff.multiplier is not None else resolved.multiplier
    )
    return ResolvedBackoffPolicy(
        strategy=strategy_tag,
        initial_delay_ms=initial_ms,
        max_delay_ms=max_ms,
        multiplier=multiplier,
    )
