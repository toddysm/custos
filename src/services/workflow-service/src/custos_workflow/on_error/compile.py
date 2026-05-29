"""Compile a step's ``on_error:`` block into a flat route table.

The output is a tuple of :class:`OnErrorRoute` entries that the
Step Coordinator walks in declaration order; the first match
wins. The compiler bakes the design's runtime rules into the
data so the runtime never re-interprets the YAML:

* A ``class: cancelled`` short-circuit route is **always**
  prepended as the first entry — regardless of what the user
  declared — so an operator-initiated cancellation can never be
  silently converted into a retry loop
  (design.md § Implicit ``on_error`` policy).

* When the step declares **no** ``on_error:`` block, the three
  implicit arms (``cancelled → fail``, ``retryable → retry``,
  ``permanent → fail``) are synthesised.

* When the step declares an ``on_error:`` block, each arm is
  validated, its inline ``maxAttempts: N`` shorthand expanded,
  and (for ``do: retry`` arms) its per-arm overlay folded on
  top of the step-level resolved policy. The implicit
  ``retryable → retry`` / ``permanent → fail`` fallback is
  appended after the user's arms so a partial declaration still
  has the documented behaviour for unmatched envelopes.

* Disallowed-kind ``retry:`` / ``on_error:`` blocks
  (design.md § Where ``retry:`` may appear — ``let:`` and
  ``workflow:`` are the only kinds modelled today) are rejected
  with :class:`RetryPolicyCompileError`.

* ``do: retry`` on a ``class: permanent`` or ``class: cancelled``
  arm and a ``retry:`` block on a ``do: skip`` / ``do: fail``
  arm are rejected with the same exception class — these are
  defence-in-depth duplicates of Catalog publish-time rules.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from custos_workflow.document import (
    ActivityStep,
    OnErrorAction,
)
from custos_workflow.graph import (
    OnErrorActionTag,
    OnErrorRoute,
)
from custos_workflow.retry import RetryResolutionError, resolve_arm_retry

if TYPE_CHECKING:
    from custos_workflow.document import OnErrorArm, Step
    from custos_workflow.graph import ResolvedRetryPolicy


# Match-class string values used by both the document model
# (:class:`~custos_workflow.document.OnErrorMatch.cls`) and the
# compiled graph (:attr:`OnErrorRoute.cls`).
_CANCELLED_CLASS = "cancelled"
_PERMANENT_CLASS = "permanent"
_RETRYABLE_CLASS = "retryable"


def compile_on_error(
    step: Step,
    step_retry: ResolvedRetryPolicy | None,
) -> tuple[OnErrorRoute, ...]:
    """Compile a step's ``on_error:`` block into an ordered route table.

    Args:
        step: The source step (document model).
        step_retry: The step-level :class:`ResolvedRetryPolicy`
            already produced by
            :func:`custos_workflow.retry.resolve_step_retry`, or
            ``None`` when the step kind does not participate in
            retry. Required when *step* is an
            :class:`ActivityStep`.

    Returns:
        An empty tuple for non-activity step kinds (which carry
        no compiled on-error routes), or an ordered tuple of
        :class:`OnErrorRoute` entries for activity steps.

    Raises:
        RetryPolicyCompileError: A structural validation failed —
            see the module docstring for the rules enforced.
    """
    # ---- Structural rejection for disallowed step kinds ----------
    # The document model attaches ``retry:`` and ``on_error:`` to
    # every step kind via ``_StepCommon`` for parsing simplicity,
    # but the design only permits them on activity steps. Reject
    # at compile time as defence-in-depth — the Catalog publish
    # validator rejects first, but the compiler must not silently
    # accept a malformed document either.
    if not isinstance(step, ActivityStep):
        # Local import to avoid a circular module dependency:
        # ``compiler`` already imports from ``on_error``.
        from custos_workflow.compiler import RetryPolicyCompileError

        kind_name = type(step).__name__
        if step.retry is not None:
            raise RetryPolicyCompileError(
                f"compile: step {step.id!r}: 'retry:' is not allowed "
                f"on step kind {kind_name} (design.md § Where retry: "
                "may appear)",
            )
        if step.on_error is not None:
            # Check presence rather than truthiness so an explicit
            # empty ``on_error: []`` on a disallowed step kind is
            # rejected the same way as a populated block.
            raise RetryPolicyCompileError(
                f"compile: step {step.id!r}: 'on_error:' is not "
                f"allowed on step kind {kind_name} (design.md § "
                "Where retry: may appear)",
            )
        return ()

    # An ActivityStep must carry a resolved retry policy (the
    # caller is responsible for computing it via
    # :func:`resolve_step_retry` first). Treat a missing policy
    # as a programmer error rather than a document validation
    # failure.
    assert step_retry is not None, (
        "compile_on_error: ActivityStep requires a resolved step-level retry policy"
    )

    # ---- Always prepend the cancelled short-circuit --------------
    # design.md § Implicit on_error policy: cancellation is
    # terminal by design and is never retried. The user may try
    # to declare ``class: cancelled → retry`` (or any other
    # action) but the prepended route always matches first.
    routes: list[OnErrorRoute] = [
        OnErrorRoute(action=OnErrorActionTag.FAIL, cls=_CANCELLED_CLASS),
    ]

    # ---- User-declared arms --------------------------------------
    for arm in step.on_error or ():
        routes.append(_compile_arm(step.id, arm, step_retry))

    # ---- Implicit fallback for retryable / permanent -------------
    # design.md § Implicit on_error policy: "If no arm matches,
    # the implicit policy above is the fallback." Encode that
    # fallback directly in the route table so the runtime walks
    # one flat list rather than consulting a separate implicit
    # policy table when nothing matches. First-match-wins makes
    # any user arm covering the same class shadow the fallback.
    routes.append(
        OnErrorRoute(
            action=OnErrorActionTag.RETRY,
            cls=_RETRYABLE_CLASS,
            retry=step_retry,
        ),
    )
    routes.append(
        OnErrorRoute(action=OnErrorActionTag.FAIL, cls=_PERMANENT_CLASS),
    )
    return tuple(routes)


def _compile_arm(
    step_id: str,
    arm: OnErrorArm,
    step_retry: ResolvedRetryPolicy,
) -> OnErrorRoute:
    """Validate one declared arm and project it onto an :class:`OnErrorRoute`."""
    # Local import — see :func:`compile_on_error` for the
    # circular-import rationale.
    from custos_workflow.compiler import RetryPolicyCompileError

    # Reject ``do: retry`` on permanent / cancelled classes.
    # design.md § Publish-time validation: "do: retry on a
    # match: { class: permanent|cancelled } arm" is rejected.
    if arm.do is OnErrorAction.RETRY and arm.match.cls in (
        _PERMANENT_CLASS,
        _CANCELLED_CLASS,
    ):
        raise RetryPolicyCompileError(
            f"compile: step {step_id!r}: 'do: retry' is not allowed "
            f"on a 'class: {arm.match.cls}' arm (design.md § Publish-"
            "time validation)",
        )

    # Reject a ``retry:`` block on a non-retry action.
    # design.md § Publish-time validation: "an on_error[] arm
    # with do: skip or do: fail carries a retry: block
    # (mechanics without retry action is incoherent)".
    if arm.do is not OnErrorAction.RETRY and arm.retry is not None:
        raise RetryPolicyCompileError(
            f"compile: step {step_id!r}: 'retry:' block is not allowed "
            f"on a 'do: {arm.do.value}' arm (design.md § Publish-time "
            "validation)",
        )

    # Fold per-arm retry overlay (shorthand expansion + field-by-
    # field merge over the step-level policy) for ``do: retry``.
    # SKIP / FAIL arms carry no retry policy.
    # Wrap ``RetryResolutionError`` (raised by the resolver for
    # validation failures such as conflicting inline
    # ``maxAttempts`` vs structured ``retry.maxAttempts``, or
    # ``maxDelay < initialDelay`` after overlay) in
    # :class:`RetryPolicyCompileError` so direct callers of
    # :func:`compile_on_error` see the same exception type the
    # :func:`~custos_workflow.compiler.compile` driver surfaces.
    if arm.do is OnErrorAction.RETRY:
        try:
            resolved: ResolvedRetryPolicy | None = resolve_arm_retry(arm, step_retry)
        except RetryResolutionError as exc:
            raise RetryPolicyCompileError(
                f"compile: step {step_id!r}: on_error arm rejected by retry-policy resolver: {exc}",
            ) from exc
    else:
        resolved = None
    return OnErrorRoute(
        action=OnErrorActionTag(arm.do.value),
        code=arm.match.code,
        code_prefix=arm.match.code_prefix,
        cls=arm.match.cls,
        retry=resolved,
    )
