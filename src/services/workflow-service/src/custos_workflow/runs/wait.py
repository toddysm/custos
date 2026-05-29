"""WF-IMPL-036 — ``WaitStepHandler`` (Run Controller's ``wait:`` step driver).

``wait:`` is the one step kind the Run Controller orchestrator
handles directly per design.md § Workflow Schema: Step Kinds
Handled — ``Wait / sleep → Run Controller → Durable timer``. No
Step Coordinator handler is consulted; the orchestrator (WF-IMPL-035)
delegates each ``kind=WAIT`` node to :class:`WaitStepHandler.execute`,
which parses the step's ISO-8601 duration, opens a Dapr durable
timer via :meth:`WorkflowContext.create_timer`, and yields the token
back to the Dapr runtime so the workflow instance suspends until
the timer fires.

Replay safety
-------------

The duration is read from :attr:`~custos_workflow.document.WaitStep.wait`
on the compiled graph (never re-fetched from Catalog), so every
replay parses the *same* constant string into the *same*
:class:`~datetime.timedelta`. The Dapr runtime issues a durable
timer keyed off the orchestrator's deterministic call-site, so
across a pod restart / replay the resumption point is byte-equal.

Defensive guard
---------------

The Catalog publish gate and the document model both reject
malformed ``wait:`` strings before they can reach the runtime.
:class:`WaitDurationError` is the defence-in-depth raise the
orchestrator surfaces if a schema-skewed graph somehow makes it
through — better to fail the run with a typed,
``compile.wait_duration``-tagged envelope than to feed garbage to
``ctx.create_timer``.
"""

from __future__ import annotations

import re
from collections.abc import Generator
from datetime import timedelta
from typing import TYPE_CHECKING, Any, Final

from custos_workflow.document import WaitStep
from custos_workflow.errors import CompileError
from custos_workflow.graph.model import StepKind
from custos_workflow.runs.step_handler import StepSucceeded

if TYPE_CHECKING:
    from custos_workflow.graph.model import ExecutionNode
    from custos_workflow.runs.step_handler import WorkflowContext

__all__ = [
    "WaitDurationError",
    "WaitStepHandler",
    "parse_wait_duration",
]


# ---------------------------------------------------------------------------
# ISO-8601 duration grammar
# ---------------------------------------------------------------------------


#: Mirror of :data:`custos_workflow.document.models._ISO8601_DURATION_PATTERN`.
#:
#: Owned independently here so the defensive guard is self-contained
#: (the document model is a publish-time concern; this module is the
#: runtime concern). The two patterns MUST stay in lockstep — a
#: stricter document-side pattern would be silently re-accepted by
#: this looser guard, and a stricter runtime-side pattern would
#: reject a graph the document model accepted. The unit tests pin
#: byte-equality between the two regex sources.
_ISO8601_DURATION_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^P(?:"
    r"(?P<weeks>\d+)W"
    r"|"
    r"(?:(?P<days>\d+)D)?"
    r"(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?"
    r")$"
)


# ---------------------------------------------------------------------------
# Typed error
# ---------------------------------------------------------------------------


class WaitDurationError(CompileError, ValueError):
    """A ``wait:`` step's duration is structurally invalid.

    Raised at orchestrator entry when a compiled graph carries a
    :class:`~custos_workflow.document.WaitStep` whose ``wait``
    string does not parse as a positive ISO-8601 duration. The
    document model and the Catalog publish gate both reject this
    shape — surfacing it here is defence in depth so a
    schema-skewed graph fails loudly rather than silently asking
    the Dapr runtime for a zero / negative / unparseable timer.

    Pinning the ``compile.wait_duration`` kind keeps the audit
    envelope aligned with the rest of the ``compile.*`` taxonomy
    (WF-IMPL-024) even though the diagnosis happens at run time.
    """

    KIND: Final[str] = "compile.wait_duration"  # type: ignore[misc]

    def __init__(self, step_id: str, duration: str, reason: str) -> None:
        super().__init__(
            f"wait step {step_id!r}: invalid duration {duration!r}: {reason}",
        )
        self.step_id: str = step_id
        self.duration: str = duration
        self.reason: str = reason

    def _extra_fields(self) -> dict[str, Any]:
        return {
            "step_id": self.step_id,
            "duration": self.duration,
            "reason": self.reason,
        }


# ---------------------------------------------------------------------------
# Duration parser
# ---------------------------------------------------------------------------


def parse_wait_duration(step_id: str, duration: str) -> timedelta:
    """Parse an ISO-8601 duration string into a positive :class:`timedelta`.

    Accepts the same grammar the document model enforces at parse
    time: ``PnW`` weeks form OR ``P[nD][T[nH][nM][nS]]`` with at
    least one component. Months / years are NOT accepted — they are
    calendar-dependent and would translate to a non-durable timer.

    Args:
        step_id: The originating step id, surfaced in the
            :class:`WaitDurationError` for operator triage.
        duration: The raw ISO-8601 duration string from
            :attr:`~custos_workflow.document.WaitStep.wait`.

    Returns:
        The parsed positive :class:`~datetime.timedelta`.

    Raises:
        WaitDurationError: ``duration`` does not match the grammar,
            or parses to a non-positive value.
    """
    match = _ISO8601_DURATION_PATTERN.match(duration)
    if match is None:
        raise WaitDurationError(step_id, duration, "not a recognised ISO-8601 duration")

    weeks = int(match.group("weeks") or 0)
    days = int(match.group("days") or 0)
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes") or 0)
    seconds = float(match.group("seconds") or 0.0)
    # The regex permits structurally-empty shapes (``P``, ``PT``)
    # because every component is optional. Surface those as a
    # distinct "no components" failure so the audit envelope
    # distinguishes a missing-payload bug from an explicit-zero bug.
    if weeks == 0 and days == 0 and hours == 0 and minutes == 0 and seconds == 0.0:
        if any(ch.isdigit() for ch in duration):
            raise WaitDurationError(step_id, duration, "duration must be greater than zero")
        raise WaitDurationError(step_id, duration, "duration must specify at least one component")
    # ``\d+`` in the regex forbids negative components, and the
    # all-zero shape is already rejected above, so the constructed
    # timedelta is guaranteed positive here.
    return timedelta(
        weeks=weeks,
        days=days,
        hours=hours,
        minutes=minutes,
        seconds=seconds,
    )


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------


class WaitStepHandler:
    """Run Controller's inline driver for ``kind=WAIT`` nodes.

    Not a :class:`~custos_workflow.runs.step_handler.StepHandler`
    implementation: the StepHandler protocol returns a
    :class:`~custos_workflow.runs.step_handler.StepResult`
    synchronously, but a durable timer requires yielding a Dapr
    task token back to the runtime so the worker can suspend.
    The orchestrator therefore drives this handler via
    ``yield from`` rather than the regular dispatch path.

    The handler is stateless; a single module-level instance is
    fine. A class (rather than a free function) is kept here so
    callers can pass it as a typed dependency and so future
    metric / tracing decorators can wrap one cohesive surface.
    """

    def execute(
        self,
        ctx: WorkflowContext,
        node: ExecutionNode,
    ) -> Generator[Any, Any, StepSucceeded]:
        """Open a Dapr durable timer for ``node.step_source.wait``.

        Args:
            ctx: The Dapr workflow context — both the real
                :class:`dapr.ext.workflow.DaprWorkflowContext` and
                the test
                :class:`~custos_workflow.runtime.FakeWorkflowContext`
                structurally satisfy the
                :class:`~custos_workflow.runs.step_handler.WorkflowContext`
                surface this argument is typed against.
            node: The compiled :class:`ExecutionNode` for this
                step. Must satisfy ``node.kind is StepKind.WAIT``
                and carry a :class:`WaitStep` in
                :attr:`~custos_workflow.graph.model.ExecutionNode.step_source`.

        Yields:
            One opaque Dapr timer task token. The orchestrator
            re-yields this to the runtime, which suspends the
            workflow until the timer fires.

        Returns:
            :class:`StepSucceeded` with empty outputs — a wait
            step produces no values, only a delay.

        Raises:
            WaitDurationError: ``node.step_source.wait`` does not
                parse as a positive ISO-8601 duration.
        """
        if node.kind is not StepKind.WAIT:
            raise WaitDurationError(
                node.step_id,
                "",
                f"WaitStepHandler dispatched on non-wait node (kind={node.kind.value})",
            )
        step_source = node.step_source
        if not isinstance(step_source, WaitStep):
            raise WaitDurationError(
                node.step_id,
                "",
                f"WaitStepHandler expected a WaitStep step_source, got "
                f"{type(step_source).__name__}",
            )
        duration = parse_wait_duration(node.step_id, step_source.wait)
        yield ctx.create_timer(duration)
        return StepSucceeded(outputs={})
