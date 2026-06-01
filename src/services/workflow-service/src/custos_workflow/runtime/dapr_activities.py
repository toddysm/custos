"""WF-IMPL-074 — activity-task yield protocol for ``ActivityStepHandler``.

Decouples the Step Coordinator's
:class:`~custos_workflow.steps.activity_step.ActivityStepHandler`
from the I/O substrate of its two outbound RPCs
(``ConnectorClient.bind_for_step`` and
``ActivityRuntimeClient.schedule_activity``). The handler exposes a
generator method (``iter_calls``) that yields
:data:`ActivityCallToken` value objects in place of calling the
underlying clients inline; the surrounding driver (production Dapr
worker, :class:`~custos_workflow.runtime.FakeWorkflowRuntime`, or
the in-process :class:`FakeDaprActivityDispatcher` defined below)
resolves each yielded token and sends the response back into the
generator via ``gen.send(response)``.

This is the prerequisite that makes a production HTTP-backed
adapter wireable: without it, calling the real ARM / Connector
adapters from inside the Run Controller orchestrator function would
violate Dapr Workflow determinism (every outbound RPC must be a
durable activity, not an inline ``requests.post``). The production
Dapr-Workflow activity registration that resolves these tokens via
``ctx.call_activity(...)`` lands in WF-IMPL-079; this module is
purely the foundation, plus the in-process driver tests and the
synchronous Step Coordinator path use to keep the existing
:meth:`StepHandler.execute` ↦ :class:`StepResult` contract intact
while the production wiring catches up.

Wire-stable activity names
--------------------------

:data:`BIND_FOR_STEP_ACTIVITY_NAME` and
:data:`SCHEDULE_ACTIVITY_ACTIVITY_NAME` are the Dapr activity
function names WF-IMPL-079 will register the resolver activities
under. Pinning the names here (rather than in WF-IMPL-079) lets
this module's tests assert the surface that the production wiring
will key off, and keeps the value-object types and the activity
names in the same file so a future rename only touches one module.
"""

from __future__ import annotations

from collections.abc import Generator
from dataclasses import dataclass
from typing import TYPE_CHECKING, Final

if TYPE_CHECKING:
    # Importing the clients eagerly would close a layering cycle
    # (``runtime`` is a lower-level subpackage than ``clients`` /
    # ``steps`` / ``runs``; see the package-level
    # ``custos_workflow.runtime`` docstring). The dataclasses
    # below use ``from __future__ import annotations`` so the
    # field type hints stay strings at module load; only static
    # type-checkers ever need the real symbols.
    from custos_workflow.clients.activity_runtime import (
        ActivityRuntimeClient,
        ScheduleActivityRequest,
    )
    from custos_workflow.clients.connector import (
        BindForStepRequest,
        ConnectorClient,
    )
    from custos_workflow.runs.step_handler import StepResult

__all__ = [
    "BIND_FOR_STEP_ACTIVITY_NAME",
    "SCHEDULE_ACTIVITY_ACTIVITY_NAME",
    "ActivityCallToken",
    "BindForStepCallToken",
    "FakeDaprActivityDispatcher",
    "ScheduleActivityCallToken",
    "drive_activity_generator",
]


# ---------------------------------------------------------------------------
# Wire-stable Dapr activity names
# ---------------------------------------------------------------------------


#: Dapr activity name that resolves :class:`BindForStepCallToken`
#: yields. WF-IMPL-079 registers the corresponding activity function
#: (which calls ``ConnectorClient.bind_for_step`` against the real
#: HTTP adapter) under this name.
BIND_FOR_STEP_ACTIVITY_NAME: Final[str] = "custos.workflow.connector.bind_for_step"

#: Dapr activity name that resolves :class:`ScheduleActivityCallToken`
#: yields. WF-IMPL-079 registers the corresponding activity function
#: (which calls ``ActivityRuntimeClient.schedule_activity`` against
#: the real HTTP adapter) under this name.
SCHEDULE_ACTIVITY_ACTIVITY_NAME: Final[str] = "custos.workflow.arm.schedule_activity"


# ---------------------------------------------------------------------------
# Token value objects
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class BindForStepCallToken:
    """Yielded value object representing a deferred ``bind_for_step`` call.

    Carries the fully-constructed
    :class:`~custos_workflow.clients.BindForStepRequest` so the
    driver can dispatch the call without rebuilding the request.
    The expected ``gen.send(...)`` reply is the
    :class:`~custos_workflow.clients.BindForStepResponse` returned
    by the resolved call.

    :param request: The bind request the handler would otherwise
        have passed inline to
        :meth:`ConnectorClient.bind_for_step`.
    """

    request: BindForStepRequest


@dataclass(frozen=True, slots=True)
class ScheduleActivityCallToken:
    """Yielded value object representing a deferred ``schedule_activity`` call.

    Carries the fully-constructed
    :class:`~custos_workflow.clients.ScheduleActivityRequest` so
    the driver can dispatch the call without rebuilding it. The
    expected ``gen.send(...)`` reply is the
    :class:`~custos_workflow.clients.ActivityResultEnvelope`
    returned by the resolved call.

    :param request: The schedule request the handler would
        otherwise have passed inline to
        :meth:`ActivityRuntimeClient.schedule_activity`.
    """

    request: ScheduleActivityRequest


#: Union of value-object tokens
#: :meth:`ActivityStepHandler.iter_calls` may yield. Driver
#: implementations dispatch on this union via ``isinstance``.
ActivityCallToken = BindForStepCallToken | ScheduleActivityCallToken


# ---------------------------------------------------------------------------
# In-process driver
# ---------------------------------------------------------------------------


def drive_activity_generator(
    gen: Generator[ActivityCallToken, object, StepResult],
    activity_client: ActivityRuntimeClient,
    connector_client: ConnectorClient,
) -> StepResult:
    """Drive an activity-handler generator to completion in-process.

    Pumps ``gen`` forward, dispatching each yielded
    :data:`ActivityCallToken` to the matching client method and
    sending the response back into the generator. Exceptions
    raised by the client methods are propagated back into the
    generator via :meth:`Generator.throw`, so the handler's own
    ``try`` / ``except`` blocks around the yield sites observe
    the same exception types they observed when the calls were
    inline (e.g.
    :class:`~custos_workflow.steps.errors.ConnectorBindError`).

    :param gen: The generator returned by
        :meth:`~custos_workflow.steps.activity_step.ActivityStepHandler.iter_calls`.
    :param activity_client: The
        :class:`~custos_workflow.clients.ActivityRuntimeClient`
        used to resolve :class:`ScheduleActivityCallToken` yields.
    :param connector_client: The
        :class:`~custos_workflow.clients.ConnectorClient` used to
        resolve :class:`BindForStepCallToken` yields.

    :returns: The :class:`StepResult` returned by the generator on
        :class:`StopIteration`.

    :raises TypeError: If a yielded value is not an
        :data:`ActivityCallToken` instance.
    """
    sent: object = None
    pending_exc: Exception | None = None
    while True:
        try:
            if pending_exc is not None:
                exc_to_throw, pending_exc = pending_exc, None
                token = gen.throw(exc_to_throw)
            else:
                token = gen.send(sent)
        except StopIteration as stop:
            # Generators returning a non-default value carry it on
            # ``StopIteration.value``; the handler's
            # ``return StepResult`` lands here.
            return stop.value  # type: ignore[no-any-return]

        sent = None
        if isinstance(token, BindForStepCallToken):
            try:
                sent = connector_client.bind_for_step(token.request)
            except Exception as exc:
                pending_exc = exc
        elif isinstance(token, ScheduleActivityCallToken):
            try:
                sent = activity_client.schedule_activity(token.request)
            except Exception as exc:
                pending_exc = exc
        else:
            raise TypeError(
                "ActivityStepHandler.iter_calls yielded an unsupported token "
                f"type: {type(token).__name__}; expected BindForStepCallToken "
                "or ScheduleActivityCallToken",
            )


class FakeDaprActivityDispatcher:
    """In-process resolver for :data:`ActivityCallToken` yields.

    Wraps :func:`drive_activity_generator` in a stateful class so
    test fixtures can preserve a single dispatcher instance across
    multiple ``handler.iter_calls(...)`` invocations against the
    same pair of in-process fakes (mirroring the production wiring
    pattern where the worker constructs the dispatcher once at
    startup and reuses it for every run).

    The class also serves as the dependency boundary
    :class:`~custos_workflow.runtime.FakeWorkflowRuntime` keys off
    so the fake's orchestrator-side ``yield from``-based dispatch
    of :class:`BindForStepCallToken` / :class:`ScheduleActivityCallToken`
    resolves against the same in-process fakes a test already
    constructed for direct handler exercise.

    :param activity_client: The
        :class:`~custos_workflow.clients.ActivityRuntimeClient`
        used to resolve :class:`ScheduleActivityCallToken` yields.
    :param connector_client: The
        :class:`~custos_workflow.clients.ConnectorClient` used to
        resolve :class:`BindForStepCallToken` yields.
    """

    __slots__ = ("_activity_client", "_connector_client")

    def __init__(
        self,
        activity_client: ActivityRuntimeClient,
        connector_client: ConnectorClient,
    ) -> None:
        self._activity_client = activity_client
        self._connector_client = connector_client

    @property
    def activity_client(self) -> ActivityRuntimeClient:
        """The :class:`ActivityRuntimeClient` this dispatcher resolves against."""
        return self._activity_client

    @property
    def connector_client(self) -> ConnectorClient:
        """The :class:`ConnectorClient` this dispatcher resolves against."""
        return self._connector_client

    def drive(
        self,
        gen: Generator[ActivityCallToken, object, StepResult],
    ) -> StepResult:
        """Drive ``gen`` to completion. See :func:`drive_activity_generator`."""
        return drive_activity_generator(
            gen,
            self._activity_client,
            self._connector_client,
        )

    def resolve(self, token: ActivityCallToken) -> object:
        """Resolve a single :data:`ActivityCallToken` against the wired clients.

        Used by drivers (e.g. :class:`FakeWorkflowRuntime`) that
        prefer to interleave token resolution with their own
        generator-driving loop rather than delegate the whole
        generator to :meth:`drive`.

        :raises TypeError: If ``token`` is not an
            :data:`ActivityCallToken` instance.
        """
        if isinstance(token, BindForStepCallToken):
            return self._connector_client.bind_for_step(token.request)
        if isinstance(token, ScheduleActivityCallToken):
            return self._activity_client.schedule_activity(token.request)
        raise TypeError(
            "FakeDaprActivityDispatcher.resolve received an unsupported token "
            f"type: {type(token).__name__}; expected BindForStepCallToken or "
            "ScheduleActivityCallToken",
        )
