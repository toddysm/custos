"""``waitFor:`` step handler — register / wait / resume lifecycle (WF-IMPL-104).

The :class:`WaitForStepHandler` is the Resume Subscription Manager's
driver for a ``waitFor:`` step (REQ-081). It owns the full lifecycle
``design.md`` § *Operation: Step Resume on External Event* pins:

1. Resolve the step's ``waitFor.eventKey`` (and optional
   ``waitFor.selector``) CEL slots against the per-run
   :class:`~custos_cel.scope.BindingScope`, and resolve the TTL
   (the step's constant ``waitFor.ttl`` or the configured default
   ``WF_RESUME_SUB_DEFAULT_TTL`` / :data:`DEFAULT_RESUME_SUB_TTL`).
2. **Persist a** :class:`~custos_workflow.steps.resume.ResumeSubscriptionMirror`
   **before** registering with the Trigger Service (Resume
   Subscription Replay Protocol rule 4 — *mirror before TS call*),
   so a crash between the mirror write and the TS call leaves the
   Workflow Service aware that registration is pending. The mirror
   is first written with a sentinel :data:`PENDING_TS_SUBSCRIPTION_ID`
   subscription id, then re-written with the real id the Trigger
   Service returns.
3. Register the subscription with the Trigger Service, retrying with
   bounded exponential backoff (capped at
   :data:`DEFAULT_REGISTER_SUB_MAX_RETRIES` /
   ``WF_REGISTER_SUB_MAX_RETRIES``). On exhaustion the wait step
   fails with a :class:`~custos_workflow.steps.errors.ResumeRegistrationFailedError`
   envelope (``class: retryable``) so the workflow-level retry
   policy decides whether to give up.
4. Block on the external event (``yield``-ing a
   :class:`WaitForExternalEventCall`).
5. On delivery, cancel the subscription (idempotent) and delete the
   mirror, then bind the event payload as the step output.

Generator-based handler
-----------------------

Like :class:`~custos_workflow.steps.sub_orchestration.SubOrchestrationManager`
and :class:`~custos_workflow.steps.activity_step.ActivityStepHandler`,
this handler is **not** a synchronous
:class:`~custos_workflow.runs.StepHandler`: a ``waitFor:`` step
suspends on a durable external event, and its mirror persistence
crosses an async repository boundary. Both concerns are expressed
as *effect tokens* the handler ``yield``s in place of performing
the I/O inline (:data:`ResumeCall`):

* :class:`PersistMirrorCall` — upsert the mirror row.
* :class:`RegisterResumeSubscriptionCall` — register / re-register
  with the Trigger Service.
* :class:`WaitForExternalEventCall` — suspend on the resume event.
* :class:`CancelResumeSubscriptionCall` — cancel the subscription.
* :class:`DeleteMirrorCall` — delete the mirror row.

A driver resolves each yielded token and feeds the result back in
via ``gen.send(...)`` (and re-injects resolution failures via
``gen.throw(...)``). :func:`drive_resume_generator` is the
in-process driver tests + single-process integration use; the
production Dapr-Workflow wiring that translates each token into a
durable ``ctx.call_activity(...)`` / ``ctx.wait_for_external_event(...)``
yield lands in WF-IMPL-108. Keeping the handler pure — every effect
described as a yielded value object — makes it deterministic under
Dapr Workflow replay: the same ``(run_id, step_id, event_key)``
always derives the same mirror id, so a replay re-registration is
idempotent and never duplicates a mirror row.

Lifecycle events (``step.waiting`` / ``step.resumed``) are **not**
emitted from this handler. Consistent with
:class:`~custos_workflow.steps.activity_step.ActivityStepHandler`,
event emission is deferred to the orchestrator-boundary wrapper so
the publisher's ``async def`` contract is honoured without forcing
this handler to grow an event loop.

Acceptance criteria (mirrored from #543):

* Happy path: register → wait → resume → cancel → delete mirror.
* Trigger Service unreachable exhausts the retry budget and returns
  a retryable :class:`StepFailed`.
* Replay-safe: no double registration within one logical attempt.
* Coverage ≥ 90 %; ``ruff`` + ``mypy --strict`` clean.
"""

from __future__ import annotations

import hashlib
from collections.abc import Generator, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import timedelta
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final

from custos_cel import CelError, evaluate
from custos_cel.scope import BindingScope, RunInfo, StepBinding, WorkflowInfo

from custos_workflow.clients.trigger import (
    CancelResumeSubscriptionRequest,
    RegisterResumeSubscriptionRequest,
    RegisterResumeSubscriptionResponse,
    TriggerServiceClient,
)
from custos_workflow.document.models import WaitForStep
from custos_workflow.graph.model import CallSiteKind, ExecutionGraph, ExecutionNode, StepKind
from custos_workflow.runs.step_handler import (
    StepExecutionContext,
    StepFailed,
    StepResult,
    StepSucceeded,
)
from custos_workflow.runs.wait import parse_wait_duration
from custos_workflow.steps.errors import (
    ResumeMirrorPersistError,
    ResumeRegistrationFailedError,
    WithInputResolutionError,
)
from custos_workflow.steps.resume.mirror import (
    ResumeSubscriptionMirror,
    ResumeSubscriptionMirrorRepository,
)

if TYPE_CHECKING:
    from custos_cel.clock import Clock

__all__ = [
    "DEFAULT_REGISTER_SUB_MAX_RETRIES",
    "DEFAULT_RESUME_SUB_TTL",
    "PENDING_TS_SUBSCRIPTION_ID",
    "CancelResumeSubscriptionCall",
    "DeleteMirrorCall",
    "PersistMirrorCall",
    "RegisterResumeSubscriptionCall",
    "ResumeCall",
    "WaitForExternalEventCall",
    "WaitForStepHandler",
    "drive_resume_generator",
    "drive_resume_registration_to_wait",
]


#: Default subscription TTL applied when a ``waitFor:`` step does
#: not pin an explicit ``ttl:``. Mirrors ``WF_RESUME_SUB_DEFAULT_TTL``
#: (``design.md`` § *Operation: Step Resume on External Event*); the
#: provider-loaded config override is wired in WF-IMPL-108.
DEFAULT_RESUME_SUB_TTL: Final[str] = "PT24H"

#: Default ceiling on ``RegisterResumeSubscription`` retries before
#: the wait step fails ``retryable``. Mirrors
#: ``WF_REGISTER_SUB_MAX_RETRIES``.
DEFAULT_REGISTER_SUB_MAX_RETRIES: Final[int] = 5

#: Sentinel ``tsSubscriptionId`` the mirror carries between its
#: pre-registration write (Replay Protocol rule 4 — *mirror before
#: TS call*) and the post-registration update that stamps the real
#: id the Trigger Service returned.
PENDING_TS_SUBSCRIPTION_ID: Final[str] = "pending"

#: Exponential-backoff base delay (seconds) between register retries.
_REGISTER_BACKOFF_BASE_SECONDS: Final[float] = 1.0

#: Exponential-backoff growth factor between register retries.
_REGISTER_BACKOFF_FACTOR: Final[float] = 2.0

#: Cap (seconds) on a single register-retry backoff delay.
_REGISTER_BACKOFF_MAX_SECONDS: Final[float] = 30.0

#: ``call_sites`` slot label for the ``waitFor.eventKey`` CEL slot.
_EVENT_KEY_SLOT: Final[str] = CallSiteKind.WAIT_FOR_EVENT_KEY.value

#: ``call_sites`` slot label for the optional ``waitFor.selector`` CEL slot.
_SELECTOR_SLOT: Final[str] = CallSiteKind.WAIT_FOR_SELECTOR.value


# ---------------------------------------------------------------------------
# Effect tokens
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class PersistMirrorCall:
    """Yielded request to upsert a :class:`ResumeSubscriptionMirror`.

    The driver MUST ``put`` the mirror through the
    :class:`~custos_workflow.steps.resume.ResumeSubscriptionMirrorRepository`
    and send the stored mirror back. A repository failure is
    re-injected via ``gen.throw`` so the handler surfaces a
    :class:`~custos_workflow.steps.errors.ResumeMirrorPersistError`.
    """

    mirror: ResumeSubscriptionMirror


@dataclass(frozen=True, slots=True)
class RegisterResumeSubscriptionCall:
    """Yielded request to (re-)register a resume subscription.

    The driver MUST call
    :meth:`TriggerServiceClient.register_resume_subscription` and
    send back the :class:`RegisterResumeSubscriptionResponse`. A
    transport failure is re-injected via ``gen.throw`` so the
    handler's bounded-retry loop observes it.
    """

    request: RegisterResumeSubscriptionRequest


@dataclass(frozen=True, slots=True)
class WaitForExternalEventCall:
    """Yielded suspend point on the resume event.

    The driver MUST block until the external event named
    :attr:`event_key` is delivered and send its payload back. Under
    the production Dapr-Workflow wiring (WF-IMPL-108) this becomes a
    ``ctx.wait_for_external_event(event_key)`` yield.
    """

    event_key: str


@dataclass(frozen=True, slots=True)
class CancelResumeSubscriptionCall:
    """Yielded request to cancel an open resume subscription.

    The driver MUST call
    :meth:`TriggerServiceClient.cancel_resume_subscription`. The
    Trigger Service treats cancellation as idempotent.
    """

    request: CancelResumeSubscriptionRequest


@dataclass(frozen=True, slots=True)
class DeleteMirrorCall:
    """Yielded request to delete a mirror row after cancellation."""

    mirror_id: str


#: Union of the value-object tokens :meth:`WaitForStepHandler.iter_resume`
#: may yield. Drivers dispatch on this union via ``isinstance``.
ResumeCall = (
    PersistMirrorCall
    | RegisterResumeSubscriptionCall
    | WaitForExternalEventCall
    | CancelResumeSubscriptionCall
    | DeleteMirrorCall
)


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------


class WaitForStepHandler:
    """Resume Subscription Manager driver for ``StepKind.WAIT_FOR``.

    Stateless: the only state is the injected
    :class:`~custos_workflow.steps.resume.ResumeSubscriptionMirrorRepository`
    (for the typed mirror surface the driver resolves
    :class:`PersistMirrorCall` / :class:`DeleteMirrorCall` against)
    and the two config knobs. A single instance is safe to share
    across runs.

    The repository is **not** invoked inline — it is injected so the
    handler can reference its types — every repository / Trigger
    Service interaction is a yielded :data:`ResumeCall` the driver
    resolves. This keeps the handler pure and replay-deterministic.

    :param mirror_repo: The
        :class:`~custos_workflow.steps.resume.ResumeSubscriptionMirrorRepository`
        the driver resolves mirror effects against. Retained so a
        future inline-async wiring can reuse the same dependency
        surface.
    :param default_ttl: Subscription TTL applied when the step pins
        no ``waitFor.ttl``. Defaults to :data:`DEFAULT_RESUME_SUB_TTL`.
    :param max_register_retries: Ceiling on register retries before a
        retryable failure. Defaults to
        :data:`DEFAULT_REGISTER_SUB_MAX_RETRIES`.
    """

    __slots__ = ("_default_ttl", "_max_register_retries", "_mirror_repo")

    def __init__(
        self,
        mirror_repo: ResumeSubscriptionMirrorRepository,
        *,
        default_ttl: str = DEFAULT_RESUME_SUB_TTL,
        max_register_retries: int = DEFAULT_REGISTER_SUB_MAX_RETRIES,
    ) -> None:
        if max_register_retries < 1:
            raise ValueError(
                f"WaitForStepHandler.max_register_retries must be >= 1; got {max_register_retries}",
            )
        # Validate the default TTL eagerly so a misconfigured default
        # fails at construction rather than on the first wait step.
        parse_wait_duration("<default-ttl>", default_ttl)
        self._mirror_repo = mirror_repo
        self._default_ttl = default_ttl
        self._max_register_retries = max_register_retries

    @property
    def mirror_repo(self) -> ResumeSubscriptionMirrorRepository:
        """The repository the driver resolves mirror effects against."""
        return self._mirror_repo

    # ------------------------------------------------------------------
    # Generator-based yield protocol
    # ------------------------------------------------------------------

    def iter_resume(
        self,
        ctx: StepExecutionContext,
        graph: ExecutionGraph,
        step_id: str,
    ) -> Generator[ResumeCall, object, StepResult]:
        """Drive the ``waitFor:`` lifecycle, yielding :data:`ResumeCall` effects.

        Sequence (each ``yield`` is resolved by the driver):

        1. Resolve ``eventKey`` / ``selector`` / TTL (no yield).
        2. :class:`PersistMirrorCall` with a
           :data:`PENDING_TS_SUBSCRIPTION_ID` mirror — *before* any
           Trigger Service call.
        3. :class:`RegisterResumeSubscriptionCall`, retried with
           bounded exponential backoff. On exhaustion → return a
           :class:`StepFailed` carrying a
           :class:`~custos_workflow.steps.errors.ResumeRegistrationFailedError`
           envelope.
        4. :class:`PersistMirrorCall` again, stamping the real
           ``tsSubscriptionId`` the Trigger Service returned.
        5. :class:`WaitForExternalEventCall` — suspend on the event.
        6. :class:`CancelResumeSubscriptionCall` + :class:`DeleteMirrorCall`.
        7. Return :class:`StepSucceeded` binding the event payload.

        :returns: A :class:`StepResult` — :class:`StepSucceeded` on
            resume, or :class:`StepFailed` on a resolution / mirror /
            register-exhaustion failure.

        :raises KeyError: If ``step_id`` is not in ``graph.nodes``.
        :raises NotImplementedError: If the node is not a
            :class:`~custos_workflow.graph.model.StepKind.WAIT_FOR`
            step.
        """
        node = _resolve_node(graph, step_id)
        if node.kind is not StepKind.WAIT_FOR:
            raise NotImplementedError(
                f"WaitForStepHandler.iter_resume received {node.kind.value!r} step "
                f"{step_id!r}; only StepKind.WAIT_FOR is supported",
            )
        wait_for_step = node.step_source
        if not isinstance(wait_for_step, WaitForStep):  # pragma: no cover - defensive
            raise NotImplementedError(
                f"WaitForStepHandler.iter_resume: node {step_id!r} has step_source "
                f"of type {type(wait_for_step).__name__}, expected WaitForStep",
            )

        run_id_str = str(ctx.run_id)
        scope = _build_scope(ctx, graph)

        # 1. Resolve eventKey / selector / TTL. CEL or type failures
        #    short-circuit to a StepFailed envelope (mirroring the
        #    ActivityStepHandler with:-resolution failure path).
        try:
            event_key = _resolve_cel_string(
                node,
                _EVENT_KEY_SLOT,
                "eventKey",
                scope,
                ctx.clock,
                run_id_str,
                expected_kind=CallSiteKind.WAIT_FOR_EVENT_KEY,
                required=True,
            )
            selector = _resolve_cel_string(
                node,
                _SELECTOR_SLOT,
                "selector",
                scope,
                ctx.clock,
                run_id_str,
                expected_kind=CallSiteKind.WAIT_FOR_SELECTOR,
                required=False,
            )
        except WithInputResolutionError as exc:
            return StepFailed(envelope=MappingProxyType(exc.to_dict()))

        # ``required=True`` guarantees a non-empty string (it raises
        # otherwise) — narrow it for the type checker.
        assert event_key is not None

        ttl = wait_for_step.wait_for.ttl or self._default_ttl
        registered_at = ctx.workflow_context.current_utc_datetime
        expires_at = registered_at + parse_wait_duration(step_id, ttl)
        mirror_id = _derive_mirror_id(run_id_str, step_id, event_key)

        # 2. Persist the mirror BEFORE the Trigger Service call
        #    (Replay Protocol rule 4) with a pending subscription id.
        pending_mirror = ResumeSubscriptionMirror(
            mirror_id=mirror_id,
            run_id=run_id_str,
            step_id=step_id,
            event_key=event_key,
            ts_subscription_id=PENDING_TS_SUBSCRIPTION_ID,
            registered_at=registered_at,
            expires_at=expires_at,
            selector=selector,
        )
        try:
            yield PersistMirrorCall(mirror=pending_mirror)
        except Exception as exc:
            persist_error = ResumeMirrorPersistError(
                f"failed to persist resume mirror for step {step_id!r} "
                f"before registration: {exc!r}",
                run_id=run_id_str,
                step_id=step_id,
                event_key=event_key,
                cause=repr(exc),
            )
            return StepFailed(envelope=MappingProxyType(persist_error.to_dict()))

        # 3. Register with the Trigger Service, retrying with bounded
        #    exponential backoff. Exhaustion fails the step retryable.
        register_request = RegisterResumeSubscriptionRequest(
            run_id=run_id_str,
            step_id=step_id,
            event_key=event_key,
            ttl=ttl,
            selector=selector,
        )
        attempt = 1
        while True:
            try:
                register_response = yield RegisterResumeSubscriptionCall(request=register_request)
            except Exception as exc:
                if attempt >= self._max_register_retries:
                    failure = ResumeRegistrationFailedError(
                        f"resume subscription registration for step {step_id!r} "
                        f"stayed unreachable after {attempt} attempts: {exc!r}",
                        run_id=run_id_str,
                        step_id=step_id,
                        attempt=attempt,
                        event_key=event_key,
                        max_retries=self._max_register_retries,
                        cause=repr(exc),
                    )
                    return StepFailed(envelope=MappingProxyType(failure.to_dict()))
                fire_at = ctx.workflow_context.current_utc_datetime + timedelta(
                    seconds=_register_backoff_seconds(attempt)
                )
                ctx.workflow_context.create_timer(fire_at)
                attempt += 1
                continue
            break

        if not isinstance(register_response, RegisterResumeSubscriptionResponse):
            raise TypeError(
                "WaitForStepHandler.iter_resume received a non-"
                "RegisterResumeSubscriptionResponse value back from the driver for "
                f"the yielded RegisterResumeSubscriptionCall: got "
                f"{type(register_response).__name__}",
            )

        # 4. Stamp the real subscription id onto the mirror (Replay
        #    Protocol rule 4 — update the mirror if TS returns a new id).
        registered_mirror = ResumeSubscriptionMirror(
            mirror_id=mirror_id,
            run_id=run_id_str,
            step_id=step_id,
            event_key=event_key,
            ts_subscription_id=register_response.ts_subscription_id,
            registered_at=registered_at,
            expires_at=expires_at,
            selector=selector,
        )
        try:
            yield PersistMirrorCall(mirror=registered_mirror)
        except Exception as exc:
            persist_error = ResumeMirrorPersistError(
                f"failed to update resume mirror for step {step_id!r} with the "
                f"registered subscription id: {exc!r}",
                run_id=run_id_str,
                step_id=step_id,
                event_key=event_key,
                cause=repr(exc),
            )
            return StepFailed(envelope=MappingProxyType(persist_error.to_dict()))

        # 5. Suspend on the resume event.
        resume_payload = yield WaitForExternalEventCall(event_key=event_key)

        # 6. Cancel the subscription (idempotent) and delete the mirror.
        #    Cleanup runs AFTER the resume payload has been received, so a
        #    failure here must not abort the step — that would wedge the
        #    workflow (the event can never be re-delivered on retry). Both
        #    are best-effort: the TTL sweep / replay reconciler reclaim a
        #    leaked subscription or mirror.
        with suppress(Exception):
            yield CancelResumeSubscriptionCall(
                request=CancelResumeSubscriptionRequest(
                    run_id=run_id_str,
                    step_id=step_id,
                    event_key=event_key,
                )
            )
        with suppress(Exception):
            yield DeleteMirrorCall(mirror_id=mirror_id)

        # 7. Bind the event payload as the step output.
        if isinstance(resume_payload, Mapping):
            outputs: dict[str, Any] = dict(resume_payload)
        else:
            outputs = {"payload": resume_payload}
        return StepSucceeded(outputs=MappingProxyType(outputs))


# ---------------------------------------------------------------------------
# In-process drivers
# ---------------------------------------------------------------------------


async def drive_resume_generator(
    gen: Generator[ResumeCall, object, StepResult],
    trigger_client: TriggerServiceClient,
    mirror_repo: ResumeSubscriptionMirrorRepository,
    *,
    resume_payload: object,
) -> StepResult:
    """Drive a :meth:`WaitForStepHandler.iter_resume` generator to completion.

    Pumps ``gen`` forward, resolving each yielded :data:`ResumeCall`
    against the injected collaborators and sending the result back.
    Any effect failure — a :class:`PersistMirrorCall`,
    :class:`RegisterResumeSubscriptionCall`,
    :class:`CancelResumeSubscriptionCall`, or :class:`DeleteMirrorCall`
    — is re-injected via :meth:`Generator.throw` so the handler's own
    ``try`` / ``except`` observes it (registration failures fail the
    step; post-resume cleanup failures are swallowed best-effort);
    a :class:`WaitForExternalEventCall` resolves immediately to
    ``resume_payload`` (the in-process driver does not block).

    :returns: The :class:`StepResult` the generator returns.
    :raises TypeError: If the generator yields an unsupported token.
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
            return stop.value  # type: ignore[no-any-return]

        sent = None
        if isinstance(token, PersistMirrorCall):
            try:
                sent = await mirror_repo.put(token.mirror)
            except Exception as exc:
                pending_exc = exc
        elif isinstance(token, RegisterResumeSubscriptionCall):
            try:
                sent = trigger_client.register_resume_subscription(token.request)
            except Exception as exc:
                pending_exc = exc
        elif isinstance(token, WaitForExternalEventCall):
            sent = resume_payload
        elif isinstance(token, CancelResumeSubscriptionCall):
            try:
                trigger_client.cancel_resume_subscription(token.request)
            except Exception as exc:
                pending_exc = exc
        elif isinstance(token, DeleteMirrorCall):
            try:
                await mirror_repo.delete(token.mirror_id)
            except Exception as exc:
                pending_exc = exc
        else:
            raise TypeError(
                "WaitForStepHandler.iter_resume yielded an unsupported token "
                f"type: {type(token).__name__}",
            )


async def drive_resume_registration_to_wait(
    gen: Generator[ResumeCall, object, StepResult],
    trigger_client: TriggerServiceClient,
    mirror_repo: ResumeSubscriptionMirrorRepository,
) -> WaitForExternalEventCall | StepResult:
    """Drive the registration phase only, halting at the resume-event wait.

    Resolves the mirror-persist and register effects against the
    injected collaborators, then **stops** at the
    :class:`WaitForExternalEventCall` without delivering an event —
    modelling the Dapr Workflow *replay* path, which re-runs the
    generator up to the suspend point on every pod restart. Running
    this twice against the same collaborators is how the
    *no double registration within one logical attempt* property is
    exercised: the idempotent Trigger Service returns the same
    ``tsSubscriptionId`` and the deterministic mirror id keeps the
    repository to a single row.

    :returns: The :class:`WaitForExternalEventCall` the handler
        suspended on, or a :class:`StepResult` if the registration
        phase terminated early (e.g. register exhaustion).
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
            return stop.value  # type: ignore[no-any-return]

        sent = None
        if isinstance(token, WaitForExternalEventCall):
            gen.close()
            return token
        if isinstance(token, PersistMirrorCall):
            try:
                sent = await mirror_repo.put(token.mirror)
            except Exception as exc:
                pending_exc = exc
        elif isinstance(token, RegisterResumeSubscriptionCall):
            try:
                sent = trigger_client.register_resume_subscription(token.request)
            except Exception as exc:
                pending_exc = exc
        else:  # pragma: no cover - cancel/delete never precede the wait yield
            raise TypeError(
                "drive_resume_registration_to_wait saw an unexpected pre-wait token "
                f"type: {type(token).__name__}",
            )


# ---------------------------------------------------------------------------
# Module-private helpers
# ---------------------------------------------------------------------------


def _resolve_node(graph: ExecutionGraph, step_id: str) -> ExecutionNode:
    """Return the node for ``step_id`` or raise :class:`KeyError`."""
    for node in graph.nodes:
        if node.step_id == step_id:
            return node
    raise KeyError(step_id)


def _build_scope(ctx: StepExecutionContext, graph: ExecutionGraph) -> BindingScope:
    """Build the per-step :class:`BindingScope` for ``eventKey`` / ``selector``.

    Mirrors :func:`custos_workflow.steps.activity_step._build_scope`
    so a ``${{ inputs.x }}`` reference under ``waitFor:`` resolves to
    the same value it would under ``with:`` / ``let:``.
    """
    run_info = RunInfo(id=str(ctx.run_id), workspace=ctx.workspace_id)
    workflow_info = WorkflowInfo(
        name=graph.metadata.workflow_name,
        version=ctx.workflow_version_id,
    )
    steps_view: Mapping[str, StepBinding] = MappingProxyType(
        {sid: StepBinding(out, sealed=True) for sid, out in ctx.outputs.items()}
    )
    return BindingScope(
        run=run_info,
        workflow=workflow_info,
        now=ctx.clock.now,
        inputs=ctx.inputs,
        steps=steps_view,
        let=MappingProxyType({}),
    )


def _resolve_cel_string(
    node: ExecutionNode,
    slot_label: str,
    field_name: str,
    scope: BindingScope,
    clock: Clock,
    run_id: str,
    *,
    expected_kind: CallSiteKind,
    required: bool,
) -> str | None:
    """Evaluate a ``waitFor`` CEL slot and validate it as a string.

    The compiler attaches one :class:`~custos_workflow.graph.model.TypedCallSite`
    per present ``waitFor`` slot (``waitFor.eventKey`` always,
    ``waitFor.selector`` only when declared). An optional slot that
    is absent — or evaluates to ``None`` — returns ``None``.

    The stored :attr:`TypedCallSite.kind` is asserted to match
    ``expected_kind`` so a malformed graph (slot-label collision)
    fails loudly instead of silently evaluating the wrong AST —
    the same guard the ``let:`` / ``with:`` resolvers apply.

    :raises WithInputResolutionError: If a required slot is missing /
        evaluates to a non-string or empty value, the stored call-site
        kind does not match ``expected_kind``, or the underlying
        CEL evaluation raises.
    """
    call_site = node.call_sites.get(slot_label)
    if call_site is None:
        if required:
            raise WithInputResolutionError(
                f"compiled graph is missing the TypedAST for waitFor {field_name!r} "
                f"on step {node.step_id!r}; this indicates a graph blob that was not "
                "produced by the WF-IMPL-020 collector",
                run_id=run_id,
                step_id=node.step_id,
                binding_name=field_name,
            )
        return None

    if call_site.kind is not expected_kind:
        raise WithInputResolutionError(
            f"waitFor {field_name!r} on step {node.step_id!r} resolved a call site "
            f"tagged {call_site.kind.value!r}, expected {expected_kind.value!r}; "
            "this indicates a malformed graph with a slot-label collision",
            run_id=run_id,
            step_id=node.step_id,
            binding_name=field_name,
        )

    try:
        value = evaluate(call_site.typed_ast, scope, clock)
    except CelError as exc:
        raise WithInputResolutionError(
            f"failed to evaluate waitFor {field_name!r} on step {node.step_id!r}: {exc}",
            run_id=run_id,
            step_id=node.step_id,
            binding_name=field_name,
            cause_kind=exc.kind,
            source=call_site.source,
        ) from exc

    if value is None and not required:
        return None
    if not isinstance(value, str) or not value:
        raise WithInputResolutionError(
            f"waitFor {field_name!r} on step {node.step_id!r} must resolve to a "
            f"non-empty string; got {value!r}",
            run_id=run_id,
            step_id=node.step_id,
            binding_name=field_name,
            source=call_site.source,
        )
    return value


def _derive_mirror_id(run_id: str, step_id: str, event_key: str) -> str:
    """Derive a replay-stable mirror id from the idempotency triple.

    Deterministic in ``(run_id, step_id, event_key)`` so a Dapr
    Workflow replay re-derives the same id and the repository upsert
    collapses to one row.
    """
    digest = hashlib.sha256(f"{run_id}|{step_id}|{event_key}".encode()).hexdigest()
    return f"rsm-{digest[:32]}"


def _register_backoff_seconds(attempt: int) -> float:
    """Return the capped exponential backoff (seconds) for ``attempt``."""
    delay = _REGISTER_BACKOFF_BASE_SECONDS * (_REGISTER_BACKOFF_FACTOR ** (attempt - 1))
    return min(delay, _REGISTER_BACKOFF_MAX_SECONDS)
