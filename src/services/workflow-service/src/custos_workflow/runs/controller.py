"""WF-IMPL-037 \u2014 :class:`RunController.start_run` lifecycle entry point.

Implements design.md \xa7 Operation: Start Run + \xa7 Idempotency Model.

Sequence
--------

A successful ``start_run`` walks through six gates, in this order:

1. **Derive** the deterministic ``run_id`` via
   :func:`~custos_workflow.runs.ids.derive_run_id`. With a non-empty
   ``idempotency_key`` the id is ``uuid5(RUN_ID_NAMESPACE,
   f"{workspace_id}|{idempotency_key}")``; without one a fresh UUID4.

2. **Deduplicate** against the existing-run window. If a prior
   :class:`RunRecord` lives at ``(workspace_id, run_id)`` and the new
   request's ``(workflow_version_id, inputs)`` are byte-equal to the
   originally-recorded ones, return the existing :class:`RunRef` and
   skip **both** Catalog and Dapr (acceptance criterion). Divergent
   inputs raise :class:`RunStateConflictError`.

3. **Fetch** the :class:`WorkflowVersion` from the Catalog via the
   injected :class:`CatalogClient` (mocked at this gate; the real
   adapter lands in WF-IMPL-043) and **compile** the document to an
   :class:`~custos_workflow.graph.model.ExecutionGraph`.

4. **Persist** ``RunRecord(status=queued, compiled_graph=\u2026)`` via
   :meth:`~custos_workflow.runs.store.RunStore.put_run`. The compiled
   graph is the run-time source of truth \u2014 the orchestrator never
   re-fetches from Catalog on replay (design.md \xa7 Pod Restart /
   Dapr Replay).

5. **Schedule** the run on Dapr via
   :meth:`~custos_workflow.runtime.WorkflowClient.schedule_new_workflow`
   with ``instance_id=run_id``. A runtime failure here is wrapped as
   :class:`WorkflowRuntimeUnavailableError` AFTER transitioning the
   persisted record to ``failed`` \u2014 design contract: a
   ``start_run`` that returns an error must not leave a
   half-persisted record stuck in ``queued`` (acceptance criterion).

6. **Transition** to ``running`` and **emit** the ``workflow.started``
   lifecycle event via the injected
   :class:`LifecycleEventPublisher`. The full Dapr Pub/Sub adapter
   lands in WF-IMPL-041; an in-memory stub is sufficient at this
   gate.

Inputs-fingerprint dedup
------------------------

:class:`RunRecord` carries ``workflow_version`` but not the original
``inputs`` (the inputs travel through the orchestrator's
:class:`~custos_workflow.runs.orchestrator.RunInput`, not the row).
To detect a same-key / divergent-inputs collision, the controller
maintains an instance-scoped fingerprint map keyed by
``(workspace_id, run_id)``. The fingerprint is a canonical-JSON
SHA-256 of ``{workflow_version_id, inputs}``, so two payloads that
differ only in dict-key ordering still collapse to the same digest.

The fingerprint map is intentionally **process-local**: it
strengthens the same-process dedup guarantee but does not survive a
worker restart. Cross-restart dedup falls back to the
:class:`RunStore` byte-equality check on ``workflow_version`` alone;
that's the floor design.md guarantees. A stronger persisted
fingerprint is tracked separately under the persistence-layer work.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Final, Protocol, runtime_checkable

from custos_workflow.compiler import RunMeta
from custos_workflow.compiler import compile as compile_workflow
from custos_workflow.graph.serialize import to_json
from custos_workflow.runs.errors import (
    RunNotFoundError,
    RunStateConflictError,
    WorkflowRuntimeUnavailableError,
)
from custos_workflow.runs.ids import RunId, derive_run_id
from custos_workflow.runs.model import RunRecord, RunStatus
from custos_workflow.runs.orchestrator import WORKFLOW_NAME, RunInput
from custos_workflow.runtime._common import (
    TERMINAL_STATUSES as RUNTIME_TERMINAL_STATUSES,
)
from custos_workflow.runtime._common import (
    GetRunStateRequest,
    PauseRunRequest,
    ResumeRunRequest,
    ScheduleWorkflowRequest,
    TerminateRunRequest,
)
from custos_workflow.runtime._common import RunState as RuntimeRunState

if TYPE_CHECKING:
    from custos_cel.clock import Clock

    from custos_workflow.bindings.registry import ActivityTypeRegistry
    from custos_workflow.document import WorkflowDocument
    from custos_workflow.runs.store import RunStore

__all__ = [
    "DEFAULT_TERMINATE_POLL_ATTEMPTS",
    "DEFAULT_TERMINATE_POLL_INTERVAL_S",
    "LIFECYCLE_KIND_WORKFLOW_CANCELLED",
    "LIFECYCLE_KIND_WORKFLOW_PAUSED",
    "LIFECYCLE_KIND_WORKFLOW_RESUMED",
    "LIFECYCLE_KIND_WORKFLOW_STARTED",
    "CatalogClient",
    "InMemoryLifecycleEventPublisher",
    "LifecycleEvent",
    "LifecycleEventPublisher",
    "RunController",
    "RunRef",
    "WorkflowVersion",
]


# ---------------------------------------------------------------------------
# Catalog client surface (mocked at this gate)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class WorkflowVersion:
    """The Catalog Service payload a ``start_run`` consumes.

    The real Catalog Service returns a richer envelope (audit
    metadata, schema digest, publish state, \u2026); the Run
    Controller only needs the fields the compiler keys off plus the
    identity fields that flow into :class:`RunRecord` and the
    lifecycle event.

    Attributes:
        id: The Catalog ``WorkflowVersion`` UUID. Equal to the
            ``workflow_version_id`` the caller passes to
            :meth:`RunController.start_run`.
        workflow_id: The owning workflow's stable id. Persisted on
            :attr:`RunRecord.workflow_id`.
        name: Denormalised workflow name. Surfaced in CEL
            expressions through :class:`~custos_cel.WorkflowInfo`.
        version_label: Human-readable workflow version label
            (e.g. ``v1``). Surfaced alongside ``name`` in CEL.
        document: The parsed :class:`WorkflowDocument` the compiler
            consumes.
    """

    id: str
    workflow_id: str
    name: str
    version_label: str
    document: WorkflowDocument


@runtime_checkable
class CatalogClient(Protocol):
    """The narrow Catalog surface :class:`RunController` depends on.

    The production adapter lives outside this package (Catalog
    Service client, WF-IMPL-043). Tests inject an in-memory fake;
    the existing-run dedup path NEVER invokes this method
    (acceptance criterion).
    """

    async def get_workflow_version(
        self,
        workspace_id: str,
        workflow_version_id: str,
    ) -> WorkflowVersion:
        """Fetch the workflow version. Raises on missing / access denied.

        The controller treats any exception as the Catalog's
        concern (not :class:`RunControllerError`-shaped) and lets
        it propagate; observability decoration is the API
        Adapter's job.
        """
        ...


# ---------------------------------------------------------------------------
# Lifecycle event publisher (in-memory stub at this gate)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LifecycleEvent:
    """One workflow-lifecycle event the controller emits.

    Wire-stable shape that the Dapr Pub/Sub adapter (WF-IMPL-041)
    will publish on the ``custos.workflow.events`` topic. The
    ``kind`` is the wire-stable string the consumer routes on
    (e.g. ``workflow.started``); :data:`extra` carries any
    event-specific payload without bloating the base envelope.
    """

    kind: str
    workspace_id: str
    run_id: RunId
    workflow_version_id: str
    occurred_at: datetime
    extra: Mapping[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """JSON-safe envelope for Pub/Sub serialisation."""
        return {
            "kind": self.kind,
            "workspace_id": self.workspace_id,
            "run_id": str(self.run_id),
            "workflow_version_id": self.workflow_version_id,
            "occurred_at": self.occurred_at.isoformat(),
            "extra": dict(self.extra),
        }


@runtime_checkable
class LifecycleEventPublisher(Protocol):
    """Sink for :class:`LifecycleEvent`-s.

    WF-IMPL-041 lands the Dapr Pub/Sub-backed adapter; this gate
    only requires the Protocol + an in-memory stub
    (:class:`InMemoryLifecycleEventPublisher`).
    """

    async def publish(self, event: LifecycleEvent) -> None:
        """Deliver *event* downstream. Failures must propagate."""
        ...


class InMemoryLifecycleEventPublisher:
    """Test-friendly :class:`LifecycleEventPublisher` that records calls.

    Each :meth:`publish` appends to :attr:`events` in invocation
    order. Tests assert on the order to verify the controller emits
    ``workflow.started`` AFTER the run reaches ``running`` (and
    nothing else for this task gate).
    """

    def __init__(self) -> None:
        self.events: list[LifecycleEvent] = []

    async def publish(self, event: LifecycleEvent) -> None:
        self.events.append(event)


# ---------------------------------------------------------------------------
# RunRef \u2014 the controller's return shape
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RunRef:
    """The caller-facing handle :meth:`RunController.start_run` returns.

    Carries the identity of the freshly-started (or re-discovered)
    run plus the current status so the caller can render a
    ``202 Accepted`` response without a follow-up store read.
    """

    workspace_id: str
    run_id: RunId
    workflow_version_id: str
    status: RunStatus


# ---------------------------------------------------------------------------
# Fingerprint helper \u2014 canonical-JSON SHA-256 of the dedup key
# ---------------------------------------------------------------------------


def _fingerprint_inputs(workflow_version_id: str, inputs: Mapping[str, Any]) -> str:
    """Stable hash of the ``(workflow_version_id, inputs)`` dedup pair.

    Uses ``json.dumps(..., sort_keys=True, separators=(",", ":"))``
    so dict-key ordering and whitespace differences collapse to the
    same digest. SHA-256 because the digest is stored alongside the
    audit trail \u2014 collision-resistance matters; speed does not.
    """
    canonical = json.dumps(
        {"workflow_version_id": workflow_version_id, "inputs": dict(inputs)},
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Lifecycle event kinds
# ---------------------------------------------------------------------------


#: Wire-stable kind tag for the start-run lifecycle event.
LIFECYCLE_KIND_WORKFLOW_STARTED: Final[str] = "workflow.started"

#: Wire-stable kind tag for the cancel-run lifecycle event.
LIFECYCLE_KIND_WORKFLOW_CANCELLED: Final[str] = "workflow.cancelled"

#: Wire-stable kind tag for the pause-run lifecycle event.
LIFECYCLE_KIND_WORKFLOW_PAUSED: Final[str] = "workflow.paused"

#: Wire-stable kind tag for the resume-run lifecycle event.
LIFECYCLE_KIND_WORKFLOW_RESUMED: Final[str] = "workflow.resumed"


# ---------------------------------------------------------------------------
# Terminate poll-loop budget defaults
# ---------------------------------------------------------------------------


#: Default number of ``get_workflow_state`` polls :meth:`RunController.cancel_run`
#: will issue after :meth:`~custos_workflow.runtime.WorkflowClient.terminate_workflow`
#: before surfacing :class:`WorkflowRuntimeUnavailableError`. Sized to give a
#: cooperatively-terminating workflow about half a minute at the default
#: per-poll interval; production callers tune this through the constructor.
DEFAULT_TERMINATE_POLL_ATTEMPTS: Final[int] = 30

#: Default per-poll sleep (seconds) between ``get_workflow_state`` calls. Tests
#: inject a no-op sleeper so the loop completes synchronously.
DEFAULT_TERMINATE_POLL_INTERVAL_S: Final[float] = 1.0


# ---------------------------------------------------------------------------
# WorkflowClient surface (structural)
# ---------------------------------------------------------------------------


@runtime_checkable
class _WorkflowClient(Protocol):
    """Subset of :class:`~custos_workflow.runtime.WorkflowClient`
    the controller depends on.

    Structural so both the real Dapr-backed
    :class:`~custos_workflow.runtime.WorkflowClient` and the
    in-memory :class:`~custos_workflow.runtime.FakeWorkflowClient`
    satisfy it without an explicit runtime check.
    """

    async def schedule_new_workflow(self, request: ScheduleWorkflowRequest) -> str:
        """Schedule a new workflow run; return Dapr's ``instance_id``."""
        ...

    async def terminate_workflow(self, request: TerminateRunRequest) -> None:
        """Terminate the Dapr Workflow instance identified by ``request.instance_id``."""
        ...

    async def get_workflow_state(self, request: GetRunStateRequest) -> RuntimeRunState | None:
        """Return the current runtime snapshot, or ``None`` if Dapr forgot the instance."""
        ...

    async def pause_workflow(self, request: PauseRunRequest) -> None:
        """Pause (suspend) the Dapr Workflow instance identified by ``request.instance_id``."""
        ...

    async def resume_workflow(self, request: ResumeRunRequest) -> None:
        """Resume the Dapr Workflow instance identified by ``request.instance_id``."""
        ...


# ---------------------------------------------------------------------------
# RunController
# ---------------------------------------------------------------------------


class RunController:
    """Lifecycle entry point for the workflow-service Run Controller.

    Bundles the injected dependencies the
    :meth:`start_run` algorithm walks (catalog, run store, workflow
    runtime client, activity-type registry, lifecycle publisher,
    clock) into a single cohesive surface. Other lifecycle
    operations (``pause_run``, ``resume_run``, \u2026) land in
    later tasks (WF-IMPL-038\u2026) and reuse the same surface.

    The controller is intentionally stateless across calls EXCEPT
    for the in-process inputs fingerprint map; see the module
    docstring for the rationale.
    """

    def __init__(
        self,
        *,
        catalog: CatalogClient,
        store: RunStore,
        workflow_client: _WorkflowClient,
        activity_registry: ActivityTypeRegistry,
        lifecycle_publisher: LifecycleEventPublisher,
        clock: Clock,
        terminate_poll_attempts: int = DEFAULT_TERMINATE_POLL_ATTEMPTS,
        terminate_poll_interval_seconds: float = DEFAULT_TERMINATE_POLL_INTERVAL_S,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        if terminate_poll_attempts < 1:
            raise ValueError("terminate_poll_attempts must be >= 1")
        if terminate_poll_interval_seconds < 0:
            raise ValueError("terminate_poll_interval_seconds must be >= 0")
        self._catalog: CatalogClient = catalog
        self._store: RunStore = store
        self._workflow_client: _WorkflowClient = workflow_client
        self._activity_registry: ActivityTypeRegistry = activity_registry
        self._lifecycle_publisher: LifecycleEventPublisher = lifecycle_publisher
        self._clock: Clock = clock
        self._terminate_poll_attempts: int = terminate_poll_attempts
        self._terminate_poll_interval: float = terminate_poll_interval_seconds
        self._sleep: Callable[[float], Awaitable[None]] = sleep or asyncio.sleep
        # In-process dedup-fingerprint map. Key is the dedup-stable
        # ``(workspace_id, run_id)`` pair; value is the SHA-256 of
        # the canonical-JSON ``(workflow_version_id, inputs)`` pair.
        self._input_fingerprints: dict[tuple[str, RunId], str] = {}

    # ------------------------------------------------------------------
    # start_run
    # ------------------------------------------------------------------

    async def start_run(
        self,
        *,
        workspace_id: str,
        workflow_version_id: str,
        inputs: Mapping[str, Any] | None = None,
        idempotency_key: str | None = None,
    ) -> RunRef:
        """Start a workflow run; return a :class:`RunRef`.

        See the module docstring for the full six-gate algorithm.
        The argument validation here is intentionally narrow: the
        API Adapter is responsible for the full request schema
        check; the controller only refuses the inputs that would
        violate the contracts it owns directly (empty
        ``workspace_id`` would short-circuit
        :func:`derive_run_id` with a confusing message).

        Args:
            workspace_id: The owning workspace; must be non-empty.
            workflow_version_id: The Catalog ``WorkflowVersion``
                UUID to start.
            inputs: User-supplied run inputs (the values keyed off
                ``inputs.*`` in CEL expressions). ``None`` is
                treated as ``{}``.
            idempotency_key: Caller-supplied idempotency token, or
                ``None``. An empty string is treated as "no key
                supplied".

        Returns:
            A :class:`RunRef` carrying the (possibly re-discovered)
            ``run_id`` and current ``status``.

        Raises:
            ValueError: ``workspace_id`` is empty (re-raised from
                :func:`derive_run_id`).
            RunStateConflictError: A prior run exists at the same
                ``(workspace_id, run_id)`` with a divergent
                ``(workflow_version_id, inputs)`` pair.
            WorkflowRuntimeUnavailableError: The Dapr runtime
                refused the schedule. The persisted record is
                transitioned to ``failed`` before this is raised,
                so no record is left stuck in ``queued``.
        """
        normalised_inputs: dict[str, Any] = dict(inputs or {})
        fingerprint = _fingerprint_inputs(workflow_version_id, normalised_inputs)
        run_id = derive_run_id(workspace_id, idempotency_key)

        # ---- Gate 2: deduplicate against the existing-run window. ----
        existing = await self._store.get_run(workspace_id, run_id)
        if existing is not None:
            self._assert_dedup_compatible(
                existing=existing,
                run_id=run_id,
                workflow_version_id=workflow_version_id,
                fingerprint=fingerprint,
            )
            return RunRef(
                workspace_id=workspace_id,
                run_id=run_id,
                workflow_version_id=existing.workflow_version,
                status=existing.status,
            )

        # ---- Gate 3: fetch Catalog + compile. -------------------------
        workflow_version = await self._catalog.get_workflow_version(
            workspace_id, workflow_version_id
        )
        now = self._clock.now()
        run_meta = RunMeta(
            workspace_id=workspace_id,
            workflow_version_id=workflow_version_id,
            workflow_name=workflow_version.name,
            workflow_version_label=workflow_version.version_label,
            started_at_default=now,
        )
        graph = compile_workflow(workflow_version.document, run_meta, self._activity_registry)

        # ---- Gate 4: persist as queued. ------------------------------
        record = RunRecord(
            workspace_id=workspace_id,
            run_id=run_id,
            workflow_id=workflow_version.workflow_id,
            workflow_version=workflow_version_id,
            status=RunStatus.QUEUED,
            reason=None,
            started_at=now,
            updated_at=now,
            compiled_graph=graph,
        )
        try:
            await self._store.put_run(record)
        except RunStateConflictError:
            # Concurrent ``start_run`` race: another caller won the
            # ``put_run`` between our Gate-2 ``get_run`` and now.
            # The loser re-reads the persisted row and re-runs the
            # dedup compatibility check; a byte-equal
            # ``(workflow_version_id, inputs)`` payload still
            # collapses to an idempotent :class:`RunRef`, matching
            # the dedup contract. ``InProcessRunStore.put_run``
            # compares the full :class:`RunRecord` (including
            # ``started_at``/``updated_at`` clock samples and
            # status), so two byte-equal requests issued back-to-
            # back commonly look divergent at the store layer even
            # when their dedup-relevant fields match — the
            # controller is the right layer to reconcile that.
            existing = await self._store.get_run(workspace_id, run_id)
            if existing is None:
                # Defensive: ``put_run`` raised state-conflict but
                # the row vanished by the time we re-read. Surface
                # as the original conflict for the operator.
                raise
            self._assert_dedup_compatible(
                existing=existing,
                run_id=run_id,
                workflow_version_id=workflow_version_id,
                fingerprint=fingerprint,
            )
            return RunRef(
                workspace_id=workspace_id,
                run_id=run_id,
                workflow_version_id=existing.workflow_version,
                status=existing.status,
            )
        self._input_fingerprints[(workspace_id, run_id)] = fingerprint

        # ---- Gate 5: schedule on Dapr. -------------------------------
        run_input = RunInput(
            workspace_id=workspace_id,
            workflow_version_id=workflow_version_id,
            compiled_graph_json=to_json(graph),
            inputs=normalised_inputs,
            idempotency_key=idempotency_key or "",
        )
        try:
            await self._workflow_client.schedule_new_workflow(
                ScheduleWorkflowRequest(
                    workflow=WORKFLOW_NAME,
                    instance_id=str(run_id),
                    input=run_input,
                )
            )
        except Exception as exc:
            # design contract: no half-persisted record. Mark the
            # row failed BEFORE surfacing the runtime error so an
            # operator triaging the failure sees a coherent state
            # transition trail. The failure cause is preserved on
            # both the row's ``reason`` and the raised error's
            # ``cause`` for cross-referencing.
            await self._store.update_run_status(
                workspace_id,
                run_id,
                RunStatus.FAILED,
                reason=f"runtime_unavailable: {exc}",
            )
            raise WorkflowRuntimeUnavailableError(
                f"failed to schedule run {run_id} on the workflow runtime",
                run_id=str(run_id),
                cause=str(exc),
            ) from exc

        # ---- Gate 6: transition to running + emit started event. ----
        updated = await self._store.update_run_status(workspace_id, run_id, RunStatus.RUNNING)
        await self._lifecycle_publisher.publish(
            LifecycleEvent(
                kind=LIFECYCLE_KIND_WORKFLOW_STARTED,
                workspace_id=workspace_id,
                run_id=run_id,
                workflow_version_id=workflow_version_id,
                occurred_at=self._clock.now(),
            )
        )
        return RunRef(
            workspace_id=workspace_id,
            run_id=run_id,
            workflow_version_id=workflow_version_id,
            status=updated.status,
        )

    # ------------------------------------------------------------------
    # cancel_run
    # ------------------------------------------------------------------

    async def cancel_run(
        self,
        *,
        workspace_id: str,
        run_id: RunId,
        reason: str | None = None,
    ) -> RunRef:
        """Cancel an in-flight run; return a :class:`RunRef`.

        Implements design.md § Operation: Cancel Run:

        1. Load the :class:`RunRecord` (raise :class:`RunNotFoundError`).
        2. Short-circuit on the idempotent path. A run that is already
           ``cancelled`` returns its current :class:`RunRef` with no
           Dapr call and no event re-publish. A run that is already
           ``cancelling`` is treated as another caller's in-flight
           cancel — we do NOT re-issue ``terminate_workflow`` and we
           do NOT re-emit ``workflow.cancelled``; the original caller
           owns the terminal transition.
        3. Transition ``{queued, running, pausing, paused} \u2192 cancelling``.
           Illegal sources (``succeeded``, ``failed``) surface
           :class:`RunStateConflictError` through
           :meth:`RunStore.update_run_status`.
        4. ``terminate_workflow(instance_id=run_id)``. Any runtime
           exception surfaces as
           :class:`WorkflowRuntimeUnavailableError`; the row stays
           ``cancelling`` so an operator can retry.
        5. Poll ``get_workflow_state`` until the runtime reports a
           terminal status (``terminated`` / ``completed`` /
           ``failed``) or until Dapr forgets the instance. The
           per-poll sleep and attempt count are constructor-injected
           for deterministic test runs. Budget exhaustion surfaces
           :class:`WorkflowRuntimeUnavailableError`.
        6. Transition to ``cancelled`` and emit
           ``workflow.cancelled`` carrying the cancellation
           ``reason`` (when supplied) on :attr:`LifecycleEvent.extra`.

        Args:
            workspace_id: The owning workspace.
            run_id: The :class:`RunId` to cancel.
            reason: Operator-supplied cancellation reason. Persisted
                on :attr:`RunRecord.reason` and surfaced in
                ``workflow.cancelled.extra.reason``.

        Returns:
            A :class:`RunRef` carrying the run's current status.
            On the active-cancel path the returned status is
            ``cancelled``; on the idempotent-replay path the status
            is whatever the store currently holds
            (``cancelling`` or ``cancelled``).

        Raises:
            RunNotFoundError: No record exists at
                ``(workspace_id, run_id)``.
            RunStateConflictError: The record is in a terminal
                non-cancel status (``succeeded`` / ``failed``)
                from which the cancel transition is illegal.
            WorkflowRuntimeUnavailableError: The Dapr runtime
                refused the terminate call OR did not confirm
                termination within
                ``terminate_poll_attempts``.
        """
        record = await self._store.get_run(workspace_id, run_id)
        if record is None:
            raise RunNotFoundError(
                f"run {run_id} not found in workspace {workspace_id!r}",
                run_id=str(run_id),
            )

        # Idempotent no-op: another caller is/has handled this cancel.
        if record.status in (RunStatus.CANCELLED, RunStatus.CANCELLING):
            return RunRef(
                workspace_id=workspace_id,
                run_id=run_id,
                workflow_version_id=record.workflow_version,
                status=record.status,
            )

        # Gate 3: transition to ``cancelling``. ``RunStateConflictError``
        # from an illegal source (``succeeded`` / ``failed``) is the
        # documented surface for the API Adapter and propagates as-is.
        await self._store.update_run_status(
            workspace_id, run_id, RunStatus.CANCELLING, reason=reason
        )

        # Gate 4: terminate the Dapr instance.
        try:
            await self._workflow_client.terminate_workflow(
                TerminateRunRequest(instance_id=str(run_id))
            )
        except Exception as exc:
            raise WorkflowRuntimeUnavailableError(
                f"failed to terminate run {run_id} on the workflow runtime",
                run_id=str(run_id),
                cause=str(exc),
            ) from exc

        # Gate 5: poll until Dapr confirms termination.
        await self._poll_until_terminated(run_id)

        # Gate 6: transition to ``cancelled`` and emit lifecycle event.
        finalised = await self._store.update_run_status(
            workspace_id, run_id, RunStatus.CANCELLED, reason=reason
        )
        extra: dict[str, Any] = {"reason": reason} if reason is not None else {}
        await self._lifecycle_publisher.publish(
            LifecycleEvent(
                kind=LIFECYCLE_KIND_WORKFLOW_CANCELLED,
                workspace_id=workspace_id,
                run_id=run_id,
                workflow_version_id=record.workflow_version,
                occurred_at=self._clock.now(),
                extra=extra,
            )
        )
        return RunRef(
            workspace_id=workspace_id,
            run_id=run_id,
            workflow_version_id=record.workflow_version,
            status=finalised.status,
        )

    # ------------------------------------------------------------------
    # pause_run
    # ------------------------------------------------------------------

    async def pause_run(
        self,
        *,
        workspace_id: str,
        run_id: RunId,
    ) -> RunRef:
        """Pause an in-flight run; return a :class:`RunRef`.

        Implements design.md § Internal Structure (Run Controller):

        1. Load the :class:`RunRecord` (raise :class:`RunNotFoundError`).
        2. Short-circuit on the idempotent path. A run that is already
           ``paused`` or ``pausing`` returns its current
           :class:`RunRef` with no Dapr re-call and no event
           re-publish.
        3. Transition ``running → pausing``. Illegal sources surface
           :class:`RunStateConflictError` through
           :meth:`RunStore.update_run_status`.
        4. ``pause_workflow(instance_id=run_id)``. Any runtime exception
           surfaces as :class:`WorkflowRuntimeUnavailableError`; the
           row stays ``pausing`` so an operator can reconcile.
        5. Transition ``pausing → paused`` and emit ``workflow.paused``.

        Args:
            workspace_id: The owning workspace.
            run_id: The :class:`RunId` to pause.

        Returns:
            A :class:`RunRef` carrying the run's current status. On
            the active-pause path the status is ``paused``; on the
            idempotent-replay path the status is whatever the store
            currently holds (``pausing`` or ``paused``).

        Raises:
            RunNotFoundError: No record exists at
                ``(workspace_id, run_id)``.
            RunStateConflictError: The record is in a status from
                which the pause transition is illegal (anything
                other than ``running`` / ``pausing`` / ``paused``).
            WorkflowRuntimeUnavailableError: The Dapr runtime
                refused the pause call.
        """
        record = await self._store.get_run(workspace_id, run_id)
        if record is None:
            raise RunNotFoundError(
                f"run {run_id} not found in workspace {workspace_id!r}",
                run_id=str(run_id),
            )

        # Idempotent no-op: another caller is/has handled this pause.
        if record.status in (RunStatus.PAUSED, RunStatus.PAUSING):
            return RunRef(
                workspace_id=workspace_id,
                run_id=run_id,
                workflow_version_id=record.workflow_version,
                status=record.status,
            )

        # Gate 3: transition to ``pausing``. ``RunStateConflictError``
        # from an illegal source (e.g. ``queued``, ``cancelling``,
        # terminal statuses) propagates as-is.
        await self._store.update_run_status(workspace_id, run_id, RunStatus.PAUSING)

        # Gate 4: pause the Dapr instance.
        try:
            await self._workflow_client.pause_workflow(PauseRunRequest(instance_id=str(run_id)))
        except Exception as exc:
            raise WorkflowRuntimeUnavailableError(
                f"failed to pause run {run_id} on the workflow runtime",
                run_id=str(run_id),
                cause=str(exc),
            ) from exc

        # Gate 5: transition to ``paused`` and emit lifecycle event.
        finalised = await self._store.update_run_status(workspace_id, run_id, RunStatus.PAUSED)
        await self._lifecycle_publisher.publish(
            LifecycleEvent(
                kind=LIFECYCLE_KIND_WORKFLOW_PAUSED,
                workspace_id=workspace_id,
                run_id=run_id,
                workflow_version_id=record.workflow_version,
                occurred_at=self._clock.now(),
            )
        )
        return RunRef(
            workspace_id=workspace_id,
            run_id=run_id,
            workflow_version_id=record.workflow_version,
            status=finalised.status,
        )

    # ------------------------------------------------------------------
    # resume_run
    # ------------------------------------------------------------------

    async def resume_run(
        self,
        *,
        workspace_id: str,
        run_id: RunId,
    ) -> RunRef:
        """Resume a paused run; return a :class:`RunRef`.

        Implements design.md § Internal Structure (Run Controller):

        1. Load the :class:`RunRecord` (raise :class:`RunNotFoundError`).
        2. Short-circuit on the idempotent path. A run that is already
           ``running`` returns its current :class:`RunRef` with no
           Dapr re-call and no event re-publish.
        3. Refuse any source state that is not ``paused`` (the only
           legal transition for a resume) with
           :class:`RunStateConflictError`. We validate here — BEFORE
           touching Dapr — because the design does not define a
           transitional ``resuming`` status, so a runtime call from
           an illegal source would leave the row and the runtime in
           an unreconcilable disagreement.
        4. ``resume_workflow(instance_id=run_id)``. Any runtime
           exception surfaces as
           :class:`WorkflowRuntimeUnavailableError`; the row stays
           ``paused`` so an operator can retry.
        5. Transition ``paused → running`` and emit ``workflow.resumed``.

        Args:
            workspace_id: The owning workspace.
            run_id: The :class:`RunId` to resume.

        Returns:
            A :class:`RunRef` carrying the run's current status. On
            the active-resume path the status is ``running``; on the
            idempotent-replay path the status is ``running`` already.

        Raises:
            RunNotFoundError: No record exists at
                ``(workspace_id, run_id)``.
            RunStateConflictError: The record is in a status from
                which the resume transition is illegal (anything
                other than ``paused`` / ``running``).
            WorkflowRuntimeUnavailableError: The Dapr runtime
                refused the resume call.
        """
        record = await self._store.get_run(workspace_id, run_id)
        if record is None:
            raise RunNotFoundError(
                f"run {run_id} not found in workspace {workspace_id!r}",
                run_id=str(run_id),
            )

        # Idempotent no-op: caller is observing an already-resumed row.
        if record.status == RunStatus.RUNNING:
            return RunRef(
                workspace_id=workspace_id,
                run_id=run_id,
                workflow_version_id=record.workflow_version,
                status=record.status,
            )

        # Gate 3: refuse non-paused sources BEFORE calling Dapr — there
        # is no transitional ``resuming`` state to fall back on, so
        # Dapr-and-row divergence here is unrecoverable.
        if record.status != RunStatus.PAUSED:
            raise RunStateConflictError(
                f"cannot resume run {run_id}: current status is "
                f"{record.status.value!r}, expected 'paused'",
                run_id=str(run_id),
                current_status=record.status.value,
                attempted_status=RunStatus.RUNNING.value,
            )

        # Gate 4: resume the Dapr instance.
        try:
            await self._workflow_client.resume_workflow(ResumeRunRequest(instance_id=str(run_id)))
        except Exception as exc:
            raise WorkflowRuntimeUnavailableError(
                f"failed to resume run {run_id} on the workflow runtime",
                run_id=str(run_id),
                cause=str(exc),
            ) from exc

        # Gate 5: transition to ``running`` and emit lifecycle event.
        finalised = await self._store.update_run_status(workspace_id, run_id, RunStatus.RUNNING)
        await self._lifecycle_publisher.publish(
            LifecycleEvent(
                kind=LIFECYCLE_KIND_WORKFLOW_RESUMED,
                workspace_id=workspace_id,
                run_id=run_id,
                workflow_version_id=record.workflow_version,
                occurred_at=self._clock.now(),
            )
        )
        return RunRef(
            workspace_id=workspace_id,
            run_id=run_id,
            workflow_version_id=record.workflow_version,
            status=finalised.status,
        )

    # ------------------------------------------------------------------
    # internals
    # ------------------------------------------------------------------

    async def _poll_until_terminated(self, run_id: RunId) -> None:
        """Poll Dapr's ``get_workflow_state`` until the instance is terminal.

        Returns silently when the runtime reports a terminal
        :class:`~custos_workflow.runtime._common.RunStatus`
        (``completed`` / ``failed`` / ``terminated``) or when Dapr
        has no record of the instance (``None`` return). Raises
        :class:`WorkflowRuntimeUnavailableError` if the
        per-controller attempt budget is exhausted without a
        terminal observation, OR if any
        :meth:`_WorkflowClient.get_workflow_state` call raises —
        every exception path out of this loop must be the
        documented :class:`WorkflowRuntimeUnavailableError` so
        callers see the single frozen Run Controller error
        taxonomy.

        The per-poll sleep is the constructor-injected
        ``sleep`` callable, defaulting to :func:`asyncio.sleep`;
        tests inject a no-op sleeper so the loop runs synchronously.
        The final attempt deliberately skips the trailing sleep
        because the loop is about to bail out either way.
        """
        request = GetRunStateRequest(instance_id=str(run_id))
        for attempt in range(self._terminate_poll_attempts):
            try:
                state = await self._workflow_client.get_workflow_state(request)
            except Exception as exc:
                raise WorkflowRuntimeUnavailableError(
                    f"failed to poll workflow runtime for run {run_id}",
                    run_id=str(run_id),
                    cause=str(exc),
                ) from exc
            if state is None or state.status in RUNTIME_TERMINAL_STATUSES:
                return
            if attempt < self._terminate_poll_attempts - 1:
                await self._sleep(self._terminate_poll_interval)
        raise WorkflowRuntimeUnavailableError(
            f"workflow runtime did not confirm termination of run {run_id} "
            f"after {self._terminate_poll_attempts} attempts",
            run_id=str(run_id),
        )

    def _assert_dedup_compatible(
        self,
        *,
        existing: RunRecord,
        run_id: RunId,
        workflow_version_id: str,
        fingerprint: str,
    ) -> None:
        """Raise :class:`RunStateConflictError` on a divergent re-start.

        Two divergence shapes:

        * The persisted ``workflow_version`` differs from the new
          request \u2014 caught by the store-side check.
        * The persisted run was started with different ``inputs``
          \u2014 caught by the controller's in-process fingerprint
          map. A missing fingerprint (cross-restart fallback) is
          treated as compatible; see module docstring for the
          guarantee floor.
        """
        if existing.workflow_version != workflow_version_id:
            raise RunStateConflictError(
                f"run {run_id} already exists with workflow_version "
                f"{existing.workflow_version!r}, refusing to restart "
                f"with {workflow_version_id!r}",
                run_id=str(run_id),
                current_status=existing.status.value,
                attempted_status=RunStatus.QUEUED.value,
            )
        prior_fingerprint = self._input_fingerprints.get((existing.workspace_id, run_id))
        if prior_fingerprint is not None and prior_fingerprint != fingerprint:
            raise RunStateConflictError(
                f"run {run_id} already exists with a divergent inputs payload",
                run_id=str(run_id),
                current_status=existing.status.value,
                attempted_status=RunStatus.QUEUED.value,
            )
