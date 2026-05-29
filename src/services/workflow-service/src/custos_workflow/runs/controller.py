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

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, Final, Protocol, runtime_checkable

from custos_workflow.compiler import RunMeta
from custos_workflow.compiler import compile as compile_workflow
from custos_workflow.graph.serialize import to_json
from custos_workflow.runs.errors import (
    RunStateConflictError,
    WorkflowRuntimeUnavailableError,
)
from custos_workflow.runs.ids import RunId, derive_run_id
from custos_workflow.runs.model import RunRecord, RunStatus
from custos_workflow.runs.orchestrator import WORKFLOW_NAME, RunInput
from custos_workflow.runtime._common import ScheduleWorkflowRequest

if TYPE_CHECKING:
    from custos_cel.clock import Clock

    from custos_workflow.bindings.registry import ActivityTypeRegistry
    from custos_workflow.document import WorkflowDocument
    from custos_workflow.runs.store import RunStore

__all__ = [
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
    ) -> None:
        self._catalog: CatalogClient = catalog
        self._store: RunStore = store
        self._workflow_client: _WorkflowClient = workflow_client
        self._activity_registry: ActivityTypeRegistry = activity_registry
        self._lifecycle_publisher: LifecycleEventPublisher = lifecycle_publisher
        self._clock: Clock = clock
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
        await self._store.put_run(record)
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
    # internals
    # ------------------------------------------------------------------

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
