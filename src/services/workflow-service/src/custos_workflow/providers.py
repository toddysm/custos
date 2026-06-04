"""Run-Controller dependency wiring for the FastAPI lifespan (WF-IMPL-043).

Centralises the in-process construction of every collaborator
:class:`~custos_workflow.runs.controller.RunController` needs so
:func:`custos_workflow.create_app` stays a thin factory. Tests inject
their own :class:`RunComponents` to keep the sidecar-free invariant;
production callers let the lifespan build defaults from the
environment.

The defaults intentionally use in-memory adapters everywhere except
the workflow runtime itself: Postgres-backed wiring for the
:class:`~custos_spl.interfaces.metadata_store.MetadataStoreProvider`
is a follow-up infrastructure task, and the Catalog Service client
is a stub that fails loudly when the API surface that needs it
lands (the existing-run dedup path never invokes Catalog, so
``RunController.start_run`` can still smoke through a re-entry).
The :class:`LifecycleEventPublisher` is the only adapter that
flips to its production shape when ``WF_PUBLISH_TOPIC`` is set and
the Dapr endpoint env vars resolve (see :func:`load_run_components`).

Design references:

* ``design/components/workflow-service/design.md`` § Configuration
* ``design/components/workflow-service/implementation-plan.md`` §
  WF-IMPL-043
"""

from __future__ import annotations

import math
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Any, Final, Protocol, cast, runtime_checkable

import httpx
from custos_spl.errors import ImmutableViolation
from custos_spl.ids import RunId as SplRunId
from custos_spl.ids import WorkspaceId as SplWorkspaceId
from custos_spl.interfaces.metadata_store import (
    MetadataStoreProvider,
    RunFilter,
)
from custos_spl.interfaces.metadata_store import (
    Run as SplRun,
)
from custos_spl.pagination import Cursor, Page

from custos_workflow.bindings.registry import (
    ActivityTypeRegistry,
    InMemoryActivityTypeRegistry,
)
from custos_workflow.clients import (
    ActivityRuntimeClient,
    ConnectorClient,
    DaprActivityRuntimeClient,
    DaprCatalogClient,
    DaprConnectorClient,
    NoopActivityRuntimeClient,
    NoopConnectorClient,
)
from custos_workflow.clients._dapr_invoke import (
    read_dapr_env,
)
from custos_workflow.clients.trigger import (
    DaprTriggerServiceClient,
    NoopTriggerServiceClient,
    TriggerServiceClient,
)
from custos_workflow.runs.controller import (
    CatalogClient,
    InMemoryLifecycleEventPublisher,
    LifecycleEventPublisher,
    RunController,
    WorkflowVersion,
    _WorkflowClient,
)
from custos_workflow.runs.events import (
    DaprPubSubLifecyclePublisher,
)
from custos_workflow.runs.replay import NoopReplayReconciler, ReplayReconciler
from custos_workflow.runs.store import InProcessRunStore, RunStore
from custos_workflow.runtime import FakeWorkflowRuntime, WorkflowClient, WorkflowRuntime
from custos_workflow.steps.resume import (
    DEFAULT_RESUME_SUB_SWEEP_INTERVAL_SECONDS,
    InMemoryResumeSubscriptionMirrorRepository,
    ResumeSubscriptionMirrorRepository,
    ResumeSubscriptionReplayReconciler,
    WaitForStepHandler,
)
from custos_workflow.steps.resume.handler import (
    DEFAULT_REGISTER_SUB_MAX_RETRIES,
    DEFAULT_RESUME_SUB_TTL,
)
from custos_workflow.steps.sub_orchestration import (
    DEFAULT_APPROVAL_TIMEOUT,
    DEFAULT_MAX_FANOUT_WIDTH,
)
from custos_workflow.validator import (
    IdempotencyLedger,
    InMemoryIdempotencyLedger,
    StartRunValidator,
)

__all__ = [
    "DEFAULT_APPROVAL_TIMEOUT",
    "DEFAULT_DAPR_PUBLISH_TOPIC",
    "DEFAULT_MAX_FANOUT_WIDTH",
    "DEFAULT_OUTBOUND_RPC_TIMEOUT_MS",
    "ENV_APPROVAL_DEFAULT_TIMEOUT",
    "ENV_ARM_APP_ID",
    "ENV_CATALOG_APP_ID",
    "ENV_CONNECTOR_APP_ID",
    "ENV_DAPR_ENDPOINT",
    "ENV_DAPR_WORKFLOW_COMPONENT",
    "ENV_ENVIRONMENT",
    "ENV_MAX_FANOUT_WIDTH",
    "ENV_OUTBOUND_RPC_TIMEOUT_MS",
    "ENV_PUBLISH_PUBSUB",
    "ENV_PUBLISH_TOPIC",
    "ENV_REGISTER_SUB_MAX_RETRIES",
    "ENV_RESUME_SUB_DEFAULT_TTL",
    "ENV_RESUME_SUB_SWEEP_INTERVAL",
    "ENV_TS_ENDPOINT",
    "RunComponents",
    "WorkflowRuntimeProtocol",
    "load_run_components",
]


# ---------------------------------------------------------------------------
# Environment surface
# ---------------------------------------------------------------------------


#: Required at lifespan startup unless a pre-built
#: :class:`RunComponents` is injected. Missing values raise
#: :class:`RuntimeError` at lifespan startup (not at import time).
ENV_DAPR_WORKFLOW_COMPONENT = "WF_DAPR_WORKFLOW_COMPONENT"

#: Optional. When set the lifespan wires
#: :class:`DaprPubSubLifecyclePublisher` instead of the in-memory
#: stub. Defaults to :data:`DEFAULT_DAPR_PUBLISH_TOPIC` when the
#: companion ``WF_PUBLISH_PUBSUB`` is set without it.
ENV_PUBLISH_TOPIC = "WF_PUBLISH_TOPIC"

#: Optional. Pub/Sub component name. When set together with
#: ``WF_PUBLISH_TOPIC`` (or alone, falling back to
#: :data:`DEFAULT_DAPR_PUBLISH_TOPIC`) the lifespan wires the
#: production :class:`DaprPubSubLifecyclePublisher`.
ENV_PUBLISH_PUBSUB = "WF_PUBLISH_PUBSUB"

#: Optional. Dapr sidecar HTTP base URL. Defaults to
#: ``http://127.0.0.1:3500`` (the Dapr SDK default).
ENV_DAPR_ENDPOINT = "WF_DAPR_HTTP_ENDPOINT"

#: design.md § Configuration default for ``WF_PUBLISH_TOPIC``.
DEFAULT_DAPR_PUBLISH_TOPIC = "custos.workflow.events"

#: Required at lifespan startup to activate the production
#: :class:`~custos_workflow.clients.DaprActivityRuntimeClient`.
#: Value is the Activity Runtime Manager Dapr app-id (used by
#: :func:`~custos_workflow.clients._dapr_invoke.read_dapr_env` to
#: build the canonical ``…/v1.0/invoke/<app-id>/method/…`` URL).
#: When unset, the lifespan falls back to
#: :class:`~custos_workflow.clients.NoopActivityRuntimeClient`.
ENV_ARM_APP_ID = "WF_ARM_ENDPOINT"

#: Required at lifespan startup to activate the production
#: :class:`~custos_workflow.clients.DaprConnectorClient`.
#: Value is the Connector Service Dapr app-id. When unset, the
#: lifespan falls back to
#: :class:`~custos_workflow.clients.NoopConnectorClient`.
ENV_CONNECTOR_APP_ID = "WF_CONNECTOR_ENDPOINT"

#: Required on the production path to activate the
#: :class:`~custos_workflow.clients.DaprCatalogClient`. Value is the
#: Catalog Service Dapr app-id, used by
#: :func:`~custos_workflow.clients._dapr_invoke.read_dapr_env` to
#: build the canonical ``…/v1.0/invoke/<app-id>/method/…`` URL the
#: client issues ``GetWorkflowVersion`` against. When unset, the
#: behaviour depends on :data:`ENV_ENVIRONMENT`: a ``production``
#: deployment fails fast (:func:`_build_catalog_client` raises
#: :class:`RuntimeError`) so a worker never silently serves
#: ``StartRun`` off the not-configured stub; any other environment
#: keeps the in-process :class:`_NotConfiguredCatalogClient`
#: fallback so the sidecar-free dev / test path stays composable
#: (the existing-run dedup branch of ``RunController.start_run``
#: never touches the Catalog client).
ENV_CATALOG_APP_ID = "WF_CATALOG_ENDPOINT"

#: Optional. Deployment environment label (e.g. ``production``,
#: ``staging``, ``development``). Consulted only to decide whether a
#: missing :data:`ENV_CATALOG_APP_ID` is a fatal misconfiguration:
#: when it equals :data:`_PRODUCTION_ENVIRONMENT` (case-insensitive)
#: the lifespan refuses to start without a Catalog endpoint. Unset /
#: any other value preserves the not-configured fallback.
ENV_ENVIRONMENT = "ENVIRONMENT"

#: Value of :data:`ENV_ENVIRONMENT` that marks a production
#: deployment (compared case-insensitively).
_PRODUCTION_ENVIRONMENT: Final[str] = "production"


#: Optional. Per-request timeout shared by both the ARM and
#: Connector outbound adapters, expressed in milliseconds.
#: Defaults to :data:`DEFAULT_OUTBOUND_RPC_TIMEOUT_MS`. The value
#: is parsed once at lifespan startup and passed to every adapter
#: as a float (seconds).
ENV_OUTBOUND_RPC_TIMEOUT_MS = "WF_OUTBOUND_RPC_TIMEOUT_MS"

#: Default for :data:`ENV_OUTBOUND_RPC_TIMEOUT_MS` (10 s, matching
#: the per-adapter
#: :data:`~custos_workflow.clients._dapr_invoke.DEFAULT_OUTBOUND_RPC_TIMEOUT_SECONDS`
#: default — kept in lock-step so an operator that does not set
#: the env var sees the same behaviour as the adapter's own
#: dataclass default).
DEFAULT_OUTBOUND_RPC_TIMEOUT_MS: int = 10_000

#: Optional. Upper bound on the number of children a single ``forEach``
#: fan-out may spawn (design.md § Configuration). Parsed once at
#: lifespan startup and threaded into the
#: :class:`~custos_workflow.steps.sub_orchestration.SubOrchestrationManager`.
#: Defaults to
#: :data:`~custos_workflow.steps.sub_orchestration.DEFAULT_MAX_FANOUT_WIDTH`.
ENV_MAX_FANOUT_WIDTH = "WF_MAX_FANOUT_WIDTH"

#: Optional. ISO-8601 duration applied as the approval-gate timeout
#: when a document leaves ``approval.timeout`` at the model default
#: (design.md § Configuration). Parsed once at lifespan startup and
#: threaded into the
#: :class:`~custos_workflow.steps.sub_orchestration.SubOrchestrationManager`.
#: Defaults to
#: :data:`~custos_workflow.steps.sub_orchestration.DEFAULT_APPROVAL_TIMEOUT`
#: (``PT24H``).
ENV_APPROVAL_DEFAULT_TIMEOUT = "WF_APPROVAL_DEFAULT_TIMEOUT"

#: Required at lifespan startup to activate the production resume
#: path. Value is the Trigger Service (COMP-004) Dapr app-id, used
#: by :func:`~custos_workflow.clients._dapr_invoke.read_dapr_env`
#: to build the canonical ``…/v1.0/invoke/<app-id>/method/…`` URL
#: the :class:`~custos_workflow.clients.trigger.DaprTriggerServiceClient`
#: posts ``RegisterResumeSubscription`` / ``CancelResumeSubscription``
#: to. Unlike the ARM / Connector app-ids (which fall back to Noop
#: in-process adapters for the sidecar-free dev path), the resume
#: path is **required** in production: :func:`_resolve_run_components`
#: fails fast with :class:`RuntimeError` when it is unset (design.md
#: § Configuration marks it ``Required: Yes``). When unset in a
#: curated test ``env`` mapping, :func:`load_run_components` still
#: wires the in-process :class:`~custos_workflow.clients.trigger.NoopTriggerServiceClient`
#: so unit tests stay sidecar-free.
ENV_TS_ENDPOINT = "WF_TS_ENDPOINT"

#: Optional. Default TTL applied to a ``RegisterResumeSubscription``
#: when a ``waitFor:`` step pins no explicit ``waitFor.ttl``
#: (design.md § Configuration). Parsed once at lifespan startup as
#: an ISO-8601 duration and threaded into both the
#: :class:`~custos_workflow.steps.resume.WaitForStepHandler` and the
#: production
#: :class:`~custos_workflow.steps.resume.ResumeSubscriptionReplayReconciler`.
#: Defaults to
#: :data:`~custos_workflow.steps.resume.DEFAULT_RESUME_SUB_TTL`
#: (``PT24H``).
ENV_RESUME_SUB_DEFAULT_TTL = "WF_RESUME_SUB_DEFAULT_TTL"

#: Optional. Upper bound on the bounded-backoff retry loop the
#: :class:`~custos_workflow.steps.resume.WaitForStepHandler` runs
#: when ``RegisterResumeSubscription`` keeps failing before it gives
#: up and fails the wait step ``class: retryable`` (design.md §
#: Configuration). Parsed once at lifespan startup as a positive
#: integer. Defaults to
#: :data:`~custos_workflow.steps.resume.DEFAULT_REGISTER_SUB_MAX_RETRIES`
#: (``5``).
ENV_REGISTER_SUB_MAX_RETRIES = "WF_REGISTER_SUB_MAX_RETRIES"

#: Optional. Wall-clock interval (in seconds) between TTL-expiry
#: garbage-collection sweeps of the resume-subscription mirror table
#: (WF-IMPL-109; design.md § Data Models — *TTL-expired mirrors are
#: garbage-collected on a periodic sweep*). Parsed once at lifespan
#: startup as a positive number and threaded into the background
#: :class:`~custos_workflow.steps.resume.ResumeSubscriptionTtlSweeper`
#: task the FastAPI lifespan runs. Defaults to
#: :data:`~custos_workflow.steps.resume.DEFAULT_RESUME_SUB_SWEEP_INTERVAL_SECONDS`
#: (``300`` — five minutes).
ENV_RESUME_SUB_SWEEP_INTERVAL = "WF_RESUME_SUB_SWEEP_INTERVAL"

#: Strict ISO-8601 duration grammar used to parse
#: :data:`ENV_APPROVAL_DEFAULT_TIMEOUT`. Mirrors
#: :data:`custos_workflow.runs.wait._ISO8601_DURATION_PATTERN` so the
#: provider has a self-contained parser and never imports a run-time
#: module from a startup-config concern; parity is asserted in the
#: unit tests.
_ISO8601_DURATION_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^P(?:"
    r"(?P<weeks>\d+)W"
    r"|"
    r"(?:(?P<days>\d+)D)?"
    r"(?:T(?:(?P<hours>\d+)H)?(?:(?P<minutes>\d+)M)?(?:(?P<seconds>\d+(?:\.\d+)?)S)?)?"
    r")$"
)


# ---------------------------------------------------------------------------
# Structural runtime protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class WorkflowRuntimeProtocol(Protocol):
    """The subset of the Dapr Workflow runtime the lifespan drives.

    Both :class:`custos_workflow.runtime.WorkflowRuntime` and
    :class:`custos_workflow.runtime.FakeWorkflowRuntime` satisfy this
    structurally — keeping the lifespan ``async def`` calls
    sidecar-free in the test path.
    """

    def register_workflow(
        self, fn: Any, *, name: str | None = None
    ) -> None:  # pragma: no cover - protocol
        ...

    async def start(self) -> None:  # pragma: no cover - protocol
        ...

    async def shutdown(self) -> None:  # pragma: no cover - protocol
        ...

    async def wait_for_worker_ready(
        self, *, timeout: float = 30.0
    ) -> bool:  # pragma: no cover - protocol
        ...

    @property
    def is_ready(self) -> bool:  # pragma: no cover - protocol
        ...


# ---------------------------------------------------------------------------
# Stub collaborators (in-process defaults)
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class _SystemClock:
    """Wall-clock :class:`~custos_cel.clock.Clock` for the Run Controller.

    The Run Controller uses ``clock.now()`` exclusively for
    persisted ``started_at`` / ``updated_at`` timestamps minted
    outside the Dapr workflow context. Replay-determinism is
    therefore irrelevant here — the orchestrator itself uses
    :class:`~custos_cel.clock.DaprWorkflowClock`.
    """

    def now(self) -> datetime:
        return datetime.now(UTC)


class _NotConfiguredCatalogClient:
    """Default :class:`CatalogClient` until the real client lands.

    Every method raises :class:`NotImplementedError`. The existing-run
    dedup branch of ``RunController.start_run`` never reaches this
    client, so a re-entry against an already-persisted run still
    succeeds. The first uncached ``start_run`` will surface a
    clear error pointing operators at the follow-up wiring task.
    """

    async def get_workflow_version(
        self, workspace_id: str, workflow_version_id: str
    ) -> WorkflowVersion:
        raise NotImplementedError(
            "Catalog client is not wired in this build: set the Catalog "
            "Service endpoint and inject a real client through "
            "RunComponents. WF-IMPL-043 defers Catalog wiring to a "
            "follow-up infrastructure task."
        )


class _InProcessMetadataStoreProvider:
    """In-memory subset of :class:`MetadataStoreProvider` for dev / tests.

    Implements only the four Run-row methods
    :class:`~custos_workflow.runs.store.InProcessRunStore`
    actually invokes. All other Protocol methods raise
    :class:`NotImplementedError` so a future caller that wires the
    same provider into a different surface (Step, Trigger, …) fails
    loudly instead of silently returning empty results.

    Postgres-backed wiring lands in a follow-up infrastructure task;
    until then this stub keeps the Run Controller composable from
    pure Python.
    """

    def __init__(self) -> None:
        # Key: ``(workspace_id, run_id)``. Value: persisted Run row.
        self._runs: dict[tuple[str, str], SplRun] = {}

    async def put_run(self, workspace_id: SplWorkspaceId, run: SplRun) -> SplRun:
        key = (str(workspace_id), str(run.run_id))
        if key in self._runs:
            raise ImmutableViolation(
                f"run already exists: workspace_id={workspace_id!r} run_id={run.run_id!r}"
            )
        self._runs[key] = run
        return run

    async def update_run_status(
        self,
        workspace_id: SplWorkspaceId,
        run_id: SplRunId,
        status: str,
        reason: str | None = None,
    ) -> SplRun:
        key = (str(workspace_id), str(run_id))
        existing = self._runs.get(key)
        if existing is None:
            raise KeyError(key)
        # Mirror the SPL Postgres adapter
        # (``custos_pg/adapters/metadata.py``: ``SET ..., updated_at = now()``)
        # so consumers see a fresh ``updated_at`` on every status transition;
        # otherwise runs started against the default in-process wiring would
        # report stale timestamps after moving queued -> running -> failed.
        updated = SplRun(
            workspace_id=existing.workspace_id,
            run_id=existing.run_id,
            workflow_id=existing.workflow_id,
            workflow_version=existing.workflow_version,
            status=status,
            reason=reason,
            started_at=existing.started_at,
            updated_at=datetime.now(UTC),
        )
        self._runs[key] = updated
        return updated

    async def get_run(self, workspace_id: SplWorkspaceId, run_id: SplRunId) -> SplRun | None:
        return self._runs.get((str(workspace_id), str(run_id)))

    async def list_runs(
        self,
        workspace_id: SplWorkspaceId,
        filter: RunFilter | None = None,
        cursor: Cursor | None = None,
        limit: int | None = None,
    ) -> Page[SplRun]:
        ws = str(workspace_id)
        all_rows = [r for (w, _), r in self._runs.items() if w == ws]
        offset = int(cursor.token) if cursor is not None else 0
        if limit is None:
            window = all_rows[offset:]
            next_cursor = None
        else:
            window = all_rows[offset : offset + limit]
            next_offset = offset + len(window)
            next_cursor = Cursor(token=str(next_offset)) if next_offset < len(all_rows) else None
        return Page(items=window, next_cursor=next_cursor)


# ---------------------------------------------------------------------------
# RunComponents bundle
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class RunComponents:
    """The collaborators the FastAPI lifespan wires onto ``app.state``.

    Tests construct this directly (with a
    :class:`~custos_workflow.runtime.FakeWorkflowRuntime` and an
    :class:`InMemoryLifecycleEventPublisher`) and inject it into
    :func:`custos_workflow.create_app`; production callers let the
    lifespan call :func:`load_run_components`.

    Attributes:
        workflow_runtime: The runtime the lifespan starts / stops.
        workflow_client: Client surface the Run Controller drives.
        run_store: Persistence adapter for :class:`RunRecord`.
        lifecycle_publisher: Pub/Sub adapter for workflow lifecycle
            events.
        replay_reconciler: Hook fired before the orchestrator
            dispatches its first step.
        run_controller: Fully-wired controller. Constructed last so
            every collaborator above is observed in the same shape
            the API layer (WF-IMPL-037+) will pull from
            ``app.state``.
        activity_client: WF-IMPL-049's
            :class:`~custos_workflow.clients.ActivityRuntimeClient`
            stub that the
            :class:`~custos_workflow.steps.activity_step.ActivityStepHandler`
            schedules attempts through on the *legacy* in-process
            :meth:`~custos_workflow.steps.activity_step.ActivityStepHandler.execute`
            path. WF-IMPL-080 deliberately keeps this slot a
            :class:`~custos_workflow.clients.NoopActivityRuntimeClient`
            even when the production env vars are set: the
            production orchestrator uses the
            :meth:`~custos_workflow.steps.activity_step.ActivityStepHandler.iter_calls`
            yield-protocol (WF-IMPL-074), which routes through
            the WF-IMPL-079 outbound-activity bridges instead and
            never touches this field. The Dapr async client lives
            on :attr:`outbound_activity_client` so the sync
            Protocol contract the Step Coordinator depends on is
            never violated.
        connector_client: WF-IMPL-050's
            :class:`~custos_workflow.clients.ConnectorClient` stub
            that the
            :class:`~custos_workflow.steps.activity_step.ActivityStepHandler`
            leases connector contexts through on the *legacy*
            in-process path. WF-IMPL-080 deliberately keeps this
            slot a :class:`~custos_workflow.clients.NoopConnectorClient`
            in production for the same reason as
            :attr:`activity_client`. The Dapr async client lives
            on :attr:`outbound_connector_client`.
        outbound_activity_client: WF-IMPL-080's outbound
            :class:`~custos_workflow.clients.DaprActivityRuntimeClient`
            (when :data:`ENV_ARM_APP_ID` is set) or
            :class:`~custos_workflow.clients.NoopActivityRuntimeClient`
            (otherwise). Threaded into
            :class:`~custos_workflow.runtime.WorkflowRuntime` so
            the WF-IMPL-079
            ``custos.workflow.arm.schedule_activity`` bridge
            registers against it at ``runtime.start()``. The
            bridge body adapts the sync / async surface via
            :func:`asyncio.run` (see ``_call_sync_or_async`` in
            :mod:`custos_workflow.runtime.dapr_activities`), so
            both flavours are safe here even though the field is
            typed as the *sync* Protocol — the bridge is the only
            consumer of this field, and it is the one place that
            owns the adaptation.
        outbound_connector_client: Mirrors
            :attr:`outbound_activity_client` for the Connector
            Service path; threaded into
            :class:`~custos_workflow.runtime.WorkflowRuntime`
            for the ``custos.workflow.connector.bind_for_step``
            bridge registration.
        dapr_http_client: HTTP client lifespan-owned and shared by
            the Dapr Pub/Sub publisher (when active) and the
            WF-IMPL-080 outbound ARM / Connector adapters (when
            their env vars are set). ``None`` when none of the
            production paths are active. The lifespan ``aclose()``
            it on shutdown.
        max_fanout_width: WF-IMPL-094 fan-out width cap
            (:data:`ENV_MAX_FANOUT_WIDTH`) threaded into the
            :class:`~custos_workflow.steps.sub_orchestration.SubOrchestrationManager`
            the lifespan builds.
        approval_default_timeout: WF-IMPL-094 default approval-gate
            timeout (:data:`ENV_APPROVAL_DEFAULT_TIMEOUT`) threaded
            into the same manager; applied when a document leaves
            ``approval.timeout`` at the model default.
        resume_handler: WF-IMPL-108
            :class:`~custos_workflow.steps.resume.WaitForStepHandler`
            the lifespan threads into the top-level orchestrator's
            ``resume_handler`` slot so ``PrimitiveHandler.RESUME_SUBSCRIPTION``
            (``waitFor:``) nodes park the run as ``status="waiting"``
            instead of raising ``step.kind_not_implemented`` (WF-IMPL-107).
            Built over an in-process
            :class:`~custos_workflow.steps.resume.InMemoryResumeSubscriptionMirrorRepository`
            with the env-resolved
            :data:`ENV_RESUME_SUB_DEFAULT_TTL` /
            :data:`ENV_REGISTER_SUB_MAX_RETRIES` knobs; the same mirror
            repository backs the production
            :class:`~custos_workflow.steps.resume.ResumeSubscriptionReplayReconciler`
            wired onto :attr:`replay_reconciler` when
            :data:`ENV_TS_ENDPOINT` is set. The Postgres-backed mirror
            adapter is a follow-up infrastructure task (same staging as
            the idempotency ledger).
        resume_sweep_interval_seconds: WF-IMPL-109 wall-clock interval
            (:data:`ENV_RESUME_SUB_SWEEP_INTERVAL`, default ``300``)
            between TTL-expiry garbage-collection sweeps. The FastAPI
            lifespan threads it into the background
            :class:`~custos_workflow.steps.resume.ResumeSubscriptionTtlSweeper`
            it runs over the same mirror repository
            :attr:`resume_handler` is built on.
    """

    workflow_runtime: WorkflowRuntimeProtocol
    workflow_client: _WorkflowClient
    run_store: RunStore
    lifecycle_publisher: LifecycleEventPublisher
    replay_reconciler: ReplayReconciler
    run_controller: RunController
    activity_client: ActivityRuntimeClient
    connector_client: ConnectorClient
    start_run_validator: StartRunValidator
    outbound_activity_client: ActivityRuntimeClient = field(
        default_factory=lambda: NoopActivityRuntimeClient()
    )
    outbound_connector_client: ConnectorClient = field(
        default_factory=lambda: NoopConnectorClient()
    )
    dapr_http_client: httpx.AsyncClient | None = field(default=None)
    max_fanout_width: int = DEFAULT_MAX_FANOUT_WIDTH
    approval_default_timeout: timedelta = DEFAULT_APPROVAL_TIMEOUT
    resume_handler: WaitForStepHandler = field(
        default_factory=lambda: WaitForStepHandler(InMemoryResumeSubscriptionMirrorRepository())
    )
    resume_sweep_interval_seconds: float = DEFAULT_RESUME_SUB_SWEEP_INTERVAL_SECONDS


# ---------------------------------------------------------------------------
# Defaults builder
# ---------------------------------------------------------------------------


def _build_workflow_client(
    workflow_runtime: WorkflowRuntimeProtocol,
) -> _WorkflowClient:
    """Pair a workflow client with the supplied runtime.

    For the in-memory :class:`FakeWorkflowRuntime` the matching
    :class:`FakeWorkflowClient` shares its instance state so
    ``schedule_new_workflow`` and friends round-trip against the
    same simulated history. For the real runtime each client owns
    its own lazy Dapr connection — the SDK is import-safe so this
    does not open a socket.
    """
    if isinstance(workflow_runtime, FakeWorkflowRuntime):
        return workflow_runtime.client()
    return WorkflowClient()


def _publisher_env_active(env: Mapping[str, str]) -> bool:
    """``True`` when the env flags request the Dapr Pub/Sub publisher.

    Mirrors the gate inside :func:`_build_lifecycle_publisher` so
    :func:`load_run_components` can decide whether the shared
    :class:`httpx.AsyncClient` needs to exist *before* the
    publisher is built. Keeping a single source of truth avoids
    the lifespan accidentally constructing a socket pool that
    nothing ever uses (and would silently leak on shutdown).
    """
    return bool(env.get(ENV_PUBLISH_TOPIC) or env.get(ENV_PUBLISH_PUBSUB))


def _build_lifecycle_publisher(
    *,
    env: Mapping[str, str],
    http_client: httpx.AsyncClient | None,
) -> LifecycleEventPublisher:
    """Return the publisher chosen by the env flags.

    Production wiring activates when *either* ``WF_PUBLISH_TOPIC``
    *or* ``WF_PUBLISH_PUBSUB`` is set. Both default to their
    design.md values when only one side is supplied so operators
    have a single knob to flip. In the production path the caller
    is responsible for supplying a non-``None`` ``http_client`` (the
    lifespan-owned shared client constructed by
    :func:`load_run_components`); the publisher does **not** own
    it, mirroring the WF-IMPL-079 adapter contract.
    """
    if not _publisher_env_active(env):
        return InMemoryLifecycleEventPublisher()
    if http_client is None:  # pragma: no cover - defensive
        raise RuntimeError(
            "_build_lifecycle_publisher: production path requires a "
            "lifespan-owned httpx.AsyncClient; load_run_components "
            "must build the shared client before invoking this helper."
        )
    topic = env.get(ENV_PUBLISH_TOPIC)
    pubsub = env.get(ENV_PUBLISH_PUBSUB)
    effective_topic = topic or DEFAULT_DAPR_PUBLISH_TOPIC
    effective_pubsub = pubsub or "pubsub"
    dapr_endpoint = env.get(ENV_DAPR_ENDPOINT, "http://127.0.0.1:3500")
    return DaprPubSubLifecyclePublisher(
        http_client=http_client,
        dapr_endpoint=dapr_endpoint,
        pubsub_name=effective_pubsub,
        topic=effective_topic,
    )


def _resolve_outbound_rpc_timeout_seconds(env: Mapping[str, str]) -> float:
    """Parse :data:`ENV_OUTBOUND_RPC_TIMEOUT_MS` once at startup.

    The env var is documented in milliseconds (the operator-facing
    unit used across the rest of the workflow-service config
    surface, e.g. ``WF_EXPR_TIMEOUT_MS``); both Dapr adapters take
    a float in *seconds*, so this helper does the conversion in a
    single place.

    :raises ValueError: When the value is set but is not a
        positive integer. The message names the env var so the
        operator can find it without grepping.
    """
    raw = env.get(ENV_OUTBOUND_RPC_TIMEOUT_MS, "").strip()
    if not raw:
        return DEFAULT_OUTBOUND_RPC_TIMEOUT_MS / 1000.0
    try:
        ms = int(raw)
    except ValueError as exc:
        raise ValueError(
            f"{ENV_OUTBOUND_RPC_TIMEOUT_MS} must be a positive integer (milliseconds), got {raw!r}"
        ) from exc
    if ms <= 0:
        raise ValueError(
            f"{ENV_OUTBOUND_RPC_TIMEOUT_MS} must be a positive integer (milliseconds), got {raw!r}"
        )
    return ms / 1000.0


def _resolve_max_fanout_width(env: Mapping[str, str]) -> int:
    """Parse :data:`ENV_MAX_FANOUT_WIDTH` once at startup.

    An unset or blank value falls back to
    :data:`~custos_workflow.steps.sub_orchestration.DEFAULT_MAX_FANOUT_WIDTH`.

    :raises ValueError: When the value is set but is not a positive
        integer. The message names the env var so the operator can
        find it without grepping.
    """
    raw = env.get(ENV_MAX_FANOUT_WIDTH, "").strip()
    if not raw:
        return DEFAULT_MAX_FANOUT_WIDTH
    try:
        width = int(raw)
    except ValueError as exc:
        raise ValueError(f"{ENV_MAX_FANOUT_WIDTH} must be a positive integer, got {raw!r}") from exc
    if width <= 0:
        raise ValueError(f"{ENV_MAX_FANOUT_WIDTH} must be a positive integer, got {raw!r}")
    return width


def _resolve_approval_default_timeout(env: Mapping[str, str]) -> timedelta:
    """Parse :data:`ENV_APPROVAL_DEFAULT_TIMEOUT` once at startup.

    Accepts the same ISO-8601 duration grammar
    :func:`custos_workflow.runs.wait.parse_wait_duration` accepts
    (``PnW`` weeks form OR ``P[nD][T[nH][nM][nS]]`` with at least one
    positive component; months / years are rejected). An unset or blank
    value falls back to
    :data:`~custos_workflow.steps.sub_orchestration.DEFAULT_APPROVAL_TIMEOUT`.

    :raises ValueError: When the value is set but does not parse as a
        positive ISO-8601 duration. The message names the env var.
    """
    raw = env.get(ENV_APPROVAL_DEFAULT_TIMEOUT, "").strip()
    if not raw:
        return DEFAULT_APPROVAL_TIMEOUT
    match = _ISO8601_DURATION_PATTERN.match(raw)
    if match is None:
        raise ValueError(
            f"{ENV_APPROVAL_DEFAULT_TIMEOUT}: {raw!r} is not a recognised ISO-8601 duration"
        )
    weeks = int(match.group("weeks") or 0)
    days = int(match.group("days") or 0)
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes") or 0)
    seconds = float(match.group("seconds") or 0.0)
    if weeks == 0 and days == 0 and hours == 0 and minutes == 0 and seconds == 0.0:
        raise ValueError(f"{ENV_APPROVAL_DEFAULT_TIMEOUT}: {raw!r} must be greater than zero")
    if seconds != int(seconds):
        # The configured default is rendered back to a whole-second
        # ISO-8601 label for the approval-timeout audit envelope
        # (``_format_iso8601_duration`` drops sub-second precision), so
        # accepting a fractional value here would silently mis-report
        # the effective timeout. Reject it at the boundary instead.
        raise ValueError(
            f"{ENV_APPROVAL_DEFAULT_TIMEOUT}: {raw!r} must be a whole number of seconds "
            "(sub-second precision is not supported)"
        )
    return timedelta(weeks=weeks, days=days, hours=hours, minutes=minutes, seconds=seconds)


def _resolve_resume_sub_default_ttl(env: Mapping[str, str]) -> str:
    """Parse :data:`ENV_RESUME_SUB_DEFAULT_TTL` once at startup.

    Accepts the same ISO-8601 duration grammar
    :func:`_resolve_approval_default_timeout` accepts — including its
    whole-seconds restriction (sub-second precision is rejected) — and
    returns the validated *string* (the
    :class:`~custos_workflow.steps.resume.WaitForStepHandler` takes a
    string ``default_ttl`` and re-validates it via
    :func:`~custos_workflow.runs.wait.parse_wait_duration`). An unset or
    blank value falls back to
    :data:`~custos_workflow.steps.resume.DEFAULT_RESUME_SUB_TTL`
    (``PT24H``).

    :raises ValueError: When the value is set but does not parse as a
        positive ISO-8601 duration. The message names the env var.
    """
    raw = env.get(ENV_RESUME_SUB_DEFAULT_TTL, "").strip()
    if not raw:
        return DEFAULT_RESUME_SUB_TTL
    match = _ISO8601_DURATION_PATTERN.match(raw)
    if match is None:
        raise ValueError(
            f"{ENV_RESUME_SUB_DEFAULT_TTL}: {raw!r} is not a recognised ISO-8601 duration"
        )
    weeks = int(match.group("weeks") or 0)
    days = int(match.group("days") or 0)
    hours = int(match.group("hours") or 0)
    minutes = int(match.group("minutes") or 0)
    seconds = float(match.group("seconds") or 0.0)
    if weeks == 0 and days == 0 and hours == 0 and minutes == 0 and seconds == 0.0:
        raise ValueError(f"{ENV_RESUME_SUB_DEFAULT_TTL}: {raw!r} must be greater than zero")
    if seconds != int(seconds):
        # Match _resolve_approval_default_timeout: a subscription TTL is
        # coarse (hours / days), so reject sub-second precision at the
        # boundary rather than silently carrying a fractional value into
        # the handler / Trigger Service register call.
        raise ValueError(
            f"{ENV_RESUME_SUB_DEFAULT_TTL}: {raw!r} must be a whole number of seconds "
            "(sub-second precision is not supported)"
        )
    return raw


def _resolve_register_sub_max_retries(env: Mapping[str, str]) -> int:
    """Parse :data:`ENV_REGISTER_SUB_MAX_RETRIES` once at startup.

    An unset or blank value falls back to
    :data:`~custos_workflow.steps.resume.DEFAULT_REGISTER_SUB_MAX_RETRIES`
    (``5``). Mirrors :func:`_resolve_max_fanout_width`.

    :raises ValueError: When the value is set but is not a positive
        integer. The message names the env var.
    """
    raw = env.get(ENV_REGISTER_SUB_MAX_RETRIES, "").strip()
    if not raw:
        return DEFAULT_REGISTER_SUB_MAX_RETRIES
    try:
        retries = int(raw)
    except ValueError as exc:
        raise ValueError(
            f"{ENV_REGISTER_SUB_MAX_RETRIES} must be a positive integer, got {raw!r}"
        ) from exc
    if retries < 1:
        raise ValueError(f"{ENV_REGISTER_SUB_MAX_RETRIES} must be a positive integer, got {raw!r}")
    return retries


def _resolve_resume_sub_sweep_interval(env: Mapping[str, str]) -> float:
    """Parse :data:`ENV_RESUME_SUB_SWEEP_INTERVAL` once at startup.

    An unset or blank value falls back to
    :data:`~custos_workflow.steps.resume.DEFAULT_RESUME_SUB_SWEEP_INTERVAL_SECONDS`
    (``300`` seconds). The value is the wall-clock interval the
    background
    :class:`~custos_workflow.steps.resume.ResumeSubscriptionTtlSweeper`
    waits between TTL-expiry sweeps.

    :raises ValueError: When the value is set but is not a positive,
        finite number. The message names the env var so the operator can
        find it without grepping.
    """
    raw = env.get(ENV_RESUME_SUB_SWEEP_INTERVAL, "").strip()
    if not raw:
        return DEFAULT_RESUME_SUB_SWEEP_INTERVAL_SECONDS
    try:
        interval = float(raw)
    except ValueError as exc:
        raise ValueError(
            f"{ENV_RESUME_SUB_SWEEP_INTERVAL} must be a positive number of seconds, got {raw!r}"
        ) from exc
    if not math.isfinite(interval) or interval <= 0:
        raise ValueError(
            f"{ENV_RESUME_SUB_SWEEP_INTERVAL} must be a positive number of seconds, got {raw!r}"
        )
    return interval


def _build_activity_client(
    *,
    env: Mapping[str, str],
    http_client: httpx.AsyncClient | None,
    timeout_seconds: float,
) -> ActivityRuntimeClient:
    """Return the production or Noop ARM client per the env flags.

    The production :class:`DaprActivityRuntimeClient` activates
    only when :data:`ENV_ARM_APP_ID` is set; otherwise the
    in-process :class:`NoopActivityRuntimeClient` keeps the dev /
    test path sidecar-free. When the production path activates,
    the caller must supply the lifespan-owned shared
    :class:`httpx.AsyncClient` so the adapter never owns its own
    socket pool (mirroring the WF-IMPL-079 contract and the
    :class:`DaprPubSubLifecyclePublisher` precedent).

    The return type is the sync :class:`ActivityRuntimeClient`
    Protocol the Step Coordinator depends on; the production
    :class:`DaprActivityRuntimeClient` exposes ``async`` methods
    and is adapted to the sync surface by the WF-IMPL-079
    activity-bridge (registered by
    :meth:`~custos_workflow.runtime.WorkflowRuntime.start`),
    which is the layer that calls :func:`inspect.isawaitable` to
    drive either flavour. The :func:`cast` therefore reflects
    the runtime contract: the orchestrator-side bridge is the
    sole consumer of the production async client.
    """
    if not env.get(ENV_ARM_APP_ID, "").strip():
        return NoopActivityRuntimeClient()
    if http_client is None:  # pragma: no cover - defensive
        raise RuntimeError(
            "_build_activity_client: production path requires a "
            "lifespan-owned httpx.AsyncClient; load_run_components "
            "must build the shared client before invoking this helper."
        )
    endpoint = read_dapr_env(env, ENV_ARM_APP_ID)
    return cast(
        ActivityRuntimeClient,
        DaprActivityRuntimeClient(
            http_client=http_client,
            endpoint=endpoint,
            timeout=timeout_seconds,
        ),
    )


def _build_connector_client(
    *,
    env: Mapping[str, str],
    http_client: httpx.AsyncClient | None,
    timeout_seconds: float,
) -> ConnectorClient:
    """Return the production or Noop Connector client per the env flags.

    Mirrors :func:`_build_activity_client` for the Connector
    Service path: production activates only when
    :data:`ENV_CONNECTOR_APP_ID` is set, and the lifespan-owned
    shared :class:`httpx.AsyncClient` is reused so a worker
    configured to talk to both upstreams keeps a single socket
    pool. See :func:`_build_activity_client` for the rationale
    behind the :func:`cast` to the sync Protocol.
    """
    if not env.get(ENV_CONNECTOR_APP_ID, "").strip():
        return NoopConnectorClient()
    if http_client is None:  # pragma: no cover - defensive
        raise RuntimeError(
            "_build_connector_client: production path requires a "
            "lifespan-owned httpx.AsyncClient; load_run_components "
            "must build the shared client before invoking this helper."
        )
    endpoint = read_dapr_env(env, ENV_CONNECTOR_APP_ID)
    return cast(
        ConnectorClient,
        DaprConnectorClient(
            http_client=http_client,
            endpoint=endpoint,
            timeout=timeout_seconds,
        ),
    )


def _build_trigger_client(
    *,
    env: Mapping[str, str],
    http_client: httpx.AsyncClient | None,
    timeout_seconds: float,
) -> TriggerServiceClient:
    """Return the production or Noop Trigger Service client per the env flags.

    Mirrors :func:`_build_activity_client` / :func:`_build_connector_client`:
    the production
    :class:`~custos_workflow.clients.trigger.DaprTriggerServiceClient`
    activates only when :data:`ENV_TS_ENDPOINT` is set, reusing the
    lifespan-owned shared :class:`httpx.AsyncClient` so a worker that
    also talks to the ARM / Connector upstreams keeps a single socket
    pool. When the endpoint is unset the in-process
    :class:`~custos_workflow.clients.trigger.NoopTriggerServiceClient`
    keeps the dev / test path sidecar-free (production callers never
    reach this branch — :func:`_resolve_run_components` fails fast on a
    missing :data:`ENV_TS_ENDPOINT` before :func:`load_run_components`
    is invoked).
    """
    if not env.get(ENV_TS_ENDPOINT, "").strip():
        return NoopTriggerServiceClient()
    if http_client is None:  # pragma: no cover - defensive
        raise RuntimeError(
            "_build_trigger_client: production path requires a "
            "lifespan-owned httpx.AsyncClient; load_run_components "
            "must build the shared client before invoking this helper."
        )
    endpoint = read_dapr_env(env, ENV_TS_ENDPOINT)
    return cast(
        TriggerServiceClient,
        DaprTriggerServiceClient(
            http_client=http_client,
            endpoint=endpoint,
            timeout=timeout_seconds,
        ),
    )


def _build_catalog_client(
    *,
    env: Mapping[str, str],
    http_client: httpx.AsyncClient | None,
    timeout_seconds: float,
) -> CatalogClient:
    """Return the production or not-configured Catalog client per the env flags.

    Production activates only when :data:`ENV_CATALOG_APP_ID` is set,
    reusing the lifespan-owned shared :class:`httpx.AsyncClient` so a
    worker that also talks to the ARM / Connector / Trigger upstreams
    keeps a single socket pool (the
    :class:`~custos_workflow.clients.DaprCatalogClient` satisfies the
    async :class:`CatalogClient` Protocol directly — unlike the ARM /
    Connector clients it needs no sync/async cast).

    When the endpoint is unset the behaviour depends on
    :data:`ENV_ENVIRONMENT`: a ``production`` deployment fails fast
    with a clear :class:`RuntimeError` so a worker never silently
    serves ``StartRun`` off the :class:`_NotConfiguredCatalogClient`
    stub; any other environment keeps that stub so the sidecar-free
    dev / test path stays composable.
    """
    if not env.get(ENV_CATALOG_APP_ID, "").strip():
        if env.get(ENV_ENVIRONMENT, "").strip().lower() == _PRODUCTION_ENVIRONMENT:
            raise RuntimeError(
                f"{ENV_CATALOG_APP_ID} environment variable is required to start "
                f"the workflow worker when {ENV_ENVIRONMENT}={_PRODUCTION_ENVIRONMENT} "
                "(design.md § Configuration). Set it to the Catalog Service Dapr "
                "app-id so StartRun can fetch and compile workflow versions."
            )
        return _NotConfiguredCatalogClient()
    if http_client is None:  # pragma: no cover - defensive
        raise RuntimeError(
            "_build_catalog_client: production path requires a "
            "lifespan-owned httpx.AsyncClient; load_run_components "
            "must build the shared client before invoking this helper."
        )
    endpoint = read_dapr_env(env, ENV_CATALOG_APP_ID)
    return DaprCatalogClient(
        http_client=http_client,
        endpoint=endpoint,
        timeout=timeout_seconds,
    )


def load_run_components(
    *,
    env: Mapping[str, str] | None = None,
    workflow_runtime: WorkflowRuntimeProtocol | None = None,
    lifecycle_publisher: LifecycleEventPublisher | None = None,
    replay_reconciler: ReplayReconciler | None = None,
    catalog: CatalogClient | None = None,
    activity_registry: ActivityTypeRegistry | None = None,
    activity_client: ActivityRuntimeClient | None = None,
    connector_client: ConnectorClient | None = None,
    idempotency_ledger: IdempotencyLedger | None = None,
) -> RunComponents:
    """Build the default :class:`RunComponents` from ``env``.

    Args:
        env: Environment mapping to consult. Defaults to
            :data:`os.environ`. Tests pass a curated mapping to
            exercise the in-memory vs Dapr-Pub/Sub branches.
        workflow_runtime: Pre-built runtime override. When ``None``
            the default :class:`WorkflowRuntime` (real Dapr) is
            constructed; the env var ``WF_DAPR_WORKFLOW_COMPONENT``
            must be set or the lifespan raises before calling this.
        lifecycle_publisher: Pre-built publisher override. When
            ``None`` the env flags choose between in-memory and
            Dapr Pub/Sub.
        replay_reconciler: Pre-built reconciler override. Defaults
            to :class:`NoopReplayReconciler`.
        catalog: Pre-built Catalog client override. When unset, the
            default is selected per the env:
            :class:`~custos_workflow.clients.DaprCatalogClient` when
            :data:`ENV_CATALOG_APP_ID` is set, otherwise the
            in-process :class:`_NotConfiguredCatalogClient`
            (or a fail-fast :class:`RuntimeError` when
            :data:`ENV_ENVIRONMENT` is ``production``). Tests inject
            :class:`~custos_workflow.clients.FakeCatalogClient`.
        activity_registry: Pre-built registry override. Defaults to
            an empty :class:`InMemoryActivityTypeRegistry`.
        activity_client: Pre-built
            :class:`~custos_workflow.clients.ActivityRuntimeClient`
            override. When unset, the default is selected per the
            env: :class:`~custos_workflow.clients.DaprActivityRuntimeClient`
            when :data:`ENV_ARM_APP_ID` is set, otherwise
            :class:`~custos_workflow.clients.NoopActivityRuntimeClient`.
            Tests inject
            :class:`~custos_workflow.clients.FakeActivityRuntimeClient`.
        connector_client: Pre-built
            :class:`~custos_workflow.clients.ConnectorClient`
            override. When unset, the default is selected per the
            env: :class:`~custos_workflow.clients.DaprConnectorClient`
            when :data:`ENV_CONNECTOR_APP_ID` is set, otherwise
            :class:`~custos_workflow.clients.NoopConnectorClient`.
            Tests inject
            :class:`~custos_workflow.clients.FakeConnectorClient`.
        idempotency_ledger: Pre-built
            :class:`~custos_workflow.validator.IdempotencyLedger`
            override that backs the WF-IMPL-063
            :class:`~custos_workflow.validator.StartRunValidator`.
            Defaults to a process-local
            :class:`~custos_workflow.validator.InMemoryIdempotencyLedger`;
            the production Postgres-backed adapter is owned by
            the deferred *Durable Idempotency Ledger* sub-module
            and lands under WF-IMPL-070+.

    Returns:
        A fully-wired :class:`RunComponents` bundle. The caller is
        responsible for starting / stopping the runtime and
        ``aclose()``-ing :attr:`RunComponents.dapr_http_client` on
        shutdown.
    """
    resolved_env: Mapping[str, str] = env if env is not None else os.environ

    # Decide upfront whether *any* lifespan-owned Dapr HTTP client
    # consumer is active. Building a shared ``httpx.AsyncClient``
    # only when needed keeps the in-memory dev / test path
    # sidecar-free (the existing WF-IMPL-043 invariant) and
    # guarantees that *one* socket pool is reused across the
    # publisher + the WF-IMPL-080 ARM / Connector adapters when
    # the production paths activate — no second client, no second
    # pool. Caller-supplied overrides (``lifecycle_publisher`` /
    # ``activity_client`` / ``connector_client``) are honoured even
    # when the corresponding env vars are set so tests can swap in
    # Fakes without spinning up an unused HTTP client.
    timeout_seconds = _resolve_outbound_rpc_timeout_seconds(resolved_env)
    need_http_for_publisher = lifecycle_publisher is None and _publisher_env_active(resolved_env)
    need_http_for_arm = activity_client is None and bool(
        resolved_env.get(ENV_ARM_APP_ID, "").strip()
    )
    need_http_for_connector = connector_client is None and bool(
        resolved_env.get(ENV_CONNECTOR_APP_ID, "").strip()
    )
    # WF-IMPL-108: the production DaprTriggerServiceClient shares the
    # same lifespan-owned client; only spin one up when the Trigger
    # Service endpoint is set and the caller did not inject a
    # pre-built reconciler (the override path supplies its own
    # trigger client / fake, so no socket pool is needed).
    need_http_for_trigger = replay_reconciler is None and bool(
        resolved_env.get(ENV_TS_ENDPOINT, "").strip()
    )
    # WF-IMPL-114: the production DaprCatalogClient shares the same
    # lifespan-owned client; only spin one up when the Catalog
    # Service endpoint is set and the caller did not inject a
    # pre-built Catalog client (the override path supplies its own
    # client / fake, so no socket pool is needed).
    need_http_for_catalog = catalog is None and bool(
        resolved_env.get(ENV_CATALOG_APP_ID, "").strip()
    )
    dapr_http_client: httpx.AsyncClient | None
    if (
        need_http_for_publisher
        or need_http_for_arm
        or need_http_for_connector
        or need_http_for_trigger
        or need_http_for_catalog
    ):
        # The per-request ``timeout=`` arg every adapter passes
        # overrides the client-default; we still set a sane
        # default here so an adapter added later without a
        # per-request timeout does not fall back to httpx's
        # implicit 5 s default.
        dapr_http_client = httpx.AsyncClient(timeout=httpx.Timeout(timeout_seconds))
    else:
        dapr_http_client = None

    # WF-IMPL-080 reconciles two distinct slots for the ARM /
    # Connector clients:
    #
    # * ``outbound_*_client`` (this pair) holds the **outbound**
    #   client the WF-IMPL-079 bridge activities register
    #   against in :class:`WorkflowRuntime`. In production this
    #   is the async
    #   :class:`DaprActivityRuntimeClient` /
    #   :class:`DaprConnectorClient`; the bridge body adapts the
    #   sync/async surface via :func:`asyncio.run` (see
    #   ``_call_sync_or_async`` in
    #   :mod:`custos_workflow.runtime.dapr_activities`), so the
    #   sync :class:`ActivityRuntimeClient` /
    #   :class:`ConnectorClient` type annotations remain accurate
    #   from the bridge's perspective. The factories cast the
    #   Dapr clients at the boundary because the bridge is the
    #   *only* consumer that ever touches them.
    # * ``activity_client`` / ``connector_client`` (assigned
    #   below) feed the *legacy* synchronous
    #   :meth:`ActivityStepHandler.execute` Step Coordinator
    #   path. The production orchestrator never reaches that path
    #   (it uses the WF-IMPL-074 ``iter_calls`` yield protocol
    #   which routes through the bridges above), so this slot
    #   stays a sync :class:`NoopActivityRuntimeClient` /
    #   :class:`NoopConnectorClient` even when the production env
    #   vars are set — guaranteeing the sync Protocol contract is
    #   not violated and no async coroutine leaks into the legacy
    #   driver if a future caller invokes ``handler.execute()``
    #   directly with the production env in place.
    outbound_activity: ActivityRuntimeClient = (
        activity_client
        if activity_client is not None
        else _build_activity_client(
            env=resolved_env,
            http_client=dapr_http_client,
            timeout_seconds=timeout_seconds,
        )
    )
    outbound_connector: ConnectorClient = (
        connector_client
        if connector_client is not None
        else _build_connector_client(
            env=resolved_env,
            http_client=dapr_http_client,
            timeout_seconds=timeout_seconds,
        )
    )
    # The legacy sync slots are *always* the in-process Noop
    # adapters in this default builder. Tests that need a Fake
    # in the sync slot pass it via the ``activity_client`` /
    # ``connector_client`` kwarg, which also feeds the outbound
    # slot above so the override semantics stay symmetric (the
    # sync Fake satisfies both surfaces).
    legacy_activity: ActivityRuntimeClient = (
        activity_client if activity_client is not None else NoopActivityRuntimeClient()
    )
    legacy_connector: ConnectorClient = (
        connector_client if connector_client is not None else NoopConnectorClient()
    )

    # WorkflowRuntime is constructed *after* the outbound clients
    # so the WF-IMPL-079 bridge activities (``schedule_activity``
    # and ``bind_for_step``) can be registered against them on
    # ``runtime.start()``. The bridges accept the Noop fallback;
    # when invoked under Noop they raise ``NotImplementedError``,
    # which the bridge body lets bubble (only ``OutboundRpcError``
    # subclasses are enveloped) — the failure is intentionally
    # loud so a misconfigured worker (e.g. ARM env unset but the
    # orchestrator still calling ``schedule_activity``) fails
    # cleanly rather than silently returning a malformed envelope.
    runtime: WorkflowRuntimeProtocol = (
        workflow_runtime
        if workflow_runtime is not None
        else WorkflowRuntime(
            activity_runtime_client=outbound_activity,
            connector_client=outbound_connector,
        )
    )
    workflow_client = _build_workflow_client(runtime)

    if lifecycle_publisher is None:
        publisher = _build_lifecycle_publisher(env=resolved_env, http_client=dapr_http_client)
    else:
        publisher = lifecycle_publisher

    # WF-IMPL-108: wire the Resume Subscription Manager into the
    # running app. A *single* in-process mirror repository backs both
    # the WaitForStepHandler (which the lifespan threads into the
    # top-level orchestrator so ``waitFor:`` nodes park the run as
    # ``status="waiting"`` per WF-IMPL-107) and — when the production
    # Trigger Service endpoint is configured — the
    # ResumeSubscriptionReplayReconciler (which re-registers
    # surviving subscriptions on replay). Sharing the repository is
    # essential: a mirror persisted by the handler must be visible to
    # the reconciler. The Postgres-backed adapter is a follow-up
    # infra task, same staging as the idempotency ledger.
    resume_mirror_repo: ResumeSubscriptionMirrorRepository = (
        InMemoryResumeSubscriptionMirrorRepository()
    )
    resume_default_ttl = _resolve_resume_sub_default_ttl(resolved_env)
    resume_handler = WaitForStepHandler(
        resume_mirror_repo,
        default_ttl=resume_default_ttl,
        max_register_retries=_resolve_register_sub_max_retries(resolved_env),
    )

    # Reconciler precedence: an explicit ``replay_reconciler`` override
    # always wins (tests inject Fakes / spies). Otherwise the
    # production ResumeSubscriptionReplayReconciler activates only when
    # the Trigger Service endpoint is set (``need_http_for_trigger``
    # implies the shared client exists); the sidecar-free dev / test
    # path falls back to the inert NoopReplayReconciler so nothing
    # reaches a Noop trigger client (whose methods raise
    # NotImplementedError). The Dapr trigger client is built only on
    # the production branch so the override path never trips the
    # defensive "endpoint set but no shared client" guard.
    reconciler: ReplayReconciler
    if replay_reconciler is not None:
        reconciler = replay_reconciler
    elif need_http_for_trigger:
        reconciler = ResumeSubscriptionReplayReconciler(
            resume_mirror_repo,
            _build_trigger_client(
                env=resolved_env,
                http_client=dapr_http_client,
                timeout_seconds=timeout_seconds,
            ),
            default_ttl=resume_default_ttl,
        )
    else:
        reconciler = NoopReplayReconciler()
    catalog_client: CatalogClient = (
        catalog
        if catalog is not None
        else _build_catalog_client(
            env=resolved_env,
            http_client=dapr_http_client,
            timeout_seconds=timeout_seconds,
        )
    )
    registry: ActivityTypeRegistry = (
        activity_registry if activity_registry is not None else InMemoryActivityTypeRegistry({})
    )
    store: RunStore = InProcessRunStore(
        cast(MetadataStoreProvider, _InProcessMetadataStoreProvider())
    )
    controller = RunController(
        catalog=catalog_client,
        store=store,
        workflow_client=workflow_client,
        activity_registry=registry,
        lifecycle_publisher=publisher,
        clock=_SystemClock(),
        replay_reconciler=reconciler,
    )
    # WF-IMPL-069: bind a :class:`StartRunValidator` so the
    # ``api.dependencies.get_validator`` Depends can resolve it
    # off ``app.state.start_run_validator``. The validator is
    # constructed last so it shares the same Catalog client the
    # Run Controller drives (no second Catalog connection per
    # request) and falls back to a process-local in-memory ledger
    # until the durable-ledger sub-module lands.
    ledger: IdempotencyLedger = (
        idempotency_ledger if idempotency_ledger is not None else InMemoryIdempotencyLedger()
    )
    validator = StartRunValidator(catalog=catalog_client, ledger=ledger)
    return RunComponents(
        workflow_runtime=runtime,
        workflow_client=workflow_client,
        run_store=store,
        lifecycle_publisher=publisher,
        replay_reconciler=reconciler,
        run_controller=controller,
        activity_client=legacy_activity,
        connector_client=legacy_connector,
        start_run_validator=validator,
        outbound_activity_client=outbound_activity,
        outbound_connector_client=outbound_connector,
        dapr_http_client=dapr_http_client,
        max_fanout_width=_resolve_max_fanout_width(resolved_env),
        approval_default_timeout=_resolve_approval_default_timeout(resolved_env),
        resume_handler=resume_handler,
        resume_sweep_interval_seconds=_resolve_resume_sub_sweep_interval(resolved_env),
    )
