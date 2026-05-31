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

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, Protocol, cast, runtime_checkable

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
    NoopActivityRuntimeClient,
    NoopConnectorClient,
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
    DEFAULT_DAPR_PUBLISH_TIMEOUT_SECONDS,
    DaprPubSubLifecyclePublisher,
)
from custos_workflow.runs.replay import NoopReplayReconciler, ReplayReconciler
from custos_workflow.runs.store import InProcessRunStore, RunStore
from custos_workflow.runtime import FakeWorkflowRuntime, WorkflowClient, WorkflowRuntime

__all__ = [
    "DEFAULT_DAPR_PUBLISH_TOPIC",
    "ENV_DAPR_ENDPOINT",
    "ENV_DAPR_WORKFLOW_COMPONENT",
    "ENV_PUBLISH_PUBSUB",
    "ENV_PUBLISH_TOPIC",
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
            schedules attempts through. WF-IMPL-057 defaults this to
            :class:`~custos_workflow.clients.NoopActivityRuntimeClient`
            — the production Dapr-backed adapter is owned by the
            deferred *Real ARM Client* sub-module.
        connector_client: WF-IMPL-050's
            :class:`~custos_workflow.clients.ConnectorClient` stub
            that the
            :class:`~custos_workflow.steps.activity_step.ActivityStepHandler`
            leases connector contexts through. WF-IMPL-057 defaults
            this to
            :class:`~custos_workflow.clients.NoopConnectorClient`
            — the production Dapr-backed adapter is owned by the
            deferred *Real Connector Client* sub-module.
        dapr_http_client: HTTP client owned by the Dapr Pub/Sub
            publisher when in production mode. ``None`` for the
            in-memory publisher path. The lifespan ``aclose()`` it
            on shutdown.
    """

    workflow_runtime: WorkflowRuntimeProtocol
    workflow_client: _WorkflowClient
    run_store: RunStore
    lifecycle_publisher: LifecycleEventPublisher
    replay_reconciler: ReplayReconciler
    run_controller: RunController
    activity_client: ActivityRuntimeClient
    connector_client: ConnectorClient
    dapr_http_client: httpx.AsyncClient | None = field(default=None)


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


def _build_lifecycle_publisher(
    *,
    env: Mapping[str, str],
) -> tuple[LifecycleEventPublisher, httpx.AsyncClient | None]:
    """Return ``(publisher, owned_http_client)`` per the env flags.

    Production wiring activates when *either* ``WF_PUBLISH_TOPIC``
    *or* ``WF_PUBLISH_PUBSUB`` is set. Both default to their
    design.md values when only one side is supplied so operators
    have a single knob to flip. The returned ``owned_http_client``
    is the lifespan's responsibility to ``aclose()`` on shutdown;
    ``None`` in the in-memory path.
    """
    topic = env.get(ENV_PUBLISH_TOPIC)
    pubsub = env.get(ENV_PUBLISH_PUBSUB)
    if not topic and not pubsub:
        return InMemoryLifecycleEventPublisher(), None
    effective_topic = topic or DEFAULT_DAPR_PUBLISH_TOPIC
    effective_pubsub = pubsub or "pubsub"
    dapr_endpoint = env.get(ENV_DAPR_ENDPOINT, "http://127.0.0.1:3500")
    http_client = httpx.AsyncClient(
        timeout=httpx.Timeout(DEFAULT_DAPR_PUBLISH_TIMEOUT_SECONDS),
    )
    publisher = DaprPubSubLifecyclePublisher(
        http_client=http_client,
        dapr_endpoint=dapr_endpoint,
        pubsub_name=effective_pubsub,
        topic=effective_topic,
    )
    return publisher, http_client


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
        catalog: Pre-built Catalog client override. Defaults to
            :class:`_NotConfiguredCatalogClient`.
        activity_registry: Pre-built registry override. Defaults to
            an empty :class:`InMemoryActivityTypeRegistry`.
        activity_client: Pre-built
            :class:`~custos_workflow.clients.ActivityRuntimeClient`
            override. Defaults to
            :class:`~custos_workflow.clients.NoopActivityRuntimeClient`
            — the production Dapr-backed adapter is owned by the
            deferred *Real ARM Client* sub-module. Tests inject
            :class:`~custos_workflow.clients.FakeActivityRuntimeClient`.
        connector_client: Pre-built
            :class:`~custos_workflow.clients.ConnectorClient`
            override. Defaults to
            :class:`~custos_workflow.clients.NoopConnectorClient`
            — the production Dapr-backed adapter is owned by the
            deferred *Real Connector Client* sub-module. Tests
            inject :class:`~custos_workflow.clients.FakeConnectorClient`.

    Returns:
        A fully-wired :class:`RunComponents` bundle. The caller is
        responsible for starting / stopping the runtime and
        ``aclose()``-ing :attr:`RunComponents.dapr_http_client` on
        shutdown.
    """
    resolved_env: Mapping[str, str] = env if env is not None else os.environ
    runtime: WorkflowRuntimeProtocol = (
        workflow_runtime if workflow_runtime is not None else WorkflowRuntime()
    )
    workflow_client = _build_workflow_client(runtime)
    if lifecycle_publisher is None:
        publisher, dapr_http_client = _build_lifecycle_publisher(env=resolved_env)
    else:
        publisher = lifecycle_publisher
        dapr_http_client = None
    reconciler: ReplayReconciler = (
        replay_reconciler if replay_reconciler is not None else NoopReplayReconciler()
    )
    catalog_client: CatalogClient = (
        catalog if catalog is not None else _NotConfiguredCatalogClient()
    )
    registry: ActivityTypeRegistry = (
        activity_registry if activity_registry is not None else InMemoryActivityTypeRegistry({})
    )
    activity: ActivityRuntimeClient = (
        activity_client if activity_client is not None else NoopActivityRuntimeClient()
    )
    connector: ConnectorClient = (
        connector_client if connector_client is not None else NoopConnectorClient()
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
    return RunComponents(
        workflow_runtime=runtime,
        workflow_client=workflow_client,
        run_store=store,
        lifecycle_publisher=publisher,
        replay_reconciler=reconciler,
        run_controller=controller,
        activity_client=activity,
        connector_client=connector,
        dapr_http_client=dapr_http_client,
    )
