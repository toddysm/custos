"""Shared end-to-end harness for the WF-IMPL-045 integration suite.

The harness wires the **real** :class:`RunController`, the
**real** :class:`make_run_orchestrator`, and the
:class:`FakeWorkflowRuntime` through a single in-process
:class:`FakeWorkflowClient` so each integration test exercises
the full lifecycle path the production wiring will:

* Controller persists the run row → calls
  :meth:`FakeWorkflowClient.schedule_new_workflow` → the fake
  drives the orchestrator generator inline (see
  :meth:`FakeWorkflowRuntime._schedule` / ``_drive``) → terminal
  ``RunOutput`` lands on ``state.output``.
* :meth:`controller.cancel_run` / :meth:`controller.pause_run` /
  :meth:`controller.resume_run` flow through the same Workflow
  Client surface and observe the fake's own state machine
  (``RUNNING -> TERMINATED`` for cancel, ``RUNNING -> SUSPENDED``
  for pause, ``SUSPENDED -> RUNNING`` for resume).

The harness deliberately keeps the catalog client **recording**
(so tests can assert ``calls`` on idempotent paths) and uses a
no-op :class:`_NoopSleeper` so cancel-poll attempts run
synchronously. The :class:`InMemoryLifecycleEventPublisher`
gives every test direct introspection over the published
lifecycle event order.

The :func:`make_harness` factory takes a :class:`StepHandler` so
each test injects the exact dispatch semantics it wants
(:class:`~custos_workflow.runs.NoopStepHandler` for the happy
path; a recording handler for the "orchestrator failure" path;
etc.).
"""

from __future__ import annotations

import asyncio
import textwrap
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, NamedTuple, cast

import yaml
from custos_cel import FixedClock
from custos_spl.interfaces.metadata_store import MetadataStoreProvider

from custos_workflow.bindings import InMemoryActivityTypeRegistry
from custos_workflow.compiler import RunMeta
from custos_workflow.compiler import compile as compile_workflow
from custos_workflow.document import WorkflowDocument
from custos_workflow.graph import to_json
from custos_workflow.graph.model import ExecutionGraph
from custos_workflow.runs import (
    WORKFLOW_NAME,
    InMemoryLifecycleEventPublisher,
    InProcessRunStore,
    NoopStepHandler,
    RunController,
    RunInput,
    StepHandler,
    WorkflowVersion,
    make_run_orchestrator,
)
from custos_workflow.runtime import FakeWorkflowClient, FakeWorkflowRuntime
from custos_workflow.runtime.fake import FakeWorkflowFn
from tests.runs._fakes import FakeMetadataStoreProvider

# ---------------------------------------------------------------------------
# Constants the integration suite shares
# ---------------------------------------------------------------------------

WORKSPACE = "ws-int"
WORKFLOW_VERSION_ID = "wfv-int"
WORKFLOW_ID = "wf-int"
IDEMPOTENCY_KEY = "client-int-1"
FIXED_NOW = datetime(2026, 6, 1, 12, 0, 0, tzinfo=UTC)


# ---------------------------------------------------------------------------
# Recording catalog client
# ---------------------------------------------------------------------------


class RecordingCatalogClient:
    """:class:`CatalogClient` that records every ``get_workflow_version``."""

    def __init__(self, version: WorkflowVersion) -> None:
        self._version = version
        self.calls: list[tuple[str, str]] = []

    async def get_workflow_version(
        self, workspace_id: str, workflow_version_id: str
    ) -> WorkflowVersion:
        self.calls.append((workspace_id, workflow_version_id))
        return self._version


# ---------------------------------------------------------------------------
# Workflow-client adapter that registers + drives the orchestrator
# ---------------------------------------------------------------------------


@dataclass
class _OrchestratorBridgeClient:
    """A workflow client that registers the orchestrator on first schedule.

    The :class:`FakeWorkflowClient` itself only forwards to the
    runtime; the orchestrator must be registered against the
    runtime under :data:`WORKFLOW_NAME` before the first
    ``schedule_new_workflow`` call. The Run Controller never
    touches the runtime directly (the production code uses a
    plain :class:`WorkflowClient`), so the integration harness
    plays the role the FastAPI lifespan plays in production:
    register the orchestrator on the runtime exactly once,
    then forward every method call to the underlying
    :class:`FakeWorkflowClient`.

    Routing every call through this bridge also gives the harness
    a natural place to capture the request log (handy for the
    replay-safety test).
    """

    runtime: FakeWorkflowRuntime
    handler: StepHandler
    inner: FakeWorkflowClient
    schedule_requests: list[Any] = field(default_factory=list)
    terminate_requests: list[Any] = field(default_factory=list)
    pause_requests: list[Any] = field(default_factory=list)
    resume_requests: list[Any] = field(default_factory=list)
    state_requests: list[Any] = field(default_factory=list)
    _registered: bool = False

    def _ensure_registered(self) -> None:
        if self._registered:
            return
        orchestrator = make_run_orchestrator(self.handler)
        self.runtime.register_workflow(cast(FakeWorkflowFn, orchestrator), name=WORKFLOW_NAME)
        self._registered = True

    async def schedule_new_workflow(self, request: Any) -> str:
        self._ensure_registered()
        self.schedule_requests.append(request)
        return await self.inner.schedule_new_workflow(request)

    async def terminate_workflow(self, request: Any) -> None:
        self.terminate_requests.append(request)
        await self.inner.terminate_workflow(request)

    async def pause_workflow(self, request: Any) -> None:
        self.pause_requests.append(request)
        await self.inner.pause_workflow(request)

    async def resume_workflow(self, request: Any) -> None:
        self.resume_requests.append(request)
        await self.inner.resume_workflow(request)

    async def get_workflow_state(self, request: Any) -> Any:
        self.state_requests.append(request)
        return await self.inner.get_workflow_state(request)


# ---------------------------------------------------------------------------
# Async no-op sleeper for cancel-poll loop
# ---------------------------------------------------------------------------


@dataclass
class _NoopSleeper:
    """Async sleeper that records durations without ever blocking."""

    durations: list[float] = field(default_factory=list)

    async def __call__(self, seconds: float) -> None:
        self.durations.append(seconds)


# ---------------------------------------------------------------------------
# Doc / graph / RunInput helpers
# ---------------------------------------------------------------------------


def _run_meta() -> RunMeta:
    return RunMeta(
        workspace_id=WORKSPACE,
        workflow_version_id=WORKFLOW_VERSION_ID,
        workflow_name="pipeline",
        workflow_version_label="v1",
        started_at_default=FIXED_NOW,
    )


def compile_doc(doc_yaml: str) -> ExecutionGraph:
    """Parse and compile ``doc_yaml`` into a deterministic graph."""

    payload = yaml.safe_load(textwrap.dedent(doc_yaml))
    doc = WorkflowDocument.model_validate(payload)
    return compile_workflow(doc, _run_meta(), InMemoryActivityTypeRegistry({}))


def make_workflow_version(doc_yaml: str) -> WorkflowVersion:
    """Wrap the parsed document in a :class:`WorkflowVersion`."""

    payload = yaml.safe_load(textwrap.dedent(doc_yaml))
    doc = WorkflowDocument.model_validate(payload)
    return WorkflowVersion(
        id=WORKFLOW_VERSION_ID,
        workflow_id=WORKFLOW_ID,
        name="pipeline",
        version_label="v1",
        document=doc,
    )


def make_run_input(
    graph: ExecutionGraph,
    *,
    inputs: Mapping[str, Any] | None = None,
    idempotency_key: str = IDEMPOTENCY_KEY,
) -> RunInput:
    """Build a :class:`RunInput` from a compiled graph."""

    return RunInput(
        workspace_id=WORKSPACE,
        workflow_version_id=WORKFLOW_VERSION_ID,
        compiled_graph_json=to_json(graph),
        inputs=dict(inputs) if inputs is not None else {},
        idempotency_key=idempotency_key,
    )


# ---------------------------------------------------------------------------
# Wired harness
# ---------------------------------------------------------------------------


class Harness(NamedTuple):
    """Bundle of every collaborator a lifecycle test typically asserts on."""

    controller: RunController
    catalog: RecordingCatalogClient
    store: InProcessRunStore
    runtime: FakeWorkflowRuntime
    workflow_client: _OrchestratorBridgeClient
    publisher: InMemoryLifecycleEventPublisher
    sleeper: _NoopSleeper


def _store() -> InProcessRunStore:
    provider = FakeMetadataStoreProvider()
    return InProcessRunStore(cast(MetadataStoreProvider, provider))


def make_harness(
    *,
    doc_yaml: str,
    handler: StepHandler | None = None,
    clock: FixedClock | None = None,
    terminate_poll_attempts: int = 5,
    terminate_poll_interval_seconds: float = 0.01,
) -> Harness:
    """Wire a complete end-to-end harness for one integration test."""

    runtime = FakeWorkflowRuntime()
    fake_client = FakeWorkflowClient(runtime=runtime)
    bridge = _OrchestratorBridgeClient(
        runtime=runtime,
        handler=handler or NoopStepHandler(),
        inner=fake_client,
    )
    catalog = RecordingCatalogClient(make_workflow_version(doc_yaml))
    store = _store()
    publisher = InMemoryLifecycleEventPublisher()
    sleeper = _NoopSleeper()
    controller = RunController(
        catalog=catalog,
        store=store,
        workflow_client=cast(Any, bridge),
        activity_registry=InMemoryActivityTypeRegistry({}),
        lifecycle_publisher=publisher,
        clock=clock or FixedClock(FIXED_NOW),
        terminate_poll_attempts=terminate_poll_attempts,
        terminate_poll_interval_seconds=terminate_poll_interval_seconds,
        sleep=cast(Any, sleeper),
    )
    return Harness(
        controller=controller,
        catalog=catalog,
        store=store,
        runtime=runtime,
        workflow_client=bridge,
        publisher=publisher,
        sleeper=sleeper,
    )


# ---------------------------------------------------------------------------
# Run-and-await helpers
# ---------------------------------------------------------------------------


def run(coro: Any) -> Any:
    """``asyncio.run`` wrapper for synchronous test bodies."""

    return asyncio.run(coro)
