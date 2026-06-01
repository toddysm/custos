"""WF-IMPL-082 — Real ARM + Connector adapter end-to-end integration suite.

Drives the **fully wired** :func:`custos_workflow.create_app` factory
over :class:`httpx.ASGITransport` through the full FastAPI lifespan, so a
``POST …/runs`` walks every production layer down to the **real**
:class:`~custos_workflow.clients.DaprActivityRuntimeClient` /
:class:`~custos_workflow.clients.DaprConnectorClient` adapters. Those
adapters post to a **single shared** :class:`httpx.AsyncClient` whose
transport is an :class:`httpx.MockTransport` — so the full path

    ``POST …/runs`` → ``BindForStep`` → ``ScheduleActivity``
    → step result → run terminal state

is exercised against the canonical Dapr Service-Invocation URL shape
(``/v1.0/invoke/<app-id>/method/<method>``) **without** a Docker / Dapr
sidecar anywhere in the loop. Only Python + the existing dev deps are
required, so the suite runs identically on Linux CI and macOS local.

Bridging note
-------------
The production adapters expose ``async`` methods, but
:func:`custos_workflow.create_app` wires them behind the **synchronous**
:class:`~custos_workflow.steps.activity_step.ActivityStepHandler`
``execute`` path (via
:func:`~custos_workflow.runtime.dapr_activities.drive_activity_generator`),
and the :class:`~custos_workflow.runtime.FakeWorkflowRuntime` drives the
orchestrator inline **inside** the request's event loop. A naïve
``asyncio.run`` bridge would raise ``RuntimeError`` (loop already
running), so each adapter call is marshalled onto a dedicated
background event-loop thread (:class:`_BackgroundLoop`) — the same
"run the coroutine on a loop that is not the calling loop" discipline
the production Dapr worker thread gets for free off the event loop.

Three scenarios pinned by ``WF-IMPL-082``:

1. **Happy path** — ``BindForStep`` (200, single slot) →
   ``ScheduleActivity`` (200, ``success`` envelope) → run terminates
   ``succeeded``.
2. **Retryable scheduling failure** — ``BindForStep`` (200) →
   ``ScheduleActivity`` attempt 1 → HTTP 503 (mapped to a ``retryable``
   envelope) → the retry driver schedules attempt 2 → HTTP 200
   ``success`` → run terminates ``succeeded``.
3. **Connector bind permanent failure** — ``BindForStep`` (422) → the
   adapter raises
   :class:`~custos_workflow.clients._errors.OutboundRpcStatusError`,
   which the handler surfaces as a ``step.connector_bind_error``
   :class:`~custos_workflow.runs.orchestrator.RunOutput` (the response
   body's ``code`` travels on the envelope ``cause``) → run terminates
   ``failed``.
"""

from __future__ import annotations

import asyncio
import threading
from collections.abc import AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any, TypeVar

import httpx
import pytest
import yaml
from httpx import ASGITransport

from custos_workflow import create_app
from custos_workflow.bindings import InMemoryActivityTypeRegistry
from custos_workflow.clients._dapr_invoke import DaprInvokeEndpoint
from custos_workflow.clients.activity_runtime import (
    ActivityResultEnvelope,
    DaprActivityRuntimeClient,
    ScheduleActivityRequest,
)
from custos_workflow.clients.connector import (
    BindForStepRequest,
    BindForStepResponse,
    DaprConnectorClient,
)
from custos_workflow.document.models import WorkflowDocument
from custos_workflow.providers import RunComponents, load_run_components
from custos_workflow.runs.controller import WorkflowVersion
from custos_workflow.runs.model import RunStatus
from custos_workflow.runtime import FakeWorkflowRuntime

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------


WORKSPACE = "ws-rce2e"
WORKFLOW_ID = "wf-rce2e"
WORKFLOW_VERSION_ID = "wfv-rce2e"

#: Dapr sidecar host / port the adapters target. The MockTransport
#: never opens a socket, but the adapter still bakes these into the
#: canonical Service-Invocation URL the request inspector asserts on.
DAPR_HOST = "127.0.0.1"
DAPR_PORT = 3500

#: Dapr app-ids for the two downstream services. They appear verbatim
#: in the ``/v1.0/invoke/<app-id>/method/<method>`` path.
ARM_APP_ID = "activity-runtime-manager"
CONNECTOR_APP_ID = "connector-service"

SCHEDULE_PATH = f"/v1.0/invoke/{ARM_APP_ID}/method/ScheduleActivity"
BIND_PATH = f"/v1.0/invoke/{CONNECTOR_APP_ID}/method/BindForStep"

#: The activity ref + connector reference the workflow doc binds.
ACTIVITY_REF = "security/scan@1"
STEP_ID = "scan"


# ---------------------------------------------------------------------------
# Workflow documents
# ---------------------------------------------------------------------------


def _single_activity_doc(*, retry_max_attempts: int | None = None) -> str:
    """A one-step ``activity:`` workflow bound to a singular connector.

    The singular ``connector: primary`` shorthand collapses to a slot
    named ``default`` — the same slot key the bind-response fixtures
    return — so the schedule request observes a valid connector
    context.
    """
    retry_block = ""
    if retry_max_attempts is not None:
        retry_block = f"""\
      retry:
        maxAttempts: {retry_max_attempts}
        backoff:
          strategy: exponential
          initialDelay: PT1S
          maxDelay: PT30S
          multiplier: 2.0
"""
    return f"""\
apiVersion: custos.dev/v1
kind: Workflow
metadata:
  name: pipeline
  workspace: {WORKSPACE}
spec:
  inputs:
    image:
      type: string
      default: 'alpine:3.19'
  steps:
    - id: {STEP_ID}
      activity: {ACTIVITY_REF}
      connector: primary
{retry_block}      with:
        image: ${{{{ inputs.image }}}}
"""


def _registry() -> InMemoryActivityTypeRegistry:
    """Registry exposing the ``security/scan@1`` output schema.

    The activity step's compile-time type check resolves
    ``steps.scan.outputs.*`` against this schema; the runtime success
    envelope echoes the same shape.
    """
    return InMemoryActivityTypeRegistry(
        {
            ACTIVITY_REF: {
                "type": "object",
                "properties": {
                    "critical": {"type": "integer"},
                    "findings": {"type": "array", "items": {"type": "string"}},
                },
            },
        }
    )


def _make_version(doc_yaml: str) -> WorkflowVersion:
    """Render ``doc_yaml`` into a :class:`WorkflowVersion`."""
    doc = WorkflowDocument.model_validate(yaml.safe_load(doc_yaml))
    return WorkflowVersion(
        id=WORKFLOW_VERSION_ID,
        workflow_id=WORKFLOW_ID,
        name="pipeline",
        version_label="v1",
        document=doc,
    )


# ---------------------------------------------------------------------------
# Catalog stub
# ---------------------------------------------------------------------------


class _CatalogStub:
    """Catalog Protocol stub returning the same :class:`WorkflowVersion`."""

    def __init__(self, version: WorkflowVersion) -> None:
        self._version = version
        self.calls: list[tuple[str, str]] = []

    async def get_workflow_version(
        self, workspace_id: str, workflow_version_id: str
    ) -> WorkflowVersion:
        self.calls.append((workspace_id, workflow_version_id))
        return self._version


# ---------------------------------------------------------------------------
# Mock-transport response bodies
# ---------------------------------------------------------------------------


def _bind_ok_body(slot: str = "default") -> dict[str, Any]:
    """Canonical ``BindForStep`` 200 body for a single slot."""
    return {
        "contexts": {
            slot: {
                "slotName": slot,
                "handle": f"ctx-{slot}-handle",
                "expiresAt": "2030-01-02T03:04:05Z",
                "connectorKind": "oci-registry",
            }
        }
    }


def _schedule_success_body(*, attempt: int) -> dict[str, Any]:
    """Canonical ``ScheduleActivity`` 200 ``success`` envelope body."""
    return {
        "class": "success",
        "outputs": {"critical": 0, "findings": []},
        "error": None,
        "attempt": attempt,
    }


# ---------------------------------------------------------------------------
# Async-to-sync bridge over a background event loop
# ---------------------------------------------------------------------------


_T = TypeVar("_T")


class _BackgroundLoop:
    """A private event loop running on a daemon thread.

    The production adapters are ``async`` but the Step Coordinator's
    ``execute`` path resolves them synchronously, and the
    :class:`FakeWorkflowRuntime` drives that path inline inside the
    request's event loop. ``asyncio.run`` would therefore raise
    ("loop already running"), so coroutines are submitted here via
    :func:`asyncio.run_coroutine_threadsafe` and joined synchronously.
    """

    def __init__(self) -> None:
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(target=self._loop.run_forever, daemon=True)
        self._thread.start()

    def run(self, coro: Coroutine[Any, Any, _T]) -> _T:
        """Run ``coro`` to completion on the background loop and return its value."""
        return asyncio.run_coroutine_threadsafe(coro, self._loop).result()

    def close(self) -> None:
        """Stop the loop and join the thread."""
        self._loop.call_soon_threadsafe(self._loop.stop)
        self._thread.join()
        self._loop.close()


@dataclass(slots=True)
class _SyncActivityClient:
    """Synchronous :class:`ActivityRuntimeClient` over the async adapter."""

    inner: DaprActivityRuntimeClient
    loop: _BackgroundLoop

    def schedule_activity(self, request: ScheduleActivityRequest) -> ActivityResultEnvelope:
        return self.loop.run(self.inner.schedule_activity(request))

    def cancel_activity(self, run_id: str, step_id: str) -> None:
        self.loop.run(self.inner.cancel_activity(run_id, step_id))


@dataclass(slots=True)
class _SyncConnectorClient:
    """Synchronous :class:`ConnectorClient` over the async adapter."""

    inner: DaprConnectorClient
    loop: _BackgroundLoop

    def bind_for_step(self, request: BindForStepRequest) -> BindForStepResponse:
        return self.loop.run(self.inner.bind_for_step(request))


# ---------------------------------------------------------------------------
# App harness
# ---------------------------------------------------------------------------


@dataclass(slots=True)
class _Harness:
    """Collaborators the tests assert on after driving the app."""

    runtime: FakeWorkflowRuntime
    catalog: _CatalogStub


@asynccontextmanager
async def _driven_app(
    *,
    doc_yaml: str,
    handler: Callable[[httpx.Request], httpx.Response],
) -> AsyncIterator[tuple[httpx.AsyncClient, _Harness]]:
    """Yield an HTTP client wired through the full ``create_app`` lifespan.

    ``handler`` is mounted on the single shared
    :class:`httpx.AsyncClient` both real adapters post through, so it
    sees — and the test can assert on — every outbound
    ``BindForStep`` / ``ScheduleActivity`` request.
    """
    loop = _BackgroundLoop()
    outbound_transport = httpx.MockTransport(handler)
    # The single shared client both adapters target (WF-IMPL-080 wires
    # exactly one lifespan-owned pool in production).
    outbound_client = httpx.AsyncClient(transport=outbound_transport)

    activity_adapter = DaprActivityRuntimeClient(
        http_client=outbound_client,
        endpoint=DaprInvokeEndpoint(host=DAPR_HOST, http_port=DAPR_PORT, app_id=ARM_APP_ID),
    )
    connector_adapter = DaprConnectorClient(
        http_client=outbound_client,
        endpoint=DaprInvokeEndpoint(host=DAPR_HOST, http_port=DAPR_PORT, app_id=CONNECTOR_APP_ID),
    )

    runtime = FakeWorkflowRuntime()
    catalog = _CatalogStub(_make_version(doc_yaml))
    components: RunComponents = load_run_components(
        env={},
        workflow_runtime=runtime,
        catalog=catalog,
        activity_registry=_registry(),
        activity_client=_SyncActivityClient(activity_adapter, loop),
        connector_client=_SyncConnectorClient(connector_adapter, loop),
    )
    app = create_app(require_call_context=False, run_components=components)

    harness = _Harness(runtime=runtime, catalog=catalog)

    asgi = ASGITransport(app=app)
    try:
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(transport=asgi, base_url="http://test") as cli,
        ):
            yield cli, harness
    finally:
        loop.run(outbound_client.aclose())
        loop.close()


def _start_payload() -> dict[str, Any]:
    return {"workflowVersionId": WORKFLOW_VERSION_ID, "inputs": {"image": "alpine:3.19"}}


def _assert_canonical_urls(requests: list[httpx.Request]) -> None:
    """Every outbound request path must match the canonical Dapr shape."""
    assert requests, "expected at least one outbound Dapr request"
    for req in requests:
        path = req.url.path
        assert path in {BIND_PATH, SCHEDULE_PATH}, f"non-canonical Dapr URL: {path}"
        assert req.method == "POST"


# ---------------------------------------------------------------------------
# Scenario 1: happy path
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_happy_path_bind_then_schedule_succeeds() -> None:
    requests: list[httpx.Request] = []

    def handler(req: httpx.Request) -> httpx.Response:
        requests.append(req)
        path = req.url.path
        if path == BIND_PATH:
            return httpx.Response(200, json=_bind_ok_body())
        if path == SCHEDULE_PATH:
            return httpx.Response(200, json=_schedule_success_body(attempt=1))
        raise AssertionError(f"unexpected outbound path: {path}")

    async with _driven_app(doc_yaml=_single_activity_doc(), handler=handler) as (cli, harness):
        resp = await cli.post(f"/v1/workspaces/{WORKSPACE}/runs", json=_start_payload())
        assert resp.status_code == 202, resp.text
        run_id = resp.json()["runId"]

        state = harness.runtime.instance(run_id)
        assert state.output is not None
        assert state.output.status == RunStatus.SUCCEEDED.value
        assert state.output.failed_step is None
        assert dict(state.output.outputs[STEP_ID]) == {"critical": 0, "findings": []}

        # Exactly one bind + one schedule, in order, on canonical URLs.
        paths = [r.url.path for r in requests]
        assert paths == [BIND_PATH, SCHEDULE_PATH]
        _assert_canonical_urls(requests)

        # The Idempotency-Key on ScheduleActivity is the canonical
        # ``run|step|attempt`` triple.
        schedule_req = next(r for r in requests if r.url.path == SCHEDULE_PATH)
        assert schedule_req.headers["Idempotency-Key"] == f"{run_id}|{STEP_ID}|1"


# ---------------------------------------------------------------------------
# Scenario 2: retryable scheduling failure → retry → success
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_retryable_schedule_failure_retries_then_succeeds() -> None:
    requests: list[httpx.Request] = []
    schedule_calls = {"n": 0}

    def handler(req: httpx.Request) -> httpx.Response:
        requests.append(req)
        path = req.url.path
        if path == BIND_PATH:
            return httpx.Response(200, json=_bind_ok_body())
        if path == SCHEDULE_PATH:
            schedule_calls["n"] += 1
            if schedule_calls["n"] == 1:
                # 503 → adapter maps to a ``retryable`` envelope.
                return httpx.Response(503, text="service unavailable")
            return httpx.Response(200, json=_schedule_success_body(attempt=2))
        raise AssertionError(f"unexpected outbound path: {path}")

    doc = _single_activity_doc(retry_max_attempts=2)
    async with _driven_app(doc_yaml=doc, handler=handler) as (cli, harness):
        resp = await cli.post(f"/v1/workspaces/{WORKSPACE}/runs", json=_start_payload())
        assert resp.status_code == 202, resp.text
        run_id = resp.json()["runId"]

        state = harness.runtime.instance(run_id)
        assert state.output is not None
        assert state.output.status == RunStatus.SUCCEEDED.value
        assert dict(state.output.outputs[STEP_ID]) == {"critical": 0, "findings": []}

        # Two attempts: each opens a fresh bind, so two binds + two
        # schedules, all canonical.
        assert schedule_calls["n"] == 2
        assert [r.url.path for r in requests] == [
            BIND_PATH,
            SCHEDULE_PATH,
            BIND_PATH,
            SCHEDULE_PATH,
        ]
        _assert_canonical_urls(requests)

        # The attempt index advances on the Idempotency-Key across the
        # two schedule calls.
        schedule_keys = [
            r.headers["Idempotency-Key"] for r in requests if r.url.path == SCHEDULE_PATH
        ]
        assert schedule_keys == [f"{run_id}|{STEP_ID}|1", f"{run_id}|{STEP_ID}|2"]


# ---------------------------------------------------------------------------
# Scenario 3: connector bind permanent failure
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_connector_bind_permanent_failure_fails_run() -> None:
    requests: list[httpx.Request] = []
    bind_code = "connector.unsupported_capability"

    def handler(req: httpx.Request) -> httpx.Response:
        requests.append(req)
        path = req.url.path
        if path == BIND_PATH:
            return httpx.Response(422, json={"code": bind_code, "message": "no such capability"})
        raise AssertionError(f"schedule must not be reached on bind failure: {path}")

    async with _driven_app(doc_yaml=_single_activity_doc(), handler=handler) as (cli, harness):
        resp = await cli.post(f"/v1/workspaces/{WORKSPACE}/runs", json=_start_payload())
        assert resp.status_code == 202, resp.text
        run_id = resp.json()["runId"]

        state = harness.runtime.instance(run_id)
        assert state.output is not None
        assert state.output.status == RunStatus.FAILED.value
        assert state.output.failed_step == STEP_ID

        envelope = state.output.failure_envelope
        assert envelope is not None
        assert envelope["kind"] == "step.connector_bind_error"
        # The 422 response body's ``code`` travels on the bind error's
        # ``cause`` (the adapter previews the body into the
        # OutboundRpcStatusError detail).
        assert "422" in str(envelope["cause"])
        assert bind_code in str(envelope["cause"])

        # Only the bind was attempted; no ScheduleActivity call.
        assert [r.url.path for r in requests] == [BIND_PATH]
        _assert_canonical_urls(requests)
