"""End-to-end tests for the WF-IMPL-066 step REST routes.

The harness mirrors the WF-IMPL-065 pattern in ``test_runs.py``:
a minimal :class:`fastapi.FastAPI` app with only the steps router
mounted plus the WF-IMPL-061 exception handlers, an
:class:`AsyncMock` :class:`RunController` injected via
:meth:`FastAPI.dependency_overrides`, and a
:class:`httpx.AsyncClient` over :class:`httpx.ASGITransport` so
the wire shape is observed exactly as a real SDK client would.

Coverage targets pinned by the issue acceptance criteria:

* Step fetch round-trips state for a compiled-graph node into
  :class:`StepResponse` (kind echoes the source step kind;
  ``status`` defaults to ``pending`` and ``attempts`` is empty
  per the deferred step-state plumbing).
* Unknown ``runId`` surfaces the RFC 7807
  ``workflow.run_not_found`` (404) envelope.
* Unknown ``stepId`` (or a record with no compiled graph yet)
  surfaces the locked ``workflow.step_not_found`` (404) envelope
  with the ``stepId`` extension populated.
* The log-stream endpoint always returns 501 with the locked
  ``workflow.api.not_implemented`` body and the verbatim
  ``LOG_STREAM_NOT_IMPLEMENTED_DETAIL`` text.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest
import yaml
from fastapi import FastAPI
from httpx import ASGITransport

from custos_workflow.api.dependencies import (
    get_call_context,
    get_run_controller,
)
from custos_workflow.api.errors import (
    LOCKED_API_KIND_TO_STATUS,
    PROBLEM_TYPE_PREFIX,
    register_exception_handlers,
)
from custos_workflow.api.routes import all_routers, steps_router
from custos_workflow.api.routes.steps import LOG_STREAM_NOT_IMPLEMENTED_DETAIL
from custos_workflow.bindings.registry import InMemoryActivityTypeRegistry
from custos_workflow.call_context import CallContext
from custos_workflow.compiler import RunMeta
from custos_workflow.compiler import compile as compile_workflow
from custos_workflow.document.models import WorkflowDocument
from custos_workflow.graph.model import ExecutionGraph
from custos_workflow.runs.controller import RunController
from custos_workflow.runs.errors import RunNotFoundError
from custos_workflow.runs.ids import RunId
from custos_workflow.runs.model import RunRecord, RunStatus

# ---------------------------------------------------------------------------
# Constants + small builders
# ---------------------------------------------------------------------------


WORKSPACE = "ws-a"
WORKFLOW_VERSION_ID = "wfv-1"
WORKFLOW_ID = "wf-1"
RUN_ID = "run-1"
STEP_ID = "a"
FIXED_NOW = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)


def _workflow_doc_yaml() -> dict[str, Any]:
    """Render a minimal valid :class:`WorkflowDocument` YAML.

    A single ``let`` step keeps the compile path tight and
    activity-registry-free.
    """
    parsed: dict[str, Any] = yaml.safe_load(
        f"""
        apiVersion: custos.dev/v1
        kind: Workflow
        metadata:
          name: pipeline
          workspace: {WORKSPACE}
        spec:
          steps:
            - id: {STEP_ID}
              let: {{x: '${{{{ true }}}}'}}
        """
    )
    return parsed


def _compile_graph() -> ExecutionGraph:
    """Compile the test document into an :class:`ExecutionGraph`."""
    doc = WorkflowDocument.model_validate(_workflow_doc_yaml())
    run_meta = RunMeta(
        workspace_id=WORKSPACE,
        workflow_version_id=WORKFLOW_VERSION_ID,
        workflow_name="pipeline",
        workflow_version_label="v1",
        started_at_default=FIXED_NOW,
    )
    return compile_workflow(doc, run_meta, InMemoryActivityTypeRegistry({}))


def _make_record(
    *,
    run_id: str = RUN_ID,
    status: RunStatus = RunStatus.RUNNING,
    compiled_graph: ExecutionGraph | None = None,
) -> RunRecord:
    """Build a :class:`RunRecord` with an optional compiled graph."""
    return RunRecord(
        workspace_id=WORKSPACE,
        run_id=RunId(run_id),
        workflow_id=WORKFLOW_ID,
        workflow_version=WORKFLOW_VERSION_ID,
        status=status,
        reason=None,
        started_at=FIXED_NOW,
        updated_at=FIXED_NOW,
        compiled_graph=compiled_graph,
    )


# ---------------------------------------------------------------------------
# App harness
# ---------------------------------------------------------------------------


class _Harness:
    """Mutable container the tests + the dependency overrides share."""

    def __init__(self) -> None:
        self.controller: AsyncMock = AsyncMock(spec=RunController)
        self.call_context = CallContext(workspace=WORKSPACE, principal="user-1")


def _build_app(harness: _Harness) -> FastAPI:
    """Mount the steps router with the dependency overrides wired."""
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(steps_router)

    app.dependency_overrides[get_run_controller] = lambda: harness.controller
    app.dependency_overrides[get_call_context] = lambda: harness.call_context
    return app


@pytest.fixture
def harness() -> _Harness:
    """A fresh :class:`_Harness` per test."""
    return _Harness()


@pytest.fixture
async def client(harness: _Harness) -> AsyncIterator[httpx.AsyncClient]:
    """ASGI client wired to the steps router app."""
    app = _build_app(harness)
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as cli:
        yield cli


# ---------------------------------------------------------------------------
# GetStep
# ---------------------------------------------------------------------------


class TestGetStep:
    """``GET /v1/workspaces/{ws}/runs/{run_id}/steps/{step_id}``."""

    async def test_happy_path_projects_compiled_graph_node(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        """A compiled-graph node is projected onto the wire shape.

        ``kind`` echoes the source step kind (``let``);
        ``status`` defaults to ``pending`` and ``attempts`` is
        empty because per-step state persistence is deferred to
        the timeline-projection follow-up.
        """
        graph = _compile_graph()
        harness.controller.get_run.return_value = _make_record(
            status=RunStatus.RUNNING, compiled_graph=graph
        )

        response = await client.get(f"/v1/workspaces/{WORKSPACE}/runs/{RUN_ID}/steps/{STEP_ID}")

        assert response.status_code == 200
        body = response.json()
        assert body["stepId"] == STEP_ID
        assert body["kind"] == "let"
        assert body["status"] == "pending"
        assert body["attempts"] == []
        assert body["startedAt"] is None
        assert body["finishedAt"] is None
        assert body["outputs"] is None

        # The controller was called exactly once with the
        # workspace + run id pair from the URL.
        harness.controller.get_run.assert_awaited_once()
        kwargs = harness.controller.get_run.await_args.kwargs
        assert kwargs["workspace_id"] == WORKSPACE
        assert str(kwargs["run_id"]) == RUN_ID

    async def test_unknown_run_returns_404_envelope(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        """Unknown ``runId`` surfaces the ``workflow.run_not_found`` envelope."""
        harness.controller.get_run.side_effect = RunNotFoundError(
            f"run {RUN_ID!r} not found",
            run_id=RUN_ID,
        )

        response = await client.get(f"/v1/workspaces/{WORKSPACE}/runs/{RUN_ID}/steps/{STEP_ID}")

        assert response.status_code == 404
        body = response.json()
        assert body["code"] == "workflow.run_not_found"
        assert body["status"] == 404
        assert body["runId"] == RUN_ID

    async def test_unknown_step_returns_step_not_found_envelope(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        """An unknown ``stepId`` surfaces the locked envelope with extensions."""
        graph = _compile_graph()
        harness.controller.get_run.return_value = _make_record(
            status=RunStatus.RUNNING, compiled_graph=graph
        )

        response = await client.get(f"/v1/workspaces/{WORKSPACE}/runs/{RUN_ID}/steps/missing-step")

        assert response.status_code == 404
        assert response.headers["content-type"].split(";", 1)[0] == "application/problem+json"
        body = response.json()
        assert body["code"] == "workflow.step_not_found"
        assert body["status"] == 404
        assert body["type"].startswith(PROBLEM_TYPE_PREFIX)
        assert body["type"].endswith("workflow/step_not_found")
        assert body["workspaceId"] == WORKSPACE
        assert body["runId"] == RUN_ID
        assert body["stepId"] == "missing-step"

    async def test_record_without_compiled_graph_returns_step_not_found(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        """A persisted record with no compiled graph yet is a 404 too.

        The route SHOULD NOT silently emit an empty body or a 200
        with stale data: every uncompiled run looks the same from
        the wire and is indistinguishable from "step id you typed
        does not exist", so we collapse both into the same locked
        envelope.
        """
        harness.controller.get_run.return_value = _make_record(
            status=RunStatus.QUEUED, compiled_graph=None
        )

        response = await client.get(f"/v1/workspaces/{WORKSPACE}/runs/{RUN_ID}/steps/{STEP_ID}")

        assert response.status_code == 404
        body = response.json()
        assert body["code"] == "workflow.step_not_found"
        assert body["stepId"] == STEP_ID

    async def test_invalid_workspace_id_returns_400_bad_request(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        """Path-level workspace grammar rejection re-envelopes as 400."""
        response = await client.get(f"/v1/workspaces/Bad_WS/runs/{RUN_ID}/steps/{STEP_ID}")

        assert response.status_code == 400
        body = response.json()
        assert body["code"] == "workflow.api.bad_request"
        # The controller is never reached for path-grammar failures.
        harness.controller.get_run.assert_not_called()


# ---------------------------------------------------------------------------
# StreamStepLogs (stub)
# ---------------------------------------------------------------------------


class TestStreamStepLogs:
    """``GET .../steps/{step_id}/logs`` — locked 501 stub."""

    async def test_always_returns_501_with_locked_envelope(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        """No matter what the controller would do, the response is 501.

        The route never calls the controller \u2014 step log
        streaming is delegated to the Observability Service
        (COMP-009) and the controller has no method for it yet.
        """
        response = await client.get(
            f"/v1/workspaces/{WORKSPACE}/runs/{RUN_ID}/steps/{STEP_ID}/logs"
        )

        assert response.status_code == 501
        assert response.headers["content-type"].split(";", 1)[0] == "application/problem+json"
        body = response.json()
        assert body["code"] == "workflow.api.not_implemented"
        assert body["status"] == 501
        assert body["title"] == "Not implemented"
        assert body["type"].startswith(PROBLEM_TYPE_PREFIX)
        assert body["type"].endswith("workflow/api/not_implemented")
        assert body["detail"] == LOG_STREAM_NOT_IMPLEMENTED_DETAIL
        assert body["workspaceId"] == WORKSPACE
        assert body["runId"] == RUN_ID
        assert body["stepId"] == STEP_ID
        harness.controller.get_run.assert_not_called()

    async def test_locked_detail_string_matches_issue_acceptance_text(self) -> None:
        """Pin the verbatim detail text from the WF-IMPL-066 issue.

        Dev docs (WF-IMPL-072) reproduce this string; if a future
        edit changes the wording, this assertion fails loudly so
        the docs can be re-flowed alongside.
        """
        assert LOG_STREAM_NOT_IMPLEMENTED_DETAIL == (
            "Step log streaming is delegated to the Observability Service "
            "(COMP-009); deferred until the Full Observability Client "
            "integration sub-module lands."
        )


# ---------------------------------------------------------------------------
# Module-level router-registration smoke tests
# ---------------------------------------------------------------------------


def test_all_routers_includes_steps_router() -> None:
    """The package-level :data:`all_routers` exposes the steps router."""
    assert steps_router in all_routers


def test_steps_router_exposes_expected_paths() -> None:
    """The router carries both the fetch + log-stream paths.

    Asserting on the route path templates (not request behaviour)
    is a cheap regression net: a future refactor that renames the
    path placeholders will fail here before any integration test
    has a chance to run.
    """
    paths = {
        (route.path, frozenset(route.methods))  # type: ignore[attr-defined]
        for route in steps_router.routes
    }
    assert (
        "/v1/workspaces/{ws}/runs/{run_id}/steps/{step_id}",
        frozenset({"GET"}),
    ) in paths
    assert (
        "/v1/workspaces/{ws}/runs/{run_id}/steps/{step_id}/logs",
        frozenset({"GET"}),
    ) in paths


def test_locked_taxonomy_contains_step_and_not_implemented_kinds() -> None:
    """The error catalog was extended for WF-IMPL-066.

    Pinning the table here means a future change that removes
    either kind without a deliberate migration fails the build.
    """
    assert LOCKED_API_KIND_TO_STATUS["workflow.step_not_found"] == 404
    assert LOCKED_API_KIND_TO_STATUS["workflow.api.not_implemented"] == 501
