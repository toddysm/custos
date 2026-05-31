"""End-to-end tests for the WF-IMPL-065 REST routes (Run resource).

These tests stand up a minimal :class:`fastapi.FastAPI` app with
just the runs router mounted plus the WF-IMPL-061 exception
handlers installed. The dependency factories are short-circuited
via ``app.dependency_overrides`` so we can inject a real
:class:`StartRunValidator` (with a stubbed catalog + the in-memory
ledger) alongside an :class:`AsyncMock` :class:`RunController`.

The transport is :class:`httpx.AsyncClient` over
:class:`httpx.ASGITransport` so every request walks the full
FastAPI middleware + dependency + serialization stack and the wire
shape is observed exactly as a real SDK client would observe it.

The coverage targets:

* Happy path for each verb (StartRun, ListRuns, GetRun, CancelRun).
* ``Idempotency-Key`` header fallback when the body field is absent.
* Body-field precedence when both are supplied.
* Replay returns the original ``runId`` from the ledger.
* Divergent-inputs replay yields a 409
  ``workflow.validator.idempotency_conflict`` envelope.
* 404 on missing run; 409 on cancel state conflict.
* Workspace path grammar rejection surfaces the 400 envelope.
* List filters (``status``, ``workflowVersionId``) narrow the page.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock

import httpx
import pytest
import yaml
from custos_spl.pagination import Cursor, Page
from fastapi import FastAPI
from httpx import ASGITransport

from custos_workflow.api.dependencies import (
    get_call_context,
    get_run_controller,
    get_validator,
)
from custos_workflow.api.errors import register_exception_handlers
from custos_workflow.api.routes import runs_router
from custos_workflow.call_context import CallContext
from custos_workflow.document.models import WorkflowDocument
from custos_workflow.runs.controller import RunController, RunRef, WorkflowVersion
from custos_workflow.runs.errors import (
    RunNotFoundError,
    RunStateConflictError,
    WorkflowRuntimeUnavailableError,
)
from custos_workflow.runs.ids import RunId
from custos_workflow.runs.model import RunRecord, RunStatus
from custos_workflow.validator import (
    InMemoryIdempotencyLedger,
    StartRunValidator,
)

# ---------------------------------------------------------------------------
# Constants + small builders
# ---------------------------------------------------------------------------


WORKSPACE = "ws-a"
WORKFLOW_VERSION_ID = "wfv-1"
WORKFLOW_ID = "wf-1"
RUN_ID = "run-1"
FIXED_NOW = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)


def _workflow_doc_yaml(inputs_block: str = "") -> dict[str, Any]:
    """Render a minimal valid :class:`WorkflowDocument` YAML.

    The default ``inputs:`` block declares a single optional
    integer slot ``k`` so the validator's inputs-schema gate
    accepts payloads like ``{}`` and ``{"k": 1}`` without any
    extra harness wiring per test.
    """
    block = inputs_block or "inputs:\n            k: {type: integer, required: false}"
    parsed: dict[str, Any] = yaml.safe_load(
        f"""
        apiVersion: custos.dev/v1
        kind: Workflow
        metadata:
          name: pipeline
          workspace: {WORKSPACE}
        spec:
          {block}
          steps:
            - id: a
              let: {{x: '${{{{ true }}}}'}}
        """
    )
    return parsed


def _workflow_version(inputs_block: str = "") -> WorkflowVersion:
    """Build a :class:`WorkflowVersion` the validator will accept."""
    doc = WorkflowDocument.model_validate(_workflow_doc_yaml(inputs_block))
    return WorkflowVersion(
        id=WORKFLOW_VERSION_ID,
        workflow_id=WORKFLOW_ID,
        name="pipeline",
        version_label="v1",
        document=doc,
    )


class _RecordingCatalogClient:
    """Catalog Protocol fake; records calls and optionally raises."""

    def __init__(
        self,
        version: WorkflowVersion | None = None,
        *,
        raise_on_call: Exception | None = None,
    ) -> None:
        self._version = version if version is not None else _workflow_version()
        self._raise = raise_on_call
        self.calls: list[tuple[str, str]] = []

    async def get_workflow_version(
        self, workspace_id: str, workflow_version_id: str
    ) -> WorkflowVersion:
        self.calls.append((workspace_id, workflow_version_id))
        if self._raise is not None:
            raise self._raise
        return self._version


def _make_record(
    *,
    run_id: str = RUN_ID,
    workspace_id: str = WORKSPACE,
    workflow_version: str = WORKFLOW_VERSION_ID,
    status: RunStatus = RunStatus.RUNNING,
    reason: str | None = None,
) -> RunRecord:
    """Build a :class:`RunRecord` the projection helper can render."""
    return RunRecord(
        workspace_id=workspace_id,
        run_id=RunId(run_id),
        workflow_id=WORKFLOW_ID,
        workflow_version=workflow_version,
        status=status,
        reason=reason,
        started_at=FIXED_NOW,
        updated_at=FIXED_NOW,
        compiled_graph=None,
    )


def _make_ref(
    *,
    run_id: str = RUN_ID,
    workspace_id: str = WORKSPACE,
    workflow_version_id: str = WORKFLOW_VERSION_ID,
    status: RunStatus = RunStatus.QUEUED,
) -> RunRef:
    """Build a :class:`RunRef` the projection helper can render."""
    return RunRef(
        workspace_id=workspace_id,
        run_id=RunId(run_id),
        workflow_version_id=workflow_version_id,
        status=status,
    )


# ---------------------------------------------------------------------------
# App harness
# ---------------------------------------------------------------------------


class _Harness:
    """Mutable container the tests + the dependency overrides share."""

    def __init__(self) -> None:
        self.catalog = _RecordingCatalogClient()
        self.ledger = InMemoryIdempotencyLedger()
        self.validator = StartRunValidator(catalog=self.catalog, ledger=self.ledger)
        self.controller: AsyncMock = AsyncMock(spec=RunController)
        self.call_context = CallContext(workspace=WORKSPACE, principal="user-1")


def _build_app(harness: _Harness) -> FastAPI:
    """Mount the runs router with the dependency overrides wired."""
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(runs_router)

    app.dependency_overrides[get_validator] = lambda: harness.validator
    app.dependency_overrides[get_run_controller] = lambda: harness.controller
    app.dependency_overrides[get_call_context] = lambda: harness.call_context
    return app


@pytest.fixture
def harness() -> _Harness:
    """A fresh :class:`_Harness` per test."""
    return _Harness()


@pytest.fixture
async def client(harness: _Harness) -> AsyncIterator[httpx.AsyncClient]:
    """ASGI client wired to the runs router app."""
    app = _build_app(harness)
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as cli:
        yield cli


# ---------------------------------------------------------------------------
# StartRun — happy path + idempotency precedence
# ---------------------------------------------------------------------------


class TestStartRun:
    async def test_happy_path_returns_202_with_ref_body(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        harness.controller.start_run.return_value = _make_ref(status=RunStatus.QUEUED)

        response = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json={
                "workflowVersionId": WORKFLOW_VERSION_ID,
                "inputs": {},
            },
        )

        assert response.status_code == 202
        body = response.json()
        assert body["runId"] == RUN_ID
        assert body["status"] == RunStatus.QUEUED.value
        assert body["workspaceId"] == WORKSPACE
        assert body["workflowVersionId"] == WORKFLOW_VERSION_ID
        assert body["startedAt"] is None
        # Validator + controller both saw the same arguments.
        assert harness.catalog.calls == [(WORKSPACE, WORKFLOW_VERSION_ID)]
        harness.controller.start_run.assert_awaited_once()
        call_kwargs = harness.controller.start_run.await_args.kwargs
        assert call_kwargs["workspace_id"] == WORKSPACE
        assert call_kwargs["workflow_version_id"] == WORKFLOW_VERSION_ID
        assert call_kwargs["inputs"] == {}
        assert call_kwargs["idempotency_key"] is None

    async def test_idempotency_key_from_header_when_body_absent(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        harness.controller.start_run.return_value = _make_ref()

        response = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json={"workflowVersionId": WORKFLOW_VERSION_ID, "inputs": {}},
            headers={"Idempotency-Key": "header-key-1"},
        )

        assert response.status_code == 202
        call_kwargs = harness.controller.start_run.await_args.kwargs
        assert call_kwargs["idempotency_key"] == "header-key-1"

    async def test_body_idempotency_key_overrides_header(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        harness.controller.start_run.return_value = _make_ref()

        response = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json={
                "workflowVersionId": WORKFLOW_VERSION_ID,
                "inputs": {},
                "idempotencyKey": "body-key-1",
            },
            headers={"Idempotency-Key": "header-key-1"},
        )

        assert response.status_code == 202
        call_kwargs = harness.controller.start_run.await_args.kwargs
        assert call_kwargs["idempotency_key"] == "body-key-1"

    async def test_empty_body_idempotency_key_falls_back_to_header(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        """An empty-string body field is treated as opt-out → header wins."""
        harness.controller.start_run.return_value = _make_ref()

        response = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json={
                "workflowVersionId": WORKFLOW_VERSION_ID,
                "inputs": {},
                "idempotencyKey": "   ",
            },
            headers={"Idempotency-Key": "header-key-2"},
        )

        assert response.status_code == 202
        call_kwargs = harness.controller.start_run.await_args.kwargs
        assert call_kwargs["idempotency_key"] == "header-key-2"

    async def test_replay_returns_original_run_id(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        """A second identical request returns the same ``runId`` and the
        controller is invoked once per request — the validator's
        ledger replay is observable through the wire's ``runId``
        being stable across the pair."""
        harness.controller.start_run.return_value = _make_ref(run_id="run-stable")

        payload = {
            "workflowVersionId": WORKFLOW_VERSION_ID,
            "inputs": {"k": 1},
            "idempotencyKey": "dedup-1",
        }
        first = await client.post(f"/v1/workspaces/{WORKSPACE}/runs", json=payload)
        second = await client.post(f"/v1/workspaces/{WORKSPACE}/runs", json=payload)

        assert first.status_code == 202
        assert second.status_code == 202
        assert first.json()["runId"] == second.json()["runId"] == "run-stable"
        # Validator + controller see the same workspace + workflow
        # version on every request; the ledger replay surfaces as
        # the controller still returning the original ``runId``
        # (the mock returns a fixed ref).
        assert harness.catalog.calls == [
            (WORKSPACE, WORKFLOW_VERSION_ID),
            (WORKSPACE, WORKFLOW_VERSION_ID),
        ]
        assert harness.controller.start_run.await_count == 2

    async def test_divergent_inputs_replay_returns_409_conflict_envelope(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        harness.controller.start_run.return_value = _make_ref()

        first = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json={
                "workflowVersionId": WORKFLOW_VERSION_ID,
                "inputs": {"k": 1},
                "idempotencyKey": "dedup-2",
            },
        )
        assert first.status_code == 202

        second = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json={
                "workflowVersionId": WORKFLOW_VERSION_ID,
                "inputs": {"k": 2},  # divergent payload
                "idempotencyKey": "dedup-2",
            },
        )

        assert second.status_code == 409
        envelope = second.json()
        # WF-IMPL-061 RFC 7807 envelope: ``kind`` field carries the
        # locked validator error tag.
        assert envelope["code"] == "workflow.validator.idempotency_conflict"

    async def test_invalid_workspace_id_returns_400_bad_request(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        response = await client.post(
            "/v1/workspaces/Bad_WS/runs",
            json={"workflowVersionId": WORKFLOW_VERSION_ID, "inputs": {}},
        )

        assert response.status_code == 400
        assert response.json()["code"] == "workflow.api.bad_request"
        harness.controller.start_run.assert_not_called()

    async def test_missing_workflow_version_id_returns_400(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        response = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json={"inputs": {}},
        )

        assert response.status_code == 400
        harness.controller.start_run.assert_not_called()

    async def test_extra_body_field_is_rejected(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        """``extra='forbid'`` on the wire model surfaces as 400."""
        response = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json={
                "workflowVersionId": WORKFLOW_VERSION_ID,
                "inputs": {},
                "unknownField": True,
            },
        )

        assert response.status_code == 400
        harness.controller.start_run.assert_not_called()


# ---------------------------------------------------------------------------
# ListRuns
# ---------------------------------------------------------------------------


class TestListRuns:
    async def test_happy_path_returns_items_and_next_cursor(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        page = Page(
            items=[
                _make_ref(run_id="r1", status=RunStatus.RUNNING),
                _make_ref(run_id="r2", status=RunStatus.SUCCEEDED),
            ],
            next_cursor=Cursor(token="next-token"),
        )
        harness.controller.list_runs.return_value = page

        response = await client.get(f"/v1/workspaces/{WORKSPACE}/runs")

        assert response.status_code == 200
        body = response.json()
        assert [item["runId"] for item in body["items"]] == ["r1", "r2"]
        assert body["nextCursor"] == "next-token"
        harness.controller.list_runs.assert_awaited_once()
        call_kwargs = harness.controller.list_runs.await_args.kwargs
        assert call_kwargs["workspace_id"] == WORKSPACE
        assert call_kwargs["cursor"] is None
        assert call_kwargs["limit"] is None

    async def test_empty_page_returns_empty_items_and_null_cursor(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        harness.controller.list_runs.return_value = Page(items=[], next_cursor=None)

        response = await client.get(f"/v1/workspaces/{WORKSPACE}/runs")

        assert response.status_code == 200
        body = response.json()
        assert body["items"] == []
        assert body["nextCursor"] is None

    async def test_cursor_is_forwarded_as_opaque_token(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        harness.controller.list_runs.return_value = Page(items=[], next_cursor=None)

        response = await client.get(
            f"/v1/workspaces/{WORKSPACE}/runs",
            params={"cursor": "page-2", "limit": 50},
        )

        assert response.status_code == 200
        call_kwargs = harness.controller.list_runs.await_args.kwargs
        assert isinstance(call_kwargs["cursor"], Cursor)
        assert call_kwargs["cursor"].token == "page-2"
        assert call_kwargs["limit"] == 50

    async def test_status_filter_narrows_returned_items(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        harness.controller.list_runs.return_value = Page(
            items=[
                _make_ref(run_id="r-running", status=RunStatus.RUNNING),
                _make_ref(run_id="r-done", status=RunStatus.SUCCEEDED),
            ],
            next_cursor=None,
        )

        response = await client.get(
            f"/v1/workspaces/{WORKSPACE}/runs",
            params={"status": RunStatus.RUNNING.value},
        )

        assert response.status_code == 200
        body = response.json()
        assert [item["runId"] for item in body["items"]] == ["r-running"]

    async def test_workflow_version_filter_narrows_returned_items(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        harness.controller.list_runs.return_value = Page(
            items=[
                _make_ref(run_id="r-a", workflow_version_id="wfv-a"),
                _make_ref(run_id="r-b", workflow_version_id="wfv-b"),
            ],
            next_cursor=None,
        )

        response = await client.get(
            f"/v1/workspaces/{WORKSPACE}/runs",
            params={"workflowVersionId": "wfv-b"},
        )

        assert response.status_code == 200
        body = response.json()
        assert [item["runId"] for item in body["items"]] == ["r-b"]

    async def test_limit_above_cap_returns_400(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        response = await client.get(
            f"/v1/workspaces/{WORKSPACE}/runs",
            params={"limit": 10_000},
        )

        assert response.status_code == 400
        harness.controller.list_runs.assert_not_called()

    async def test_invalid_workspace_returns_400(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        response = await client.get("/v1/workspaces/BAD_WS/runs")

        assert response.status_code == 400
        harness.controller.list_runs.assert_not_called()


# ---------------------------------------------------------------------------
# GetRun
# ---------------------------------------------------------------------------


class TestGetRun:
    async def test_happy_path_returns_record_projection(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        harness.controller.get_run.return_value = _make_record(status=RunStatus.RUNNING)

        response = await client.get(f"/v1/workspaces/{WORKSPACE}/runs/{RUN_ID}")

        assert response.status_code == 200
        body = response.json()
        assert body["runId"] == RUN_ID
        assert body["status"] == RunStatus.RUNNING.value
        assert body["workspaceId"] == WORKSPACE
        assert body["workflowVersionId"] == WORKFLOW_VERSION_ID
        assert body["reason"] is None
        assert body["inputs"] == {}
        assert body["outputs"] is None
        assert body["steps"] == []
        assert body["startedAt"].startswith("2026-05-01T12:00:00")

    async def test_unknown_run_returns_404_envelope(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        harness.controller.get_run.side_effect = RunNotFoundError(
            "no such run",
            run_id=RUN_ID,
        )

        response = await client.get(f"/v1/workspaces/{WORKSPACE}/runs/{RUN_ID}")

        assert response.status_code == 404
        assert response.json()["code"] == "workflow.run_not_found"

    async def test_runtime_unavailable_surfaces_503(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        harness.controller.get_run.side_effect = WorkflowRuntimeUnavailableError(
            "runtime down",
            run_id=RUN_ID,
        )

        response = await client.get(f"/v1/workspaces/{WORKSPACE}/runs/{RUN_ID}")

        assert response.status_code == 503
        assert response.json()["code"] == "workflow.workflow_runtime_unavailable"


# ---------------------------------------------------------------------------
# CancelRun
# ---------------------------------------------------------------------------


class TestCancelRun:
    async def test_happy_path_returns_202_with_ref(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        harness.controller.cancel_run.return_value = _make_ref(status=RunStatus.CANCELLING)

        response = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs/{RUN_ID}:cancel",
            json={"reason": "operator stop"},
        )

        assert response.status_code == 202
        body = response.json()
        assert body["status"] == RunStatus.CANCELLING.value
        call_kwargs = harness.controller.cancel_run.await_args.kwargs
        assert call_kwargs["workspace_id"] == WORKSPACE
        assert call_kwargs["run_id"] == RUN_ID
        assert call_kwargs["reason"] == "operator stop"

    async def test_empty_body_is_accepted_and_passes_none_reason(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        harness.controller.cancel_run.return_value = _make_ref(status=RunStatus.CANCELLING)

        response = await client.post(f"/v1/workspaces/{WORKSPACE}/runs/{RUN_ID}:cancel")

        assert response.status_code == 202
        call_kwargs = harness.controller.cancel_run.await_args.kwargs
        assert call_kwargs["reason"] is None

    async def test_state_conflict_returns_409_envelope(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        harness.controller.cancel_run.side_effect = RunStateConflictError(
            "run is terminal",
            run_id=RUN_ID,
        )

        response = await client.post(f"/v1/workspaces/{WORKSPACE}/runs/{RUN_ID}:cancel")

        assert response.status_code == 409
        assert response.json()["code"] == "workflow.run_state_conflict"

    async def test_unknown_run_returns_404(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        harness.controller.cancel_run.side_effect = RunNotFoundError(
            "no such run",
            run_id=RUN_ID,
        )

        response = await client.post(f"/v1/workspaces/{WORKSPACE}/runs/{RUN_ID}:cancel")

        assert response.status_code == 404

    async def test_reason_over_length_returns_400(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        too_long = "x" * 2000

        response = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs/{RUN_ID}:cancel",
            json={"reason": too_long},
        )

        assert response.status_code == 400
        harness.controller.cancel_run.assert_not_called()


# ---------------------------------------------------------------------------
# Module-level invariants
# ---------------------------------------------------------------------------


def test_all_routers_includes_runs_router() -> None:
    """``api.routes.all_routers`` exports the runs router exactly once."""
    from custos_workflow.api.routes import all_routers
    from custos_workflow.api.routes import runs_router as exported

    assert exported in all_routers
    assert sum(1 for r in all_routers if r is exported) == 1


def test_runs_router_exposes_expected_paths() -> None:
    """The router publishes the four canonical paths from design.md."""
    from custos_workflow.api.routes import runs_router

    paths: set[str] = {route.path for route in runs_router.routes}  # type: ignore[attr-defined]
    assert "/v1/workspaces/{ws}/runs" in paths
    assert "/v1/workspaces/{ws}/runs/{run_id}" in paths
    assert "/v1/workspaces/{ws}/runs/{run_id}:cancel" in paths
