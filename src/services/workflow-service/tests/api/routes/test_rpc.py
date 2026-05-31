"""End-to-end tests for the WF-IMPL-067 Internal RPC routes.

The harness mirrors the WF-IMPL-065 ``test_runs.py`` pattern:
a minimal :class:`fastapi.FastAPI` app with the RPC router
mounted plus the WF-IMPL-061 exception handlers, a real
:class:`StartRunValidator` over the stubbed catalog +
in-memory ledger, and an :class:`AsyncMock`
:class:`RunController`. The transport is
:class:`httpx.AsyncClient` over :class:`httpx.ASGITransport`
so the wire shape is observed exactly as a Trigger Service
client would observe it.

Coverage targets pinned by the issue acceptance criteria:

* Internal ``StartRun`` accepts the same body shape as the
  public POST plus an explicit ``workspaceId``.
* Idempotency header / body precedence works exactly like
  the public surface (body wins; empty string opts out).
* Replay against the in-memory ledger returns the same
  ``runId``; divergent inputs surface the
  ``workflow.validator.idempotency_conflict`` envelope.
* ``CancelRun`` dispatches to
  :meth:`RunController.cancel_run` and returns 202 with the
  current :class:`RunRef`; unknown run id surfaces the
  ``workflow.run_not_found`` (404) envelope.
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
    get_validator,
)
from custos_workflow.api.errors import register_exception_handlers
from custos_workflow.api.routes import all_routers, rpc_router
from custos_workflow.call_context import CallContext
from custos_workflow.document.models import WorkflowDocument
from custos_workflow.runs.controller import RunController, RunRef, WorkflowVersion
from custos_workflow.runs.errors import RunNotFoundError, RunStateConflictError
from custos_workflow.runs.ids import RunId
from custos_workflow.runs.model import RunStatus
from custos_workflow.validator import (
    InMemoryIdempotencyLedger,
    StartRunValidator,
)

# ---------------------------------------------------------------------------
# Constants + small builders (re-mirrored to keep this test file standalone)
# ---------------------------------------------------------------------------


WORKSPACE = "ws-a"
WORKFLOW_VERSION_ID = "wfv-1"
WORKFLOW_ID = "wf-1"
RUN_ID = "run-1"
FIXED_NOW = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)


def _workflow_doc_yaml(inputs_block: str = "") -> dict[str, Any]:
    """Render a minimal valid :class:`WorkflowDocument` YAML."""
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


def _workflow_version() -> WorkflowVersion:
    doc = WorkflowDocument.model_validate(_workflow_doc_yaml())
    return WorkflowVersion(
        id=WORKFLOW_VERSION_ID,
        workflow_id=WORKFLOW_ID,
        name="pipeline",
        version_label="v1",
        document=doc,
    )


class _RecordingCatalogClient:
    """Catalog Protocol fake; records calls and optionally raises."""

    def __init__(self) -> None:
        self._version = _workflow_version()
        self.calls: list[tuple[str, str]] = []

    async def get_workflow_version(
        self, workspace_id: str, workflow_version_id: str
    ) -> WorkflowVersion:
        self.calls.append((workspace_id, workflow_version_id))
        return self._version


def _make_ref(
    *,
    run_id: str = RUN_ID,
    workspace_id: str = WORKSPACE,
    workflow_version_id: str = WORKFLOW_VERSION_ID,
    status: RunStatus = RunStatus.QUEUED,
) -> RunRef:
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
    """Mount the RPC router with the dependency overrides wired."""
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(rpc_router)

    app.dependency_overrides[get_validator] = lambda: harness.validator
    app.dependency_overrides[get_run_controller] = lambda: harness.controller
    app.dependency_overrides[get_call_context] = lambda: harness.call_context
    return app


@pytest.fixture
def harness() -> _Harness:
    return _Harness()


@pytest.fixture
async def client(harness: _Harness) -> AsyncIterator[httpx.AsyncClient]:
    app = _build_app(harness)
    transport = ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as cli:
        yield cli


# ---------------------------------------------------------------------------
# /internal/runs:start
# ---------------------------------------------------------------------------


class TestInternalStartRun:
    async def test_happy_path_returns_202_with_ref_body(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        """Trigger-Service-shaped StartRun returns the wire-stable handle."""
        harness.controller.start_run.return_value = _make_ref(status=RunStatus.QUEUED)

        response = await client.post(
            "/internal/runs:start",
            json={
                "workspaceId": WORKSPACE,
                "workflowVersionId": WORKFLOW_VERSION_ID,
                "inputs": {"k": 1},
            },
        )

        assert response.status_code == 202
        body = response.json()
        assert body["runId"] == RUN_ID
        assert body["status"] == RunStatus.QUEUED.value
        assert body["workspaceId"] == WORKSPACE
        assert body["workflowVersionId"] == WORKFLOW_VERSION_ID
        # Validator + controller saw the body's workspaceId.
        assert harness.catalog.calls == [(WORKSPACE, WORKFLOW_VERSION_ID)]
        call_kwargs = harness.controller.start_run.await_args.kwargs
        assert call_kwargs["workspace_id"] == WORKSPACE
        assert call_kwargs["workflow_version_id"] == WORKFLOW_VERSION_ID
        assert call_kwargs["inputs"] == {"k": 1}
        assert call_kwargs["idempotency_key"] is None

    async def test_missing_workspace_id_in_body_is_rejected(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        """The Internal RPC body MUST carry an explicit workspaceId."""
        response = await client.post(
            "/internal/runs:start",
            json={
                "workflowVersionId": WORKFLOW_VERSION_ID,
                "inputs": {},
            },
        )

        assert response.status_code == 400
        assert response.json()["code"] == "workflow.api.bad_request"
        harness.controller.start_run.assert_not_called()

    async def test_idempotency_body_overrides_header(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        """Body field wins, mirroring the public surface contract."""
        harness.controller.start_run.return_value = _make_ref()

        response = await client.post(
            "/internal/runs:start",
            json={
                "workspaceId": WORKSPACE,
                "workflowVersionId": WORKFLOW_VERSION_ID,
                "inputs": {},
                "idempotencyKey": "body-key-1",
            },
            headers={"Idempotency-Key": "header-key-1"},
        )

        assert response.status_code == 202
        call_kwargs = harness.controller.start_run.await_args.kwargs
        assert call_kwargs["idempotency_key"] == "body-key-1"

    async def test_idempotency_key_from_header_when_body_absent(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        harness.controller.start_run.return_value = _make_ref()

        response = await client.post(
            "/internal/runs:start",
            json={
                "workspaceId": WORKSPACE,
                "workflowVersionId": WORKFLOW_VERSION_ID,
                "inputs": {},
            },
            headers={"Idempotency-Key": "header-key-1"},
        )

        assert response.status_code == 202
        call_kwargs = harness.controller.start_run.await_args.kwargs
        assert call_kwargs["idempotency_key"] == "header-key-1"

    async def test_replay_returns_same_run_id(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        """The validator's ledger replay surfaces as a stable wire runId."""
        harness.controller.start_run.return_value = _make_ref(run_id="run-stable")

        payload = {
            "workspaceId": WORKSPACE,
            "workflowVersionId": WORKFLOW_VERSION_ID,
            "inputs": {"k": 1},
            "idempotencyKey": "dedup-1",
        }
        first = await client.post("/internal/runs:start", json=payload)
        second = await client.post("/internal/runs:start", json=payload)

        assert first.status_code == 202
        assert second.status_code == 202
        assert first.json()["runId"] == second.json()["runId"] == "run-stable"
        assert harness.controller.start_run.await_count == 2

    async def test_divergent_inputs_replay_returns_409_envelope(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        harness.controller.start_run.return_value = _make_ref()

        first = await client.post(
            "/internal/runs:start",
            json={
                "workspaceId": WORKSPACE,
                "workflowVersionId": WORKFLOW_VERSION_ID,
                "inputs": {"k": 1},
                "idempotencyKey": "dedup-2",
            },
        )
        assert first.status_code == 202

        second = await client.post(
            "/internal/runs:start",
            json={
                "workspaceId": WORKSPACE,
                "workflowVersionId": WORKFLOW_VERSION_ID,
                "inputs": {"k": 2},
                "idempotencyKey": "dedup-2",
            },
        )
        assert second.status_code == 409
        assert second.json()["code"] == "workflow.validator.idempotency_conflict"

    async def test_extra_body_field_is_rejected(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        """``extra='forbid'`` on the wire model surfaces as 400."""
        response = await client.post(
            "/internal/runs:start",
            json={
                "workspaceId": WORKSPACE,
                "workflowVersionId": WORKFLOW_VERSION_ID,
                "inputs": {},
                "unknownField": True,
            },
        )

        assert response.status_code == 400
        harness.controller.start_run.assert_not_called()


# ---------------------------------------------------------------------------
# /internal/runs/{run_id}:cancel
# ---------------------------------------------------------------------------


class TestInternalCancelRun:
    async def test_happy_path_dispatches_and_returns_202(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        """Cancel-by-id forwards reason + workspace to the controller."""
        harness.controller.cancel_run.return_value = _make_ref(status=RunStatus.CANCELLING)

        response = await client.post(
            f"/internal/runs/{RUN_ID}:cancel",
            json={"workspaceId": WORKSPACE, "reason": "operator-cancel"},
        )

        assert response.status_code == 202
        body = response.json()
        assert body["runId"] == RUN_ID
        assert body["status"] == RunStatus.CANCELLING.value
        call_kwargs = harness.controller.cancel_run.await_args.kwargs
        assert call_kwargs["workspace_id"] == WORKSPACE
        assert str(call_kwargs["run_id"]) == RUN_ID
        assert call_kwargs["reason"] == "operator-cancel"

    async def test_reason_defaults_to_none(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        harness.controller.cancel_run.return_value = _make_ref(status=RunStatus.CANCELLING)

        response = await client.post(
            f"/internal/runs/{RUN_ID}:cancel",
            json={"workspaceId": WORKSPACE},
        )

        assert response.status_code == 202
        call_kwargs = harness.controller.cancel_run.await_args.kwargs
        assert call_kwargs["reason"] is None

    async def test_unknown_run_returns_404_envelope(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        harness.controller.cancel_run.side_effect = RunNotFoundError(
            f"run {RUN_ID!r} not found",
            run_id=RUN_ID,
        )

        response = await client.post(
            f"/internal/runs/{RUN_ID}:cancel",
            json={"workspaceId": WORKSPACE},
        )

        assert response.status_code == 404
        body = response.json()
        assert body["code"] == "workflow.run_not_found"
        assert body["runId"] == RUN_ID

    async def test_already_cancelled_returns_409_envelope(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        harness.controller.cancel_run.side_effect = RunStateConflictError(
            f"run {RUN_ID!r} already cancelled",
            run_id=RUN_ID,
            current_status=RunStatus.CANCELLED.value,
            attempted_status="cancelling",
        )

        response = await client.post(
            f"/internal/runs/{RUN_ID}:cancel",
            json={"workspaceId": WORKSPACE},
        )

        assert response.status_code == 409
        assert response.json()["code"] == "workflow.run_state_conflict"

    async def test_missing_workspace_id_returns_400(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        response = await client.post(
            f"/internal/runs/{RUN_ID}:cancel",
            json={"reason": "no ws"},
        )

        assert response.status_code == 400
        harness.controller.cancel_run.assert_not_called()


# ---------------------------------------------------------------------------
# Module-level router-registration smoke tests
# ---------------------------------------------------------------------------


def test_all_routers_includes_rpc_router() -> None:
    """``api.routes.all_routers`` exports the RPC router exactly once."""
    assert rpc_router in all_routers
    assert sum(1 for r in all_routers if r is rpc_router) == 1


def test_rpc_router_exposes_expected_paths() -> None:
    """The router carries both the start + cancel RPC paths."""
    paths = {
        (route.path, frozenset(route.methods))  # type: ignore[attr-defined]
        for route in rpc_router.routes
    }
    assert ("/internal/runs:start", frozenset({"POST"})) in paths
    assert ("/internal/runs/{run_id}:cancel", frozenset({"POST"})) in paths
