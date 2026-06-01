"""HTTP-server observability tests for the Workflow API Adapter (WF-IMPL-070).

Every public + internal route the Workflow Service exposes must
emit exactly one ``custos_workflow.http.request`` span tagged with
the standard HTTP-server semconv attributes (``http.method``,
``http.route``, ``http.status_code``) plus the workflow-service
``wf.*`` attributes when the route resolves them
(``wf.workspace.id``, ``wf.workflow_version.id``, ``wf.run.id``,
``wf.idempotency.outcome``, ``wf.error.kind``). The
:data:`custos_workflow_http_server_duration_ms` histogram
receives one sample per request keyed by
``(http.route, http.method, http.status_code)``.

The idempotency-cache outcome counter
:data:`custos_workflow_idempotency_outcomes_total` ticks
``fresh`` / ``replay`` / ``conflict`` exactly once per StartRun
that consulted the ledger, and the API error counter
:data:`custos_workflow_api_errors_total` ticks exactly once per
Problem+JSON envelope keyed by the locked ``wf.error.kind``.

These tests piggyback on the SDK provider install in
:mod:`tests.test_observability` so a single in-memory tracer +
meter pair captures emissions across both modules; the rebinding
dance is duplicated here only for the new WF-IMPL-070 instruments
that ``test_observability`` does not know about.
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

from custos_workflow import _telemetry
from custos_workflow.api.dependencies import (
    get_call_context,
    get_run_controller,
    get_validator,
)
from custos_workflow.api.errors import register_exception_handlers
from custos_workflow.api.observability import register_http_observability
from custos_workflow.api.routes import all_routers
from custos_workflow.call_context import CallContext
from custos_workflow.document.models import WorkflowDocument
from custos_workflow.runs.controller import RunController, RunRef, WorkflowVersion
from custos_workflow.runs.ids import RunId
from custos_workflow.runs.model import RunStatus
from custos_workflow.validator import (
    InMemoryIdempotencyLedger,
    StartRunValidator,
)

# Importing tests.test_observability installs the SDK provider and
# rebinds the compile / Run Controller instruments. The bare
# ``noqa: F401`` import is intentional — we just need the side
# effects.
from tests.test_observability import (
    _by_name,
    _collect_points,
    _metric_reader,
    _span_exporter,
)

# ---------------------------------------------------------------------------
# OTel SDK rebind for WF-IMPL-070 instruments
# ---------------------------------------------------------------------------
#
# ``tests.test_observability`` already swapped the tracer + meter
# on ``_telemetry`` for SDK-backed ones. The new WF-IMPL-070
# instruments were created at ``_telemetry`` import time against
# the API-default no-op meter, so we re-create them here against
# the now-installed SDK meter so this module's emissions reach the
# shared in-memory reader.

_telemetry.HTTP_SERVER_DURATION_MS = _telemetry._meter.create_histogram(  # type: ignore[misc]
    name="custos_workflow_http_server_duration_ms",
    unit="ms",
    description=(
        "Wall-clock time spent inside the FastAPI router stack "
        "for one inbound HTTP request, labelled by HTTP route, "
        "method, and status code."
    ),
)
_telemetry.API_ERRORS_TOTAL = _telemetry._meter.create_counter(  # type: ignore[misc]
    name="custos_workflow_api_errors_total",
    description=(
        "Count of Problem+JSON envelopes emitted by the Workflow "
        "API Adapter, labelled by the locked ``wf.error.kind``."
    ),
)
_telemetry.IDEMPOTENCY_OUTCOMES_TOTAL = _telemetry._meter.create_counter(  # type: ignore[misc]
    name="custos_workflow_idempotency_outcomes_total",
    description=(
        "Count of idempotency ledger outcomes observed by the "
        "StartRunValidator, labelled by outcome ∈ "
        "{fresh, replay, conflict}."
    ),
)


# ---------------------------------------------------------------------------
# Test harness — mirrors tests/api/routes/test_runs.py
# ---------------------------------------------------------------------------


# ``tests.test_observability`` defines an autouse ``_reset_otel_state``
# fixture, but pytest autouse scoping is per-module. Re-declare it
# here so every test in this file starts with an empty span exporter
# + drained metric reader; otherwise spans and counter samples leak
# between cases and the per-test assertions over-count.
@pytest.fixture(autouse=True)
def _reset_otel_state() -> Any:
    """Clear captured spans + drain metric points before each test."""
    _span_exporter.clear()
    _metric_reader.get_metrics_data()
    yield


WORKSPACE = "ws-a"
WORKFLOW_VERSION_ID = "wfv-1"
WORKFLOW_ID = "wf-1"
RUN_ID = "run-1"
FIXED_NOW = datetime(2026, 5, 1, 12, 0, 0, tzinfo=UTC)


def _workflow_doc_yaml() -> dict[str, Any]:
    parsed: dict[str, Any] = yaml.safe_load(
        f"""
        apiVersion: custos.dev/v1
        kind: Workflow
        metadata:
          name: pipeline
          workspace: {WORKSPACE}
        spec:
          inputs:
            k: {{type: integer, required: false}}
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
    """Catalog Protocol fake; optional ``raise_on_call`` for not-found cases."""

    def __init__(self, *, raise_on_call: Exception | None = None) -> None:
        self._version = _workflow_version()
        self._raise = raise_on_call
        self.calls: list[tuple[str, str]] = []

    async def get_workflow_version(
        self, workspace_id: str, workflow_version_id: str
    ) -> WorkflowVersion:
        self.calls.append((workspace_id, workflow_version_id))
        if self._raise is not None:
            raise self._raise
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


class _Harness:
    def __init__(self, *, catalog: _RecordingCatalogClient | None = None) -> None:
        self.catalog = catalog if catalog is not None else _RecordingCatalogClient()
        self.ledger = InMemoryIdempotencyLedger()
        self.validator = StartRunValidator(catalog=self.catalog, ledger=self.ledger)
        self.controller: AsyncMock = AsyncMock(spec=RunController)
        self.call_context = CallContext(workspace=WORKSPACE, principal="user-1")


def _build_app(harness: _Harness) -> FastAPI:
    """Mount every API router on a fresh FastAPI app with the
    WF-IMPL-070 middleware + WF-IMPL-061 exception handlers wired."""
    app = FastAPI()
    register_exception_handlers(app)
    register_http_observability(app)
    for router in all_routers:
        app.include_router(router)
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
# Span shape — happy paths
# ---------------------------------------------------------------------------


def _http_request_spans() -> list[Any]:
    """All finished ``custos_workflow.http.request`` spans, in finish order."""
    return [
        s for s in _span_exporter.get_finished_spans() if s.name == "custos_workflow.http.request"
    ]


class TestSpanEmission:
    async def test_start_run_emits_one_span_with_route_template_and_workspace_id(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        """StartRun emits exactly one server span carrying the
        templated route, the workspace path-param, and the validated
        workflow version + run id mirrored from ``request.state``."""
        harness.controller.start_run.return_value = _make_ref()

        response = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json={"workflowVersionId": WORKFLOW_VERSION_ID, "inputs": {}},
        )
        assert response.status_code == 202

        spans = _http_request_spans()
        assert len(spans) == 1
        attrs = dict(spans[0].attributes or {})
        assert attrs["http.method"] == "POST"
        assert attrs["http.route"] == "/v1/workspaces/{ws}/runs"
        assert attrs["http.status_code"] == 202
        assert attrs["wf.workspace.id"] == WORKSPACE
        assert attrs["wf.workflow_version.id"] == WORKFLOW_VERSION_ID
        assert attrs["wf.run.id"] == RUN_ID

    async def test_get_run_emits_span_with_run_id_from_path_param(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        """GET /runs/{run_id} sets ``wf.run.id`` from the path-param
        side (no need for the handler to stash it)."""
        from custos_workflow.runs.errors import RunNotFoundError

        harness.controller.get_run.side_effect = RunNotFoundError(
            "run not found", run_id="run-missing"
        )

        response = await client.get(f"/v1/workspaces/{WORKSPACE}/runs/run-missing")
        assert response.status_code == 404

        spans = _http_request_spans()
        assert len(spans) == 1
        attrs = dict(spans[0].attributes or {})
        assert attrs["http.route"] == "/v1/workspaces/{ws}/runs/{run_id}"
        assert attrs["http.status_code"] == 404
        assert attrs["wf.workspace.id"] == WORKSPACE
        assert attrs["wf.run.id"] == "run-missing"
        assert attrs["wf.error.kind"] == "workflow.run_not_found"

    async def test_unmatched_path_falls_back_to_url_path_with_404(
        self, client: httpx.AsyncClient
    ) -> None:
        """Requests to unknown URLs still emit a span; with no
        matching route the helper falls back to the raw URL path."""
        response = await client.get("/does-not-exist")
        assert response.status_code == 404

        spans = _http_request_spans()
        assert len(spans) == 1
        attrs = dict(spans[0].attributes or {})
        assert attrs["http.method"] == "GET"
        assert attrs["http.route"] == "/does-not-exist"
        assert attrs["http.status_code"] == 404


# ---------------------------------------------------------------------------
# Duration histogram — labels partition by route + method + status
# ---------------------------------------------------------------------------


class TestDurationHistogram:
    async def test_two_distinct_routes_yield_two_distinct_label_sets(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        harness.controller.start_run.return_value = _make_ref()
        await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json={"workflowVersionId": WORKFLOW_VERSION_ID, "inputs": {}},
        )
        await client.get("/does-not-exist")

        points = _collect_points()
        samples = _by_name(points, "custos_workflow_http_server_duration_ms")
        label_sets = sorted(tuple(sorted(attrs.items())) for attrs, _ in samples)
        assert len(label_sets) == 2
        # Each sample has the three required labels.
        for attrs, _ in samples:
            assert set(attrs) == {"http.route", "http.method", "http.status_code"}


# ---------------------------------------------------------------------------
# Idempotency outcome counter — fresh / replay / conflict
# ---------------------------------------------------------------------------


class TestIdempotencyOutcomeCounter:
    async def test_fresh_then_replay_then_conflict_each_bump_once(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        """Three POSTs against the same key: fresh, replay (same
        inputs), conflict (divergent inputs). The validator emits
        one counter sample per request that consulted the ledger."""
        harness.controller.start_run.return_value = _make_ref()
        key = "dedup-1"

        # fresh
        r1 = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json={
                "workflowVersionId": WORKFLOW_VERSION_ID,
                "inputs": {"k": 1},
                "idempotencyKey": key,
            },
        )
        assert r1.status_code == 202

        # replay
        r2 = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json={
                "workflowVersionId": WORKFLOW_VERSION_ID,
                "inputs": {"k": 1},
                "idempotencyKey": key,
            },
        )
        assert r2.status_code == 202

        # conflict
        r3 = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json={
                "workflowVersionId": WORKFLOW_VERSION_ID,
                "inputs": {"k": 2},
                "idempotencyKey": key,
            },
        )
        assert r3.status_code == 409

        points = _collect_points()
        samples = _by_name(points, "custos_workflow_idempotency_outcomes_total")
        # DELTA temporality: one point per (outcome) label set with
        # ``value=1`` apiece across the three calls.
        outcomes = sorted(attrs["wf.idempotency.outcome"] for attrs, _ in samples)
        assert outcomes == ["conflict", "fresh", "replay"]
        for _, value in samples:
            assert value == 1

    async def test_request_without_idempotency_key_does_not_bump_counter(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        """StartRun calls that omit the key skip the ledger entirely
        and therefore never reach the outcome counter."""
        harness.controller.start_run.return_value = _make_ref()

        response = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json={"workflowVersionId": WORKFLOW_VERSION_ID, "inputs": {}},
        )
        assert response.status_code == 202

        points = _collect_points()
        assert _by_name(points, "custos_workflow_idempotency_outcomes_total") == []

    async def test_conflict_envelope_carries_idempotency_outcome_on_span(
        self, client: httpx.AsyncClient, harness: _Harness
    ) -> None:
        """The conflict path's span carries
        ``wf.idempotency.outcome=conflict`` even though the route
        handler never ran (it's set by the exception handler on
        ``request.state``)."""
        harness.controller.start_run.return_value = _make_ref()
        key = "dedup-2"

        await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json={
                "workflowVersionId": WORKFLOW_VERSION_ID,
                "inputs": {"k": 1},
                "idempotencyKey": key,
            },
        )
        # Clear the spans from the first (fresh) call so we only see
        # the conflict span below.
        _span_exporter.clear()

        response = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json={
                "workflowVersionId": WORKFLOW_VERSION_ID,
                "inputs": {"k": 2},
                "idempotencyKey": key,
            },
        )
        assert response.status_code == 409

        spans = _http_request_spans()
        assert len(spans) == 1
        attrs = dict(spans[0].attributes or {})
        assert attrs["http.status_code"] == 409
        assert attrs["wf.idempotency.outcome"] == "conflict"
        assert attrs["wf.error.kind"] == "workflow.validator.idempotency_conflict"


# ---------------------------------------------------------------------------
# API error counter — one bump per Problem+JSON envelope
# ---------------------------------------------------------------------------


class TestApiErrorCounter:
    async def test_validator_404_bumps_counter_with_locked_kind(self, harness: _Harness) -> None:
        """A request that triggers
        :class:`WorkflowVersionNotFoundError` produces one counter
        sample labelled with the locked kind string."""
        from custos_workflow.validator.errors import WorkflowVersionNotFoundError

        harness.catalog = _RecordingCatalogClient(
            raise_on_call=WorkflowVersionNotFoundError(
                "workflow version not found",
                workspace_id=WORKSPACE,
                workflow_id=WORKFLOW_ID,
                workflow_version=WORKFLOW_VERSION_ID,
            )
        )
        # Rebuild the validator + app so the new catalog takes effect.
        harness.validator = StartRunValidator(catalog=harness.catalog, ledger=harness.ledger)
        app = _build_app(harness)
        transport = ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as cli:
            response = await cli.post(
                f"/v1/workspaces/{WORKSPACE}/runs",
                json={"workflowVersionId": WORKFLOW_VERSION_ID, "inputs": {}},
            )

        assert response.status_code == 404
        points = _collect_points()
        samples = _by_name(points, "custos_workflow_api_errors_total")
        assert len(samples) == 1
        attrs, value = samples[0]
        assert attrs["wf.error.kind"] == "workflow.validator.workflow_version_not_found"
        assert value == 1

    async def test_bad_request_envelope_bumps_counter_with_api_bad_request(
        self, client: httpx.AsyncClient
    ) -> None:
        """A FastAPI ``HTTPException`` (e.g. from the workspace path
        grammar guard) bumps the counter via
        :func:`handle_http_exception` rather than via
        :func:`_problem_response`."""
        response = await client.post(
            "/v1/workspaces/Bad_WS/runs",
            json={"workflowVersionId": WORKFLOW_VERSION_ID, "inputs": {}},
        )
        assert response.status_code == 400

        points = _collect_points()
        samples = _by_name(points, "custos_workflow_api_errors_total")
        assert len(samples) == 1
        attrs, value = samples[0]
        assert attrs["wf.error.kind"] == "workflow.api.bad_request"
        assert value == 1
