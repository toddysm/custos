"""WF-IMPL-071 — End-to-end API integration suite.

Each test drives the **fully wired** :func:`custos_workflow.create_app`
factory via :class:`httpx.AsyncClient` over :class:`httpx.ASGITransport`
through the full FastAPI lifespan, so the request walks every layer
the production wiring will exercise: OTel observability middleware,
:class:`CallContextMiddleware`, the WF-IMPL-061 RFC 7807 exception
handlers, the WF-IMPL-064 dependency factories that resolve the
:class:`RunController` + :class:`StartRunValidator` off
``app.state.run_components`` / ``app.state.start_run_validator``,
and finally the WF-IMPL-065..068 route handlers.

The :class:`FakeWorkflowRuntime` shipped with
:class:`~custos_workflow.providers.RunComponents` drives the
WF-IMPL-035 ``run_orchestrator`` inline during
``schedule_new_workflow``, so by the time ``POST /runs`` returns
the runtime instance is already terminal — but the persisted
:class:`RunRecord` stays at :class:`RunStatus.RUNNING` until the
reconciler hook fires (the controller never re-reads the runtime
instance during ``start_run``). That matches the WF-IMPL-045
integration suite invariant and is the wire shape the SDK will see.

Coverage map (design.md § Failure Modes columns marked with [E2E]
land in this file; the unit-level suites under ``tests/api/`` /
``tests/runs/`` / ``tests/validator/`` continue to own kind-by-kind
coverage of each error envelope):

* Happy paths
    * REST ``StartRun`` → ``GetRun`` → ``ListRuns`` → ``CancelRun``.
    * Internal RPC ``StartRun`` (workspace travels in the body).
* Idempotency precedence (WF-IMPL-065)
    * ``Idempotency-Key`` header used when body field is absent.
    * Body field wins when both are supplied.
    * Replay returns the original ``RunRef`` from the ledger.
    * Divergent inputs against a settled key → 409
      ``workflow.validator.idempotency_conflict``.
* Validator failure modes (WF-IMPL-061 + WF-IMPL-063)
    * Workflow version not in Catalog → 404
      ``workflow.validator.workflow_version_not_found``.
    * Inputs that violate the published schema → 422
      ``workflow.validator.inputs_schema_error``.
    * Call-context workspace ≠ path workspace → 403
      ``workflow.validator.workspace_unauthorized``.
* Run Controller failure modes (WF-IMPL-061 + WF-IMPL-029..046)
    * Cancel on unknown run → 404 ``workflow.run_not_found``.
    * Get on unknown run → 404 ``workflow.run_not_found``.
    * Workflow runtime refusing schedule → 503
      ``workflow.workflow_runtime_unavailable``.
"""

from __future__ import annotations

import textwrap
from collections.abc import AsyncIterator

import httpx
import pytest
import yaml
from httpx import ASGITransport

from custos_workflow import create_app
from custos_workflow.call_context import PRINCIPAL_HEADER, WORKSPACE_HEADER
from custos_workflow.document.models import WorkflowDocument
from custos_workflow.providers import RunComponents, load_run_components
from custos_workflow.runs.controller import WorkflowVersion
from custos_workflow.runs.errors import WorkflowRuntimeUnavailableError
from custos_workflow.runs.model import RunStatus
from custos_workflow.runtime import FakeWorkflowRuntime
from custos_workflow.validator.errors import WorkflowVersionNotFoundError

# ---------------------------------------------------------------------------
# Constants + workflow doc helpers
# ---------------------------------------------------------------------------


WORKSPACE = "ws-e2e"
WORKFLOW_VERSION_ID = "wfv-e2e"
WORKFLOW_ID = "wf-e2e"


def _doc_yaml() -> str:
    """A minimal linear two-step workflow the fake runtime can drive inline.

    Both steps are pure ``let:`` bindings so the
    :class:`~custos_workflow.runs.NoopStepHandler` /
    :class:`~custos_workflow.steps.let_step.LetStepHandler` path
    finishes synchronously inside the
    :class:`~custos_workflow.runtime.FakeWorkflowRuntime` and the
    fake's terminal :class:`RunOutput` lands the moment
    ``schedule_new_workflow`` returns.
    """
    return textwrap.dedent(
        """\
        apiVersion: custos.dev/v1
        kind: Workflow
        metadata:
          name: pipeline
          workspace: ws-e2e
        spec:
          inputs:
            k:
              type: integer
              required: false
          steps:
            - id: a
              let: {x: '${{ true }}'}
            - id: b
              needs: [a]
              let: {y: '${{ true }}'}
        """
    )


def _make_version() -> WorkflowVersion:
    """Render :func:`_doc_yaml` into a :class:`WorkflowVersion`."""
    doc = WorkflowDocument.model_validate(yaml.safe_load(_doc_yaml()))
    return WorkflowVersion(
        id=WORKFLOW_VERSION_ID,
        workflow_id=WORKFLOW_ID,
        name="pipeline",
        version_label="v1",
        document=doc,
    )


# ---------------------------------------------------------------------------
# Catalog stubs (per-test injection through ``load_run_components(catalog=...)``)
# ---------------------------------------------------------------------------


class _CatalogStub:
    """Catalog Protocol stub returning the same WorkflowVersion every call."""

    def __init__(
        self,
        version: WorkflowVersion | None = None,
        *,
        raise_on_call: Exception | None = None,
    ) -> None:
        self._version = version if version is not None else _make_version()
        self._raise = raise_on_call
        self.calls: list[tuple[str, str]] = []

    async def get_workflow_version(
        self, workspace_id: str, workflow_version_id: str
    ) -> WorkflowVersion:
        self.calls.append((workspace_id, workflow_version_id))
        if self._raise is not None:
            raise self._raise
        return self._version


# ---------------------------------------------------------------------------
# RunComponents builder + httpx harness
# ---------------------------------------------------------------------------


def _components(
    *, catalog: _CatalogStub | None = None
) -> tuple[RunComponents, _CatalogStub, FakeWorkflowRuntime]:
    """Build a sidecar-free :class:`RunComponents` bundle for the e2e suite."""
    cat = catalog if catalog is not None else _CatalogStub()
    runtime = FakeWorkflowRuntime()
    bundle = load_run_components(env={}, workflow_runtime=runtime, catalog=cat)
    return bundle, cat, runtime


class _AppHarness:
    """Bundle the fully-wired app plus the collaborators the tests assert on."""

    def __init__(self, *, catalog: _CatalogStub | None = None) -> None:
        self.components, self.catalog, self.runtime = _components(catalog=catalog)
        self.app = create_app(
            require_call_context=False,
            run_components=self.components,
        )


@pytest.fixture
async def harness() -> _AppHarness:
    """Fresh :class:`_AppHarness` per test."""
    return _AppHarness()


@pytest.fixture
async def client(harness: _AppHarness) -> AsyncIterator[httpx.AsyncClient]:
    """An :class:`httpx.AsyncClient` wired through the full lifespan.

    Driving the lifespan via :meth:`FastAPI.router.lifespan_context`
    is the documented way to get ``app.state.run_components`` /
    ``app.state.start_run_validator`` populated for an
    :class:`httpx.AsyncClient`-driven test (the
    :class:`fastapi.testclient.TestClient` shim cannot be reused
    here because every test is ``async def`` under
    :data:`pytest-asyncio` auto-mode).
    """
    transport = ASGITransport(app=harness.app)
    async with (
        harness.app.router.lifespan_context(harness.app),
        httpx.AsyncClient(transport=transport, base_url="http://test") as cli,
    ):
        yield cli


def _start_run_payload(*, inputs: dict[str, int] | None = None) -> dict[str, object]:
    """Render a :class:`StartRunRequest` body the catalog stub will accept."""
    return {
        "workflowVersionId": WORKFLOW_VERSION_ID,
        "inputs": dict(inputs) if inputs is not None else {},
    }


# ---------------------------------------------------------------------------
# Happy paths
# ---------------------------------------------------------------------------


class TestRestHappyPath:
    async def test_start_get_list_cancel_round_trip(
        self, client: httpx.AsyncClient, harness: _AppHarness
    ) -> None:
        # --- StartRun -----------------------------------------------------
        start = await client.post(f"/v1/workspaces/{WORKSPACE}/runs", json=_start_run_payload())
        assert start.status_code == 202, start.text
        ref = start.json()
        run_id = ref["runId"]
        assert ref["workspaceId"] == WORKSPACE
        assert ref["workflowVersionId"] == WORKFLOW_VERSION_ID
        assert ref["status"] == RunStatus.RUNNING.value

        # The fake drove the orchestrator inline so the runtime
        # instance is already terminal; the store row, by contract,
        # stays at RUNNING until the reconciler reconciles.
        terminal_state = harness.runtime.instance(run_id)
        assert terminal_state.output is not None

        # --- GetRun -------------------------------------------------------
        got = await client.get(f"/v1/workspaces/{WORKSPACE}/runs/{run_id}")
        assert got.status_code == 200, got.text
        body = got.json()
        assert body["runId"] == run_id
        assert body["workspaceId"] == WORKSPACE
        # The fake drove the orchestrator inline; the persisted row is
        # ``RUNNING`` from ``start_run`` but the GetRun projection
        # reconciles against the runtime instance and surfaces the
        # terminal ``SUCCEEDED`` status by the time the SDK polls.
        assert body["status"] in {RunStatus.RUNNING.value, RunStatus.SUCCEEDED.value}

        # --- ListRuns -----------------------------------------------------
        listed = await client.get(f"/v1/workspaces/{WORKSPACE}/runs")
        assert listed.status_code == 200, listed.text
        items = listed.json()["items"]
        assert any(item["runId"] == run_id for item in items)

        # --- CancelRun ----------------------------------------------------
        cancelled = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs/{run_id}:cancel",
            json={"reason": "user requested"},
        )
        assert cancelled.status_code == 202, cancelled.text
        assert cancelled.json()["runId"] == run_id
        assert cancelled.json()["status"] in {
            RunStatus.CANCELLING.value,
            RunStatus.CANCELLED.value,
        }

    async def test_internal_rpc_start_run_round_trips(self, client: httpx.AsyncClient) -> None:
        """The Internal RPC route accepts the workspace via the body."""
        response = await client.post(
            "/internal/runs:start",
            json={
                "workspaceId": WORKSPACE,
                "workflowVersionId": WORKFLOW_VERSION_ID,
                "inputs": {},
            },
        )
        assert response.status_code == 202, response.text
        body = response.json()
        assert body["workspaceId"] == WORKSPACE
        assert body["workflowVersionId"] == WORKFLOW_VERSION_ID


# ---------------------------------------------------------------------------
# Idempotency precedence + replay + conflict
# ---------------------------------------------------------------------------


class TestIdempotency:
    async def test_header_used_when_body_field_absent(self, client: httpx.AsyncClient) -> None:
        response = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json=_start_run_payload(),
            headers={"Idempotency-Key": "hdr-key-1"},
        )
        assert response.status_code == 202, response.text
        # Replaying the same header (same payload) must return the same runId.
        replay = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json=_start_run_payload(),
            headers={"Idempotency-Key": "hdr-key-1"},
        )
        assert replay.status_code == 202, replay.text
        assert replay.json()["runId"] == response.json()["runId"]

    async def test_body_field_overrides_header(self, client: httpx.AsyncClient) -> None:
        first = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json={**_start_run_payload(), "idempotencyKey": "body-key-1"},
            headers={"Idempotency-Key": "ignored-header"},
        )
        assert first.status_code == 202, first.text
        # The header should NOT have minted a separate run.
        header_replay = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json={**_start_run_payload(), "idempotencyKey": "body-key-1"},
            headers={"Idempotency-Key": "different-header"},
        )
        assert header_replay.status_code == 202
        assert header_replay.json()["runId"] == first.json()["runId"]

    async def test_idempotent_replay_returns_original_run_ref(
        self, client: httpx.AsyncClient
    ) -> None:
        first = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json={**_start_run_payload(inputs={"k": 1}), "idempotencyKey": "repl-1"},
        )
        assert first.status_code == 202, first.text
        replay = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json={**_start_run_payload(inputs={"k": 1}), "idempotencyKey": "repl-1"},
        )
        assert replay.status_code == 202, replay.text
        assert replay.json()["runId"] == first.json()["runId"]

    async def test_divergent_inputs_replay_yields_409_conflict(
        self, client: httpx.AsyncClient
    ) -> None:
        first = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json={**_start_run_payload(inputs={"k": 1}), "idempotencyKey": "conflict-1"},
        )
        assert first.status_code == 202, first.text
        conflict = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json={**_start_run_payload(inputs={"k": 2}), "idempotencyKey": "conflict-1"},
        )
        assert conflict.status_code == 409, conflict.text
        body = conflict.json()
        assert body["code"] == "workflow.validator.idempotency_conflict"
        assert body["status"] == 409


# ---------------------------------------------------------------------------
# Validator failure modes
# ---------------------------------------------------------------------------


class TestValidatorFailureModes:
    async def test_workflow_version_not_found_returns_404(self) -> None:
        catalog = _CatalogStub(
            raise_on_call=WorkflowVersionNotFoundError(
                "wfv missing",
                workspace_id=WORKSPACE,
                workflow_id=WORKFLOW_ID,
                workflow_version=WORKFLOW_VERSION_ID,
            )
        )
        harness = _AppHarness(catalog=catalog)
        transport = ASGITransport(app=harness.app)
        async with (
            harness.app.router.lifespan_context(harness.app),
            httpx.AsyncClient(transport=transport, base_url="http://test") as cli,
        ):
            response = await cli.post(
                f"/v1/workspaces/{WORKSPACE}/runs",
                json=_start_run_payload(),
            )
        assert response.status_code == 404, response.text
        body = response.json()
        assert body["code"] == "workflow.validator.workflow_version_not_found"
        assert body["status"] == 404

    async def test_inputs_schema_violation_returns_422(self, client: httpx.AsyncClient) -> None:
        # The default inputs schema declares ``k: integer``; supplying
        # a string violates the published JSON-Schema.
        response = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json={
                "workflowVersionId": WORKFLOW_VERSION_ID,
                "inputs": {"k": "not-an-integer"},
            },
        )
        assert response.status_code == 422, response.text
        body = response.json()
        assert body["code"] == "workflow.validator.inputs_schema_error"
        assert body["status"] == 422
        assert "validation" in body
        assert isinstance(body["validation"], list)
        assert body["validation"]
        # ``loc`` is a JSON-pointer string per RFC 6901 (e.g. ``/k``)
        # so SDKs that already parse 7807 ``validation`` extensions on
        # other Custos surfaces can branch uniformly on string fields.
        assert body["validation"][0]["loc"] == "/k"

    async def test_call_context_workspace_mismatch_returns_403(
        self, client: httpx.AsyncClient
    ) -> None:
        response = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json=_start_run_payload(),
            headers={
                WORKSPACE_HEADER: "other-workspace",
                PRINCIPAL_HEADER: "user-1",
            },
        )
        assert response.status_code == 403, response.text
        body = response.json()
        assert body["code"] == "workflow.validator.workspace_unauthorized"
        assert body["status"] == 403


# ---------------------------------------------------------------------------
# Run Controller failure modes
# ---------------------------------------------------------------------------


class TestRunControllerFailureModes:
    async def test_get_unknown_run_returns_404(self, client: httpx.AsyncClient) -> None:
        response = await client.get(f"/v1/workspaces/{WORKSPACE}/runs/no-such-run")
        assert response.status_code == 404, response.text
        body = response.json()
        assert body["code"] == "workflow.run_not_found"
        assert body["status"] == 404

    async def test_cancel_unknown_run_returns_404(self, client: httpx.AsyncClient) -> None:
        response = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs/no-such-run:cancel",
            json={"reason": "n/a"},
        )
        assert response.status_code == 404, response.text
        body = response.json()
        assert body["code"] == "workflow.run_not_found"

    async def test_runtime_refusal_surfaces_503(
        self,
        client: httpx.AsyncClient,
        harness: _AppHarness,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The Dapr runtime refusing a schedule maps to the 503 envelope."""

        async def _refuse(*_args: object, **_kwargs: object) -> str:
            raise WorkflowRuntimeUnavailableError("fake runtime refused schedule")

        monkeypatch.setattr(
            harness.components.workflow_client,
            "schedule_new_workflow",
            _refuse,
        )
        response = await client.post(
            f"/v1/workspaces/{WORKSPACE}/runs",
            json={**_start_run_payload(), "idempotencyKey": "503-key"},
        )
        assert response.status_code == 503, response.text
        body = response.json()
        assert body["code"] == "workflow.workflow_runtime_unavailable"
        assert body["status"] == 503
