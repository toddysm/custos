"""WorkflowServiceClient (Dapr adapter) tests (TS-IMPL-013)."""

from __future__ import annotations

import httpx
import pytest

from custos_trigger.clients.workflow import (
    DEFAULT_DAPR_HTTP_HOST,
    DEFAULT_DAPR_HTTP_PORT,
    DaprEndpoint,
    DaprWorkflowServiceClient,
    FakeWorkflowServiceClient,
    NoopWorkflowServiceClient,
    RaiseExternalEventRequest,
    RunRef,
    StartRunRequest,
    WorkflowClientDecodeError,
    WorkflowClientStatusError,
    WorkflowClientTransportError,
    WorkflowServiceClient,
    build_invoke_url,
    raise_event_method,
    read_dapr_endpoint,
)

pytestmark = pytest.mark.asyncio

_ENDPOINT = DaprEndpoint(host="127.0.0.1", http_port=3500, app_id="workflow-service")

_RUN_REF_BODY = {
    "runId": "run-1",
    "status": "queued",
    "workspaceId": "ws-1",
    "workflowVersionId": "wfv-1",
    "startedAt": "2026-06-04T12:00:00Z",
}


def _start_request() -> StartRunRequest:
    return StartRunRequest(
        workspace_id="ws-1",
        workflow_version_id="wfv-1",
        inputs={"a": "b"},
        idempotency_key="idem-1",
    )


def _raise_request() -> RaiseExternalEventRequest:
    return RaiseExternalEventRequest(
        workspace_id="ws-1",
        event_name="pr.merged",
        payload={"pr": 1},
    )


def _client(handler: httpx.MockTransport) -> DaprWorkflowServiceClient:
    return DaprWorkflowServiceClient(
        http_client=httpx.AsyncClient(transport=handler),
        endpoint=_ENDPOINT,
    )


# --- URL + endpoint helpers --------------------------------------------------


def test_build_invoke_url() -> None:
    assert build_invoke_url(_ENDPOINT, "internal/runs:start") == (
        "http://127.0.0.1:3500/v1.0/invoke/workflow-service/method/internal/runs:start"
    )


def test_build_invoke_url_strips_leading_slash() -> None:
    assert build_invoke_url(_ENDPOINT, "/internal/runs:start").endswith(
        "/method/internal/runs:start"
    )


def test_raise_event_method() -> None:
    assert raise_event_method("run-1", "step-1") == ("internal/runs/run-1/steps/step-1:raiseEvent")


def test_read_dapr_endpoint_defaults() -> None:
    endpoint = read_dapr_endpoint({}, app_id="workflow-service")
    assert endpoint == DaprEndpoint(
        host=DEFAULT_DAPR_HTTP_HOST, http_port=DEFAULT_DAPR_HTTP_PORT, app_id="workflow-service"
    )


def test_read_dapr_endpoint_overrides() -> None:
    endpoint = read_dapr_endpoint(
        {"DAPR_HTTP_HOST": "sidecar", "DAPR_HTTP_PORT": "3600"}, app_id="wf"
    )
    assert endpoint == DaprEndpoint(host="sidecar", http_port=3600, app_id="wf")


def test_read_dapr_endpoint_requires_app_id() -> None:
    with pytest.raises(ValueError, match="app_id is required"):
        read_dapr_endpoint({}, app_id="")


def test_read_dapr_endpoint_rejects_non_int_port() -> None:
    with pytest.raises(ValueError, match="DAPR_HTTP_PORT must be an integer"):
        read_dapr_endpoint({"DAPR_HTTP_PORT": "abc"}, app_id="wf")


# --- start_run ---------------------------------------------------------------


async def test_start_run_success() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = request.read()
        captured["idem"] = request.headers.get("Idempotency-Key")
        return httpx.Response(202, json=_RUN_REF_BODY)

    client = _client(httpx.MockTransport(handler))
    result = await client.start_run(_start_request())

    assert isinstance(result, RunRef)
    assert result.run_id == "run-1"
    assert result.workspace_id == "ws-1"
    assert captured["url"] == (
        "http://127.0.0.1:3500/v1.0/invoke/workflow-service/method/internal/runs:start"
    )
    assert b'"workflowVersionId":"wfv-1"' in captured["body"]  # type: ignore[operator]
    assert b'"idempotencyKey":"idem-1"' in captured["body"]  # type: ignore[operator]
    assert captured["idem"] == "idem-1"


async def test_start_run_omits_idempotency_header_when_absent() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["idem"] = request.headers.get("Idempotency-Key")
        return httpx.Response(202, json=_RUN_REF_BODY)

    client = _client(httpx.MockTransport(handler))
    await client.start_run(StartRunRequest(workspace_id="ws-1", workflow_version_id="wfv-1"))
    assert captured["idem"] is None


async def test_start_run_retryable_on_5xx() -> None:
    client = _client(httpx.MockTransport(lambda _r: httpx.Response(503, text="down")))
    with pytest.raises(WorkflowClientStatusError) as excinfo:
        await client.start_run(_start_request())
    assert excinfo.value.status_code == 503
    assert excinfo.value.retryable is True


async def test_start_run_retryable_on_429() -> None:
    client = _client(httpx.MockTransport(lambda _r: httpx.Response(429)))
    with pytest.raises(WorkflowClientStatusError) as excinfo:
        await client.start_run(_start_request())
    assert excinfo.value.retryable is True


async def test_start_run_permanent_on_4xx() -> None:
    client = _client(httpx.MockTransport(lambda _r: httpx.Response(400, text="bad")))
    with pytest.raises(WorkflowClientStatusError) as excinfo:
        await client.start_run(_start_request())
    assert excinfo.value.status_code == 400
    assert excinfo.value.retryable is False


async def test_start_run_transport_error_is_retryable() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("no route")

    client = _client(httpx.MockTransport(handler))
    with pytest.raises(WorkflowClientTransportError) as excinfo:
        await client.start_run(_start_request())
    assert excinfo.value.retryable is True


async def test_start_run_invalid_json_is_decode_error() -> None:
    client = _client(httpx.MockTransport(lambda _r: httpx.Response(202, text="not json")))
    with pytest.raises(WorkflowClientDecodeError):
        await client.start_run(_start_request())


async def test_start_run_wrong_shape_is_decode_error() -> None:
    client = _client(httpx.MockTransport(lambda _r: httpx.Response(202, json={"unexpected": 1})))
    with pytest.raises(WorkflowClientDecodeError):
        await client.start_run(_start_request())


# --- raise_external_event ----------------------------------------------------


async def test_raise_external_event_success() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["body"] = request.read()
        return httpx.Response(202)

    client = _client(httpx.MockTransport(handler))
    await client.raise_external_event("run-1", "step-1", _raise_request())

    assert captured["url"] == (
        "http://127.0.0.1:3500/v1.0/invoke/workflow-service/method/"
        "internal/runs/run-1/steps/step-1:raiseEvent"
    )
    assert b'"eventName":"pr.merged"' in captured["body"]  # type: ignore[operator]


async def test_raise_external_event_status_error() -> None:
    client = _client(httpx.MockTransport(lambda _r: httpx.Response(404, text="gone")))
    with pytest.raises(WorkflowClientStatusError) as excinfo:
        await client.raise_external_event("run-1", "step-1", _raise_request())
    assert excinfo.value.status_code == 404
    assert excinfo.value.retryable is False


# --- doubles -----------------------------------------------------------------


async def test_noop_client() -> None:
    client = NoopWorkflowServiceClient()
    assert isinstance(client, WorkflowServiceClient)
    ref = await client.start_run(_start_request())
    assert ref.status == "noop"
    assert ref.workspace_id == "ws-1"
    await client.raise_external_event("run-1", "step-1", _raise_request())


async def test_fake_client_records_and_returns_default() -> None:
    client = FakeWorkflowServiceClient()
    assert isinstance(client, WorkflowServiceClient)
    req = _start_request()
    ref = await client.start_run(req)
    assert ref.run_id == "run-fake"
    assert client.start_run_calls == [req]

    raise_req = _raise_request()
    await client.raise_external_event("run-1", "step-1", raise_req)
    assert client.raise_event_calls == [("run-1", "step-1", raise_req)]


async def test_fake_client_returns_configured_run_ref() -> None:
    ref = RunRef(
        run_id="run-custom", status="running", workspace_id="ws-1", workflow_version_id="wfv-1"
    )
    client = FakeWorkflowServiceClient(run_ref=ref)
    assert await client.start_run(_start_request()) is ref


async def test_fake_client_raises_configured_error() -> None:
    err = WorkflowClientStatusError("boom", status_code=503)
    client = FakeWorkflowServiceClient(error=err)
    with pytest.raises(WorkflowClientStatusError):
        await client.start_run(_start_request())
    with pytest.raises(WorkflowClientStatusError):
        await client.raise_external_event("run-1", "step-1", _raise_request())
    # The calls are still recorded before raising.
    assert len(client.start_run_calls) == 1
    assert len(client.raise_event_calls) == 1
