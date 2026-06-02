"""Tests for ``DaprTriggerServiceClient`` (WF-IMPL-103).

The adapter is the Resume Subscription Manager's only path to the
Trigger Service in production, so these tests cover the full
transport-error -> exception matrix locked in WF-IMPL-075 plus the
camelCase wire contract pinned in ``design.md`` § *Operation: Step
Resume on External Event*.

Coverage emphasis:

* Happy-path register round-trips the canonical wire envelope and
  parses the ``subscriptionId`` into a
  :class:`RegisterResumeSubscriptionResponse`.
* ``selector=None`` serializes to JSON ``null``.
* Cancel posts the idempotency triple and returns ``None``; HTTP
  404 / 409 are idempotent no-ops.
* Status-code matrix raises :class:`OutboundRpcStatusError` with
  the observed ``status_code``; 499 raises
  :class:`OutboundRpcCancelledError`; transport failures raise
  :class:`OutboundRpcTransportError`; malformed register responses
  raise :class:`OutboundRpcDecodeError`.
"""

from __future__ import annotations

import json as _json
from collections.abc import Callable

import httpx
import pytest

from custos_workflow.clients._dapr_invoke import (
    DEFAULT_OUTBOUND_RPC_TIMEOUT_SECONDS,
    DaprInvokeEndpoint,
    build_invoke_url,
)
from custos_workflow.clients._errors import (
    OutboundRpcCancelledError,
    OutboundRpcDecodeError,
    OutboundRpcStatusError,
    OutboundRpcTransportError,
)
from custos_workflow.clients.trigger import (
    CANCEL_RESUME_SUBSCRIPTION_DAPR_METHOD,
    REGISTER_RESUME_SUBSCRIPTION_DAPR_METHOD,
    CancelResumeSubscriptionRequest,
    DaprTriggerServiceClient,
    RegisterResumeSubscriptionRequest,
    RegisterResumeSubscriptionResponse,
    _cancel_request_to_wire,
    _parse_register_response,
    _register_request_to_wire,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def endpoint() -> DaprInvokeEndpoint:
    return DaprInvokeEndpoint(host="127.0.0.1", http_port=3500, app_id="trigger-service")


@pytest.fixture
def register_url(endpoint: DaprInvokeEndpoint) -> str:
    return build_invoke_url(endpoint, REGISTER_RESUME_SUBSCRIPTION_DAPR_METHOD)


@pytest.fixture
def cancel_url(endpoint: DaprInvokeEndpoint) -> str:
    return build_invoke_url(endpoint, CANCEL_RESUME_SUBSCRIPTION_DAPR_METHOD)


@pytest.fixture
def register_request() -> RegisterResumeSubscriptionRequest:
    return RegisterResumeSubscriptionRequest(
        run_id="run-1",
        step_id="step-a",
        event_key="evt-1",
        ttl="PT24H",
        selector="$.payload.id",
    )


@pytest.fixture
def cancel_request() -> CancelResumeSubscriptionRequest:
    return CancelResumeSubscriptionRequest(run_id="run-1", step_id="step-a", event_key="evt-1")


def _make_client(
    endpoint: DaprInvokeEndpoint,
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    timeout: float = DEFAULT_OUTBOUND_RPC_TIMEOUT_SECONDS,
) -> DaprTriggerServiceClient:
    transport = httpx.MockTransport(handler)
    return DaprTriggerServiceClient(
        http_client=httpx.AsyncClient(transport=transport),
        endpoint=endpoint,
        timeout=timeout,
    )


async def _drive_register(
    client: DaprTriggerServiceClient, request: RegisterResumeSubscriptionRequest
) -> RegisterResumeSubscriptionResponse:
    try:
        return await client.register_resume_subscription(request)
    finally:
        await client.http_client.aclose()


async def _drive_cancel(
    client: DaprTriggerServiceClient, request: CancelResumeSubscriptionRequest
) -> None:
    try:
        await client.cancel_resume_subscription(request)
    finally:
        await client.http_client.aclose()


# ---------------------------------------------------------------------------
# Wire helpers
# ---------------------------------------------------------------------------


def test_register_request_to_wire_uses_camelcase(
    register_request: RegisterResumeSubscriptionRequest,
) -> None:
    assert _register_request_to_wire(register_request) == {
        "runId": "run-1",
        "stepId": "step-a",
        "eventKey": "evt-1",
        "selector": "$.payload.id",
        "ttl": "PT24H",
    }


def test_register_request_to_wire_emits_null_selector() -> None:
    request = RegisterResumeSubscriptionRequest(
        run_id="run-1", step_id="step-a", event_key="evt-1", ttl="PT24H"
    )
    assert _register_request_to_wire(request)["selector"] is None


def test_cancel_request_to_wire_uses_camelcase(
    cancel_request: CancelResumeSubscriptionRequest,
) -> None:
    assert _cancel_request_to_wire(cancel_request) == {
        "runId": "run-1",
        "stepId": "step-a",
        "eventKey": "evt-1",
    }


def test_parse_register_response_happy_path() -> None:
    resp = _parse_register_response({"subscriptionId": "ts-sub-42"})
    assert resp == RegisterResumeSubscriptionResponse(ts_subscription_id="ts-sub-42")


@pytest.mark.parametrize(
    ("body", "match"),
    [
        (["not", "a", "mapping"], "must be a JSON object"),
        ({"other": "field"}, "missing the required 'subscriptionId' field"),
        ({"subscriptionId": 123}, "must be a string"),
        ({"subscriptionId": ""}, "failed RegisterResumeSubscriptionResponse invariants"),
    ],
)
def test_parse_register_response_rejects_bad_bodies(body: object, match: str) -> None:
    with pytest.raises(OutboundRpcDecodeError, match=match):
        _parse_register_response(body)


# ---------------------------------------------------------------------------
# register_resume_subscription — happy path + wire contract
# ---------------------------------------------------------------------------


async def test_register_round_trips_wire_envelope(
    endpoint: DaprInvokeEndpoint,
    register_url: str,
    register_request: RegisterResumeSubscriptionRequest,
) -> None:
    captured: dict[str, object] = {}

    def handler(req: httpx.Request) -> httpx.Response:
        captured["url"] = str(req.url)
        captured["method"] = req.method
        captured["content_type"] = req.headers.get("Content-Type")
        captured["body"] = _json.loads(req.content)
        return httpx.Response(200, json={"subscriptionId": "ts-sub-7"})

    client = _make_client(endpoint, handler)
    resp = await _drive_register(client, register_request)

    assert resp.ts_subscription_id == "ts-sub-7"
    assert captured["url"] == register_url
    assert captured["method"] == "POST"
    assert captured["content_type"] == "application/json"
    assert captured["body"] == {
        "runId": "run-1",
        "stepId": "step-a",
        "eventKey": "evt-1",
        "selector": "$.payload.id",
        "ttl": "PT24H",
    }


async def test_register_serializes_null_selector(endpoint: DaprInvokeEndpoint) -> None:
    captured: dict[str, object] = {}

    def handler(req: httpx.Request) -> httpx.Response:
        captured["body"] = _json.loads(req.content)
        return httpx.Response(200, json={"subscriptionId": "ts-sub-1"})

    request = RegisterResumeSubscriptionRequest(
        run_id="run-1", step_id="step-a", event_key="evt-1", ttl="PT24H"
    )
    client = _make_client(endpoint, handler)
    await _drive_register(client, request)
    assert captured["body"] == {
        "runId": "run-1",
        "stepId": "step-a",
        "eventKey": "evt-1",
        "selector": None,
        "ttl": "PT24H",
    }


# ---------------------------------------------------------------------------
# register_resume_subscription — error matrix
# ---------------------------------------------------------------------------


async def test_register_transport_failure_raises_transport_error(
    endpoint: DaprInvokeEndpoint, register_request: RegisterResumeSubscriptionRequest
) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("sidecar down")

    client = _make_client(endpoint, handler)
    with pytest.raises(OutboundRpcTransportError, match="transport failure"):
        await _drive_register(client, register_request)


async def test_register_499_raises_cancelled(
    endpoint: DaprInvokeEndpoint, register_request: RegisterResumeSubscriptionRequest
) -> None:
    client = _make_client(endpoint, lambda req: httpx.Response(499))
    with pytest.raises(OutboundRpcCancelledError, match="cancelled upstream"):
        await _drive_register(client, register_request)


@pytest.mark.parametrize("status_code", [400, 404, 429, 500, 503])
async def test_register_non_2xx_raises_status_error(
    endpoint: DaprInvokeEndpoint,
    register_request: RegisterResumeSubscriptionRequest,
    status_code: int,
) -> None:
    client = _make_client(endpoint, lambda req: httpx.Response(status_code, text="boom"))
    with pytest.raises(OutboundRpcStatusError) as excinfo:
        await _drive_register(client, register_request)
    assert excinfo.value.status_code == status_code


async def test_register_invalid_json_raises_decode_error(
    endpoint: DaprInvokeEndpoint, register_request: RegisterResumeSubscriptionRequest
) -> None:
    client = _make_client(endpoint, lambda req: httpx.Response(200, text="not json"))
    with pytest.raises(OutboundRpcDecodeError, match="not valid JSON"):
        await _drive_register(client, register_request)


async def test_register_bad_body_raises_decode_error(
    endpoint: DaprInvokeEndpoint, register_request: RegisterResumeSubscriptionRequest
) -> None:
    client = _make_client(endpoint, lambda req: httpx.Response(200, json={"wrong": "shape"}))
    with pytest.raises(OutboundRpcDecodeError, match="missing the required 'subscriptionId'"):
        await _drive_register(client, register_request)


# ---------------------------------------------------------------------------
# cancel_resume_subscription — happy path + idempotency
# ---------------------------------------------------------------------------


async def test_cancel_posts_idempotency_triple(
    endpoint: DaprInvokeEndpoint,
    cancel_url: str,
    cancel_request: CancelResumeSubscriptionRequest,
) -> None:
    captured: dict[str, object] = {}

    def handler(req: httpx.Request) -> httpx.Response:
        captured["url"] = str(req.url)
        captured["body"] = _json.loads(req.content)
        return httpx.Response(200)

    client = _make_client(endpoint, handler)
    await _drive_cancel(client, cancel_request)

    assert captured["url"] == cancel_url
    assert captured["body"] == {"runId": "run-1", "stepId": "step-a", "eventKey": "evt-1"}


@pytest.mark.parametrize("status_code", [404, 409])
async def test_cancel_unknown_key_is_noop(
    endpoint: DaprInvokeEndpoint,
    cancel_request: CancelResumeSubscriptionRequest,
    status_code: int,
) -> None:
    client = _make_client(endpoint, lambda req: httpx.Response(status_code))
    # Idempotent: a 404 / 409 for an already-gone key returns cleanly.
    await _drive_cancel(client, cancel_request)


async def test_cancel_transport_failure_raises_transport_error(
    endpoint: DaprInvokeEndpoint, cancel_request: CancelResumeSubscriptionRequest
) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow")

    client = _make_client(endpoint, handler)
    with pytest.raises(OutboundRpcTransportError, match="transport failure"):
        await _drive_cancel(client, cancel_request)


async def test_cancel_499_raises_cancelled(
    endpoint: DaprInvokeEndpoint, cancel_request: CancelResumeSubscriptionRequest
) -> None:
    client = _make_client(endpoint, lambda req: httpx.Response(499))
    with pytest.raises(OutboundRpcCancelledError, match="cancelled upstream"):
        await _drive_cancel(client, cancel_request)


@pytest.mark.parametrize("status_code", [400, 500, 503])
async def test_cancel_non_2xx_raises_status_error(
    endpoint: DaprInvokeEndpoint,
    cancel_request: CancelResumeSubscriptionRequest,
    status_code: int,
) -> None:
    client = _make_client(endpoint, lambda req: httpx.Response(status_code, text="boom"))
    with pytest.raises(OutboundRpcStatusError) as excinfo:
        await _drive_cancel(client, cancel_request)
    assert excinfo.value.status_code == status_code


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_dapr_client_satisfies_protocol(endpoint: DaprInvokeEndpoint) -> None:
    from custos_workflow.clients.trigger import TriggerServiceClient

    client = _make_client(endpoint, lambda req: httpx.Response(200))
    assert isinstance(client, TriggerServiceClient)
