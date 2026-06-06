"""Tests for the downstream router + response shaper (AGW-IMPL-012)."""

from __future__ import annotations

import httpx
import pytest

from custos_gateway.errors import GatewayError, GatewayErrorCode
from custos_gateway.router import (
    HOP_BY_HOP_HEADERS,
    DownstreamCall,
    DownstreamResponse,
    DownstreamRouter,
    is_transient_status,
    shape_response_headers,
)


def _router_with(handler: httpx.MockTransport) -> DownstreamRouter:
    return DownstreamRouter(
        http_client=httpx.AsyncClient(transport=handler),
        host="127.0.0.1",
        http_port=3500,
    )


# --- is_transient_status -----------------------------------------------------


@pytest.mark.parametrize("status_code", [500, 502, 503, 504, 599])
def test_5xx_is_transient(status_code: int) -> None:
    assert is_transient_status(status_code) is True


@pytest.mark.parametrize("status_code", [200, 201, 204, 301, 400, 404, 409, 422, 499])
def test_non_5xx_is_not_transient(status_code: int) -> None:
    assert is_transient_status(status_code) is False


# --- shape_response_headers --------------------------------------------------


def test_shape_drops_hop_by_hop_headers() -> None:
    raw = httpx.Headers(
        [
            ("content-type", "application/json"),
            ("content-length", "12"),
            ("connection", "keep-alive"),
            ("transfer-encoding", "chunked"),
            ("x-custom", "value"),
        ]
    )
    shaped = shape_response_headers(raw)
    names = {name.lower() for name, _ in shaped}
    assert "content-type" in names
    assert "x-custom" in names
    assert names.isdisjoint(HOP_BY_HOP_HEADERS)


def test_shape_preserves_content_encoding() -> None:
    raw = httpx.Headers([("content-encoding", "gzip"), ("content-type", "application/json")])
    shaped = shape_response_headers(raw)
    assert ("content-encoding", "gzip") in shaped


# --- invoke: raw pass-through ------------------------------------------------


async def test_invoke_passes_2xx_through_raw() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["method"] = request.method
        captured["body"] = request.content
        captured["callctx"] = request.headers.get("x-custos-callctx")
        captured["correlation"] = request.headers.get("x-correlation-id")
        return httpx.Response(
            201,
            headers={"content-type": "application/json", "location": "/runs/abc"},
            content=b'{"id":"abc"}',
        )

    router = _router_with(httpx.MockTransport(handler))
    call = DownstreamCall(
        app_id="custos-workflow",
        http_method="POST",
        method_path="v1/workspaces/ws1/runs",
        headers={
            "x-custos-callctx": "signed-token",
            "x-correlation-id": "corr-1",
            "content-type": "application/json",
        },
        body=b'{"workflow":"wf1"}',
    )

    result = await router.invoke(call)

    assert isinstance(result, DownstreamResponse)
    assert result.status_code == 201
    assert result.body == b'{"id":"abc"}'
    assert ("location", "/runs/abc") in result.headers
    assert (
        captured["url"]
        == "http://127.0.0.1:3500/v1.0/invoke/custos-workflow/method/v1/workspaces/ws1/runs"
    )
    assert captured["method"] == "POST"
    assert captured["body"] == b'{"workflow":"wf1"}'
    assert captured["callctx"] == "signed-token"
    assert captured["correlation"] == "corr-1"


async def test_invoke_passes_downstream_4xx_through_raw() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            409,
            headers={"content-type": "application/problem+json"},
            content=b'{"code":"conflict"}',
        )

    router = _router_with(httpx.MockTransport(handler))
    call = DownstreamCall(
        app_id="custos-catalog",
        http_method="POST",
        method_path="v1/workspaces/ws1/workflows",
        body=b"{}",
    )

    result = await router.invoke(call)

    assert result.status_code == 409
    assert result.body == b'{"code":"conflict"}'


async def test_invoke_forwards_get_without_body() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.content == b""
        return httpx.Response(200, content=b"[]")

    router = _router_with(httpx.MockTransport(handler))
    call = DownstreamCall(
        app_id="custos-observability",
        http_method="GET",
        method_path="v1/workspaces/ws1/audit",
    )

    result = await router.invoke(call)

    assert result.status_code == 200
    assert result.body == b"[]"


# --- invoke: transient failures → 503 ---------------------------------------


@pytest.mark.parametrize("status_code", [500, 502, 503, 504])
async def test_invoke_maps_downstream_5xx_to_503(status_code: int) -> None:
    router = _router_with(httpx.MockTransport(lambda r: httpx.Response(status_code, text="boom")))
    call = DownstreamCall(
        app_id="custos-workflow",
        http_method="POST",
        method_path="v1/workspaces/ws1/runs",
        body=b"{}",
    )

    with pytest.raises(GatewayError) as excinfo:
        await router.invoke(call)

    error = excinfo.value
    assert error.code is GatewayErrorCode.DOWNSTREAM_UNAVAILABLE
    assert error.status == 503


async def test_invoke_maps_transport_error_to_503() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("sidecar down")

    router = _router_with(httpx.MockTransport(handler))
    call = DownstreamCall(
        app_id="custos-trigger",
        http_method="POST",
        method_path="v1/workspaces/ws1/triggers",
        body=b"{}",
    )

    with pytest.raises(GatewayError) as excinfo:
        await router.invoke(call)

    error = excinfo.value
    assert error.code is GatewayErrorCode.DOWNSTREAM_UNAVAILABLE
    assert error.status == 503


# --- router defaults ---------------------------------------------------------


def test_router_defaults_to_local_sidecar() -> None:
    router = DownstreamRouter(http_client=httpx.AsyncClient())
    assert router.host == "127.0.0.1"
    assert router.http_port == 3500
