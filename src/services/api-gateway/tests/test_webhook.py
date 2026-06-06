"""Tests for the anonymous webhook pass-through (AGW-IMPL-014)."""

from __future__ import annotations

import uuid

import httpx
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from custos_gateway.errors import register_exception_handlers
from custos_gateway.middleware.auth import WEBHOOK_BYPASS_PREFIX
from custos_gateway.router import DownstreamRouter
from custos_gateway.routes.webhook import (
    FORWARDED_FOR_HEADER,
    STRIPPED_INBOUND_HEADERS,
    WEBHOOK_BODY_MAX_BYTES,
    WEBHOOK_PATH,
    build_webhook_router,
    forward_headers,
)

# --- router shape ------------------------------------------------------------


def test_webhook_router_mounts_single_anonymous_post_route() -> None:
    router = build_webhook_router()
    routes = [route for route in router.routes]
    assert len(routes) == 1
    route = routes[0]
    assert route.path == WEBHOOK_PATH  # type: ignore[attr-defined]
    assert route.methods == {"POST"}  # type: ignore[attr-defined]
    # Anonymous: no require_permission (or any) dependency is attached.
    assert route.dependencies == []  # type: ignore[attr-defined]
    # The path is under the auth-bypass prefix so authentication is skipped.
    assert WEBHOOK_PATH.startswith(WEBHOOK_BYPASS_PREFIX)


# --- forward_headers unit ----------------------------------------------------


def _request_with_headers(headers: list[tuple[str, str]]) -> Request:
    raw = [(name.lower().encode("latin-1"), value.encode("latin-1")) for name, value in headers]
    return Request({"type": "http", "headers": raw})


def test_forward_headers_strips_authorization_and_host() -> None:
    request = _request_with_headers(
        [
            ("Authorization", "Bearer secret"),
            ("Host", "gateway.example.com"),
            ("X-Custos-Callctx", "smuggled-context"),
            ("X-Hub-Signature-256", "sha256=abc"),
            ("Content-Type", "application/json"),
        ]
    )
    headers = forward_headers(request, correlation_id="cid-1", source_ip="9.9.9.9")
    assert "authorization" not in {name.lower() for name in headers}
    assert "host" not in {name.lower() for name in headers}
    # A caller-supplied call context is never forwarded on this anonymous hop:
    # forwarding one would let a caller smuggle/replay an authenticated context.
    assert "x-custos-callctx" not in {name.lower() for name in headers}
    # The signature header (verified downstream) is forwarded untouched.
    assert headers["x-hub-signature-256"] == "sha256=abc"
    assert headers["content-type"] == "application/json"
    assert headers[FORWARDED_FOR_HEADER] == "9.9.9.9"
    assert headers["x-correlation-id"] == "cid-1"


def test_forward_headers_appends_to_existing_forwarded_for() -> None:
    request = _request_with_headers([(FORWARDED_FOR_HEADER, "1.2.3.4")])
    headers = forward_headers(request, correlation_id="cid-1", source_ip="5.6.7.8")
    assert headers[FORWARDED_FOR_HEADER] == "1.2.3.4, 5.6.7.8"


def test_forward_headers_omits_forwarded_for_without_source_ip() -> None:
    request = _request_with_headers([("X-Custom", "v")])
    headers = forward_headers(request, correlation_id="cid-1", source_ip=None)
    assert FORWARDED_FOR_HEADER not in {name.lower() for name in headers}


def test_authorization_and_host_are_in_the_strip_set() -> None:
    assert {"authorization", "host", "x-custos-callctx"} <= STRIPPED_INBOUND_HEADERS


# --- forwarding seam ---------------------------------------------------------


def _router_with(handler: httpx.MockTransport) -> DownstreamRouter:
    return DownstreamRouter(
        http_client=httpx.AsyncClient(transport=handler),
        host="127.0.0.1",
        http_port=3500,
    )


def _app_with(handler: httpx.MockTransport, *, bind_router: bool = True) -> FastAPI:
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(build_webhook_router())
    if bind_router:
        app.state.downstream_router = _router_with(handler)
    return app


def test_webhook_forwards_body_and_headers_minus_authorization() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        captured["method"] = request.method
        captured["body"] = request.content
        captured["authorization"] = request.headers.get("authorization")
        captured["callctx"] = request.headers.get("x-custos-callctx")
        captured["forwarded_for"] = request.headers.get(FORWARDED_FOR_HEADER)
        captured["correlation_id"] = request.headers.get("x-correlation-id")
        captured["signature"] = request.headers.get("x-hub-signature-256")
        return httpx.Response(202, headers={"x-downstream": "yes"}, content=b"accepted")

    client = TestClient(_app_with(httpx.MockTransport(handler)))
    response = client.post(
        "/v1/webhooks/ci-1",
        content=b'{"event":"push"}',
        headers={
            "content-type": "application/json",
            "authorization": "Bearer leaked",
            "x-custos-callctx": "smuggled-context",
            "x-hub-signature-256": "sha256=abc",
        },
    )

    assert response.status_code == 202
    assert response.content == b"accepted"
    assert response.headers["x-downstream"] == "yes"
    assert captured["method"] == "POST"
    assert captured["body"] == b'{"event":"push"}'
    # Anonymous hop: the caller's bearer is dropped, no call context is minted
    # and a caller-supplied call context is never forwarded.
    assert captured["authorization"] is None
    assert captured["callctx"] is None
    # Signature header (verified downstream) survives; correlation id propagated.
    assert captured["signature"] == "sha256=abc"
    assert captured["correlation_id"]
    # Source IP is carried to the downstream.
    assert captured["forwarded_for"] is not None
    assert "testclient" in str(captured["forwarded_for"])
    assert str(captured["url"]).endswith("/v1.0/invoke/trigger-service/method/v1/webhooks/ci-1")


def test_webhook_carries_query_string() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["url"] = str(request.url)
        return httpx.Response(202, content=b"")

    client = TestClient(_app_with(httpx.MockTransport(handler)))
    response = client.post("/v1/webhooks/ci-1", params={"token": "t1"})

    assert response.status_code == 202
    assert str(captured["url"]).endswith(
        "/v1.0/invoke/trigger-service/method/v1/webhooks/ci-1?token=t1"
    )


def test_webhook_propagates_inbound_correlation_id() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["correlation_id"] = request.headers.get("x-correlation-id")
        return httpx.Response(202, content=b"")

    app = _app_with(httpx.MockTransport(handler))

    @app.middleware("http")
    async def _bind_correlation_id(request: Request, call_next):  # type: ignore[no-untyped-def]
        request.state.correlation_id = "corr-from-ingress"
        return await call_next(request)

    client = TestClient(app)
    response = client.post("/v1/webhooks/ci-1", content=b"{}")

    assert response.status_code == 202
    assert captured["correlation_id"] == "corr-from-ingress"


def test_webhook_generates_correlation_id_when_absent() -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["correlation_id"] = request.headers.get("x-correlation-id")
        return httpx.Response(202, content=b"")

    client = TestClient(_app_with(httpx.MockTransport(handler)))
    response = client.post("/v1/webhooks/ci-1", content=b"{}")

    assert response.status_code == 202
    # A fresh, well-formed correlation id is minted for the anonymous hop.
    assert uuid.UUID(str(captured["correlation_id"]))


def test_webhook_rejects_body_over_cap() -> None:
    calls: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        calls.append(request)
        return httpx.Response(202, content=b"")

    client = TestClient(_app_with(httpx.MockTransport(handler)))
    response = client.post(
        "/v1/webhooks/ci-1",
        content=b"x" * (WEBHOOK_BODY_MAX_BYTES + 1),
    )

    assert response.status_code == 413
    assert response.json()["code"] == "body-too-large"
    # The oversize body is rejected at the gateway; the downstream is never hit.
    assert calls == []


def test_webhook_passes_through_downstream_404_raw() -> None:
    body = b'{"code":"webhook-route-not-found"}'

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            404,
            headers={"content-type": "application/problem+json"},
            content=body,
        )

    client = TestClient(_app_with(httpx.MockTransport(handler)))
    response = client.post("/v1/webhooks/unknown", content=b"{}")

    # An unknown connector instance is the downstream's call; the 404 passes
    # back through the response shaper unchanged.
    assert response.status_code == 404
    assert response.content == body


def test_webhook_returns_503_when_router_unbound() -> None:
    def handler(request: httpx.Request) -> httpx.Response:  # pragma: no cover - unused
        return httpx.Response(202, content=b"")

    client = TestClient(_app_with(httpx.MockTransport(handler), bind_router=False))
    response = client.post("/v1/webhooks/ci-1", content=b"{}")

    assert response.status_code == 503
    assert response.json()["code"] == "downstream-unavailable"
