"""Tests for the correlation-id middleware (AGW-IMPL-003)."""

from __future__ import annotations

import uuid

from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from custos_gateway.errors import (
    CORRELATION_ID_HEADER,
    GatewayError,
    GatewayErrorCode,
    register_exception_handlers,
)
from custos_gateway.middleware import CorrelationIdMiddleware, new_correlation_id


def _app() -> FastAPI:
    app = FastAPI()
    app.add_middleware(CorrelationIdMiddleware)
    register_exception_handlers(app)

    @app.get("/echo")
    async def echo(request: Request) -> dict[str, str]:
        return {"correlationId": request.state.correlation_id}

    @app.get("/fail")
    async def fail(request: Request) -> dict[str, str]:  # pragma: no cover - raises
        raise GatewayError(GatewayErrorCode.DOWNSTREAM_UNAVAILABLE, detail="down")

    return app


def test_generates_uuid7_when_no_inbound_header() -> None:
    with TestClient(_app()) as client:
        response = client.get("/echo")
    assert response.status_code == 200
    header_id = response.headers[CORRELATION_ID_HEADER]
    # The body id (request.state) equals the response header id.
    assert response.json()["correlationId"] == header_id
    parsed = uuid.UUID(header_id)
    assert parsed.version == 7


def test_propagates_inbound_header_unchanged() -> None:
    inbound = "trace-from-upstream-mesh-001"
    with TestClient(_app()) as client:
        response = client.get("/echo", headers={CORRELATION_ID_HEADER: inbound})
    assert response.headers[CORRELATION_ID_HEADER] == inbound
    assert response.json()["correlationId"] == inbound


def test_blank_inbound_header_is_replaced_with_generated_id() -> None:
    with TestClient(_app()) as client:
        response = client.get("/echo", headers={CORRELATION_ID_HEADER: "   "})
    header_id = response.headers[CORRELATION_ID_HEADER]
    assert header_id.strip() != ""
    assert uuid.UUID(header_id).version == 7


def test_header_present_on_error_responses() -> None:
    with TestClient(_app(), raise_server_exceptions=False) as client:
        response = client.get("/fail", headers={CORRELATION_ID_HEADER: "err-trace-7"})
    assert response.status_code == 503
    assert response.headers[CORRELATION_ID_HEADER] == "err-trace-7"
    assert response.json()["correlationId"] == "err-trace-7"


def test_new_correlation_id_is_unique_and_time_ordered() -> None:
    ids = [new_correlation_id() for _ in range(50)]
    assert len(set(ids)) == len(ids)
    parsed = [uuid.UUID(value) for value in ids]
    for value in parsed:
        assert value.version == 7
    # The leading 48 bits are the Unix-millisecond timestamp; because the clock
    # only moves forward, that prefix is monotonically non-decreasing across the
    # batch (RFC 9562 § 5.7). Ids minted in the same millisecond carry random
    # low bits, so only the timestamp prefix is guaranteed ordered.
    timestamps = [value.int >> 80 for value in parsed]
    assert timestamps == sorted(timestamps)


def test_middleware_ignores_non_http_scope() -> None:
    # Entering and exiting the TestClient context drives a `lifespan` scope
    # through the middleware, exercising the non-http passthrough branch.
    with TestClient(_app()) as client:
        assert client.get("/echo").status_code == 200
