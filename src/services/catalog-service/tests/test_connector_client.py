"""Unit tests for :mod:`custos_catalog.clients.connector`.

Covers the wire-level contract of :class:`HttpConnectorClient` against
an in-process ``httpx.MockTransport`` (no event-loop network sockets):

* 200 → ``True``.
* 400 → ``True`` + an INFO log line (config drift; existence satisfied).
* 404 → ``False`` + populates the negative cache.
* 5xx → :class:`ConnectorServiceUnavailable` carrying the status code.
* :class:`httpx.TimeoutException` / :class:`httpx.TransportError`
  → :class:`ConnectorServiceUnavailable`.
* 401 / 403 → :class:`ConnectorServiceUnavailable` (catalog mis-wired).
* Negative-result cache short-circuits a repeat 404 (no second wire call).
* TTL=0 disables caching.
* ``x-custos-callctx`` header is forwarded verbatim.
* ``build_connector_client_factory(use_stub=True)`` returns the offline
  stub and logs a WARNING.

These tests are intentionally hermetic — they construct
:class:`ConnectorClientFactory` directly with a custom transport and do
not exercise the FastAPI lifespan. End-to-end wiring through
``create_app`` is covered by ``tests/integration/test_connector_wire.py``.
"""

from __future__ import annotations

import logging
from collections.abc import Callable

import httpx
import pytest

from custos_catalog.clients.connector import (
    CALLCTX_HEADER,
    ConnectorClient,
    ConnectorClientFactory,
    ConnectorServiceUnavailable,
    StubConnectorClient,
    build_connector_client_factory,
    request_callctx_header,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _factory(
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    ttl_seconds: float = 5.0,
    endpoint: str = "http://connector-service.test",
    timeout_seconds: float = 2.0,
) -> ConnectorClientFactory:
    """Build a factory pointed at a ``MockTransport``-backed handler."""
    return ConnectorClientFactory(
        endpoint=endpoint,
        timeout_seconds=timeout_seconds,
        negative_cache_ttl_seconds=ttl_seconds,
        transport=httpx.MockTransport(handler),
    )


# ---------------------------------------------------------------------------
# Status-code → bool / exception mapping
# ---------------------------------------------------------------------------


async def test_exists_returns_true_on_200() -> None:
    received: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        received.append(request)
        return httpx.Response(200, json={"ok": True})

    factory = _factory(handler)
    try:
        client = factory.for_request(callctx_header_value="ctx-1")
        assert await client.exists_connector_instance("ws-1", "name-a") is True
        assert len(received) == 1
        # Wire contract: POST /internal/v1/connectors:validate with
        # mode=instance + connectorInstanceId in body.
        assert received[0].method == "POST"
        assert received[0].url.path == "/internal/v1/connectors:validate"
    finally:
        await factory.aclose()


async def test_exists_returns_true_on_400_and_logs_info(
    caplog: pytest.LogCaptureFixture,
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            400,
            json={"code": "connector.instance_config_drift", "detail": "schema drift"},
        )

    factory = _factory(handler)
    try:
        client = factory.for_request(callctx_header_value="ctx-1")
        with caplog.at_level(logging.INFO, logger="custos_catalog.clients.connector"):
            assert await client.exists_connector_instance("ws-1", "name-a") is True
        info_messages = [
            r.getMessage()
            for r in caplog.records
            if r.levelno == logging.INFO and r.name == "custos_catalog.clients.connector"
        ]
        assert any("config drift" in msg or "treating as exists" in msg for msg in info_messages)
    finally:
        await factory.aclose()


async def test_exists_returns_false_on_404() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"code": "connector.instance_not_found"})

    factory = _factory(handler)
    try:
        client = factory.for_request(callctx_header_value="ctx-1")
        assert await client.exists_connector_instance("ws-1", "missing") is False
    finally:
        await factory.aclose()


@pytest.mark.parametrize("status", [500, 502, 503, 504])
async def test_exists_raises_unavailable_on_5xx(status: int) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, text="upstream sad")

    factory = _factory(handler)
    try:
        client = factory.for_request(callctx_header_value="ctx-1")
        with pytest.raises(ConnectorServiceUnavailable) as exc:
            await client.exists_connector_instance("ws-1", "name-a")
        assert exc.value.code == "catalog.dependency_unavailable"
        assert exc.value.status_code == status
    finally:
        await factory.aclose()


@pytest.mark.parametrize("status", [401, 403])
async def test_exists_raises_unavailable_on_unexpected_4xx(status: int) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status, text="nope")

    factory = _factory(handler)
    try:
        client = factory.for_request(callctx_header_value="ctx-1")
        with pytest.raises(ConnectorServiceUnavailable) as exc:
            await client.exists_connector_instance("ws-1", "name-a")
        assert exc.value.status_code == status
    finally:
        await factory.aclose()


async def test_exists_raises_unavailable_on_timeout() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("slow upstream", request=request)

    factory = _factory(handler)
    try:
        client = factory.for_request(callctx_header_value="ctx-1")
        with pytest.raises(ConnectorServiceUnavailable) as exc:
            await client.exists_connector_instance("ws-1", "name-a")
        assert "timed out" in str(exc.value)
    finally:
        await factory.aclose()


async def test_exists_raises_unavailable_on_transport_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    factory = _factory(handler)
    try:
        client = factory.for_request(callctx_header_value="ctx-1")
        with pytest.raises(ConnectorServiceUnavailable) as exc:
            await client.exists_connector_instance("ws-1", "name-a")
        assert "unreachable" in str(exc.value)
    finally:
        await factory.aclose()


# ---------------------------------------------------------------------------
# Negative-result cache
# ---------------------------------------------------------------------------


async def test_negative_cache_short_circuits_repeat_404() -> None:
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(404, json={"code": "connector.instance_not_found"})

    factory = _factory(handler, ttl_seconds=60.0)
    try:
        client = factory.for_request(callctx_header_value="ctx-1")
        assert await client.exists_connector_instance("ws-1", "missing") is False
        # Second call is served from the cache; no second wire request.
        assert await client.exists_connector_instance("ws-1", "missing") is False
        assert call_count == 1
        # Different name bypasses the cache.
        assert await client.exists_connector_instance("ws-1", "other") is False
        assert call_count == 2
    finally:
        await factory.aclose()


async def test_negative_cache_disabled_when_ttl_zero() -> None:
    call_count = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal call_count
        call_count += 1
        return httpx.Response(404, json={"code": "connector.instance_not_found"})

    factory = _factory(handler, ttl_seconds=0.0)
    try:
        client = factory.for_request(callctx_header_value="ctx-1")
        assert await client.exists_connector_instance("ws-1", "missing") is False
        assert await client.exists_connector_instance("ws-1", "missing") is False
        assert call_count == 2
    finally:
        await factory.aclose()


# ---------------------------------------------------------------------------
# Call-context header forwarding
# ---------------------------------------------------------------------------


async def test_forwards_callctx_header_when_present() -> None:
    received_headers: list[httpx.Headers] = []

    def handler(request: httpx.Request) -> httpx.Response:
        received_headers.append(request.headers)
        return httpx.Response(200, json={"ok": True})

    factory = _factory(handler)
    try:
        client = factory.for_request(callctx_header_value="opaque-token-xyz")
        await client.exists_connector_instance("ws-1", "name-a")
        assert received_headers[0].get(CALLCTX_HEADER) == "opaque-token-xyz"
    finally:
        await factory.aclose()


async def test_omits_callctx_header_when_empty() -> None:
    received_headers: list[httpx.Headers] = []

    def handler(request: httpx.Request) -> httpx.Response:
        received_headers.append(request.headers)
        return httpx.Response(200, json={"ok": True})

    factory = _factory(handler)
    try:
        client = factory.for_request(callctx_header_value="")
        await client.exists_connector_instance("ws-1", "name-a")
        assert CALLCTX_HEADER not in received_headers[0]
    finally:
        await factory.aclose()


# ---------------------------------------------------------------------------
# request_callctx_header helper
# ---------------------------------------------------------------------------


def test_request_callctx_header_returns_empty_when_absent() -> None:
    assert request_callctx_header({}) == ""


def test_request_callctx_header_passes_through_when_present() -> None:
    assert request_callctx_header({CALLCTX_HEADER: "abc"}) == "abc"


# ---------------------------------------------------------------------------
# Factory factory
# ---------------------------------------------------------------------------


def test_build_factory_returns_stub_when_flag_enabled(
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.WARNING, logger="custos_catalog.clients.connector"):
        client = build_connector_client_factory(
            endpoint="http://connector-service.test",
            timeout_seconds=2.0,
            negative_cache_ttl_seconds=5.0,
            use_stub=True,
        )
    assert isinstance(client, StubConnectorClient)
    warnings = [
        r.getMessage()
        for r in caplog.records
        if r.levelno == logging.WARNING and r.name == "custos_catalog.clients.connector"
    ]
    assert any("StubConnectorClient" in msg or "CAT_USE_STUB" in msg for msg in warnings)


def test_build_factory_returns_live_factory_by_default() -> None:
    factory = build_connector_client_factory(
        endpoint="http://connector-service.test",
        timeout_seconds=2.0,
        negative_cache_ttl_seconds=5.0,
        use_stub=False,
    )
    assert isinstance(factory, ConnectorClientFactory)


def test_factory_rejects_empty_endpoint() -> None:
    with pytest.raises(ValueError, match="non-empty endpoint"):
        ConnectorClientFactory(
            endpoint="",
            timeout_seconds=2.0,
            negative_cache_ttl_seconds=5.0,
        )


# ---------------------------------------------------------------------------
# Protocol conformance (smoke)
# ---------------------------------------------------------------------------


async def test_http_client_satisfies_connector_client_protocol() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True})

    factory = _factory(handler)
    try:
        client = factory.for_request(callctx_header_value="ctx-1")
        assert isinstance(client, ConnectorClient)
    finally:
        await factory.aclose()


def test_stub_satisfies_connector_client_protocol() -> None:
    assert isinstance(StubConnectorClient(), ConnectorClient)
