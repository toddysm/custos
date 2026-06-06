"""Tests for the shared downstream-forwarding helpers (AGW-IMPL-014)."""

from __future__ import annotations

import pytest
from fastapi import Request

from custos_gateway.errors import GatewayError, GatewayErrorCode
from custos_gateway.router import DownstreamResponse
from custos_gateway.routes._forwarding import (
    DOWNSTREAM_ROUTER_STATE_ATTR,
    get_downstream_router,
    shaped_response,
)


def _request_with_router(router: object | None) -> Request:
    """Build a minimal ``Request`` whose ``app.state`` carries ``router``."""
    state = type("_State", (), {})()
    if router is not None:
        setattr(state, DOWNSTREAM_ROUTER_STATE_ATTR, router)
    app = type("_App", (), {"state": state})()
    return Request({"type": "http", "app": app, "headers": []})


def test_shaped_response_preserves_status_body_and_repeated_headers() -> None:
    reply = DownstreamResponse(
        status_code=207,
        headers=[
            ("set-cookie", "a=1"),
            ("set-cookie", "b=2"),
            ("content-type", "application/json"),
        ],
        body=b'{"ok":true}',
    )
    response = shaped_response(reply)
    assert response.status_code == 207
    assert response.body == b'{"ok":true}'
    cookies = [value for name, value in response.raw_headers if name == b"set-cookie"]
    assert cookies == [b"a=1", b"b=2"]
    # content-length is recomputed from the forwarded body.
    lengths = [value for name, value in response.raw_headers if name == b"content-length"]
    assert lengths == [str(len(reply.body)).encode("latin-1")]


def test_get_downstream_router_returns_bound_router() -> None:
    sentinel = object()
    request = _request_with_router(sentinel)
    assert get_downstream_router(request) is sentinel


def test_get_downstream_router_raises_503_when_unbound() -> None:
    request = _request_with_router(None)
    with pytest.raises(GatewayError) as excinfo:
        get_downstream_router(request)
    assert excinfo.value.code is GatewayErrorCode.DOWNSTREAM_UNAVAILABLE
    assert excinfo.value.status == 503
