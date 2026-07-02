"""Tests for the typed gateway API client (DEVCLI-IMPL-004)."""

from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest

from custosctl.api import ApiClient, ApiError, build_client
from custosctl.config import Settings, Target

Handler = Callable[[httpx.Request], httpx.Response]


def _client(handler: Handler, *, verify: bool = True) -> ApiClient:
    return ApiClient(
        base_url="https://gw.example",
        token="cst_secret",
        verify=verify,
        transport=httpx.MockTransport(handler),
    )


def test_get_decodes_json_and_sends_bearer() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["auth"] = request.headers.get("authorization", "")
        seen["path"] = request.url.path
        return httpx.Response(200, json={"items": [1, 2]})

    with _client(handler) as client:
        body = client.get("/v1/catalog/connector-types")
    assert body == {"items": [1, 2]}
    assert seen["auth"] == "Bearer cst_secret"
    assert seen["path"] == "/v1/catalog/connector-types"


def test_post_sends_json_and_idempotency_key() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["idem"] = request.headers.get("idempotency-key")
        seen["content"] = request.content
        return httpx.Response(201, json={"ok": True})

    with _client(handler) as client:
        body = client.post("/v1/thing", json={"a": 1}, idempotency_key="key-123")
    assert body == {"ok": True}
    assert seen["idem"] == "key-123"
    assert b'"a"' in seen["content"]  # type: ignore[operator]


def test_no_idempotency_header_when_unset() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert "idempotency-key" not in request.headers
        return httpx.Response(200, json={})

    with _client(handler) as client:
        client.post("/v1/thing", json={})


def test_204_and_empty_body_decode_to_none() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(204)

    with _client(handler) as client:
        assert client.request("DELETE", "/v1/thing") is None


def test_problem_json_error_is_mapped() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403,
            headers={"content-type": "application/problem+json"},
            json={
                "type": "https://custos.dev/errors/permission-denied",
                "title": "Permission denied",
                "status": 403,
                "detail": "not permitted to register connector-types",
            },
        )

    with _client(handler) as client, pytest.raises(ApiError) as excinfo:
        client.get("/v1/catalog/connector-types")
    err = excinfo.value
    assert err.status_code == 403
    assert err.code == "permission-denied"
    assert err.title == "Permission denied"
    assert err.detail == "not permitted to register connector-types"
    assert "not permitted" in str(err)


def test_non_json_error_still_raises() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    with _client(handler) as client, pytest.raises(ApiError) as excinfo:
        client.get("/v1/thing")
    assert excinfo.value.status_code == 500
    assert excinfo.value.code is None


def test_transport_error_becomes_api_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused", request=request)

    with _client(handler) as client, pytest.raises(ApiError) as excinfo:
        client.get("/v1/thing")
    assert excinfo.value.status_code == 0
    assert "API request failed" in str(excinfo.value)


def _settings(**overrides: object) -> Settings:
    base: dict[str, object] = {"target": Target.REMOTE}
    base.update(overrides)
    return Settings(**base)  # type: ignore[arg-type]


def test_build_client_requires_gateway() -> None:
    with pytest.raises(RuntimeError, match="CUSTOS_GATEWAY is required"):
        build_client(_settings(token="cst_x"))


def test_build_client_requires_token() -> None:
    with pytest.raises(RuntimeError, match="CUSTOS_TOKEN is required"):
        build_client(_settings(gateway="https://gw.example"))


def test_build_client_verify_toggles_with_insecure() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"ok": True})

    settings = _settings(gateway="https://gw.example", token="cst_x", insecure=True)
    with build_client(settings, transport=httpx.MockTransport(handler)) as client:
        assert client.get("/v1/ping") == {"ok": True}
