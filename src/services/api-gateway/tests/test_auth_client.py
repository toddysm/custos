"""Tests for the Auth Service Dapr client (AGW-IMPL-004)."""

from __future__ import annotations

import httpx
import pytest

from custos_gateway.clients.auth import (
    AUTH_APP_ID,
    CALLCTX_SIGN_METHOD,
    GET_PERMISSIONS_METHOD,
    VERIFY_AND_AUTHORIZE_METHOD,
    AuthServiceClient,
    AuthServiceClientDecodeError,
    AuthServiceClientStatusError,
    AuthServiceClientTransportError,
    CallctxSignRequest,
    CallctxSignResponse,
    DaprAuthServiceClient,
    DaprEndpoint,
    DeclaredPermission,
    FakeAuthServiceClient,
    NoopAuthServiceClient,
    VerifyAndAuthorizeRequest,
    VerifyAndAuthorizeResponse,
    build_invoke_url,
    read_dapr_endpoint,
)


def _endpoint() -> DaprEndpoint:
    return DaprEndpoint(host="127.0.0.1", http_port=3500, app_id=AUTH_APP_ID)


def _client_with(handler: httpx.MockTransport) -> DaprAuthServiceClient:
    return DaprAuthServiceClient(
        http_client=httpx.AsyncClient(transport=handler),
        endpoint=_endpoint(),
    )


# --- Endpoint helpers --------------------------------------------------------


def test_build_invoke_url_targets_dapr_method() -> None:
    url = build_invoke_url(_endpoint(), VERIFY_AND_AUTHORIZE_METHOD)
    assert url == (
        "http://127.0.0.1:3500/v1.0/invoke/custos-auth/method/rpc/authz.verifyAndAuthorize"
    )


def test_build_invoke_url_strips_leading_slashes() -> None:
    url = build_invoke_url(_endpoint(), "/rpc/callctx.sign")
    assert url.endswith("/method/rpc/callctx.sign")


def test_build_invoke_url_rejects_empty_method() -> None:
    with pytest.raises(ValueError):
        build_invoke_url(_endpoint(), "///")


def test_endpoint_validates_fields() -> None:
    with pytest.raises(ValueError):
        DaprEndpoint(host="", http_port=3500, app_id="x")
    with pytest.raises(ValueError):
        DaprEndpoint(host="h", http_port=3500, app_id="")
    with pytest.raises(ValueError):
        DaprEndpoint(host="h", http_port=0, app_id="x")


def test_read_dapr_endpoint_defaults_and_overrides() -> None:
    default = read_dapr_endpoint({})
    assert default.host == "127.0.0.1"
    assert default.http_port == 3500
    assert default.app_id == AUTH_APP_ID

    custom = read_dapr_endpoint(
        {"DAPR_HTTP_HOST": "dapr.local", "DAPR_HTTP_PORT": "3600"}, app_id="other"
    )
    assert custom.host == "dapr.local"
    assert custom.http_port == 3600
    assert custom.app_id == "other"


def test_read_dapr_endpoint_rejects_empty_app_id() -> None:
    with pytest.raises(ValueError):
        read_dapr_endpoint({}, app_id="")


def test_read_dapr_endpoint_rejects_non_int_port() -> None:
    with pytest.raises(ValueError):
        read_dapr_endpoint({"DAPR_HTTP_PORT": "not-a-number"})


# --- Real client: happy paths ------------------------------------------------


async def test_verify_and_authorize_posts_and_decodes() -> None:
    seen: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        return httpx.Response(
            200,
            json={
                "principal_id": "sa_1",
                "allowed": False,
                "reason": "no-binding",
                "audit_event_id": "evt_9",
            },
        )

    client = _client_with(httpx.MockTransport(handler))
    result = await client.verify_and_authorize(
        VerifyAndAuthorizeRequest(token="t", permission="runs:start", workspace_id="ws_1")
    )
    assert isinstance(result, VerifyAndAuthorizeResponse)
    assert result.allowed is False
    assert result.principal_id == "sa_1"
    assert result.audit_event_id == "evt_9"
    assert str(seen["url"]).endswith(f"/method/{VERIFY_AND_AUTHORIZE_METHOD}")


async def test_callctx_sign_posts_and_decodes() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert str(request.url).endswith(f"/method/{CALLCTX_SIGN_METHOD}")
        return httpx.Response(
            200,
            json={"token": "jwt", "kid": "k1", "jti": "j1", "iat": 100, "exp": 200},
        )

    client = _client_with(httpx.MockTransport(handler))
    result = await client.callctx_sign(
        CallctxSignRequest(
            principal_id="sa_1",
            caller_component="catalog",
            workspace_id="ws_1",
            permissions=["catalog:read"],
            audience="custos.catalog",
        )
    )
    assert isinstance(result, CallctxSignResponse)
    assert result.token == "jwt"
    assert result.exp == 200


async def test_get_permissions_unwraps_envelope() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        assert str(request.url).endswith(f"/method/{GET_PERMISSIONS_METHOD}")
        return httpx.Response(
            200,
            json={
                "permissions": [
                    {"name": "runs:start", "description": "Start a run", "declared_by": "workflow"},
                    {"name": "catalog:read", "description": "Read", "declared_by": "catalog"},
                ]
            },
        )

    client = _client_with(httpx.MockTransport(handler))
    perms = await client.get_permissions()
    assert [p.name for p in perms] == ["runs:start", "catalog:read"]
    assert all(isinstance(p, DeclaredPermission) for p in perms)


# --- Real client: error mapping ----------------------------------------------


@pytest.mark.parametrize("status_code", [408, 429, 500, 503])
async def test_transient_status_is_retryable(status_code: int) -> None:
    client = _client_with(httpx.MockTransport(lambda r: httpx.Response(status_code, text="boom")))
    with pytest.raises(AuthServiceClientStatusError) as excinfo:
        await client.verify_and_authorize(
            VerifyAndAuthorizeRequest(token="t", permission="p", workspace_id="ws")
        )
    assert excinfo.value.retryable is True
    assert excinfo.value.status_code == status_code


@pytest.mark.parametrize("status_code", [400, 401, 403, 404])
async def test_permanent_4xx_is_not_retryable(status_code: int) -> None:
    client = _client_with(httpx.MockTransport(lambda r: httpx.Response(status_code, text="nope")))
    with pytest.raises(AuthServiceClientStatusError) as excinfo:
        await client.callctx_sign(CallctxSignRequest(principal_id="sa", caller_component="c"))
    assert excinfo.value.retryable is False
    assert excinfo.value.status_code == status_code


async def test_transport_failure_is_retryable() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    client = _client_with(httpx.MockTransport(handler))
    with pytest.raises(AuthServiceClientTransportError) as excinfo:
        await client.get_permissions()
    assert excinfo.value.retryable is True


async def test_post_transport_failure_is_retryable() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    client = _client_with(httpx.MockTransport(handler))
    with pytest.raises(AuthServiceClientTransportError) as excinfo:
        await client.verify_and_authorize(
            VerifyAndAuthorizeRequest(token="t", permission="p", workspace_id="ws")
        )
    assert excinfo.value.retryable is True


async def test_non_json_body_raises_decode_error() -> None:
    client = _client_with(httpx.MockTransport(lambda r: httpx.Response(200, text="not json")))
    with pytest.raises(AuthServiceClientDecodeError) as excinfo:
        await client.verify_and_authorize(
            VerifyAndAuthorizeRequest(token="t", permission="p", workspace_id="ws")
        )
    assert excinfo.value.retryable is False


async def test_schema_mismatch_raises_decode_error() -> None:
    client = _client_with(
        httpx.MockTransport(lambda r: httpx.Response(200, json={"unexpected": 1}))
    )
    with pytest.raises(AuthServiceClientDecodeError):
        await client.callctx_sign(CallctxSignRequest(principal_id="sa", caller_component="c"))


# --- Doubles -----------------------------------------------------------------


async def test_noop_client_is_permissive() -> None:
    client = NoopAuthServiceClient()
    assert isinstance(client, AuthServiceClient)
    decision = await client.verify_and_authorize(
        VerifyAndAuthorizeRequest(token="t", permission="p", workspace_id="ws")
    )
    assert decision.allowed is True
    signed = await client.callctx_sign(CallctxSignRequest(principal_id="sa", caller_component="c"))
    assert signed.token == "noop"
    assert await client.get_permissions() == []


async def test_fake_client_records_calls_and_returns_canned() -> None:
    fake = FakeAuthServiceClient(
        decision=VerifyAndAuthorizeResponse(
            principal_id="p", allowed=False, reason="deny", audit_event_id="e"
        ),
        signed=CallctxSignResponse(token="tok", kid="k", jti="j", iat=1, exp=2),
        permissions=[DeclaredPermission(name="n", description="d", declared_by="b")],
    )
    assert isinstance(fake, AuthServiceClient)

    req = VerifyAndAuthorizeRequest(token="t", permission="p", workspace_id="ws")
    decision = await fake.verify_and_authorize(req)
    assert decision.allowed is False
    assert fake.verify_calls == [req]

    sign_req = CallctxSignRequest(principal_id="sa", caller_component="c")
    signed = await fake.callctx_sign(sign_req)
    assert signed.token == "tok"
    assert fake.sign_calls == [sign_req]

    perms = await fake.get_permissions()
    assert [p.name for p in perms] == ["n"]
    assert fake.get_permissions_calls == 1


async def test_fake_client_defaults_when_unconfigured() -> None:
    fake = FakeAuthServiceClient()
    decision = await fake.verify_and_authorize(
        VerifyAndAuthorizeRequest(token="t", permission="p", workspace_id="ws")
    )
    assert decision.allowed is True
    signed = await fake.callctx_sign(CallctxSignRequest(principal_id="sa", caller_component="c"))
    assert signed.token == "token-fake"
    assert await fake.get_permissions() == []


async def test_fake_client_raises_configured_error() -> None:
    boom = AuthServiceClientTransportError("down")
    fake = FakeAuthServiceClient(error=boom)
    with pytest.raises(AuthServiceClientTransportError):
        await fake.verify_and_authorize(
            VerifyAndAuthorizeRequest(token="t", permission="p", workspace_id="ws")
        )
    with pytest.raises(AuthServiceClientTransportError):
        await fake.callctx_sign(CallctxSignRequest(principal_id="sa", caller_component="c"))
    with pytest.raises(AuthServiceClientTransportError):
        await fake.get_permissions()
    # The failed calls are still recorded.
    assert len(fake.verify_calls) == 1
    assert len(fake.sign_calls) == 1
    assert fake.get_permissions_calls == 1
