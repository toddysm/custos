"""Tests for ``DaprConnectorClient`` (WF-IMPL-078).

The adapter is the Step Coordinator's only path to Connector
Service in production, so these tests cover the full
transport-error → exception matrix locked in WF-IMPL-075 plus the
camelCase wire contract pinned in the Connector Service design
§ *Internal RPCs*.

Coverage emphasis:

* Happy-path bind round-trips the canonical wire envelope and
  reconstructs a :class:`MappingProxyType`-frozen
  :class:`BindForStepResponse`.
* Multi-slot ordering and capability ordering are preserved on
  the wire.
* Every shape-validation failure mode (missing key, non-mapping
  body, slot-name mismatch, naïve ``expiresAt``) raises
  :class:`OutboundRpcDecodeError` (always permanent).
* Status-code matrix raises :class:`OutboundRpcStatusError` with
  the observed ``status_code`` so the WF-IMPL-075 mapper can
  classify retryable vs permanent downstream.
* Transport failures raise :class:`OutboundRpcTransportError`
  and HTTP 499 raises :class:`OutboundRpcCancelledError`.
"""

from __future__ import annotations

import json as _json
from collections.abc import Callable
from datetime import UTC, datetime
from types import MappingProxyType
from typing import Any

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
from custos_workflow.clients.connector import (
    BIND_FOR_STEP_DAPR_METHOD,
    BindForStepRequest,
    BindForStepResponse,
    DaprConnectorClient,
    SlotSpec,
    _parse_iso_utc,
    _request_to_wire,
    _response_from_wire,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def endpoint() -> DaprInvokeEndpoint:
    return DaprInvokeEndpoint(host="127.0.0.1", http_port=3500, app_id="connector-service")


@pytest.fixture
def bind_url(endpoint: DaprInvokeEndpoint) -> str:
    return build_invoke_url(endpoint, BIND_FOR_STEP_DAPR_METHOD)


@pytest.fixture
def request_obj() -> BindForStepRequest:
    return BindForStepRequest(
        step_key="run-1/step-a",
        slots=(
            SlotSpec(
                name="registry",
                connector_ref="oci-registry/main",
                capabilities=("oci.pull", "oci.inspect"),
            ),
        ),
    )


@pytest.fixture
def multi_slot_request() -> BindForStepRequest:
    return BindForStepRequest(
        step_key="run-2/step-b",
        slots=(
            SlotSpec(
                name="registry",
                connector_ref="oci-registry/main",
                capabilities=("oci.pull",),
            ),
            SlotSpec(
                name="secrets",
                connector_ref="vault/team-a",
                capabilities=("secret.read", "secret.list", "secret.rotate"),
            ),
            SlotSpec(
                name="object-store",
                connector_ref="s3/builds",
                capabilities=(),
            ),
        ),
    )


def _make_client(
    endpoint: DaprInvokeEndpoint,
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    timeout: float = DEFAULT_OUTBOUND_RPC_TIMEOUT_SECONDS,
) -> DaprConnectorClient:
    transport = httpx.MockTransport(handler)
    return DaprConnectorClient(
        http_client=httpx.AsyncClient(transport=transport),
        endpoint=endpoint,
        timeout=timeout,
    )


async def _drive(client: DaprConnectorClient, request: BindForStepRequest) -> BindForStepResponse:
    try:
        return await client.bind_for_step(request)
    finally:
        await client.http_client.aclose()


def _ok_body(slot_name: str = "registry") -> dict[str, Any]:
    """Build a minimal valid response body for ``slot_name``."""
    return {
        "contexts": {
            slot_name: {
                "slotName": slot_name,
                "handle": f"ctx-{slot_name}-token",
                "expiresAt": "2030-01-02T03:04:05Z",
                "connectorKind": "oci-registry",
            }
        }
    }


# ---------------------------------------------------------------------------
# Helpers (pure)
# ---------------------------------------------------------------------------


class TestParseIsoUtc:
    def test_accepts_z_suffix(self) -> None:
        parsed = _parse_iso_utc("2030-01-02T03:04:05Z")
        assert parsed == datetime(2030, 1, 2, 3, 4, 5, tzinfo=UTC)
        assert parsed.tzinfo is not None

    def test_accepts_explicit_offset(self) -> None:
        parsed = _parse_iso_utc("2030-01-02T03:04:05+00:00")
        assert parsed == datetime(2030, 1, 2, 3, 4, 5, tzinfo=UTC)

    def test_accepts_non_utc_offset(self) -> None:
        # Non-UTC offsets are valid ISO-8601 — the helper just
        # enforces tz-awareness, not a specific zone.
        parsed = _parse_iso_utc("2030-01-02T03:04:05+02:00")
        assert parsed.tzinfo is not None
        assert parsed.utcoffset() is not None

    def test_rejects_naive(self) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            _parse_iso_utc("2030-01-02T03:04:05")

    def test_rejects_garbage(self) -> None:
        with pytest.raises(ValueError, match="not a valid ISO-8601"):
            _parse_iso_utc("not a date")

    def test_rejects_non_string(self) -> None:
        with pytest.raises(ValueError, match="must be an ISO-8601 string"):
            _parse_iso_utc(12345)


class TestRequestToWire:
    def test_single_slot_round_trip(self, request_obj: BindForStepRequest) -> None:
        wire = _request_to_wire(request_obj)
        assert wire == {
            "stepKey": "run-1/step-a",
            "slots": [
                {
                    "name": "registry",
                    "connectorRef": "oci-registry/main",
                    "capabilities": ["oci.pull", "oci.inspect"],
                }
            ],
        }

    def test_multi_slot_preserves_order(self, multi_slot_request: BindForStepRequest) -> None:
        wire = _request_to_wire(multi_slot_request)
        # Slot declaration order is preserved.
        assert [slot["name"] for slot in wire["slots"]] == [
            "registry",
            "secrets",
            "object-store",
        ]
        # Capability declaration order is preserved within each slot.
        secrets_slot = next(s for s in wire["slots"] if s["name"] == "secrets")
        assert secrets_slot["capabilities"] == [
            "secret.read",
            "secret.list",
            "secret.rotate",
        ]

    def test_empty_capabilities_serialised_as_empty_list(
        self, multi_slot_request: BindForStepRequest
    ) -> None:
        wire = _request_to_wire(multi_slot_request)
        store_slot = next(s for s in wire["slots"] if s["name"] == "object-store")
        assert store_slot["capabilities"] == []


class TestResponseFromWire:
    def test_happy_path(self) -> None:
        result = _response_from_wire(_ok_body())
        assert isinstance(result, BindForStepResponse)
        assert type(result.contexts) is MappingProxyType
        assert set(result.contexts) == {"registry"}
        ctx = result.contexts["registry"]
        assert ctx.handle == "ctx-registry-token"
        assert ctx.expires_at == datetime(2030, 1, 2, 3, 4, 5, tzinfo=UTC)

    def test_non_mapping_body(self) -> None:
        with pytest.raises(OutboundRpcDecodeError, match="must be a JSON object"):
            _response_from_wire([])

    def test_missing_contexts(self) -> None:
        with pytest.raises(OutboundRpcDecodeError, match="missing the required 'contexts'"):
            _response_from_wire({})

    def test_contexts_not_mapping(self) -> None:
        with pytest.raises(OutboundRpcDecodeError, match="'contexts' must be a JSON object"):
            _response_from_wire({"contexts": ["nope"]})

    def test_context_not_mapping(self) -> None:
        with pytest.raises(OutboundRpcDecodeError, match="must be a JSON object"):
            _response_from_wire({"contexts": {"registry": "scalar"}})

    def test_context_missing_field(self) -> None:
        body = _ok_body()
        del body["contexts"]["registry"]["handle"]
        with pytest.raises(OutboundRpcDecodeError, match="missing required field"):
            _response_from_wire(body)

    def test_context_naive_expires_at(self) -> None:
        body = _ok_body()
        body["contexts"]["registry"]["expiresAt"] = "2030-01-02T03:04:05"
        with pytest.raises(OutboundRpcDecodeError, match="expiresAt"):
            _response_from_wire(body)

    def test_context_invalid_expires_at(self) -> None:
        body = _ok_body()
        body["contexts"]["registry"]["expiresAt"] = "not a date"
        with pytest.raises(OutboundRpcDecodeError, match="expiresAt"):
            _response_from_wire(body)

    def test_slot_name_mismatch_with_key(self) -> None:
        # ``ConnectorContext`` itself doesn't care, but
        # ``BindForStepResponse.__post_init__`` does — it must
        # surface as a decode error so callers get a permanent.
        body = {
            "contexts": {
                "registry": {
                    "slotName": "registry-but-not-quite",
                    "handle": "ctx-token",
                    "expiresAt": "2030-01-02T03:04:05Z",
                    "connectorKind": "oci-registry",
                }
            }
        }
        with pytest.raises(OutboundRpcDecodeError, match="BindForStepResponse invariants"):
            _response_from_wire(body)

    def test_context_invariant_violation(self) -> None:
        # Empty ``handle`` violates ``ConnectorContext.__post_init__``.
        body = _ok_body()
        body["contexts"]["registry"]["handle"] = ""
        with pytest.raises(OutboundRpcDecodeError, match="ConnectorContext invariants"):
            _response_from_wire(body)


# ---------------------------------------------------------------------------
# bind_for_step — happy path
# ---------------------------------------------------------------------------


async def test_bind_for_step_happy_path_single_slot(
    endpoint: DaprInvokeEndpoint, request_obj: BindForStepRequest
) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_ok_body())

    client = _make_client(endpoint, handler)
    response = await _drive(client, request_obj)

    assert isinstance(response, BindForStepResponse)
    # Locked invariant from the issue's acceptance criteria.
    assert type(response.contexts) is MappingProxyType
    assert set(response.contexts) == {"registry"}
    assert response.contexts["registry"].handle == "ctx-registry-token"


async def test_bind_for_step_happy_path_multi_slot_preserves_capability_order(
    endpoint: DaprInvokeEndpoint, multi_slot_request: BindForStepRequest
) -> None:
    captured: list[dict[str, Any]] = []

    def handler(req: httpx.Request) -> httpx.Response:
        captured.append(_json.loads(req.content.decode("utf-8")))
        body = {
            "contexts": {
                "registry": {
                    "slotName": "registry",
                    "handle": "ctx-r",
                    "expiresAt": "2030-01-02T03:04:05Z",
                    "connectorKind": "oci-registry",
                },
                "secrets": {
                    "slotName": "secrets",
                    "handle": "ctx-s",
                    "expiresAt": "2030-01-02T03:04:05Z",
                    "connectorKind": "vault",
                },
                "object-store": {
                    "slotName": "object-store",
                    "handle": "ctx-o",
                    "expiresAt": "2030-01-02T03:04:05Z",
                    "connectorKind": "s3",
                },
            }
        }
        return httpx.Response(200, json=body)

    client = _make_client(endpoint, handler)
    response = await _drive(client, multi_slot_request)

    # Slot order preserved on the wire.
    assert [s["name"] for s in captured[0]["slots"]] == ["registry", "secrets", "object-store"]
    # Capability order preserved within each slot on the wire.
    secrets_slot = next(s for s in captured[0]["slots"] if s["name"] == "secrets")
    assert secrets_slot["capabilities"] == ["secret.read", "secret.list", "secret.rotate"]
    # All three contexts arrived in the response.
    assert set(response.contexts) == {"registry", "secrets", "object-store"}
    assert type(response.contexts) is MappingProxyType


async def test_bind_for_step_targets_connector_bind_url(
    endpoint: DaprInvokeEndpoint, bind_url: str, request_obj: BindForStepRequest
) -> None:
    captured: list[str] = []

    def handler(req: httpx.Request) -> httpx.Response:
        captured.append(str(req.url))
        return httpx.Response(200, json=_ok_body())

    client = _make_client(endpoint, handler)
    await _drive(client, request_obj)

    assert captured == [bind_url]
    assert "/v1.0/invoke/connector-service/method/BindForStep" in captured[0]


async def test_bind_for_step_sends_content_type_json(
    endpoint: DaprInvokeEndpoint, request_obj: BindForStepRequest
) -> None:
    captured_headers: list[httpx.Headers] = []

    def handler(req: httpx.Request) -> httpx.Response:
        captured_headers.append(req.headers)
        return httpx.Response(200, json=_ok_body())

    client = _make_client(endpoint, handler)
    await _drive(client, request_obj)

    assert captured_headers
    assert captured_headers[0].get("content-type") == "application/json"


# ---------------------------------------------------------------------------
# bind_for_step — response shape validation
# ---------------------------------------------------------------------------


async def test_bind_for_step_slot_name_mismatch_raises_decode_error(
    endpoint: DaprInvokeEndpoint, request_obj: BindForStepRequest
) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        body = {
            "contexts": {
                "registry": {
                    "slotName": "WRONG",
                    "handle": "ctx-token",
                    "expiresAt": "2030-01-02T03:04:05Z",
                    "connectorKind": "oci-registry",
                }
            }
        }
        return httpx.Response(200, json=body)

    client = _make_client(endpoint, handler)
    with pytest.raises(OutboundRpcDecodeError, match="BindForStepResponse invariants"):
        await _drive(client, request_obj)


async def test_bind_for_step_naive_expires_at_raises_decode_error(
    endpoint: DaprInvokeEndpoint, request_obj: BindForStepRequest
) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        body = {
            "contexts": {
                "registry": {
                    "slotName": "registry",
                    "handle": "ctx-token",
                    # Missing offset / Z suffix — naïve.
                    "expiresAt": "2030-01-02T03:04:05",
                    "connectorKind": "oci-registry",
                }
            }
        }
        return httpx.Response(200, json=body)

    client = _make_client(endpoint, handler)
    with pytest.raises(OutboundRpcDecodeError, match="expiresAt"):
        await _drive(client, request_obj)


async def test_bind_for_step_invalid_json_body_raises_decode_error(
    endpoint: DaprInvokeEndpoint, request_obj: BindForStepRequest
) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        # 200 with non-JSON body — sidecar contract violation.
        return httpx.Response(200, content=b"not json at all")

    client = _make_client(endpoint, handler)
    with pytest.raises(OutboundRpcDecodeError, match="not valid JSON"):
        await _drive(client, request_obj)


async def test_bind_for_step_non_mapping_body_raises_decode_error(
    endpoint: DaprInvokeEndpoint, request_obj: BindForStepRequest
) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=["unexpected", "list"])

    client = _make_client(endpoint, handler)
    with pytest.raises(OutboundRpcDecodeError, match="must be a JSON object"):
        await _drive(client, request_obj)


# ---------------------------------------------------------------------------
# bind_for_step — status-code matrix
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
async def test_bind_for_step_permanent_4xx_raises_status_error(
    endpoint: DaprInvokeEndpoint, request_obj: BindForStepRequest, status: int
) -> None:
    # 4xx excluding 408 / 429 -> WF-IMPL-075 taxonomy will
    # classify these as permanent via map_to_activity_envelope.
    # The adapter just surfaces OutboundRpcStatusError with the
    # observed status_code so the mapper can do its job.
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(status, content=b"forbidden body")

    client = _make_client(endpoint, handler)
    with pytest.raises(OutboundRpcStatusError) as exc_info:
        await _drive(client, request_obj)
    assert exc_info.value.status_code == status
    # Body preview is captured for debuggability.
    assert "forbidden body" in str(exc_info.value)


@pytest.mark.parametrize("status", [408, 429, 500, 502, 503, 504])
async def test_bind_for_step_retryable_status_raises_status_error(
    endpoint: DaprInvokeEndpoint, request_obj: BindForStepRequest, status: int
) -> None:
    # 408 / 429 / 5xx -> WF-IMPL-075 taxonomy will classify these
    # as retryable. The adapter just surfaces the structured
    # status error.
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(status)

    client = _make_client(endpoint, handler)
    with pytest.raises(OutboundRpcStatusError) as exc_info:
        await _drive(client, request_obj)
    assert exc_info.value.status_code == status


async def test_bind_for_step_499_raises_cancelled_error(
    endpoint: DaprInvokeEndpoint, request_obj: BindForStepRequest
) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        return httpx.Response(499)

    client = _make_client(endpoint, handler)
    with pytest.raises(OutboundRpcCancelledError, match="cancelled upstream"):
        await _drive(client, request_obj)


# ---------------------------------------------------------------------------
# bind_for_step — transport failures
# ---------------------------------------------------------------------------


async def test_bind_for_step_transport_timeout_raises_transport_error(
    endpoint: DaprInvokeEndpoint, request_obj: BindForStepRequest
) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectTimeout("simulated connect timeout")

    client = _make_client(endpoint, handler)
    with pytest.raises(OutboundRpcTransportError) as exc_info:
        await _drive(client, request_obj)
    # Original httpx exception preserved on __cause__.
    assert isinstance(exc_info.value.__cause__, httpx.ConnectTimeout)


async def test_bind_for_step_connect_error_raises_transport_error(
    endpoint: DaprInvokeEndpoint, request_obj: BindForStepRequest
) -> None:
    def handler(req: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    client = _make_client(endpoint, handler)
    with pytest.raises(OutboundRpcTransportError) as exc_info:
        await _drive(client, request_obj)
    assert isinstance(exc_info.value.__cause__, httpx.ConnectError)


# ---------------------------------------------------------------------------
# bind_for_step — wire envelope
# ---------------------------------------------------------------------------


async def test_bind_for_step_posts_camelcase_envelope(
    endpoint: DaprInvokeEndpoint, request_obj: BindForStepRequest
) -> None:
    captured: list[dict[str, Any]] = []

    def handler(req: httpx.Request) -> httpx.Response:
        captured.append(_json.loads(req.content.decode("utf-8")))
        return httpx.Response(200, json=_ok_body())

    client = _make_client(endpoint, handler)
    await _drive(client, request_obj)

    assert captured == [
        {
            "stepKey": "run-1/step-a",
            "slots": [
                {
                    "name": "registry",
                    "connectorRef": "oci-registry/main",
                    "capabilities": ["oci.pull", "oci.inspect"],
                }
            ],
        }
    ]


async def test_bind_for_step_timeout_propagated_to_post(
    endpoint: DaprInvokeEndpoint, request_obj: BindForStepRequest
) -> None:
    captured: list[Any] = []

    def handler(req: httpx.Request) -> httpx.Response:
        captured.append(req.extensions.get("timeout"))
        return httpx.Response(200, json=_ok_body())

    client = _make_client(endpoint, handler, timeout=4.75)
    await _drive(client, request_obj)

    assert captured
    extension = captured[0]
    assert isinstance(extension, dict)
    assert all(value == 4.75 for value in extension.values())


# ---------------------------------------------------------------------------
# Constructor + defaults
# ---------------------------------------------------------------------------


async def test_default_timeout_matches_constant(endpoint: DaprInvokeEndpoint) -> None:
    transport = httpx.MockTransport(lambda req: httpx.Response(200, json=_ok_body()))
    async with httpx.AsyncClient(transport=transport) as http:
        client = DaprConnectorClient(http_client=http, endpoint=endpoint)
        assert client.timeout == DEFAULT_OUTBOUND_RPC_TIMEOUT_SECONDS


async def test_timeout_override_honoured(endpoint: DaprInvokeEndpoint) -> None:
    transport = httpx.MockTransport(lambda req: httpx.Response(200, json=_ok_body()))
    async with httpx.AsyncClient(transport=transport) as http:
        client = DaprConnectorClient(http_client=http, endpoint=endpoint, timeout=0.75)
        assert client.timeout == 0.75
