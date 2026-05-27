"""Unit tests for :mod:`custos_sidecar.lease_gateway`."""

from __future__ import annotations

import json

import httpx
import pytest

from custos_sidecar.lease_gateway import (
    GatewayLeaseError,
    GatewayTransportError,
    LeaseGateway,
)


def _lease_wire() -> dict[str, object]:
    return {
        "workspaceId": "ws_test",
        "leaseId": "lease_01",
        "runId": "r1",
        "stepId": "s1",
        "attempt": 1,
        "slot": "primary",
        "capability": "read",
        "connectorInstanceId": "ci_01",
        "tokenType": "Bearer",
        "issuedAt": "2026-05-27T00:00:00+00:00",
        "expiresAt": "2026-05-27T01:00:00+00:00",
        "releasedAt": None,
        "revokedAt": None,
        "revokeReason": None,
        "createdAt": "2026-05-27T00:00:00+00:00",
        "updatedAt": "2026-05-27T00:00:00+00:00",
    }


def _gateway(handler: httpx.MockTransport) -> LeaseGateway:
    client = httpx.AsyncClient(transport=handler, base_url="http://test")
    return LeaseGateway(client=client, call_context='{"workspace_id":"ws_test"}')


async def test_issue_happy_path():
    seen: dict[str, object] = {}

    def handler(req: httpx.Request) -> httpx.Response:
        seen["url"] = str(req.url)
        seen["headers"] = dict(req.headers)
        seen["body"] = json.loads(req.content)
        return httpx.Response(200, json={"lease": _lease_wire()})

    gw = _gateway(httpx.MockTransport(handler))
    try:
        rec = await gw.issue(
            {
                "runId": "r1",
                "stepId": "s1",
                "attempt": 1,
                "slot": "primary",
                "capability": "read",
                "connectorInstanceId": "ci_01",
                "tokenType": "Bearer",
            }
        )
    finally:
        await gw.aclose()
    assert rec.lease_id == "lease_01"
    assert rec.workspace_id == "ws_test"
    assert seen["url"] == "http://test/internal/v1/leases:issue"
    assert seen["headers"]["x-call-context"] == '{"workspace_id":"ws_test"}'  # type: ignore[index]


async def test_issue_capacity_exceeded_carries_retry_after():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(
            429,
            json={"error": {"code": "CAPACITY_EXCEEDED", "detail": "cap=1"}},
            headers={"Retry-After": "5"},
        )

    gw = _gateway(httpx.MockTransport(handler))
    try:
        with pytest.raises(GatewayLeaseError) as info:
            await gw.issue({"runId": "r1", "stepId": "s", "attempt": 1, "slot": "p"})
    finally:
        await gw.aclose()
    assert info.value.code == "CAPACITY_EXCEEDED"
    assert info.value.http_status == 429
    assert info.value.retry_after_sec == 5


async def test_issue_invalid_request():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"error": {"code": "INVALID_REQUEST", "detail": "past"}})

    gw = _gateway(httpx.MockTransport(handler))
    try:
        with pytest.raises(GatewayLeaseError) as info:
            await gw.issue({"runId": "r1"})
    finally:
        await gw.aclose()
    assert info.value.code == "INVALID_REQUEST"


async def test_issue_5xx_is_transport():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(503, text="upstream gone")

    gw = _gateway(httpx.MockTransport(handler))
    try:
        with pytest.raises(GatewayTransportError):
            await gw.issue({})
    finally:
        await gw.aclose()


async def test_issue_malformed_200_is_transport():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"unexpected": True})

    gw = _gateway(httpx.MockTransport(handler))
    try:
        with pytest.raises(GatewayTransportError):
            await gw.issue({})
    finally:
        await gw.aclose()


async def test_refresh_not_found():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": {"code": "NOT_FOUND", "detail": "x"}})

    gw = _gateway(httpx.MockTransport(handler))
    try:
        with pytest.raises(GatewayLeaseError) as info:
            await gw.refresh({"leaseId": "lease_x"})
    finally:
        await gw.aclose()
    assert info.value.code == "NOT_FOUND"


async def test_refresh_already_released():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(410, json={"error": {"code": "ALREADY_RELEASED", "detail": "gone"}})

    gw = _gateway(httpx.MockTransport(handler))
    try:
        with pytest.raises(GatewayLeaseError) as info:
            await gw.refresh({"leaseId": "lease_x"})
    finally:
        await gw.aclose()
    assert info.value.code == "ALREADY_RELEASED"


async def test_release_swallows_4xx():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": {"code": "NOT_FOUND", "detail": "x"}})

    gw = _gateway(httpx.MockTransport(handler))
    try:
        await gw.release("lease_x")  # no exception
    finally:
        await gw.aclose()


async def test_release_5xx_is_transport():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    gw = _gateway(httpx.MockTransport(handler))
    try:
        with pytest.raises(GatewayTransportError):
            await gw.release("lease_x")
    finally:
        await gw.aclose()


async def test_transport_error_wraps_httpx_error():
    def handler(_: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    gw = _gateway(httpx.MockTransport(handler))
    try:
        with pytest.raises(GatewayTransportError):
            await gw.issue({})
    finally:
        await gw.aclose()


# ---------------------------------------------------------------------------
# revoke_many (CONN-IMPL-020)
# ---------------------------------------------------------------------------


async def test_revoke_many_happy_path_returns_per_lease_acks():
    seen: dict[str, object] = {}

    def handler(req: httpx.Request) -> httpx.Response:
        seen["url"] = str(req.url)
        seen["body"] = json.loads(req.content)
        return httpx.Response(
            200,
            json={
                "results": [
                    {"leaseId": "lease_A", "status": "revoked"},
                    {"leaseId": "lease_B", "status": "not-found"},
                ]
            },
        )

    gw = _gateway(httpx.MockTransport(handler))
    try:
        acks = await gw.revoke_many(["lease_A", "lease_B"], reason="rotate")
    finally:
        await gw.aclose()
    assert acks == [
        {"leaseId": "lease_A", "status": "revoked"},
        {"leaseId": "lease_B", "status": "not-found"},
    ]
    assert seen["url"] == "http://test/internal/v1/leases:revoke"
    assert seen["body"] == {"leaseIds": ["lease_A", "lease_B"], "reason": "rotate"}


async def test_revoke_many_transport_error_on_connect():
    def handler(_: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("refused")

    gw = _gateway(httpx.MockTransport(handler))
    try:
        with pytest.raises(GatewayTransportError):
            await gw.revoke_many(["lease_A"], reason="x")
    finally:
        await gw.aclose()


async def test_revoke_many_5xx_is_transport():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    gw = _gateway(httpx.MockTransport(handler))
    try:
        with pytest.raises(GatewayTransportError):
            await gw.revoke_many(["lease_A"], reason="x")
    finally:
        await gw.aclose()


async def test_revoke_many_4xx_is_transport():
    """Any non-200 (including 422) is transport-level for revoke."""

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(422, json={"detail": "bad"})

    gw = _gateway(httpx.MockTransport(handler))
    try:
        with pytest.raises(GatewayTransportError):
            await gw.revoke_many(["lease_A"], reason="x")
    finally:
        await gw.aclose()


async def test_revoke_many_malformed_body_is_transport():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"unexpected": True})

    gw = _gateway(httpx.MockTransport(handler))
    try:
        with pytest.raises(GatewayTransportError):
            await gw.revoke_many(["lease_A"], reason="x")
    finally:
        await gw.aclose()


async def test_revoke_many_non_list_results_is_transport():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"results": "nope"})

    gw = _gateway(httpx.MockTransport(handler))
    try:
        with pytest.raises(GatewayTransportError, match="must be a list"):
            await gw.revoke_many(["lease_A"], reason="x")
    finally:
        await gw.aclose()


async def test_revoke_many_entry_missing_status_is_transport():
    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"results": [{"leaseId": "lease_A"}]})

    gw = _gateway(httpx.MockTransport(handler))
    try:
        with pytest.raises(GatewayTransportError, match="missing key"):
            await gw.revoke_many(["lease_A"], reason="x")
    finally:
        await gw.aclose()
