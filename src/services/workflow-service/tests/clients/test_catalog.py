"""Tests for ``DaprCatalogClient`` and the Catalog client test doubles (WF-IMPL-113).

The adapter is the Start-Run validator's only path to the Catalog
Service in production, so these tests cover:

* the canonical Dapr Service-Invocation URL (the triple-encoded
  ``workflowVersionId`` handle forwarded verbatim as a ``:path``
  segment);
* the happy-path mapping of a Catalog ``WorkflowVersionBody`` onto a
  :class:`~custos_workflow.runs.controller.WorkflowVersion`;
* the full failure matrix — HTTP 404 →
  :class:`CatalogWorkflowVersionNotFound`, HTTP 499 →
  :class:`OutboundRpcCancelledError`, any other non-2xx →
  :class:`OutboundRpcStatusError`, transport failure →
  :class:`OutboundRpcTransportError`, non-JSON / contract-violating
  body → :class:`OutboundRpcDecodeError`;
* the cross-workspace defence-in-depth guard;
* :class:`runtime_checkable` Protocol conformance for the adapter
  and both test doubles.
"""

from __future__ import annotations

from collections.abc import Callable
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
from custos_workflow.clients.catalog import (
    GET_WORKFLOW_VERSION_DAPR_METHOD_PREFIX,
    CatalogWorkflowVersionNotFound,
    DaprCatalogClient,
    FakeCatalogClient,
    NoopCatalogClient,
    _parse_workflow_version_response,
)
from custos_workflow.runs.controller import CatalogClient, WorkflowVersion

WORKSPACE_ID = "ws-1"
WORKFLOW_NAME = "pipeline"
WORKFLOW_VERSION = 3
WORKFLOW_VERSION_ID = f"{WORKSPACE_ID}/{WORKFLOW_NAME}@{WORKFLOW_VERSION}"


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _document_dict() -> dict[str, Any]:
    """A minimal valid ``WorkflowDocument`` as a JSON-shaped dict."""
    return {
        "apiVersion": "custos.dev/v1",
        "kind": "Workflow",
        "metadata": {"name": WORKFLOW_NAME, "workspace": WORKSPACE_ID},
        "spec": {
            "steps": [
                {"id": "a", "let": {"x": "${{ true }}"}},
            ],
        },
    }


def _workflow_version_body(**overrides: Any) -> dict[str, Any]:
    """A canonical Catalog ``WorkflowVersionBody`` response payload."""
    body: dict[str, Any] = {
        "workspaceId": WORKSPACE_ID,
        "workflowName": WORKFLOW_NAME,
        "version": WORKFLOW_VERSION,
        "document": _document_dict(),
        "derivedFromTemplateVersionId": None,
        "parentDeprecated": False,
        "publishedAt": "2026-01-01T00:00:00Z",
    }
    body.update(overrides)
    return body


@pytest.fixture
def endpoint() -> DaprInvokeEndpoint:
    return DaprInvokeEndpoint(host="127.0.0.1", http_port=3500, app_id="catalog-service")


def _make_client(
    endpoint: DaprInvokeEndpoint,
    handler: Callable[[httpx.Request], httpx.Response],
    *,
    timeout: float = DEFAULT_OUTBOUND_RPC_TIMEOUT_SECONDS,
) -> DaprCatalogClient:
    transport = httpx.MockTransport(handler)
    return DaprCatalogClient(
        http_client=httpx.AsyncClient(transport=transport),
        endpoint=endpoint,
        timeout=timeout,
    )


async def _drive(
    client: DaprCatalogClient,
    *,
    workspace_id: str = WORKSPACE_ID,
    workflow_version_id: str = WORKFLOW_VERSION_ID,
) -> WorkflowVersion:
    try:
        return await client.get_workflow_version(workspace_id, workflow_version_id)
    finally:
        await client.http_client.aclose()


# ---------------------------------------------------------------------------
# URL / transport
# ---------------------------------------------------------------------------


async def test_get_workflow_version_builds_canonical_url(endpoint: DaprInvokeEndpoint) -> None:
    captured: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append(request)
        return httpx.Response(200, json=_workflow_version_body())

    client = _make_client(endpoint, handler)
    await _drive(client)

    assert len(captured) == 1
    request = captured[0]
    assert request.method == "GET"
    expected_method = f"{GET_WORKFLOW_VERSION_DAPR_METHOD_PREFIX}{WORKFLOW_VERSION_ID}"
    assert str(request.url) == build_invoke_url(endpoint, expected_method)
    assert request.headers["Accept"] == "application/json"


async def test_get_workflow_version_maps_response(endpoint: DaprInvokeEndpoint) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=_workflow_version_body())

    client = _make_client(endpoint, handler)
    version = await _drive(client)

    assert version.id == WORKFLOW_VERSION_ID
    assert version.workflow_id == f"{WORKSPACE_ID}/{WORKFLOW_NAME}"
    assert version.name == WORKFLOW_NAME
    assert version.version_label == str(WORKFLOW_VERSION)
    assert version.document.metadata.name == WORKFLOW_NAME


async def test_get_workflow_version_404_raises_not_found(endpoint: DaprInvokeEndpoint) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(404, json={"error": "catalog.not_found"})

    client = _make_client(endpoint, handler)
    with pytest.raises(CatalogWorkflowVersionNotFound) as excinfo:
        await _drive(client)

    assert isinstance(excinfo.value, LookupError)
    assert excinfo.value.workspace_id == WORKSPACE_ID
    assert excinfo.value.workflow_version_id == WORKFLOW_VERSION_ID


async def test_get_workflow_version_499_raises_cancelled(endpoint: DaprInvokeEndpoint) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(499)

    client = _make_client(endpoint, handler)
    with pytest.raises(OutboundRpcCancelledError):
        await _drive(client)


@pytest.mark.parametrize("status_code", [400, 403, 500, 503])
async def test_get_workflow_version_other_non_2xx_raises_status(
    endpoint: DaprInvokeEndpoint, status_code: int
) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(status_code, text="boom")

    client = _make_client(endpoint, handler)
    with pytest.raises(OutboundRpcStatusError) as excinfo:
        await _drive(client)

    assert excinfo.value.status_code == status_code


async def test_get_workflow_version_transport_error(endpoint: DaprInvokeEndpoint) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused")

    client = _make_client(endpoint, handler)
    with pytest.raises(OutboundRpcTransportError):
        await _drive(client)


async def test_get_workflow_version_non_json_body(endpoint: DaprInvokeEndpoint) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="not json", headers={"Content-Type": "text/plain"})

    client = _make_client(endpoint, handler)
    with pytest.raises(OutboundRpcDecodeError):
        await _drive(client)


# ---------------------------------------------------------------------------
# Response-body contract (via the public adapter and the parser directly)
# ---------------------------------------------------------------------------


def _parse(body: Any) -> WorkflowVersion:
    return _parse_workflow_version_response(
        body,
        workspace_id=WORKSPACE_ID,
        workflow_version_id=WORKFLOW_VERSION_ID,
    )


def test_parse_rejects_non_object_body() -> None:
    with pytest.raises(OutboundRpcDecodeError):
        _parse(["not", "an", "object"])


def test_parse_rejects_missing_workspace_id() -> None:
    body = _workflow_version_body()
    del body["workspaceId"]
    with pytest.raises(OutboundRpcDecodeError):
        _parse(body)


def test_parse_rejects_empty_workspace_id() -> None:
    with pytest.raises(OutboundRpcDecodeError):
        _parse(_workflow_version_body(workspaceId=""))


def test_parse_rejects_non_string_workflow_name() -> None:
    with pytest.raises(OutboundRpcDecodeError):
        _parse(_workflow_version_body(workflowName=123))


def test_parse_rejects_cross_workspace_body() -> None:
    with pytest.raises(CatalogWorkflowVersionNotFound):
        _parse(_workflow_version_body(workspaceId="ws-other"))


@pytest.mark.parametrize("version", ["3", 3.0, True, None])
def test_parse_rejects_non_int_version(version: Any) -> None:
    with pytest.raises(OutboundRpcDecodeError):
        _parse(_workflow_version_body(version=version))


def test_parse_rejects_non_object_document() -> None:
    with pytest.raises(OutboundRpcDecodeError):
        _parse(_workflow_version_body(document="not-a-mapping"))


def test_parse_rejects_invalid_document() -> None:
    with pytest.raises(OutboundRpcDecodeError):
        _parse(_workflow_version_body(document={"apiVersion": "custos.dev/v1"}))


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


async def test_noop_catalog_client_raises() -> None:
    client = NoopCatalogClient()
    with pytest.raises(NotImplementedError):
        await client.get_workflow_version(WORKSPACE_ID, WORKFLOW_VERSION_ID)


async def test_fake_catalog_client_returns_and_records() -> None:
    version = _parse(_workflow_version_body())
    fake = FakeCatalogClient(versions={WORKFLOW_VERSION_ID: version})

    result = await fake.get_workflow_version(WORKSPACE_ID, WORKFLOW_VERSION_ID)

    assert result is version
    assert fake.calls == [(WORKSPACE_ID, WORKFLOW_VERSION_ID)]


async def test_fake_catalog_client_unknown_id_raises_not_found() -> None:
    fake = FakeCatalogClient()
    with pytest.raises(CatalogWorkflowVersionNotFound):
        await fake.get_workflow_version(WORKSPACE_ID, "ws-1/missing@1")
    assert fake.calls == [(WORKSPACE_ID, "ws-1/missing@1")]


async def test_fake_catalog_client_cross_workspace_raises_not_found() -> None:
    version = _parse(_workflow_version_body())
    fake = FakeCatalogClient(versions={WORKFLOW_VERSION_ID: version})

    with pytest.raises(CatalogWorkflowVersionNotFound):
        await fake.get_workflow_version("ws-other", WORKFLOW_VERSION_ID)
    assert fake.calls == [("ws-other", WORKFLOW_VERSION_ID)]


# ---------------------------------------------------------------------------
# Protocol conformance
# ---------------------------------------------------------------------------


def test_clients_satisfy_catalog_protocol(endpoint: DaprInvokeEndpoint) -> None:
    dapr = DaprCatalogClient(
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(
                lambda request: httpx.Response(200, json=_workflow_version_body())
            )
        ),
        endpoint=endpoint,
    )
    assert isinstance(dapr, CatalogClient)
    assert isinstance(NoopCatalogClient(), CatalogClient)
    assert isinstance(FakeCatalogClient(), CatalogClient)
