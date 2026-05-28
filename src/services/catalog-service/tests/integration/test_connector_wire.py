"""End-to-end integration: catalog \u2194 Connector Service wire contract.

Phase M / CONN-IMPL-034: the publish pipeline must consult Connector
Service for every connector-instance reference. These tests exercise
the full chain inside one process:

* A small Starlette app stands in for Connector Service, mounted via
  :class:`httpx.ASGITransport` on the catalog's
  :class:`ConnectorClientFactory` so no sockets are opened.
* The catalog app is built with the alternate factory injected through
  :func:`custos_catalog.create_app(connector_factory=...)`.
* We publish a workflow whose step carries a ``connector:`` ref and
  assert the catalog\u2019s response reflects the test-double\u2019s reply:

    - 200 from Connector Service \u2192 publish 201.
    - 404 \u2192 publish 400 with ``resolve.connector_instance_missing``.
    - 5xx \u2192 publish 503 with ``catalog.dependency_unavailable``.

The catalog REST surface stays the same as the rest of the integration
suite; only the connector-side transport is intercepted.
"""

from __future__ import annotations

import json
from collections.abc import Iterator
from typing import Any

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient

from custos_catalog import create_app
from custos_catalog.clients.connector import ConnectorClientFactory
from custos_catalog.providers import Providers, load_providers
from custos_catalog.settings import load_settings
from tests.integration.conftest import (
    admin_header,
    seed_echo_activity,
)

pytestmark = pytest.mark.integration

# ---------------------------------------------------------------------------
# Connector Service test double
# ---------------------------------------------------------------------------


class _Double:
    """Mutable state shared between the test double and the test body."""

    def __init__(self) -> None:
        self.known: set[str] = set()
        self.force_status: int | None = None
        self.received: list[dict[str, Any]] = []

    def reset(self) -> None:
        self.known.clear()
        self.force_status = None
        self.received.clear()


def _build_connector_double(state: _Double) -> FastAPI:
    """Tiny FastAPI app implementing ``POST /internal/v1/connectors:validate``."""
    app = FastAPI()

    @app.post("/internal/v1/connectors:validate")
    async def validate(request: Request) -> JSONResponse:
        body = await request.json()
        state.received.append(body)
        if state.force_status is not None:
            return JSONResponse(
                status_code=state.force_status,
                content={"code": "connector.internal_error", "detail": "forced"},
            )
        if body.get("mode") != "instance":
            return JSONResponse(
                status_code=400,
                content={"code": "connector.invalid_request"},
            )
        name = body.get("connectorInstanceId")
        if name in state.known:
            return JSONResponse(status_code=200, content={"ok": True})
        return JSONResponse(
            status_code=404,
            content={"code": "connector.instance_not_found"},
        )

    return app


# ---------------------------------------------------------------------------
# Catalog app fixture wired to the double via ASGITransport
# ---------------------------------------------------------------------------


@pytest.fixture
def connector_double() -> _Double:
    return _Double()


@pytest.fixture
def wired_client(
    pg_dsn: str,
    connector_double: _Double,
) -> Iterator[TestClient]:
    """Catalog ``TestClient`` whose connector traffic targets ``connector_double``."""
    env: dict[str, str] = {
        "CAT_DEFINITION_STORE": pg_dsn,
        "CAT_CATALOG_STORE": pg_dsn,
        "CAT_METADATA_STORE": pg_dsn,
        # Base URL is irrelevant when ``ASGITransport`` short-circuits the
        # network, but must still be a valid URL.
        "CAT_CONNECTOR_ENDPOINT": "http://connector-service-double.test",
        # Disable the negative cache to keep tests order-independent.
        "CAT_CONNECTOR_NEGATIVE_CACHE_TTL_SECONDS": "0",
    }
    settings = load_settings(env)
    providers: Providers = load_providers(settings)
    double_app = _build_connector_double(connector_double)
    factory = ConnectorClientFactory(
        endpoint=settings.connector_endpoint,
        timeout_seconds=settings.connector_timeout_seconds,
        negative_cache_ttl_seconds=settings.connector_negative_cache_ttl_seconds,
        transport=httpx.ASGITransport(app=double_app),
    )
    app = create_app(settings=settings, providers=providers, connector_factory=factory)
    with TestClient(app) as test_client:
        yield test_client


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _workflow_with_connector(
    *,
    ws: str = "ws-1",
    name: str = "wf-with-conn",
    activity_ref: str = "ws-1/echo@1",
    connector_name: str = "my-registry",
) -> dict[str, Any]:
    return {
        "apiVersion": "custos.dev/v1",
        "kind": "Workflow",
        "metadata": {"name": name, "workspace": ws},
        "spec": {
            "inputs": {"image": {"type": "string"}},
            "steps": [
                {
                    "id": "push-image",
                    "activity": activity_ref,
                    "connector": connector_name,
                    "with": {"message": "hello"},
                },
            ],
        },
    }


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_publish_with_known_connector_succeeds(
    wired_client: TestClient,
    connector_double: _Double,
) -> None:
    """200 from Connector Service \u2192 catalog publishes the workflow."""
    seed_echo_activity(wired_client)
    connector_double.known.add("my-registry")

    resp = wired_client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": json.dumps(_workflow_with_connector())},
        headers=admin_header("ws-1"),
    )
    assert resp.status_code == 201, resp.text
    # Catalog forwarded mode=instance and the connector name to the double.
    assert connector_double.received[0]["mode"] == "instance"
    assert connector_double.received[0]["connectorInstanceId"] == "my-registry"


def test_publish_with_missing_connector_returns_resolve_error(
    wired_client: TestClient,
    connector_double: _Double,
) -> None:
    """404 from Connector Service \u2192 catalog returns publish-resolve 400."""
    seed_echo_activity(wired_client)
    # Intentionally do not add the connector name.

    resp = wired_client.post(
        "/v1/workspaces/ws-1/workflows",
        json={
            "definition": json.dumps(
                _workflow_with_connector(
                    connector_name="not-registered",
                ),
            ),
        },
        headers=admin_header("ws-1"),
    )
    assert resp.status_code == 400, resp.text
    body = resp.json()
    assert body["error"]["code"] == "catalog.publish.resolve"
    issues = body["error"].get("issues", [])
    assert any(issue.get("code") == "resolve.connector_instance_missing" for issue in issues), body


def test_publish_returns_503_when_connector_service_returns_5xx(
    wired_client: TestClient,
    connector_double: _Double,
) -> None:
    """5xx from Connector Service → catalog returns 503 dependency_unavailable."""
    seed_echo_activity(wired_client)
    connector_double.force_status = 503

    resp = wired_client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": json.dumps(_workflow_with_connector())},
        headers=admin_header("ws-1"),
    )
    assert resp.status_code == 503, resp.text
    assert resp.json()["error"]["code"] == "catalog.dependency_unavailable"


def test_publish_treats_connector_400_as_existing(
    wired_client: TestClient,
    connector_double: _Double,
) -> None:
    """400 from Connector Service → catalog accepts as existing (config drift).

    The existence contract is satisfied even when manifest validation
    reports schema drift; runtime re-checks per step.
    """
    seed_echo_activity(wired_client)
    connector_double.force_status = 400

    resp = wired_client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": json.dumps(_workflow_with_connector())},
        headers=admin_header("ws-1"),
    )
    assert resp.status_code == 201, resp.text
