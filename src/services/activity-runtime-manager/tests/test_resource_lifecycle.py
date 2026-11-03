"""Production composition and admission with faulted external acquisition boundaries."""

import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import httpx
import pytest
from custos_pg import PgMetadataAdapter
from custos_pg.adapters.lease import PgLeaseAdapter
from custos_pg.pool import LazyPool
from kubernetes import client as kube_client
from kubernetes import config as kube_config

from custos_arm import create_app
from custos_arm.config import Settings
from custos_arm.credential_proxy import CredentialProxy
from custos_arm.resources import open_runtime
from custos_arm.scheduler import ActivityScheduler


@pytest.mark.parametrize("failure", [None, "schema", "snapshot", "http", "kubernetes"])
async def test_production_resource_graph_and_failure_cleanup(
    tmp_path: Path,
    settings: Settings,
    monkeypatch: pytest.MonkeyPatch,
    failure: str | None,
) -> None:
    """Keep actual adapters and clients; emulate only external readiness I/O."""
    events: list[str] = []
    unhealthy = [False]
    artifact_root = tmp_path / "artifacts"
    artifact_root.mkdir()
    monkeypatch.setenv("CUSTOS_CSI_PVC_MOUNT", str(artifact_root))
    monkeypatch.setenv("CUSTOS_EXECUTION_IDENTITY_ENABLED", "true")
    monkeypatch.setenv(
        "CUSTOS_EXECUTION_BINDINGS",
        json.dumps(
            {
                "bindings": [
                    {
                        "workspaceId": "ws",
                        "principalId": "executor",
                        "bindingId": "stable",
                        "secretRef": "source",
                        "secretKey": "token",
                    }
                ],
                "defaults": {"ws": "stable"},
            }
        ),
    )
    selected = replace(
        settings,
        runtime_enabled=True,
        credential_proxy_url="https://runtime.internal",
        authz_endpoint="http://auth",
        artifact_store="csi",
        staging_root=str(tmp_path / "staging"),
        catalog_endpoint="http://catalog",
        connector_endpoint="http://connector",
    )

    async def schema(self: PgMetadataAdapter) -> None:
        """Replace only the remote schema-query result."""
        events.append("schema")
        if failure == "schema":
            raise RuntimeError("schema unavailable")

    async def read(self: PgMetadataAdapter, *args: object) -> None:
        """Prove the production factory checks the actual ownership table before admission."""
        events.append("snapshot")
        if failure == "snapshot":
            raise RuntimeError("ownership table unavailable")

    async def close_pool(self: LazyPool) -> None:
        """Observe the actual lifespan's pool ownership on every exit path."""
        events.append("pool-closed")

    async def lease_schema(self: PgLeaseAdapter) -> None:
        """Supply the external optional lease-schema result without changing its gate."""
        self._applied_revisions = {1, 2}

    def http_response(request: httpx.Request) -> httpx.Response:
        """Probe real HTTP clients without external network access."""
        events.append("http")
        return httpx.Response(503 if failure == "http" or unhealthy[0] else 200)

    original_http = httpx.AsyncClient

    def owned_http(**kwargs: Any) -> httpx.AsyncClient:
        """Use a real client and transport while retaining normal context-manager teardown."""
        return original_http(transport=httpx.MockTransport(http_response), **kwargs)

    def load_config(*, client_configuration: kube_client.Configuration) -> None:
        """Avoid requiring a local service-account mount during this unit boundary test."""
        client_configuration.host = "https://cluster.invalid"

    def list_objects(self: object, *args: object, **kwargs: object) -> None:
        """Emulate the bounded real Kubernetes API readiness request."""
        assert kwargs["_request_timeout"] == 10
        events.append("kubernetes")
        if failure == "kubernetes":
            raise RuntimeError("Kubernetes unavailable")

    original_close = kube_client.ApiClient.close

    def close_api(self: kube_client.ApiClient) -> None:
        """Observe closure of the actual API client, not a stand-in resource bundle."""
        events.append("api-closed")
        original_close(self)

    monkeypatch.setattr(PgMetadataAdapter, "check_activity_execution_ready", schema)
    monkeypatch.setattr(PgMetadataAdapter, "check_execution_identity_ready", schema)
    monkeypatch.setattr(PgMetadataAdapter, "read_activity_execution", read)
    monkeypatch.setattr(LazyPool, "aclose", close_pool)
    monkeypatch.setattr(PgLeaseAdapter, "refresh_declared", lease_schema)
    monkeypatch.setattr(httpx, "AsyncClient", owned_http)
    monkeypatch.setattr(kube_config, "load_incluster_config", load_config)
    monkeypatch.setattr(kube_client.BatchV1Api, "list_namespaced_job", list_objects)
    monkeypatch.setattr(kube_client.CoreV1Api, "list_namespaced_pod", list_objects)
    monkeypatch.setattr(kube_client.ApiClient, "close", close_api)
    app = create_app(selected, resource_factory=open_runtime)
    if failure:
        with pytest.raises(RuntimeError):
            async with app.router.lifespan_context(app):
                pytest.fail("faulted resource graph opened admission")
        assert not app.state.ready
    else:
        async with app.router.lifespan_context(app):
            assert isinstance(app.state.scheduler, ActivityScheduler)
            assert isinstance(app.state.credential_proxy, CredentialProxy)
            async with original_http(
                transport=httpx.ASGITransport(app=app), base_url="http://arm"
            ) as caller:
                assert (await caller.get("/readyz")).status_code == 200
                assert (await caller.get("/healthz")).status_code == 200
                unhealthy[0] = True
                assert (await caller.get("/readyz")).status_code == 503
                assert not app.state.dependencies_ready
                assert (await caller.get("/healthz")).status_code == 200
                unhealthy[0] = False
                assert (await caller.get("/readyz")).status_code == 200
                assert app.state.dependencies_ready
                denied = await caller.post(
                    "/internal/v1/activity-credentials",
                    json={
                        "workspaceId": "ws",
                        "runId": "run",
                        "stepId": "step",
                        "attempt": 1,
                        "operation": "issue",
                        "slot": "source",
                        "capability": "oci.pull",
                    },
                )
                assert denied.status_code == 403
        assert not app.state.ready
        assert app.state.scheduler is None and app.state.credential_proxy is None
    assert events[-1] == "pool-closed"
    if "kubernetes" in events:
        assert "api-closed" in events
    assert not list(artifact_root.rglob("*.json"))
