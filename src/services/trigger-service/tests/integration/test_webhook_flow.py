"""Gateway, Trigger and Connector HTTP flow with real scoped PostgreSQL state and credentials."""

from __future__ import annotations

import hashlib
import hmac
import json
from collections.abc import Mapping
from contextlib import AsyncExitStack
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, ClassVar, cast

import asyncpg
import httpx
import pytest
from custos_connector import create_app as connector_app
from custos_connector.identity import IdentityResolverRegistry, ResolvedIdentity
from custos_connector.identity.protocols import IdentityResolverContext
from custos_connector.loader.identity import IdentityCategory
from custos_connector.providers import load_providers as connector_providers
from custos_connector.settings import load_settings as connector_settings
from custos_gateway.errors import register_exception_handlers
from custos_gateway.router import DownstreamRouter
from custos_gateway.routes.webhook import build_webhook_router
from custos_pg import (
    PgCatalogAdapter,
    PgConnectorInstanceAdapter,
    PgLeaseAdapter,
    PgMetadataAdapter,
)
from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.catalog_store import CatalogStoreProvider
from custos_spl.interfaces.connector_instance_store import (
    ConnectorInstance,
    ConnectorInstanceStoreProvider,
)
from custos_spl.interfaces.lease_store import LeaseStoreProvider
from custos_spl.interfaces.metadata_store import MetadataStoreProvider
from fastapi import FastAPI

from custos_trigger.app import create_app
from custos_trigger.clients import FakeWorkflowServiceClient
from custos_trigger.clients.workflow import DaprEndpoint
from custos_trigger.middleware.callctx import DaprWorkflowCallerVerifier
from custos_trigger.models import SourceType
from custos_trigger.providers import Providers
from custos_trigger.receivers.sources import NORMALIZED_EVENTS_PATH
from custos_trigger.receivers.webhook import DaprWebhookTransport
from tests._source_helpers import SourceStorage, dispatcher, resume, start

pytestmark = pytest.mark.integration
TRIGGER_TOKEN = "unit-trigger-sidecar"
CONNECTOR_TOKEN = "unit-connector-sidecar"


class ScopedResolver:
    """Resolve distinct test credentials from the exact persisted workspace/reference pair."""

    authentication_type: ClassVar[str] = "azure-key-vault"
    category: ClassVar[IdentityCategory] = IdentityCategory.KMS

    async def resolve(
        self, *, credentials_authentication: Mapping[str, Any], context: IdentityResolverContext
    ) -> ResolvedIdentity:
        """A wrong scope/reference fails rather than falling back to another tenant's key."""
        assert credentials_authentication["secretName"] == context.workspace_id
        return ResolvedIdentity.build(
            authentication_type=self.authentication_type,
            category=self.category,
            material={"secret": "unit-key-" + context.workspace_id},
            descriptor="test-reference",
            issued_at=context.now(),
            expires_at=None,
        )


@pytest.mark.parametrize("scheme", ["hmac-sha256", "bearer"])
async def test_real_scoped_webhook_flow_and_redelivery(
    pg_dsn: str,
    scheme: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Verify scoped signatures and rejection accounting before idempotent consumption."""
    monkeypatch.setenv("TRIGGER_GATEWAY_NAMESPACE", "custos")
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=5)
    assert pool is not None
    try:
        catalog, instances = PgCatalogAdapter(pool=pool), PgConnectorInstanceAdapter(pool=pool)
        metadata, leases = PgMetadataAdapter(pool=pool), PgLeaseAdapter(pool=pool)
        for adapter in (catalog, instances, metadata, leases):
            await adapter.apply_pending()
        assert isinstance(metadata, SourceStorage)
        settings = connector_settings(
            {
                "CONN_CATALOG_STORE": pg_dsn,
                "CONN_METADATA_STORE": pg_dsn,
                "CONN_CATALOG_ENDPOINT": "http://catalog-service:8080",
                "APP_API_TOKEN": CONNECTOR_TOKEN,
                "CONN_TRIGGER_NAMESPACE": "custos",
            }
        )
        providers = connector_providers(settings)
        await providers.identity_registry.aclose()
        # Adapters' SCHEMA_REVISION lacks ClassVar; the provider factory uses these same casts.
        providers = replace(
            providers,
            catalog_store=cast(CatalogStoreProvider, catalog),
            instance_store=cast(ConnectorInstanceStoreProvider, instances),
            metadata_store=cast(MetadataStoreProvider, metadata),
            lease_store=cast(LeaseStoreProvider, leases),
            identity_registry=IdentityResolverRegistry(resolvers=[ScopedResolver()]),
        )
        await catalog.put_connector_type_version(
            type="registry",
            version="1",
            digest="sha256:unit-webhook",
            image_ref="unit",
            normalized_manifest={
                "spec": {
                    "events": {
                        "delivery": ["push"],
                        "produced": ["registry.push"],
                    }
                }
            },
        )
        for workspace in ("source-a", "source-b"):
            now = datetime.now(UTC)
            await instances.put_connector_instance(
                WorkspaceId(workspace),
                ConnectorInstance(
                    workspace_id=WorkspaceId(workspace),
                    instance_id=ConnectorInstanceId("same"),
                    type="registry",
                    version="1",
                    name=None,
                    lease_ttl_seconds=60,
                    enabled=True,
                    status="active",
                    health_status=None,
                    target_config={},
                    credentials_authentication={
                        "webhookVerification": {
                            "scheme": scheme,
                            "authenticationType": "azure-key-vault",
                            "authentication": {
                                "vaultUri": "https://vault.example",
                                "secretName": workspace,
                            },
                            "materialKey": "secret",
                        }
                    },
                    used_capabilities=None,
                    created_at=now,
                    updated_at=now,
                ),
            )
            await start(metadata, "start", workspace=workspace, source=SourceType.WEBHOOK)
            await resume(metadata, "stored-run", workspace=workspace)
        connector = connector_app(settings=settings, providers=providers)
        workflows = FakeWorkflowServiceClient()
        queued: list[dict[str, Any]] = []
        async with AsyncExitStack() as stack:
            await stack.enter_async_context(connector.router.lifespan_context(connector))
            verifier = await stack.enter_async_context(
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=connector),
                    base_url="http://connector",
                )
            )

            async def trigger_sidecar(request: httpx.Request) -> httpx.Response:
                """Emulate authenticated Dapr invocation and completed broker publication only."""
                if request.url.path.endswith("/VerifyWebhook"):
                    return await verifier.post(
                        "/VerifyWebhook",
                        content=request.content,
                        headers={
                            "content-type": "application/json",
                            "dapr-api-token": CONNECTOR_TOKEN,
                            "dapr-caller-app-id": "trigger-service",
                            "dapr-caller-namespace": "custos",
                        },
                    )
                assert request.url.path.endswith("/custos.triggers.normalized")
                queued.append(json.loads(request.content))
                return httpx.Response(204)

            outbound = await stack.enter_async_context(
                httpx.AsyncClient(
                    transport=httpx.MockTransport(trigger_sidecar),
                )
            )
            trigger = create_app(
                authz_endpoint="",
                providers=Providers(metadata_store=metadata),
                dispatcher=dispatcher(metadata, workflows),
                receiver_configuration_enabled=True,
                source_receivers_enabled=True,
                webhook_receiver_enabled=True,
                app_api_token=TRIGGER_TOKEN,
                workflow_caller_verifier=DaprWorkflowCallerVerifier(
                    TRIGGER_TOKEN, namespace="custos"
                ),
                webhook_transport=DaprWebhookTransport(
                    outbound,
                    DaprEndpoint("127.0.0.1", 3500, "connector-service"),
                    pubsub="custos-pubsub",
                    topic="custos.triggers.normalized",
                ),
            )
            await stack.enter_async_context(trigger.router.lifespan_context(trigger))
            ingress = await stack.enter_async_context(
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=trigger),
                    base_url="http://trigger",
                )
            )

            async def gateway_sidecar(request: httpx.Request) -> httpx.Response:
                """Replace mesh identity as a real sidecar would, preserving raw forwarding data."""
                path = request.url.path.removeprefix("/v1.0/invoke/trigger-service/method")
                return await ingress.post(
                    path,
                    content=request.content,
                    headers={
                        **dict(request.headers),
                        "dapr-api-token": TRIGGER_TOKEN,
                        "dapr-caller-app-id": "api-gateway",
                        "dapr-caller-namespace": "custos",
                    },
                )

            gateway_outbound = await stack.enter_async_context(
                httpx.AsyncClient(
                    transport=httpx.MockTransport(gateway_sidecar),
                )
            )
            gateway = FastAPI()
            register_exception_handlers(gateway)
            gateway.include_router(build_webhook_router())
            gateway.state.downstream_router = DownstreamRouter(
                http_client=gateway_outbound,
                host="127.0.0.1",
                http_port=3500,
            )
            public = await stack.enter_async_context(
                httpx.AsyncClient(
                    transport=httpx.ASGITransport(app=gateway),
                    base_url="http://gateway",
                )
            )
            raw = (
                json.dumps(
                    {
                        "events": [
                            {
                                "eventId": "same-event",
                                "eventType": "registry.push",
                                "route": "match",
                                "amount": 7,
                                "workspace": "forged-workspace",
                                "runId": "forged-run",
                            },
                            {"eventId": "missing-kind"},
                            {
                                "eventId": "unsafe",
                                "eventType": "registry.push",
                                "credentials": {"secret": "never-retained"},
                            },
                        ]
                    },
                    indent=2,
                ).encode()
                + b"\r\n"
            )

            def headers(workspace: str) -> dict[str, str]:
                """Generate independent test credentials; forged context must be discarded."""
                key = "unit-key-" + workspace
                signature = "sha256=" + hmac.new(key.encode(), raw, hashlib.sha256).hexdigest()
                return {
                    **(
                        {"authorization": "Bearer " + key}
                        if scheme == "bearer"
                        else {"x-custos-signature": signature}
                    ),
                    "x-custos-webhook-authorization": "forged",
                    "x-custos-callctx": "forged",
                }

            path = "/v1/webhooks/workspaces/{}/connectors/same/events"
            denied = await public.post(
                path.format("source-b"), content=raw, headers=headers("source-a")
            )
            assert denied.status_code == 401 and not queued
            for workspace in ("source-a", "source-a", "source-b"):
                result = await public.post(
                    path.format(workspace), content=raw, headers=headers(workspace)
                )
                assert result.status_code == 200, result.text
                assert result.json() == {"accepted": 1, "rejected": 2}
            assert not workflows.start_run_calls and not workflows.raise_event_calls
            assert [event["workspaceId"] for event in queued] == [
                "source-a",
                "source-a",
                "source-b",
            ]
            for event in queued:
                consumed = await ingress.post(
                    NORMALIZED_EVENTS_PATH,
                    json={"data": event},
                    headers={"dapr-api-token": TRIGGER_TOKEN},
                )
                assert consumed.json() == {"status": "SUCCESS"}, consumed.text
            assert len(workflows.start_run_calls) == len(workflows.raise_event_calls) == 2
            assert {call.workspace_id for call in workflows.start_run_calls} == {
                "source-a",
                "source-b",
            }
            assert all(
                call.inputs == {"amount": 7, "label": "ref-same"}
                for call in workflows.start_run_calls
            )
            assert {run for run, _, _ in workflows.raise_event_calls} == {"stored-run"}
            assert all("raw" not in event["event"] for event in queued)
    finally:
        await pool.close()
