"""Exercise actual Gateway/Workflow HTTP boundaries with the production Auth signer.

Only external credential verification, Catalog storage and the durable runtime
are doubles. Recovery traverses both real factories, transport routing, JWT
verification and immutable Workflow acceptance; hosted smoke supplies real
Auth exchange, storage and Dapr evidence separately.
"""

import base64
from dataclasses import dataclass
from typing import Any
from unittest.mock import AsyncMock
from uuid import UUID

import httpx
import pytest
from custos_auth.callctx_signer import (
    CallContextSigner,
    SigningKey,
    StaticSigningKeyResolver,
)
from custos_callctx import CallContextVerifier, JwksCache
from custos_callctx.execution import (
    ExecutionBinding,
    ExecutionBindings,
    ExecutionIdentity,
)
from custos_gateway.app import create_app as gateway_app
from custos_gateway.clients.auth import (
    CallctxSignRequest,
    CallctxSignResponse,
    DeclaredPermission,
    FakeAuthServiceClient,
)
from custos_gateway.router import DownstreamRouter
from custos_gateway.routes.registry import registry_required_permissions
from custos_gateway.settings import load_settings
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.delivery import DeliveryStateProvider
from custos_workflow import create_app as workflow_app
from custos_workflow.bindings.registry import InMemoryActivityTypeRegistry
from custos_workflow.clients.trigger import FakeTriggerServiceClient
from custos_workflow.document import WorkflowDocument
from custos_workflow.providers import RunComponents, load_run_components
from custos_workflow.runs.controller import WorkflowVersion
from custos_workflow.runs.trigger_delivery import TriggerDeliveryController
from custos_workflow.runtime import FakeWorkflowRuntime
from fastapi import FastAPI

WORKSPACE = "ws"
PRINCIPAL = "principal-fake"
START = f"/v1/workspaces/{WORKSPACE}/runs"
MESH_HEADERS = {
    "dapr-api-token": "synthetic-app-token",
    "dapr-caller-namespace": "custos",
}


class _PinnedCatalog:
    """Supply a bounded waiting definition only to the caller-pinned Catalog path."""

    async def get_workflow_version(
        self, workspace: str, version: str
    ) -> WorkflowVersion:
        """Fail if either factory falls back to unsigned Catalog authority."""
        raise AssertionError("unsigned Catalog lookup")

    async def get_execution_workflow_version(
        self, workspace: str, version: str, identity: ExecutionIdentity
    ) -> WorkflowVersion:
        """Require the public caller rather than the configured background default."""
        assert identity.principal_id == PRINCIPAL
        document = WorkflowDocument.model_validate(
            {
                "apiVersion": "custos.dev/v1",
                "kind": "Workflow",
                "metadata": {"name": "proof", "workspace": workspace},
                "spec": {
                    "steps": [
                        {
                            "id": "await-cancel",
                            "waitFor": {"eventKey": "${{ 'never' }}", "ttl": "PT5M"},
                        }
                    ]
                },
            }
        )
        return WorkflowVersion(version, workspace + "/proof", "proof", "1", document)


@dataclass
class _Services:
    """Share one generated Auth key and a fully wired durable Workflow factory per test."""

    workflow: FastAPI
    signer: CallContextSigner
    components: RunComponents


@pytest.fixture
def services() -> _Services:
    """Activate the exact public/private boundaries without sockets or foreign test fixtures."""
    key = SigningKey.generate()
    signer = CallContextSigner(StaticSigningKeyResolver(key))

    async def fetch(url: str) -> tuple[dict[str, str], dict[str, Any]]:
        """Expose only the actual Auth signing key's public portion."""
        encoded = (
            base64.urlsafe_b64encode(key.public_key.public_bytes_raw())
            .decode()
            .rstrip("=")
        )
        return {}, {
            "keys": [{"kty": "OKP", "crv": "Ed25519", "kid": key.kid, "x": encoded}]
        }

    components = load_run_components(
        env={}, workflow_runtime=FakeWorkflowRuntime(), catalog=_PinnedCatalog()
    )
    provider = components.metadata_store
    assert isinstance(provider, DeliveryStateProvider)
    components.trigger_delivery_controller = TriggerDeliveryController(
        provider,
        components.run_store,
        components.workflow_client,
        InMemoryActivityTypeRegistry({}),
        components.lifecycle_publisher,
    )
    components.durable_resume_client = FakeTriggerServiceClient()
    bindings = ExecutionBindings(
        (
            ExecutionBinding(
                ExecutionIdentity(WORKSPACE, PRINCIPAL, "caller"), "secret", "token"
            ),
            ExecutionBinding(
                ExecutionIdentity(WORKSPACE, "background", "background"),
                "other-secret",
                "token",
            ),
        ),
        {WORKSPACE: "background"},
    )
    app = workflow_app(
        public_execution_enabled=True,
        run_components=components,
        execution_bindings=bindings,
        call_context_verifier=CallContextVerifier(
            jwks_cache=JwksCache(
                jwks_url="http://auth/.well-known/jwks.json", fetcher=fetch
            )
        ),
        app_api_token=MESH_HEADERS["dapr-api-token"],
        gateway_namespace="custos",
        trigger_namespace="custos",
    )
    return _Services(app, signer, components)


class _SigningClient(FakeAuthServiceClient):
    """Double remote authorization while retaining actual Gateway minting and Auth signatures."""

    def __init__(self, signer: CallContextSigner) -> None:
        """Declare real registered permissions so the actual Gateway startup checks still run."""
        super().__init__(
            permissions=[
                DeclaredPermission(name=name, description=name, declared_by="test")
                for name in registry_required_permissions()
            ]
        )
        self.signer = signer

    async def callctx_sign(self, request: CallctxSignRequest) -> CallctxSignResponse:
        """Sign the exact Gateway-selected principal, tenant and grant without legacy headers."""
        signed = await self.signer.sign(
            principal_id=request.principal_id,
            workspace_id=request.workspace_id,
            caller_component=request.caller_component,
            permissions=request.permissions,
        )
        return CallctxSignResponse(
            token=signed.token,
            kid=signed.kid,
            jti=signed.jti,
            iat=signed.iat,
            exp=signed.exp,
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "key_source", ["header", "body", "trimmed-body", "blank-body", "keyless"]
)
async def test_real_gateway_lost_acceptance_reaches_workflow_recovery(
    services: _Services, monkeypatch: pytest.MonkeyPatch, key_source: str
) -> None:
    """A lost accepted response recovers the keyed run; genuinely keyless requests stay UUIDv4."""
    accepted: list[dict[str, Any]] = []
    schedule = AsyncMock(
        wraps=services.components.workflow_client.schedule_new_workflow
    )
    monkeypatch.setattr(
        services.components.workflow_client, "schedule_new_workflow", schedule
    )

    async def forward(request: httpx.Request) -> httpx.Response:
        """Model only Dapr's authenticated peer metadata and one lost downstream HTTP response."""
        path = "/" + request.url.path.split("/method/", 1)[1]
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=services.workflow),
            base_url="http://workflow",
        ) as client:
            response = await client.request(
                request.method,
                path,
                content=request.content,
                headers={
                    **request.headers,
                    **MESH_HEADERS,
                    "dapr-caller-app-id": "api-gateway",
                },
            )
        assert response.status_code == 202
        accepted.append(response.json())
        if len(accepted) == 1:
            raise httpx.ReadError("synthetic lost acceptance", request=request)
        return httpx.Response(
            response.status_code, content=response.content, headers=response.headers
        )

    gateway = gateway_app(
        settings=load_settings(
            {
                "CUSTOS_GATEWAY_TLS_CERT_REF": "secretref://tls/cert",
                "CUSTOS_GATEWAY_TLS_KEY_REF": "secretref://tls/key",
                "CUSTOS_GATEWAY_CORS_ALLOWED_ORIGINS": '["http://localhost:3000"]',
            }
        ),
        auth_client=_SigningClient(services.signer),
        downstream_router=DownstreamRouter(
            http_client=httpx.AsyncClient(transport=httpx.MockTransport(forward))
        ),
        metadata_store=services.components.metadata_store,
    )
    payload = {"workflowVersionId": WORKSPACE + "/proof@1"}
    headers = {"authorization": "Bearer synthetic-public-caller"}
    if key_source in {"body", "trimmed-body"}:
        payload["idempotencyKey"] = (
            " effective " if key_source == "trimmed-body" else "effective"
        )
        headers["idempotency-key"] = "ignored-first"
    elif key_source == "blank-body":
        payload["idempotencyKey"] = " "
        headers["idempotency-key"] = " effective "
    elif key_source == "header":
        headers["idempotency-key"] = "effective"
    async with (
        services.workflow.router.lifespan_context(services.workflow),
        gateway.router.lifespan_context(gateway),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=gateway), base_url="http://gateway"
        ) as client,
    ):
        first = await client.post(START, json=payload, headers=headers)
        if key_source in {"body", "trimmed-body"}:
            headers["idempotency-key"] = "ignored-second"
        retry = await client.post(START, json=payload, headers=headers)
    assert [first.status_code, retry.status_code] == [503, 202]
    ids = [row["runId"] for row in accepted]
    assert len(set(ids)) == (2 if key_source == "keyless" else 1)
    assert all(
        UUID(run_id).version == (4 if key_source == "keyless" else 5) for run_id in ids
    )
    assert schedule.await_count == (2 if key_source == "keyless" else 1)


@pytest.mark.asyncio
async def test_auth_signed_private_cancel_administrates_only_its_workspace(
    services: _Services, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The actual factory/handler accepts another workspace operator, never substituted authority."""
    public = await services.signer.sign(
        principal_id=PRINCIPAL,
        workspace_id=WORKSPACE,
        caller_component="api-gateway",
        permissions=["workflow:execute"],
    )
    cancellation = await services.signer.sign(
        principal_id="different-operator",
        workspace_id=WORKSPACE,
        caller_component="trigger-service",
        permissions=["run:cancel"],
        execution_operation="workflow-cancel",
    )
    cancel = AsyncMock(wraps=services.components.run_controller.cancel_run)
    monkeypatch.setattr(services.components.run_controller, "cancel_run", cancel)
    async with (
        services.workflow.router.lifespan_context(services.workflow),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=services.workflow),
            base_url="http://workflow",
        ) as client,
    ):
        started = await client.post(
            START,
            json={
                "workflowVersionId": WORKSPACE + "/proof@1",
                "idempotencyKey": "cancel-proof",
            },
            headers={
                **MESH_HEADERS,
                "dapr-caller-app-id": "api-gateway",
                "x-custos-callctx": public.token,
            },
        )
        assert started.status_code == 202
        provider = services.components.metadata_store
        assert isinstance(provider, DeliveryStateProvider)
        original = await provider.get_workflow_start(
            WorkspaceId(WORKSPACE), started.json()["runId"]
        )
        assert original is not None
        path = "/internal/runs/" + started.json()["runId"] + ":cancel"
        headers = {
            **MESH_HEADERS,
            "dapr-caller-app-id": "trigger-service",
            "x-custos-callctx": cancellation.token,
        }
        for override, status in (
            ({"dapr-caller-app-id": "api-gateway"}, 403),
            ({"dapr-caller-namespace": "other"}, 403),
            ({"dapr-api-token": "wrong"}, 401),
            ({"x-custos-callctx": public.token}, 403),
            ({"x-custos-callctx": ""}, 401),
            ({"x-custos-callctx": "malformed"}, 401),
        ):
            denied = await client.post(
                path, json={"workspaceId": WORKSPACE}, headers={**headers, **override}
            )
            assert denied.status_code == status
        for field, status in (
            ("dapr-api-token", 401),
            ("dapr-caller-app-id", 403),
            ("dapr-caller-namespace", 403),
        ):
            duplicate = await client.post(
                path,
                json={"workspaceId": WORKSPACE},
                headers=[*headers.items(), (field, headers[field])],
            )
            assert duplicate.status_code == status
        cross_tenant = await client.post(
            path, json={"workspaceId": "other"}, headers=headers
        )
        assert cross_tenant.status_code == 403
        cancel.assert_not_awaited()
        response = await client.post(
            path, json={"workspaceId": WORKSPACE}, headers=headers
        )
        assert response.status_code == 202
        assert response.json()["status"] == "cancelled"
        assert cancel.await_count == 1
        assert (
            await provider.get_workflow_start(
                WorkspaceId(WORKSPACE), started.json()["runId"]
            )
            == original
        )
