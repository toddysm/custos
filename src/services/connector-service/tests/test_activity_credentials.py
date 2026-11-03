"""Real resolver issuance behind signed runtime identity and durable accepted attempt scope."""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from typing import Any, cast

import httpx
import pytest
from custos_arm.credential_proxy import CredentialProxy
from custos_arm.credential_proxy import router as proxy_router
from custos_callctx.bootstrap import BoundTriple, mint_bootstrap_token
from custos_callctx.execution import ExecutionBinding, ExecutionBindings, ExecutionIdentity
from custos_callctx.execution_client import ExecutionContextClient
from custos_sidecar.auth import BootstrapTokenVerifier
from custos_sidecar.broker import RuntimeBundle, create_broker_app
from custos_spl.errors import BackendUnavailable, LeaseBusy
from custos_spl.ids import WorkspaceId
from custos_spl.interfaces.activity_execution import ActivityExecutionRecord
from custos_spl.interfaces.lease_store import Lease, LeaseStoreProvider
from custos_spl.interfaces.metadata_store import AuditEvent, MetadataStoreProvider
from fastapi import FastAPI

from custos_connector import create_app
from custos_connector.identity import (
    DaprSecretResolver,
    HttpxAsyncHttpClient,
    IdentityResolverRegistry,
)
from custos_connector.loader.identity import IdentityCategory
from tests._fakes import (
    FakeCatalogAdapter,
    FakeConnectorInstanceAdapter,
    FakeLeaseAdapter,
    FakeMetadataAdapter,
    StubPluginBinder,
    build_bind_for_step_service,
)
from tests.test_binding_router import _BASE_SETTINGS, _build_providers, _make_instance, _seed
from tests.test_signed_callctx import Signing
from tests.test_signed_callctx import signing as signing


class Attempts(FakeMetadataAdapter):
    """External accepted-state seam, independent of request-supplied grants."""

    def __init__(self) -> None:
        """Start without an accepted attempt so unknown coordinates fail closed."""
        super().__init__(applied_revisions={1, 2, 3, 4})
        self.row: ActivityExecutionRecord | None = None
        self.fail_issuance_audit = False

    async def append_audit(
        self, workspace_id: object, event: AuditEvent, tx: object = None
    ) -> None:
        """Fail required issuance audit without changing the existing lease audit behavior."""
        if self.fail_issuance_audit and event.event_type == "connector.credential.issued":
            raise BackendUnavailable("private-audit-diagnostic")
        await super().append_audit(workspace_id, event, tx)

    async def check_activity_execution_ready(self) -> None:
        """The explicit test seam has no external migration dependency."""

    async def read_activity_execution(
        self, workspace_id: str, run_id: str, step_id: str, attempt: int
    ) -> ActivityExecutionRecord | None:
        """Expose only the exact workspace and attempt record."""
        row = self.row
        return (
            row
            if row
            and (workspace_id, run_id, step_id, attempt)
            == (
                row.workspace_id,
                row.run_id,
                row.step_id,
                row.attempt,
            )
            else None
        )

    async def claim_activity_execution(self, *args: object) -> ActivityExecutionRecord:
        """Issuance must never create or reassign accepted runtime authority."""
        raise AssertionError("issuer attempted to create runtime authority")

    async def write_activity_execution(
        self, *args: object, **kwargs: object
    ) -> ActivityExecutionRecord:
        """Issuance must never rewrite runtime ownership or authority."""
        raise AssertionError("issuer attempted to rewrite runtime authority")

    async def revoke_activity_authority(
        self, record: ActivityExecutionRecord
    ) -> ActivityExecutionRecord:
        """Only runtime ownership may revoke the accepted attempt."""
        raise AssertionError("issuer attempted to rewrite runtime authority")


class AtomicLeases(FakeLeaseAdapter):
    """Capacity seam shared by service instances; native concurrency is covered in Pg tests."""

    def __init__(self) -> None:
        """Own one lock for the explicit external-store test double."""
        super().__init__(applied_revisions={1})
        self.lock = asyncio.Lock()

    async def put_lease_bounded(self, workspace_id: WorkspaceId, lease: Lease, limit: int) -> Lease:
        """Match atomic capacity behavior while exercising the actual LeaseManager."""
        async with self.lock:
            count = await self.count_active_for_step_attempt(
                workspace_id,
                lease.run_id,
                lease.step_id,
                lease.attempt,
                datetime.now(UTC),
            )
            if count >= limit:
                raise LeaseBusy("capacity exhausted")
            return await self.put_lease(workspace_id, lease)


@pytest.mark.parametrize(
    "failure",
    [
        None,
        "full-chain",
        "unknown",
        "completed",
        "expired",
        "principal",
        "capability",
        "slot",
        "namespace",
        "unsigned-provenance",
        "provider",
        "transport",
        "disabled",
        "late-completion",
        "authority-revoked",
        "audit",
    ],
)
async def test_real_credential_issuer_scope_rotation_and_rejection(
    signing: Signing,
    failure: str | None,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """Use actual signed middleware, binder, registry, Dapr resolver and lease manager."""
    catalog = FakeCatalogAdapter(applied_revisions={1, 2})
    instances = FakeConnectorInstanceAdapter(applied_revisions={1})
    instance = replace(
        _make_instance(),
        credentials_authentication={"type": "x-dapr-secret", "secretName": "registry"},
        enabled=failure != "disabled",
    )
    await _seed(catalog, instances, instance)
    metadata, leases = Attempts(), AtomicLeases()
    metadata.fail_issuance_audit = failure == "audit"
    proof = "private-attempt-proof-" + "x" * 32
    accepted = {
        "credential_grant_hash": hashlib.sha256(proof.encode()).hexdigest(),
        "state": "running",
        "deadline": (datetime.now(UTC) + timedelta(minutes=5)).isoformat(),
        "execution_identity": {
            "workspaceId": "ws-1",
            "principalId": "executor",
            "bindingId": "stable",
        },
        "credential_scopes": {
            "source": {
                "instanceId": str(instance.instance_id),
                "kind": instance.type,
                "capabilities": ["events.delivery"],
            }
        },
    }
    if failure == "expired":
        accepted["deadline"] = (datetime.now(UTC) - timedelta(seconds=1)).isoformat()
    metadata.row = ActivityExecutionRecord(
        "ws-1",
        "run",
        "step",
        1,
        "b" * 64,
        "a" * 32,
        json.dumps(accepted),
        failure == "completed",
        authority_revoked=failure == "authority-revoked",
    )
    upstream_calls: list[httpx.Request] = []

    def upstream(request: httpx.Request) -> httpx.Response:
        """Emulate only the private Dapr secret boundary, not credential resolution."""
        upstream_calls.append(request)
        assert request.url.path == "/v1.0/secrets/kubernetes/registry"
        if failure == "late-completion":
            assert metadata.row is not None
            metadata.row = replace(metadata.row, completed=True)
        if failure == "provider":
            return httpx.Response(503, text="private-provider-value")
        if failure == "transport":
            raise httpx.ConnectError("private-provider-value", request=request)
        return httpx.Response(
            200,
            json={
                "username": "real-resolved-user",
                "token": f"private-provider-value-{len(upstream_calls)}",
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(upstream)) as http:
        registry = IdentityResolverRegistry()
        registry.register_vendor_resolver(
            DaprSecretResolver(
                http=HttpxAsyncHttpClient(http, owns_client=False), default_store="kubernetes"
            ),
            category=IdentityCategory.KMS,
        )
        binder = build_bind_for_step_service(
            catalog_store=catalog,
            instance_store=instances,
            metadata_store=metadata,
            identity_registry=registry,
            plugin_binder=StubPluginBinder(),
        )
        providers = replace(
            _build_providers(catalog=catalog, instances=instances, metadata=metadata),
            metadata_store=cast(MetadataStoreProvider, metadata),
            lease_store=cast(LeaseStoreProvider, leases),
            identity_registry=registry,
            bind_for_step_service=binder,
        )
        app = create_app(
            settings=replace(
                _BASE_SETTINGS,
                authz_endpoint="http://auth",
                app_api_token="local-token",
                runtime_namespace="custos",
            ),
            providers=providers,
            call_context_verifier=signing.verifier,
        )
        claims: dict[str, Any] = {
            "workspaceId": "ws-1",
            "actingPrincipalId": "other" if failure == "principal" else "executor",
            "callerComponent": "activity-runtime-manager",
            "permissions": ["connector:bind"],
        }
        if failure != "unsigned-provenance":
            claims["executionOperation"] = "connector-credentials"
        headers = {
            "x-custos-callctx": signing.token(claims),
            "dapr-api-token": "local-token",
            "dapr-caller-app-id": "activity-runtime-manager",
            "dapr-caller-namespace": "other" if failure == "namespace" else "custos",
        }
        base = {"runId": "run", "stepId": "step", "attempt": 2 if failure == "unknown" else 1}
        request = {
            **base,
            "operation": "issue",
            "slot": "other" if failure == "slot" else "source",
            "capability": "objects.read" if failure == "capability" else "events.delivery",
        }
        async with (
            app.router.lifespan_context(app),
            httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://connector",
            ) as client,
        ):
            if failure == "full-chain":
                identity = ExecutionIdentity("ws-1", "executor", "stable")
                bindings = ExecutionBindings(
                    (ExecutionBinding(identity, "credential-source", "token"),),
                    {"ws-1": "stable"},
                )

                async def control_plane(request: httpx.Request) -> httpx.Response:
                    """Simulate only Dapr transport/Auth issuance, not either credential API."""
                    if "/v1.0/secrets/" in request.url.path:
                        return httpx.Response(200, json={"token": "private-account-token"})
                    if request.url.path.endswith("/rpc/callctx.exchangeExecution"):
                        assert json.loads(request.content)["operation"] == "connector-credentials"
                        timestamp = int(time.time())
                        return httpx.Response(
                            200,
                            json={
                                "token": signing.token(claims),
                                "iat": timestamp,
                                "exp": timestamp + 300,
                            },
                        )
                    assert proof.encode() not in request.content
                    forwarded = await client.post(
                        "/internal/v1/activity-credentials",
                        content=request.content,
                        headers={
                            **request.headers,
                            **headers,
                            "x-custos-callctx": request.headers["x-custos-callctx"],
                        },
                    )
                    assert forwarded.status_code == 200, forwarded.text
                    return httpx.Response(forwarded.status_code, json=forwarded.json())

                async with httpx.AsyncClient(
                    transport=httpx.MockTransport(control_plane)
                ) as control:
                    runtime = FastAPI()
                    runtime.include_router(proxy_router)
                    runtime.state.credential_proxy = CredentialProxy(
                        metadata,
                        ExecutionContextClient(control, bindings),
                        control,
                        "http://connector",
                    )
                    bundle = RuntimeBundle.model_validate(
                        {
                            "workspaceId": "ws-1",
                            "runId": "run",
                            "stepId": "step",
                            "attempt": 1,
                            "endpoint": "https://runtime",
                            "proof": proof,
                        }
                    )
                    triple = BoundTriple("run", "step", 1)
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=runtime)
                    ) as proxy_http:
                        sidecar = create_broker_app(
                            bundle,
                            BootstrapTokenVerifier(key=b"k" * 32, triple=triple),
                            http_client=proxy_http,
                        )
                        async with (
                            sidecar.router.lifespan_context(sidecar),
                            httpx.AsyncClient(
                                transport=httpx.ASGITransport(app=sidecar),
                                base_url="http://uds",
                            ) as activity,
                        ):
                            token = mint_bootstrap_token(key=b"k" * 32, triple=triple, ttl_sec=300)
                            result = await activity.get(
                                "/v1/token?slot=source&purpose=events.delivery",
                                headers={"Custos-Sidecar-Token": token},
                            )
                            assert result.status_code == 200, result.text
                            assert (
                                result.json()["credential"]["material"]["token"]
                                == "private-provider-value-1"
                            )
                            assert result.json()["credential"]["expiresAt"] is None
                            assert (
                                proof not in result.text
                                and "private-account-token" not in result.text
                            )
                assert len(upstream_calls) == 1
                return
            response = await client.post(
                "/internal/v1/activity-credentials", json=request, headers=headers
            )
            if failure is not None:
                assert response.status_code in {403, 503}, response.text
                assert "private-provider-value" not in response.text
                assert "private-provider-value" not in caplog.text
                if failure not in {"provider", "transport", "late-completion", "audit"}:
                    assert not upstream_calls
                return
            assert response.status_code == 200, response.text
            payload = response.json()
            assert payload["credential"]["material"] == {
                "username": "real-resolved-user",
                "token": "private-provider-value-1",
            }
            assert payload["credential"]["expiresAt"] is None
            assert payload["lease"]["expiresAt"] is not None
            assert response.headers["cache-control"] == "no-store"
            lease_id = payload["lease"]["leaseId"]
            refreshed = await client.post(
                "/internal/v1/activity-credentials",
                json={**base, "operation": "refresh", "leaseId": lease_id},
                headers=headers,
            )
            assert refreshed.status_code == 200, refreshed.text
            assert refreshed.json()["credential"]["material"]["token"] == "private-provider-value-2"
            await leases.revoke_lease(WorkspaceId("ws-1"), lease_id, "operator", datetime.now(UTC))
            revoked = await client.post(
                "/internal/v1/activity-credentials",
                json={**base, "operation": "refresh", "leaseId": lease_id},
                headers=headers,
            )
            assert revoked.status_code == 403
            assert len(upstream_calls) == 2
            capacity = await asyncio.gather(
                *(
                    client.post("/internal/v1/activity-credentials", json=request, headers=headers)
                    for _ in range(17)
                )
            )
            assert sum(item.status_code == 200 for item in capacity) == 16
            assert sum(item.status_code == 429 for item in capacity) == 1
