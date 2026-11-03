"""Signed background workflow reads without generic or cross-workspace Catalog authority."""

from __future__ import annotations

import base64
import json
import time
from dataclasses import replace
from typing import Any

import jwt
import pytest
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from custos_callctx import CallContextVerifier, JwksCache
from fastapi.testclient import TestClient

from custos_catalog import create_app
from custos_catalog.providers import Providers
from custos_catalog.settings import load_settings
from tests.api.conftest import (
    _ENV,
    FakeCatalogStore,
    FakeDefinitionStore,
    FakeMetadataStore,
    admin_header,
    minimal_workflow,
    seed_builtin_echo,
)


@pytest.mark.parametrize(
    "operation,workspace,status,resource",
    [
        (None, "ws-1", 403, "workflow"),
        ("catalog-read", "ws-1", 200, "workflow"),
        ("catalog-read", "ws-2", 403, "workflow"),
        ("catalog-activity-read", "ws-1", 200, "activity"),
        ("catalog-activity-read", "ws-2", 403, "activity"),
        ("catalog-read", "ws-1", 403, "activity"),
        ("catalog-activity-read", "ws-1", 403, "workflow"),
    ],
)
def test_execution_read_is_credential_backed_and_workspace_scoped(
    client: TestClient,
    stores: tuple[FakeDefinitionStore, FakeCatalogStore, FakeMetadataStore],
    providers: Providers,
    operation: str | None,
    workspace: str,
    status: int,
    resource: str,
) -> None:
    """Use the actual factory, signature verifier and read manager with narrow permissions."""
    seed_builtin_echo(stores[1])
    seeded = client.post(
        "/v1/workspaces/ws-1/workflows",
        json={"definition": json.dumps(minimal_workflow())},
        headers=admin_header(),
    )
    assert seeded.status_code == 201
    key = Ed25519PrivateKey.generate()
    public = base64.urlsafe_b64encode(key.public_key().public_bytes_raw()).decode().rstrip("=")

    async def fetch(url: str) -> tuple[dict[str, str], dict[str, Any]]:
        """Supply only the actual test public key to the production verifier."""
        assert url == "http://auth/.well-known/jwks.json"
        return {"cache-control": "max-age=300"}, {
            "keys": [{"kty": "OKP", "crv": "Ed25519", "kid": "unit", "x": public}],
        }

    verifier = CallContextVerifier(
        jwks_cache=JwksCache(
            jwks_url="http://auth/.well-known/jwks.json",
            fetcher=fetch,
        )
    )
    app = create_app(
        settings=replace(
            load_settings(_ENV), authz_endpoint="http://auth", environment="production"
        ),
        providers=providers,
        call_context_verifier=verifier,
    )
    now = int(time.time())
    claims: dict[str, Any] = {
        "iss": "custos-auth",
        "aud": "custos.internal",
        "iat": now,
        "exp": now + 300,
        "jti": "unit",
        "actingPrincipalId": "executor",
        "workspaceId": workspace,
        "callerComponent": "workflow-service",
        "permissions": [
            "catalog:activity-types:read"
            if operation == "catalog-activity-read"
            else "catalog:workflows:read"
        ],
    }
    if operation is not None:
        claims["executionOperation"] = operation
    token = jwt.encode(claims, key, algorithm="EdDSA", headers={"kid": "unit"})
    with TestClient(app) as signed_client:
        response = signed_client.get(
            "/rpc/v1/workflow-versions/ws-1/orders@1"
            if resource == "workflow"
            else "/v1/workspaces/ws-1/activity-types/custos.builtin/echo@1.0.0",
            headers={"x-custos-callctx": token},
        )
        assert response.status_code == status, response.text
        if status == 200:
            if resource == "workflow":
                assert response.json()["workspaceId"] == "ws-1"
                assert response.json()["workflowName"] == "orders"
            else:
                assert response.json()["namespace"] == "custos.builtin"
                assert response.json()["version"] == "1.0.0"
            assert (
                signed_client.get(
                    "/rpc/v1/connector-types/http@1", headers={"x-custos-callctx": token}
                ).status_code
                == 403
            )
