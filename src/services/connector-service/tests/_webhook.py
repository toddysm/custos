"""Shared webhook fixtures for real HTTP routes over memory and PostgreSQL providers."""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any, ClassVar, cast

from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.catalog_store import CatalogStoreProvider
from custos_spl.interfaces.connector_instance_store import (
    ConnectorInstance,
    ConnectorInstanceStoreProvider,
)
from custos_spl.interfaces.lease_store import LeaseStoreProvider
from custos_spl.interfaces.metadata_store import MetadataStoreProvider

from custos_connector.identity import (
    IdentityResolverError,
    IdentityResolverRegistry,
    ResolvedIdentity,
)
from custos_connector.identity.protocols import IdentityResolverContext
from custos_connector.loader.identity import IdentityCategory
from custos_connector.providers import Providers
from custos_connector.settings import Settings, load_settings
from tests._fakes import (
    FakeCatalogAdapter,
    FakeConnectorInstanceAdapter,
    FakeLeaseAdapter,
    FakeMetadataAdapter,
    build_bind_for_step_service,
    build_lease_manager,
)

MESH_HEADERS = {
    "dapr-api-token": "unit-sidecar-hop",
    "dapr-caller-app-id": "trigger-service",
    "dapr-caller-namespace": "custos",
}
RAW = b'{"events":[{"eventId":"first","eventType":"registry.push","value":42}]}'


class RotatingResolver:
    """Exercise the real registry while controlling upstream rotation and availability."""

    authentication_type: ClassVar[str] = "azure-key-vault"
    category: ClassVar[IdentityCategory] = IdentityCategory.KMS

    def __init__(self) -> None:
        """Start with independent, test-only signing values for colliding instance IDs."""
        self.secrets: dict[str, str] = {"a": "unit-key-a", "b": "unit-key-b"}
        self.calls: list[tuple[str, str, str]] = []
        self.failure: IdentityResolverError | None = None

    async def resolve(
        self, *, credentials_authentication: Mapping[str, Any], context: IdentityResolverContext
    ) -> ResolvedIdentity:
        """Resolve the configured reference, recording the exact registry scope."""
        name = str(credentials_authentication["secretName"])
        self.calls.append((context.workspace_id, context.instance_id, name))
        if self.failure is not None:
            raise self.failure
        material = {"secret": self.secrets[name]} if name in self.secrets else {}
        return ResolvedIdentity.build(
            authentication_type=self.authentication_type,
            category=self.category,
            material=material,
            descriptor="unit-reference",
            issued_at=context.now(),
            expires_at=None,
        )


def settings(dsn: str = "postgresql://unused/unused") -> Settings:
    """Configure private mesh verification without changing the existing user development shim."""
    return load_settings(
        {
            "CONN_CATALOG_STORE": dsn,
            "CONN_METADATA_STORE": dsn,
            "CONN_CATALOG_ENDPOINT": "http://catalog-service:8080",
            "APP_API_TOKEN": MESH_HEADERS["dapr-api-token"],
            "CONN_TRIGGER_NAMESPACE": "custos",
        }
    )


def memory_providers(resolver: RotatingResolver) -> Providers:
    """Compose the repository's existing partial provider fakes for HTTP contract tests."""
    catalog = cast(CatalogStoreProvider, FakeCatalogAdapter())
    instances = cast(ConnectorInstanceStoreProvider, FakeConnectorInstanceAdapter())
    metadata = cast(MetadataStoreProvider, FakeMetadataAdapter())
    leases = cast(LeaseStoreProvider, FakeLeaseAdapter())
    registry = IdentityResolverRegistry(resolvers=[resolver])
    return Providers(
        catalog_store=catalog,
        instance_store=instances,
        metadata_store=metadata,
        lease_store=leases,
        identity_registry=registry,
        bind_for_step_service=build_bind_for_step_service(
            catalog_store=catalog,
            instance_store=instances,
            metadata_store=metadata,
            identity_registry=registry,
        ),
        lease_manager=build_lease_manager(lease_store=leases, metadata_store=metadata),
    )


async def seed(
    providers: Providers,
    workspace: str,
    scheme: str = "hmac-sha256",
    signature_header: str = "x-custos-signature",
) -> None:
    """Store an actual active pair and immutable catalog, with references rather than secrets."""
    await providers.catalog_store.put_connector_type_version(
        type="registry",
        version="1",
        digest="sha256:unit",
        image_ref="unit",
        normalized_manifest={
            "spec": {"events": {"delivery": ["push"], "produced": ["registry.push"]}}
        },
    )
    now = datetime.now(UTC)
    await providers.instance_store.put_connector_instance(
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
                    "signatureHeader": signature_header,
                },
            },
            used_capabilities=None,
            created_at=now,
            updated_at=now,
        ),
    )


def signature(raw: bytes, secret: str = "unit-key-a") -> str:
    """Sign exact test bytes using the configured generic HMAC-SHA256 wire format."""
    return "sha256=" + hmac.new(secret.encode(), raw, hashlib.sha256).hexdigest()


def envelope(
    raw: bytes = RAW, *, workspace: str = "a", headers: dict[str, str] | None = None
) -> dict[str, Any]:
    """Build the private version-one request without confusing mesh and vendor headers."""
    return {
        "schemaVersion": "1",
        "workspaceId": workspace,
        "connectorInstanceId": "same",
        "bodyBase64": base64.b64encode(raw).decode(),
        "headers": headers if headers is not None else {"x-custos-signature": signature(raw)},
    }


def encoded_envelope(raw: bytes) -> bytes:
    """Serialize a request for exact byte-boundary and streamed body tests."""
    return json.dumps(envelope(raw), separators=(",", ":")).encode()
