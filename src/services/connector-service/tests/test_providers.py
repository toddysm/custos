"""Tests for :mod:`custos_connector.providers` (CONN-IMPL-003)."""

from __future__ import annotations

import pytest
from custos_spl import MigrationRequired

from custos_connector.identity import IdentityResolverRegistry
from custos_connector.providers import (
    Providers,
    schema_gate_explainer,
    verify_schema_revisions,
)
from tests._fakes import (
    FakeCatalogAdapter,
    FakeConnectorInstanceAdapter,
    FakeLeaseAdapter,
    FakeMetadataAdapter,
    build_bind_for_step_service,
    build_lease_manager,
)


@pytest.fixture
def providers() -> Providers:
    leases = FakeLeaseAdapter()
    metadata = FakeMetadataAdapter()
    return Providers(
        catalog_store=FakeCatalogAdapter(),  # type: ignore[arg-type]
        instance_store=FakeConnectorInstanceAdapter(),  # type: ignore[arg-type]
        lease_store=leases,  # type: ignore[arg-type]
        metadata_store=metadata,  # type: ignore[arg-type]
        identity_registry=IdentityResolverRegistry(),
        bind_for_step_service=build_bind_for_step_service(),
        lease_manager=build_lease_manager(lease_store=leases, metadata_store=metadata),
    )


async def test_verify_schema_revisions_refreshes_each_adapter(providers: Providers) -> None:
    await verify_schema_revisions(providers)
    assert providers.catalog_store.refresh_calls == 1  # type: ignore[attr-defined]
    assert providers.metadata_store.refresh_calls == 1  # type: ignore[attr-defined]


async def test_verify_schema_revisions_passes_when_ledger_is_current(
    providers: Providers,
) -> None:
    await verify_schema_revisions(providers)  # does not raise


async def test_verify_schema_revisions_raises_when_catalog_store_is_behind() -> None:
    leases = FakeLeaseAdapter()
    metadata = FakeMetadataAdapter()
    providers = Providers(
        catalog_store=FakeCatalogAdapter(applied_revisions=set()),  # type: ignore[arg-type]
        instance_store=FakeConnectorInstanceAdapter(),  # type: ignore[arg-type]
        lease_store=leases,  # type: ignore[arg-type]
        metadata_store=metadata,  # type: ignore[arg-type]
        identity_registry=IdentityResolverRegistry(),
        bind_for_step_service=build_bind_for_step_service(),
        lease_manager=build_lease_manager(lease_store=leases, metadata_store=metadata),
    )
    with pytest.raises(MigrationRequired) as exc_info:
        await verify_schema_revisions(providers)
    assert ("CatalogStoreProvider", 2) in exc_info.value.gaps


async def test_verify_schema_revisions_raises_when_metadata_store_is_behind() -> None:
    leases = FakeLeaseAdapter()
    metadata = FakeMetadataAdapter(applied_revisions=set())
    providers = Providers(
        catalog_store=FakeCatalogAdapter(),  # type: ignore[arg-type]
        instance_store=FakeConnectorInstanceAdapter(),  # type: ignore[arg-type]
        lease_store=leases,  # type: ignore[arg-type]
        metadata_store=metadata,  # type: ignore[arg-type]
        identity_registry=IdentityResolverRegistry(),
        bind_for_step_service=build_bind_for_step_service(),
        lease_manager=build_lease_manager(lease_store=leases, metadata_store=metadata),
    )
    with pytest.raises(MigrationRequired) as exc_info:
        await verify_schema_revisions(providers)
    assert ("MetadataStoreProvider", 4) in exc_info.value.gaps


async def test_verify_schema_revisions_raises_when_instance_store_is_behind() -> None:
    leases = FakeLeaseAdapter()
    metadata = FakeMetadataAdapter()
    providers = Providers(
        catalog_store=FakeCatalogAdapter(),  # type: ignore[arg-type]
        instance_store=FakeConnectorInstanceAdapter(  # type: ignore[arg-type]
            applied_revisions=set()
        ),
        lease_store=leases,  # type: ignore[arg-type]
        metadata_store=metadata,  # type: ignore[arg-type]
        identity_registry=IdentityResolverRegistry(),
        bind_for_step_service=build_bind_for_step_service(),
        lease_manager=build_lease_manager(lease_store=leases, metadata_store=metadata),
    )
    with pytest.raises(MigrationRequired) as exc_info:
        await verify_schema_revisions(providers)
    assert ("ConnectorInstanceStoreProvider", 1) in exc_info.value.gaps


async def test_verify_schema_revisions_raises_when_lease_store_is_behind() -> None:
    leases = FakeLeaseAdapter(applied_revisions=set())
    metadata = FakeMetadataAdapter()
    providers = Providers(
        catalog_store=FakeCatalogAdapter(),  # type: ignore[arg-type]
        instance_store=FakeConnectorInstanceAdapter(),  # type: ignore[arg-type]
        lease_store=leases,  # type: ignore[arg-type]
        metadata_store=metadata,  # type: ignore[arg-type]
        identity_registry=IdentityResolverRegistry(),
        bind_for_step_service=build_bind_for_step_service(),
        lease_manager=build_lease_manager(lease_store=leases, metadata_store=metadata),
    )
    with pytest.raises(MigrationRequired) as exc_info:
        await verify_schema_revisions(providers)
    assert ("LeaseStoreProvider", 1) in exc_info.value.gaps


async def test_verify_schema_revisions_collects_gaps_from_both_stores() -> None:
    leases = FakeLeaseAdapter()
    metadata = FakeMetadataAdapter(applied_revisions=set())
    providers = Providers(
        catalog_store=FakeCatalogAdapter(applied_revisions=set()),  # type: ignore[arg-type]
        instance_store=FakeConnectorInstanceAdapter(),  # type: ignore[arg-type]
        lease_store=leases,  # type: ignore[arg-type]
        metadata_store=metadata,  # type: ignore[arg-type]
        identity_registry=IdentityResolverRegistry(),
        bind_for_step_service=build_bind_for_step_service(),
        lease_manager=build_lease_manager(lease_store=leases, metadata_store=metadata),
    )
    with pytest.raises(MigrationRequired) as exc_info:
        await verify_schema_revisions(providers)
    gaps = set(exc_info.value.gaps)
    assert ("CatalogStoreProvider", 2) in gaps
    assert ("MetadataStoreProvider", 4) in gaps


def test_schema_gate_explainer_mentions_each_interface_and_remediation() -> None:
    err = MigrationRequired(
        [
            ("CatalogStoreProvider", 2),
            ("MetadataStoreProvider", 4),
        ],
    )
    text = schema_gate_explainer(err)
    assert "CatalogStoreProvider@rev2" in text
    assert "MetadataStoreProvider@rev4" in text
    assert "custos migrate up" in text
    assert "CONN_CATALOG_STORE" in text
    assert "CONN_METADATA_STORE" in text


def test_load_providers_constructs_postgres_adapters_from_settings() -> None:
    """load_providers imports the real Pg adapters which require a DSN but
    do not connect synchronously thanks to LazyPool."""
    from custos_connector.providers import load_providers
    from custos_connector.settings import load_settings

    settings = load_settings(
        {
            "CONN_CATALOG_STORE": "postgresql://u:p@h:5432/cat",
            "CONN_METADATA_STORE": "postgresql://u:p@h:5432/meta",
            "CONN_CATALOG_ENDPOINT": "http://catalog-service:8080",
        },
    )
    bundle = load_providers(settings)
    assert bundle.catalog_store is not None
    assert bundle.metadata_store is not None
    # Each adapter must declare its own interface name once refreshed.
    assert hasattr(bundle.catalog_store, "declared_revisions")
    assert hasattr(bundle.metadata_store, "declared_revisions")


def test_load_identity_registry_registers_dapr_secret_vendor_resolver() -> None:
    """The first-party x-dapr-secret resolver is wired at startup (CONN-DAPRSEC-01)."""
    from custos_connector.identity.resolvers.dapr_secret import DaprSecretResolver
    from custos_connector.loader.identity import IdentityCategory
    from custos_connector.providers import load_identity_registry

    registry = load_identity_registry(
        metadata_store=FakeMetadataAdapter(),  # type: ignore[arg-type]
        dapr_secret_store="custos-secretstore",
    )
    # The vendor resolver is registered with the KMS identity category and
    # carries the operator-configured default store name.
    assert "x-dapr-secret" in registry._vendor
    resolver = registry._vendor["x-dapr-secret"]
    assert isinstance(resolver, DaprSecretResolver)
    assert registry._vendor_categories["x-dapr-secret"] is IdentityCategory.KMS
    assert resolver._default_store == "custos-secretstore"


# ---------------------------------------------------------------------------
# CONN-REG (#898 T1/T2) — connector-type registration Loader wiring
# ---------------------------------------------------------------------------

_REG_ENV = {
    "CONN_CATALOG_STORE": "postgresql://u:p@h:5432/cat",
    "CONN_METADATA_STORE": "postgresql://u:p@h:5432/meta",
    "CONN_CATALOG_ENDPOINT": "http://catalog-service:8080",
}


async def test_load_registration_registry_client_returns_none_when_url_unset() -> None:
    from custos_connector.providers import load_registration_registry_client
    from custos_connector.settings import load_settings

    settings = load_settings(_REG_ENV)
    assert load_registration_registry_client(settings) is None


async def test_load_registration_registry_client_builds_client_with_auth() -> None:
    from custos_connector.providers import load_registration_registry_client
    from custos_connector.settings import load_settings

    settings = load_settings(
        {
            **_REG_ENV,
            "CONN_CONNECTOR_REGISTRY_URL": "https://ghcr.io",
            "CONN_CONNECTOR_REGISTRY_TOKEN": "tok",
        }
    )
    client = load_registration_registry_client(settings)
    assert client is not None
    try:
        assert client.base_url.host == "ghcr.io"
        assert client.headers["authorization"] == "Bearer tok"
    finally:
        await client.aclose()


async def test_load_registration_registry_client_anonymous_when_token_unset() -> None:
    from custos_connector.providers import load_registration_registry_client
    from custos_connector.settings import load_settings

    settings = load_settings({**_REG_ENV, "CONN_CONNECTOR_REGISTRY_URL": "https://ghcr.io"})
    client = load_registration_registry_client(settings)
    assert client is not None
    try:
        assert "authorization" not in client.headers
    finally:
        await client.aclose()


async def test_load_providers_leaves_loader_unset_without_registry_url() -> None:
    from custos_connector.providers import load_providers
    from custos_connector.settings import load_settings

    bundle = load_providers(load_settings(_REG_ENV))
    try:
        assert bundle.loader is None
        assert bundle.registration_registry_client is None
    finally:
        await bundle.identity_registry.aclose()


async def test_load_providers_wires_loader_with_registry_vendor_categories() -> None:
    """The Loader's vendor identity-category overrides are sourced from the
    resolver registry snapshot — single source of truth, no drift (#896)."""
    from custos_connector.loader.identity import IdentityCategory
    from custos_connector.providers import load_providers
    from custos_connector.settings import load_settings

    bundle = load_providers(
        load_settings({**_REG_ENV, "CONN_CONNECTOR_REGISTRY_URL": "https://ghcr.io"})
    )
    try:
        assert bundle.loader is not None
        assert bundle.registration_registry_client is not None
        # x-dapr-secret -> KMS is registered on the resolver registry...
        assert bundle.identity_registry.vendor_categories["x-dapr-secret"] is IdentityCategory.KMS
        # ...and the Loader was fed exactly that snapshot (drift guard).
        assert bundle.loader._vendor_overrides is not None
        assert dict(bundle.loader._vendor_overrides) == dict(
            bundle.identity_registry.vendor_categories
        )
    finally:
        await bundle.identity_registry.aclose()
        if bundle.registration_registry_client is not None:
            await bundle.registration_registry_client.aclose()
