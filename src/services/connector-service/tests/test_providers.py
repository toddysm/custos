"""Tests for :mod:`custos_connector.providers` (CONN-IMPL-003)."""

from __future__ import annotations

import pytest
from custos_spl import MigrationRequired

from custos_connector.providers import (
    Providers,
    schema_gate_explainer,
    verify_schema_revisions,
)
from tests._fakes import (
    FakeCatalogAdapter,
    FakeConnectorInstanceAdapter,
    FakeMetadataAdapter,
)


@pytest.fixture
def providers() -> Providers:
    return Providers(
        catalog_store=FakeCatalogAdapter(),  # type: ignore[arg-type]
        instance_store=FakeConnectorInstanceAdapter(),  # type: ignore[arg-type]
        metadata_store=FakeMetadataAdapter(),  # type: ignore[arg-type]
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
    providers = Providers(
        catalog_store=FakeCatalogAdapter(applied_revisions=set()),  # type: ignore[arg-type]
        instance_store=FakeConnectorInstanceAdapter(),  # type: ignore[arg-type]
        metadata_store=FakeMetadataAdapter(),  # type: ignore[arg-type]
    )
    with pytest.raises(MigrationRequired) as exc_info:
        await verify_schema_revisions(providers)
    assert ("CatalogStoreProvider", 1) in exc_info.value.gaps


async def test_verify_schema_revisions_raises_when_metadata_store_is_behind() -> None:
    providers = Providers(
        catalog_store=FakeCatalogAdapter(),  # type: ignore[arg-type]
        instance_store=FakeConnectorInstanceAdapter(),  # type: ignore[arg-type]
        metadata_store=FakeMetadataAdapter(applied_revisions=set()),  # type: ignore[arg-type]
    )
    with pytest.raises(MigrationRequired) as exc_info:
        await verify_schema_revisions(providers)
    assert ("MetadataStoreProvider", 4) in exc_info.value.gaps


async def test_verify_schema_revisions_raises_when_instance_store_is_behind() -> None:
    providers = Providers(
        catalog_store=FakeCatalogAdapter(),  # type: ignore[arg-type]
        instance_store=FakeConnectorInstanceAdapter(  # type: ignore[arg-type]
            applied_revisions=set()
        ),
        metadata_store=FakeMetadataAdapter(),  # type: ignore[arg-type]
    )
    with pytest.raises(MigrationRequired) as exc_info:
        await verify_schema_revisions(providers)
    assert ("ConnectorInstanceStoreProvider", 1) in exc_info.value.gaps


async def test_verify_schema_revisions_collects_gaps_from_both_stores() -> None:
    providers = Providers(
        catalog_store=FakeCatalogAdapter(applied_revisions=set()),  # type: ignore[arg-type]
        instance_store=FakeConnectorInstanceAdapter(),  # type: ignore[arg-type]
        metadata_store=FakeMetadataAdapter(applied_revisions=set()),  # type: ignore[arg-type]
    )
    with pytest.raises(MigrationRequired) as exc_info:
        await verify_schema_revisions(providers)
    gaps = set(exc_info.value.gaps)
    assert ("CatalogStoreProvider", 1) in gaps
    assert ("MetadataStoreProvider", 4) in gaps


def test_schema_gate_explainer_mentions_each_interface_and_remediation() -> None:
    err = MigrationRequired(
        [
            ("CatalogStoreProvider", 1),
            ("MetadataStoreProvider", 4),
        ],
    )
    text = schema_gate_explainer(err)
    assert "CatalogStoreProvider@rev1" in text
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
