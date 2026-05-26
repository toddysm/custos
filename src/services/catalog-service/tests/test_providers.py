"""Tests for :mod:`custos_catalog.providers` (CS-IMPL-003)."""

from __future__ import annotations

import pytest
from custos_spl import MigrationRequired

from custos_catalog.providers import (
    Providers,
    schema_gate_explainer,
    verify_schema_revisions,
)
from tests._fakes import FakeCatalogAdapter, FakeDefinitionAdapter, FakeMetadataAdapter


@pytest.fixture
def providers() -> Providers:
    return Providers(
        definition_store=FakeDefinitionAdapter(),  # type: ignore[arg-type]
        catalog_store=FakeCatalogAdapter(),  # type: ignore[arg-type]
        metadata_store=FakeMetadataAdapter(),  # type: ignore[arg-type]
    )


async def test_verify_schema_revisions_refreshes_each_adapter(providers: Providers) -> None:
    await verify_schema_revisions(providers)
    assert providers.definition_store.refresh_calls == 1  # type: ignore[attr-defined]
    assert providers.catalog_store.refresh_calls == 1  # type: ignore[attr-defined]
    assert providers.metadata_store.refresh_calls == 1  # type: ignore[attr-defined]


async def test_verify_schema_revisions_passes_when_ledger_is_current(
    providers: Providers,
) -> None:
    await verify_schema_revisions(providers)  # does not raise


async def test_verify_schema_revisions_raises_when_definition_store_is_behind() -> None:
    providers = Providers(
        definition_store=FakeDefinitionAdapter(applied_revisions=set()),  # type: ignore[arg-type]
        catalog_store=FakeCatalogAdapter(),  # type: ignore[arg-type]
        metadata_store=FakeMetadataAdapter(),  # type: ignore[arg-type]
    )
    with pytest.raises(MigrationRequired) as exc_info:
        await verify_schema_revisions(providers)
    assert ("DefinitionStoreProvider", 1) in exc_info.value.gaps


async def test_verify_schema_revisions_raises_when_catalog_store_is_behind() -> None:
    providers = Providers(
        definition_store=FakeDefinitionAdapter(),  # type: ignore[arg-type]
        catalog_store=FakeCatalogAdapter(applied_revisions=set()),  # type: ignore[arg-type]
        metadata_store=FakeMetadataAdapter(),  # type: ignore[arg-type]
    )
    with pytest.raises(MigrationRequired) as exc_info:
        await verify_schema_revisions(providers)
    assert ("CatalogStoreProvider", 2) in exc_info.value.gaps


async def test_verify_schema_revisions_raises_when_metadata_store_is_behind() -> None:
    providers = Providers(
        definition_store=FakeDefinitionAdapter(),  # type: ignore[arg-type]
        catalog_store=FakeCatalogAdapter(),  # type: ignore[arg-type]
        metadata_store=FakeMetadataAdapter(applied_revisions=set()),  # type: ignore[arg-type]
    )
    with pytest.raises(MigrationRequired) as exc_info:
        await verify_schema_revisions(providers)
    assert ("MetadataStoreProvider", 4) in exc_info.value.gaps


def test_schema_gate_explainer_mentions_each_interface_and_remediation() -> None:
    err = MigrationRequired(
        [
            ("DefinitionStoreProvider", 1),
            ("CatalogStoreProvider", 2),
            ("MetadataStoreProvider", 4),
        ],
    )
    text = schema_gate_explainer(err)
    assert "DefinitionStoreProvider@rev1" in text
    assert "CatalogStoreProvider@rev2" in text
    assert "MetadataStoreProvider@rev4" in text
    assert "custos migrate up" in text
    assert "CAT_DEFINITION_STORE" in text
    assert "CAT_CATALOG_STORE" in text
    assert "CAT_METADATA_STORE" in text


def test_load_providers_constructs_postgres_adapters_from_settings() -> None:
    # load_providers imports the real PgDefinitionAdapter/PgCatalogAdapter/
    # PgMetadataAdapter which require a DSN but do not connect synchronously
    # thanks to LazyPool.
    from custos_catalog.providers import load_providers
    from custos_catalog.settings import load_settings

    settings = load_settings(
        {
            "CAT_DEFINITION_STORE": "postgresql://u:p@h:5432/def",
            "CAT_CATALOG_STORE": "postgresql://u:p@h:5432/cat",
            "CAT_METADATA_STORE": "postgresql://u:p@h:5432/meta",
            "CAT_CONNECTOR_ENDPOINT": "http://connector-service:8080",
        },
    )
    bundle = load_providers(settings)
    assert bundle.definition_store is not None
    assert bundle.catalog_store is not None
    assert bundle.metadata_store is not None
    # Each adapter must declare its own interface name once refreshed.
    assert hasattr(bundle.definition_store, "declared_revisions")
    assert hasattr(bundle.catalog_store, "declared_revisions")
    assert hasattr(bundle.metadata_store, "declared_revisions")
