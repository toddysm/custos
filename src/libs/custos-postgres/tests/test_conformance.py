"""Conformance tests for Postgres adapters.

These tests verify that Postgres adapters can be instantiated and configured
properly. Full conformance tests for AuthStoreProvider, CatalogStoreProvider,
DefinitionStoreProvider, and MetadataStoreProvider will be added when SPL
defines conformance test base classes for these interfaces.

Currently this verifies:
- Adapters can be imported
- Adapters can be instantiated with proper configuration
- Database connectivity via testcontainers

Run with: pytest tests/test_conformance.py -v -m integration
Skip without Postgres: pytest tests/test_conformance.py -v -m "not integration"
"""

from __future__ import annotations

import asyncio

import pytest

pytest.importorskip("custos_pg")

from custos_pg.adapters.auth import PgAuthAdapter
from custos_pg.adapters.catalog import PgCatalogAdapter
from custos_pg.adapters.definition import PgDefinitionAdapter
from custos_pg.adapters.metadata import PgMetadataAdapter


@pytest.mark.integration
class TestPostgresAdapterInstantiation:
    """Verify Postgres adapters can be instantiated and connected.

    These tests are placeholders pending SPL conformance test base classes
    for AuthStoreProvider, CatalogStoreProvider, DefinitionStoreProvider,
    and MetadataStoreProvider interfaces.
    """

    @pytest.fixture(scope="class", autouse=True)
    def _check_postgres_available(self, pg_pool: object) -> None:
        """Verify Postgres pool is available (via conftest fixture)."""
        # pg_pool fixture from conftest.py handles database setup/connectivity
        # If we reach here, Postgres is available
        assert pg_pool is not None

    @pytest.mark.asyncio
    async def test_postgres_auth_adapter_instantiation(
        self, pg_pool: object
    ) -> None:
        """Auth adapter can be instantiated with the shared Postgres pool."""
        adapter = PgAuthAdapter(pool=pg_pool)
        assert adapter is not None
        # Full interface tests pending SPL conformance definition

    @pytest.mark.asyncio
    async def test_postgres_catalog_adapter_instantiation(
        self, pg_pool: object
    ) -> None:
        """Catalog adapter can be instantiated with the shared Postgres pool."""
        adapter = PgCatalogAdapter(pool=pg_pool)
        assert adapter is not None
        # Full interface tests pending SPL conformance definition

    @pytest.mark.asyncio
    async def test_postgres_definition_adapter_instantiation(
        self, pg_pool: object
    ) -> None:
        """Definition adapter can be instantiated with the shared Postgres pool."""
        adapter = PgDefinitionAdapter(pool=pg_pool)
        assert adapter is not None
        # Full interface tests pending SPL conformance definition

    @pytest.mark.asyncio
    async def test_postgres_metadata_adapter_instantiation(
        self, pg_pool: object
    ) -> None:
        """Metadata adapter can be instantiated with the shared Postgres pool."""
        adapter = PgMetadataAdapter(pool=pg_pool)
        assert adapter is not None
        # Full interface tests pending SPL conformance definition
