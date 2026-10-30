"""Verify exact scoped credential references and normalized batches with real PostgreSQL reads."""

from dataclasses import replace
from typing import cast

import asyncpg
import pytest
from custos_pg import PgCatalogAdapter, PgConnectorInstanceAdapter
from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.catalog_store import CatalogStoreProvider
from custos_spl.interfaces.connector_instance_store import ConnectorInstanceStoreProvider

from custos_connector.listen.verification import (
    WebhookVerificationError,
    WebhookVerificationService,
)
from tests._webhook import RAW, RotatingResolver, memory_providers, seed, signature

pytestmark = pytest.mark.integration


@pytest.mark.parametrize("scheme", ["hmac-sha256", "bearer"])
async def test_persisted_pairs_rotation_and_fresh_adapter(pg_dsn: str, scheme: str) -> None:
    """Fresh adapters preserve scope, rotation and persisted disable state."""
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=3)
    assert pool is not None
    try:
        resolver = RotatingResolver()
        providers = replace(
            memory_providers(resolver),
            catalog_store=cast(CatalogStoreProvider, PgCatalogAdapter(pool=pool)),
            instance_store=cast(
                ConnectorInstanceStoreProvider, PgConnectorInstanceAdapter(pool=pool)
            ),
        )
        await seed(providers, "a", scheme)
        await seed(providers, "b", scheme)
        service = WebhookVerificationService(
            providers.instance_store, providers.catalog_store, providers.identity_registry
        )
        headers = (
            {"authorization": "Bearer unit-key-a"}
            if scheme == "bearer"
            else {"x-custos-signature": signature(RAW)}
        )
        before = await providers.instance_store.get_connector_instance(
            WorkspaceId("a"), ConnectorInstanceId("same")
        )
        result = await service.verify(
            workspace_id="a", instance_id="same", body=RAW, headers=headers
        )
        assert result.received_count == 1 and not result.rejections
        assert result.events[0].event["source"]["workspaceId"] == "a"
        assert (
            await providers.instance_store.get_connector_instance(
                WorkspaceId("a"), ConnectorInstanceId("same")
            )
            == before
        )
        with pytest.raises(WebhookVerificationError) as wrong:
            await service.verify(workspace_id="b", instance_id="same", body=RAW, headers=headers)
        assert wrong.value.status_code == 401
        resolver.secrets["a"] = "unit-rotated"
        fresh = WebhookVerificationService(
            cast(ConnectorInstanceStoreProvider, PgConnectorInstanceAdapter(pool=pool)),
            cast(CatalogStoreProvider, PgCatalogAdapter(pool=pool)),
            providers.identity_registry,
        )
        with pytest.raises(WebhookVerificationError) as old:
            await fresh.verify(workspace_id="a", instance_id="same", body=RAW, headers=headers)
        assert old.value.status_code == 401
        new_headers = (
            {"authorization": "Bearer unit-rotated"}
            if scheme == "bearer"
            else {"x-custos-signature": signature(RAW, "unit-rotated")}
        )
        assert (
            await fresh.verify(workspace_id="a", instance_id="same", body=RAW, headers=new_headers)
        ).received_count == 1
        await providers.instance_store.patch_connector_instance(
            WorkspaceId("a"), ConnectorInstanceId("same"), {"enabled": False, "status": "disabled"}
        )
        with pytest.raises(WebhookVerificationError) as disabled:
            await fresh.verify(workspace_id="a", instance_id="same", body=RAW, headers=new_headers)
        assert disabled.value.status_code == 404
    finally:
        await pool.close()
