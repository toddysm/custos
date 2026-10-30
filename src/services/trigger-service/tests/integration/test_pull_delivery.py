"""Actual Connector polling and Trigger dispatch share PostgreSQL recovery identities."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import replace
from datetime import UTC, datetime
from typing import Any, cast

import asyncpg
import pytest
from custos_connector.identity import IdentityResolverRegistry
from custos_connector.listen.normalizer import NormalizedEvent as ConnectorEvent
from custos_connector.providers import (
    Providers as ConnectorProviders,
)
from custos_connector.providers import (
    load_bind_for_step_service,
    load_lease_manager,
    wire_durable_pull_runtime,
)
from custos_connector.runtime import HookRunResult, PluginInvoker
from custos_connector.settings import load_settings
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

from custos_trigger.clients import FakeWorkflowServiceClient
from custos_trigger.models import SourceType
from custos_trigger.pipeline.dispatch import NoopAuditSink
from custos_trigger.receivers.sources import adapt_connector_event, ingest_source_event
from custos_trigger.selector import SelectorEvaluator
from custos_trigger.stores import ResumeSubscriptionStore, SubscriptionStore
from tests._source_helpers import WS, SourceStorage, dispatcher, start

pytestmark = pytest.mark.integration


class PullRunner:
    """A real PluginInvoker hook transport returning stable IDs when a cursor is replayed."""

    def __init__(self) -> None:
        """Record upstream calls without running Docker or contacting an external source."""
        self.calls = 0

    async def run(
        self,
        *,
        image_ref: str,
        hook: str,
        payload: Mapping[str, Any],
        timeout_seconds: float,
    ) -> HookRunResult:
        """Emit the same two logical source events and next cursor on every failed-batch retry."""
        self.calls += 1
        return HookRunResult(
            exit_code=0,
            stderr=b"",
            stdout=json.dumps(
                {
                    "ok": True,
                    "result": {
                        "events": [
                            {
                                "eventId": "first",
                                "eventType": "registry.push",
                                "route": "match",
                                "amount": 7,
                            },
                            {
                                "eventId": "second",
                                "eventType": "registry.push",
                                "route": "match",
                                "amount": 8,
                            },
                        ],
                        "nextCursor": {"encoding": "unit-v1", "value": "cursor-2"},
                        "receiverEndpoint": None,
                    },
                }
            ).encode(),
        )


class DeliveringPublisher:
    """Deliver actual normalized events, with one failure after the first event is consumed."""

    def __init__(self, store: SourceStorage, workflow: FakeWorkflowServiceClient) -> None:
        """Bind the real reliable Trigger pipeline and count acknowledged publication attempts."""
        self.store, self.workflow = store, workflow
        self.attempts = 0
        self.fail_at: int | None = 2

    async def publish(self, event: ConnectorEvent) -> None:
        """Use stored Trigger mappings/receipts, not a test-side deduplication approximation."""
        self.attempts += 1
        if self.attempts == self.fail_at:
            raise OSError("test publication failure")
        await ingest_source_event(
            adapt_connector_event(event.to_wire()),
            dispatcher=dispatcher(self.store, self.workflow),
            evaluator=SelectorEvaluator(),
            subscriptions=SubscriptionStore(self.store),
            resumes=ResumeSubscriptionStore(self.store),
            audit=NoopAuditSink(),
            scheduler=self.store,
        )


async def test_poll_batch_replay_produces_no_duplicate_workflow_effects(pg_dsn: str) -> None:
    """Replay preserves the cursor and downstream producer receipt identities."""
    pool = await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=6)
    assert pool is not None
    try:
        catalog, instances = PgCatalogAdapter(pool=pool), PgConnectorInstanceAdapter(pool=pool)
        leases, metadata = PgLeaseAdapter(pool=pool), PgMetadataAdapter(pool=pool)
        for adapter in (catalog, instances, leases, metadata):
            await adapter.apply_pending()
        assert isinstance(metadata, SourceStorage)
        workflow = FakeWorkflowServiceClient()
        publisher = DeliveringPublisher(metadata, workflow)
        registry = IdentityResolverRegistry(resolvers=[])
        settings = load_settings(
            {
                "CONN_CATALOG_STORE": pg_dsn,
                "CONN_METADATA_STORE": pg_dsn,
                "CONN_CATALOG_ENDPOINT": "http://catalog-service:8080",
                "CONN_DAPR_HTTP_ENDPOINT": "http://127.0.0.1:3500",
                "CONN_DURABLE_PULL_ENABLED": "true",
                "APP_API_TOKEN": "unit-pull-sidecar",
                "CONN_TRIGGER_NAMESPACE": "custos",
            }
        )
        typed_catalog = cast(CatalogStoreProvider, catalog)
        typed_instances = cast(ConnectorInstanceStoreProvider, instances)
        typed_metadata = cast(MetadataStoreProvider, metadata)
        typed_leases = cast(LeaseStoreProvider, leases)
        providers = ConnectorProviders(
            catalog_store=typed_catalog,
            instance_store=typed_instances,
            metadata_store=typed_metadata,
            lease_store=typed_leases,
            identity_registry=registry,
            bind_for_step_service=load_bind_for_step_service(
                catalog_store=typed_catalog,
                instance_store=typed_instances,
                metadata_store=typed_metadata,
                identity_registry=registry,
            ),
            lease_manager=load_lease_manager(
                lease_store=typed_leases,
                metadata_store=typed_metadata,
                settings=settings,
            ),
            event_publisher=publisher,
        )
        await catalog.put_connector_type_version(
            type="pull-fixture",
            version="1",
            digest="sha256:pull-fixture",
            image_ref="unit",
            normalized_manifest={
                "spec": {
                    "events": {
                        "delivery": ["pull"],
                        "produced": ["registry.push"],
                        "pull": {"cursorEncoding": "unit-v1"},
                    }
                }
            },
        )
        now = datetime.now(UTC)
        for workspace in (WS, "other"):
            await instances.put_connector_instance(
                WorkspaceId(workspace),
                ConnectorInstance(
                    workspace_id=WorkspaceId(workspace),
                    instance_id=ConnectorInstanceId("same"),
                    type="pull-fixture",
                    version="1",
                    name=None,
                    lease_ttl_seconds=60,
                    enabled=True,
                    status="active",
                    health_status=None,
                    target_config={},
                    credentials_authentication={},
                    used_capabilities=None,
                    created_at=now,
                    updated_at=now,
                ),
            )
            await metadata.initialize_pull_control(
                WorkspaceId(workspace), ConnectorInstanceId("same"), actor="creator"
            )
            await start(metadata, "start", workspace=workspace, source=SourceType.PULL)
        first_runner = PullRunner()
        first = wire_durable_pull_runtime(
            providers, settings, plugin_invoker=PluginInvoker(first_runner)
        )
        assert first.pull_demand_reconciler is not None and first.pull_loop_scheduler is not None
        await first.pull_demand_reconciler.reconcile(WS)
        assert (await first.pull_loop_scheduler.tick_once()).outcomes[0].status == "failed"
        cursor = await metadata.read_cursor(WS, "same")
        assert cursor is not None and cursor.value == ""
        assert len(workflow.start_run_calls) == 1
        second_metadata = PgMetadataAdapter(pool=pool)
        second_runner = PullRunner()
        second = wire_durable_pull_runtime(
            replace(providers, metadata_store=cast(MetadataStoreProvider, second_metadata)),
            settings,
            plugin_invoker=PluginInvoker(second_runner),
        )
        assert second.pull_demand_reconciler is not None and second.pull_loop_scheduler is not None
        await second.pull_demand_reconciler.reconcile(WS)
        assert not (await second.pull_loop_scheduler.tick_once()).outcomes
        assert second_runner.calls == 0
        # Age only this test reservation to exercise recovery without sleeping.
        async with pool.acquire() as connection:
            await connection.execute(
                "UPDATE custos_state.connector_pull_control SET next_eligible_at="
                "clock_timestamp()-interval '1 second' WHERE workspace_id=$1 AND instance_id=$2",
                WS,
                "same",
            )
        publisher.fail_at = None
        assert (await second.pull_loop_scheduler.tick_once()).outcomes[0].status == "ticked"
        assert len(workflow.start_run_calls) == 2
        assert [call.inputs["amount"] for call in workflow.start_run_calls] == [7, 8]
        assert all(call.workspace_id == WS for call in workflow.start_run_calls)
        cursor = await metadata.read_cursor(WS, "same")
        assert cursor is not None and cursor.value == '"cursor-2"'
    finally:
        await pool.close()
