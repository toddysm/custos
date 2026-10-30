"""Exercise source fan-out, stored resume routing and durable acknowledgement on both backends."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Any, Literal, cast
from unittest.mock import AsyncMock

import pytest
from custos_pg import PgMetadataAdapter
from custos_spl.errors import BackendUnavailable
from custos_spl.ids import SubscriptionId, WorkspaceId
from custos_spl.interfaces.dead_letters import json_value
from custos_spl.interfaces.trigger_queries import SubscriptionSnapshot

from custos_trigger.clients import FakeWorkflowServiceClient
from custos_trigger.clients.workflow import WorkflowClientTransportError
from custos_trigger.errors import TriggerError
from custos_trigger.models import ResumeRegistration, SourceType
from custos_trigger.pipeline.dispatch import NoopAuditSink
from custos_trigger.providers import InMemoryTriggerMetadataStore
from custos_trigger.receivers.sources import (
    SOURCE_PAGE_SIZE,
    adapt_connector_event,
    ingest_source_event,
)
from custos_trigger.selector import SelectorEvaluator
from custos_trigger.source_config import SourceConfig
from custos_trigger.stores import ResumeSubscriptionStore, SubscriptionStore
from tests._source_helpers import NOW, WS, SourceStorage, connector_event, dispatcher, resume, start


@pytest.fixture(params=["memory", pytest.param("postgres", marks=pytest.mark.integration)])
def source_dsn(request: pytest.FixtureRequest) -> str | None:
    """Reset the real database before entering the test's event loop."""
    return None if request.param == "memory" else cast(str, request.getfixturevalue("pg_dsn"))


@pytest.fixture
async def source_store(source_dsn: str | None) -> AsyncIterator[SourceStorage]:
    """Use actual providers, not test-only query hydration."""
    if source_dsn is None:
        memory = InMemoryTriggerMetadataStore(now=lambda: NOW)
        assert isinstance(memory, SourceStorage)
        yield memory
        return
    import asyncpg

    pool = await asyncpg.create_pool(dsn=source_dsn, min_size=1, max_size=4)
    assert pool is not None
    try:
        postgres = PgMetadataAdapter(pool=pool)
        await postgres.apply_pending()
        assert isinstance(postgres, SourceStorage)
        yield postgres
    finally:
        await pool.close()


async def consume(
    store: SourceStorage,
    client: FakeWorkflowServiceClient,
    mode: Literal["push", "pull"] = "push",
    payload: dict[str, Any] | None = None,
) -> None:
    """Run the same scoped ingestion collaborator used by HTTP callbacks."""
    await ingest_source_event(
        adapt_connector_event(payload if payload is not None else connector_event(mode=mode)),
        dispatcher=dispatcher(store, client),
        evaluator=SelectorEvaluator(),
        subscriptions=SubscriptionStore(store),
        resumes=ResumeSubscriptionStore(store),
        audit=NoopAuditSink(),
        scheduler=store,
    )


@pytest.mark.parametrize("mode", ["push", "pull"])
async def test_external_start_and_resume_use_only_stored_scope(
    source_store: SourceStorage, mode: Literal["push", "pull"]
) -> None:
    """No payload run/step/workspace value can redirect source-associated delivery."""
    source = SourceType.PULL if mode == "pull" else SourceType.VENDOR_PUSH
    await start(source_store, "start", source=source)
    await start(source_store, "other-connector", source=source, connector="other")
    await start(source_store, "start", source=source, workspace="source-b")
    await resume(source_store, "stored-run-1", mode=mode)
    await resume(source_store, "stored-run-2/child", mode=mode)
    await resume(source_store, "stored-run-1", mode=mode, workspace="source-b")
    await resume(source_store, "expired", mode=mode, expired=True)
    await resume(source_store, "wrong-mode", mode="pull" if mode == "push" else "push")
    await resume(source_store, "wrong-connector", mode=mode, connector="other")
    client = FakeWorkflowServiceClient()
    await consume(source_store, client, mode)
    await consume(source_store, client, mode)
    assert len(client.start_run_calls) == 1
    assert client.start_run_calls[0].inputs == {"amount": 7, "label": "ref-same"}
    assert {run for run, _, _ in client.raise_event_calls} == {"stored-run-1", "stored-run-2/child"}
    assert len(client.raise_event_calls) == 2
    assert all(step == "stored-step" for _, step, _ in client.raise_event_calls)
    assert all(
        request.workspace_id == WS and request.delivery_protocol == 2
        for _, _, request in client.raise_event_calls
    )
    assert all(
        request.payload["runId"] == "forged-run" for _, _, request in client.raise_event_calls
    )


async def test_paged_fanout_does_not_stop_at_first_candidate_page(
    source_store: SourceStorage,
) -> None:
    """Every candidate beyond the exact page boundary is delivered once."""
    for index in range(SOURCE_PAGE_SIZE + 1):
        await start(source_store, f"start-{index:03}")
        await resume(source_store, f"run-{index:03}")
    client = FakeWorkflowServiceClient()
    await consume(source_store, client)
    assert len(client.start_run_calls) == SOURCE_PAGE_SIZE + 1
    assert len(client.raise_event_calls) == SOURCE_PAGE_SIZE + 1
    assert len({call.idempotency_key for call in client.start_run_calls}) == SOURCE_PAGE_SIZE + 1


@pytest.mark.parametrize("mode", ["push", "pull"])
async def test_resume_only_source_needs_no_payload_routing_ids(
    source_store: SourceStorage, mode: Literal["push", "pull"]
) -> None:
    """Source waits work without start subscriptions or supplied run/step fields."""
    await resume(source_store, "stored-only", mode=mode, event_key="image-ready")
    body = connector_event(mode=mode)
    body["payload"] = {"route": "match", "amount": 7}
    client = FakeWorkflowServiceClient()
    await consume(source_store, client, mode, body)
    assert not client.start_run_calls
    assert len(client.raise_event_calls) == 1
    run, step, request = client.raise_event_calls[0]
    assert (run, step, request.workspace_id) == ("stored-only", "stored-step", WS)
    assert request.payload == body["payload"]
    assert request.event_name == "image-ready"


async def test_downstream_failure_is_durably_captured_before_ack(
    source_store: SourceStorage,
) -> None:
    """Broker redelivery does not duplicate a failed original attempt once capture is committed."""
    await start(source_store, "start")
    failed = FakeWorkflowServiceClient(error=WorkflowClientTransportError("offline"))
    await consume(source_store, failed)
    captured = (await source_store.query_dead_letters(WorkspaceId(WS))).items
    assert len(captured) == 1
    assert captured[0].data.event["eventId"] == "event"
    assert json_value(captured[0].data.target["inputs"]) == {"amount": 7, "label": "ref-same"}
    await consume(source_store, failed)
    assert len(failed.start_run_calls) == 1


async def test_connector_rebinding_does_not_alias_prior_event_ids(
    source_store: SourceStorage,
) -> None:
    """Different connector instances may reuse producer IDs without false duplicate suppression."""
    subscription = await start(source_store, "start")
    client = FakeWorkflowServiceClient()
    await consume(source_store, client)
    await SubscriptionStore(source_store).update(
        subscription.model_copy(
            update={"source_config": SourceConfig(connector_instance_id="other")}
        ),
        expected_revision=subscription.store_revision,
    )
    incoming = connector_event()
    incoming["source"]["instanceId"] = "other"
    await consume(source_store, client, payload=incoming)
    assert len(client.start_run_calls) == 2
    assert client.start_run_calls[0].idempotency_key != client.start_run_calls[1].idempotency_key
    assert client.start_run_calls[1].inputs["label"] == "ref-other"


async def test_active_revision_conflict_retries_instead_of_losing_delivery(
    source_store: SourceStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A mapping change after candidate selection cannot become a successful skipped delivery."""
    subscription = await start(source_store, "changed")
    original = source_store.get_subscription
    changed = False

    async def racing_get(
        workspace_id: WorkspaceId, subscription_id: SubscriptionId
    ) -> SubscriptionSnapshot | None:
        """Commit a real metadata edit immediately before the final candidate read."""
        nonlocal changed
        if not changed and subscription_id == "changed":
            changed = True
            await SubscriptionStore(source_store).update(
                subscription.model_copy(update={"input_mapping": {"amount": 99}}),
                expected_revision=subscription.store_revision,
            )
        return await original(workspace_id, subscription_id)

    monkeypatch.setattr(source_store, "get_subscription", racing_get)
    client = FakeWorkflowServiceClient()
    with pytest.raises(TriggerError, match="changed before delivery"):
        await consume(source_store, client)
    assert not client.start_run_calls
    await consume(source_store, client)
    assert len(client.start_run_calls) == 1
    assert client.start_run_calls[0].inputs == {"amount": 99}


async def test_same_connector_event_across_modes_resumes_once(source_store: SourceStorage) -> None:
    """A wait allowing both modes still sees one logical producer event, not two effects."""
    registration = ResumeRegistration.model_validate(
        {
            "runId": "both-modes",
            "stepId": "stored-step",
            "eventKey": "registry.push",
            "workspaceId": WS,
            "deliveryProtocol": 2,
            "selector": "event.data.route == 'match'",
            "source": {
                "connectorInstanceId": "same",
                "deliveryModes": ["push", "pull"],
                "pollIntervalSeconds": 10,
            },
        }
    )
    from datetime import timedelta

    await ResumeSubscriptionStore(source_store).register(
        registration,
        workspace_id=WS,
        resume_id="both-modes",
        expires_at=await source_store.delivery_now() + timedelta(hours=1),
    )
    client = FakeWorkflowServiceClient()
    await consume(source_store, client, "push")
    await consume(source_store, client, "pull")
    assert len(client.raise_event_calls) == 1


async def test_failed_capture_remains_unacknowledged(
    source_store: SourceStorage, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A successful-looking dead-letter outcome cannot hide failed durable persistence."""
    await start(source_store, "start")
    monkeypatch.setattr(
        source_store,
        "capture_failed_delivery",
        AsyncMock(side_effect=BackendUnavailable("offline")),
    )
    with pytest.raises(TriggerError, match="not acknowledged"):
        await consume(
            source_store,
            FakeWorkflowServiceClient(error=WorkflowClientTransportError("downstream")),
        )
