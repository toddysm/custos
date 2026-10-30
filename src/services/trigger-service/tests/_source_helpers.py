"""Reusable source-ingestion fixtures over actual memory and PostgreSQL capability providers."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import Any, Literal, Protocol, runtime_checkable

from custos_spl.interfaces.scheduler import SchedulerStateProvider
from custos_spl.interfaces.trigger_queries import TriggerQueryProvider

from custos_trigger.api.routes.rpc import compute_resume_id
from custos_trigger.clients import FakeWorkflowServiceClient
from custos_trigger.dedup import Deduplicator
from custos_trigger.models import ResumeRegistration, SourceType, Subscription
from custos_trigger.pipeline.dispatch import Dispatcher, NoopAuditSink
from custos_trigger.pipeline.reliable import ReliableDispatcher, ReliableStorage
from custos_trigger.source_config import SourceConfig
from custos_trigger.stores import ResumeSubscriptionStore, SubscriptionStore
from custos_trigger.stores.base import TriggerMetadataStore

NOW = datetime(2026, 6, 4, 12, tzinfo=UTC)
WS = "source-a"


@runtime_checkable
class SourceStorage(
    TriggerMetadataStore, TriggerQueryProvider, SchedulerStateProvider, ReliableStorage, Protocol
):
    """The real storage capabilities used by source ingestion and its fixture writers."""


def connector_event(
    *, workspace: str = WS, mode: Literal["push", "pull"] = "push", event_id: str = "event"
) -> dict[str, Any]:
    """Provide an unchanged producer envelope with intentionally untrusted payload routing keys."""
    return {
        "schemaVersion": "1",
        "eventId": event_id,
        "eventType": "registry.push",
        "source": {
            "workspaceId": workspace,
            "instanceId": "same",
            "deliveryMode": mode,
            "receivedAt": NOW.isoformat(),
        },
        "payload": {
            "route": "match",
            "amount": 7,
            "workspace": "forged-workspace",
            "runId": "forged-run",
            "stepId": "forged-step",
        },
    }


async def start(
    store: SourceStorage,
    identifier: str,
    *,
    workspace: str = WS,
    source: SourceType = SourceType.VENDOR_PUSH,
    connector: str = "same",
) -> Subscription:
    """Create persisted source intent with event-root input mapping and a selective expression."""
    now = await store.delivery_now()
    return await SubscriptionStore(store).create(
        Subscription(
            workspace_id=workspace,
            subscription_id=identifier,
            workflow_id="workflow",
            target_workflow_version_id="version",
            source_type=source,
            source_config=SourceConfig(
                connector_instance_id=connector,
                poll_interval_seconds=10 if source is SourceType.PULL else None,
            ),
            selector="event.data.route == 'match'",
            input_mapping={
                "amount": "${{ event.data.amount }}",
                "label": "ref-${{ event.source.connectorInstanceId }}",
            },
            created_at=now,
            updated_at=now,
        )
    )


async def resume(
    store: SourceStorage,
    identifier: str,
    *,
    workspace: str = WS,
    mode: Literal["push", "pull"] = "push",
    connector: str = "same",
    expired: bool = False,
    event_key: str = "registry.push",
) -> str:
    """Persist a source-associated version-two wait whose routing IDs never come from the event."""
    now = await store.delivery_now()
    registration = ResumeRegistration.model_validate(
        {
            "runId": identifier,
            "stepId": "stored-step",
            "eventKey": event_key,
            "workspaceId": workspace,
            "deliveryProtocol": 2,
            "selector": "event.data.route == 'match'",
            "source": {
                "connectorInstanceId": connector,
                "deliveryModes": [mode],
                **({"pollIntervalSeconds": 10} if mode == "pull" else {}),
            },
        }
    )
    key = compute_resume_id(identifier, "stored-step", event_key)
    await ResumeSubscriptionStore(store).register(
        registration,
        workspace_id=workspace,
        resume_id=key,
        expires_at=now + timedelta(seconds=-1 if expired else 3600),
    )
    return key


def dispatcher(store: SourceStorage, client: FakeWorkflowServiceClient) -> Dispatcher:
    """Use actual durable delivery/capture logic with one deterministic downstream attempt."""
    return Dispatcher(
        client,
        Deduplicator(store),
        reliable=ReliableDispatcher(client, store, NoopAuditSink(), max_retries=0),
    )
