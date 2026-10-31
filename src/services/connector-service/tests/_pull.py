"""Reusable durable-poll fixtures with real plugin decoding, normalization and shared storage."""

from __future__ import annotations

import json
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from typing import cast

from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.connector_instance_store import ConnectorInstance
from custos_spl.interfaces.metadata_store import MetadataStoreProvider

from custos_connector.listen.normalizer import NormalizedEvent
from custos_connector.listen.publisher import RecordingEventPublisher
from custos_connector.providers import Providers, wire_durable_pull_runtime
from custos_connector.runtime import HookRunResult, PluginInvoker
from custos_connector.settings import Settings, load_settings
from tests._fakes import FakeMetadataAdapter
from tests._webhook import RotatingResolver, memory_providers
from tests.test_runtime import _FakeRunner

WS = "pull-a"
INSTANCE = "same"
MESH_HEADERS = {
    "dapr-api-token": "unit-pull-sidecar",
    "dapr-caller-app-id": "trigger-service",
    "dapr-caller-namespace": "custos",
}


@dataclass
class PullClock:
    """A shared storage clock independent of either scheduler replica."""

    value: datetime = datetime(2026, 10, 30, tzinfo=UTC)

    def __call__(self) -> datetime:
        """Return the authoritative test instant without advancing it implicitly."""
        return self.value

    def advance(self, seconds: int) -> None:
        """Move storage time explicitly between staggered attempts."""
        self.value += timedelta(seconds=seconds)


class FailablePublisher(RecordingEventPublisher):
    """Acknowledge individual normalized events, with a controllable partial-batch failure."""

    def __init__(self) -> None:
        """Begin with no failed attempt or acknowledged event."""
        super().__init__()
        self.attempts = 0
        self.fail_at: int | None = None

    async def publish(self, event: NormalizedEvent) -> None:
        """Never add a failed publication to the acknowledged-event log."""
        self.attempts += 1
        if self.attempts == self.fail_at:
            raise OSError("test broker unavailable")
        await super().publish(event)


def runner() -> _FakeRunner:
    """Return stable producer IDs and the same next cursor on a replayed batch."""
    return _FakeRunner(
        result=HookRunResult(
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
    )


def settings() -> Settings:
    """Enable explicit durable wiring while all real transports remain injected."""
    return load_settings(
        {
            "CONN_CATALOG_STORE": "postgresql://unused/unused",
            "CONN_METADATA_STORE": "postgresql://unused/unused",
            "CONN_CATALOG_ENDPOINT": "http://catalog-service:8080",
            "CONN_DAPR_HTTP_ENDPOINT": "http://127.0.0.1:3500",
            "CONN_DURABLE_PULL_ENABLED": "true",
            "APP_API_TOKEN": MESH_HEADERS["dapr-api-token"],
            "CONN_TRIGGER_NAMESPACE": "custos",
        }
    )


def memory_runtime(
    clock: PullClock,
    publisher: FailablePublisher | None = None,
) -> tuple[Providers, FakeMetadataAdapter, _FakeRunner, FailablePublisher]:
    """Assemble the production collaborators over shared in-memory test state."""
    metadata = FakeMetadataAdapter(clock=clock)
    outbound = publisher if publisher is not None else FailablePublisher()
    providers = replace(
        memory_providers(RotatingResolver()),
        metadata_store=cast(MetadataStoreProvider, metadata),
        event_publisher=outbound,
    )
    plugin = runner()
    return (
        wire_durable_pull_runtime(providers, settings(), plugin_invoker=PluginInvoker(plugin)),
        metadata,
        plugin,
        outbound,
    )


async def seed_instance(
    providers: Providers,
    *,
    workspace: str = WS,
    identifier: str = INSTANCE,
    initialize: bool = True,
    image_ref: str = "unit",
) -> ConnectorInstance:
    """Create a supported pull source; optionally retain unknown legacy pause state."""
    await providers.catalog_store.put_connector_type_version(
        type="registry",
        version="1",
        digest="sha256:unit-pull",
        image_ref=image_ref,
        normalized_manifest={
            "spec": {
                "events": {
                    "delivery": ["pull"],
                    "produced": ["registry.push"],
                    "pull": {"cursorEncoding": "unit-v1"},
                },
                "target": {"config": {}},
                "credentials": {"authenticationType": "none"},
            }
        },
    )
    now = datetime.now(UTC)
    instance = ConnectorInstance(
        workspace_id=WorkspaceId(workspace),
        instance_id=ConnectorInstanceId(identifier),
        type="registry",
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
    )
    if initialize:
        assert providers.pull_control is not None
        await providers.pull_control.initialize_pull_control(
            instance.workspace_id, instance.instance_id, actor="creator"
        )
    return await providers.instance_store.put_connector_instance(instance.workspace_id, instance)


def start_demand(
    metadata: FakeMetadataAdapter,
    *,
    workspace: str = WS,
    identifier: str = "start",
    interval: int = 10,
    state: str = "active",
) -> None:
    """Persist the selector projection consumed by the memory backend's workspace query."""
    metadata.pull_subscriptions[(workspace, identifier)] = (
        state,
        {
            "sourceType": "pull",
            "sourceConfig": {
                "connectorInstanceId": INSTANCE,
                "pollIntervalSeconds": interval,
            },
        },
    )
