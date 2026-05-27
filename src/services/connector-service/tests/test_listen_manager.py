"""Unit tests for :mod:`custos_connector.listen.manager` (CONN-IMPL-025, #308).

Covers:

* Activating a pull-mode connector type registers the instance with
  the :class:`PullLoopScheduler`.
* Activating a push-mode connector type adds a
  :class:`PushRegistration` keyed by
  ``(workspace_id, instance_id)``.
* Activating a sink connector (no ``events`` block) returns
  :class:`ActivationResult` with ``skipped=True, reason="sink-connector"``
  (issue #308 acceptance criterion).
* Re-activating the same instance returns the cached result without
  re-touching the scheduler / registry.
* Deactivating reverses both registrations and is idempotent.
* Activating with an unknown ``(type, version)`` raises
  :class:`UnknownConnectorTypeError`.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import pytest
from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.connector_instance_store import ConnectorInstance

from custos_connector.listen import (
    DELIVERY_MODE_PULL,
    DELIVERY_MODE_PUSH,
    REASON_SINK_CONNECTOR,
    AllowAllSignatureVerifier,
    ListenManager,
    UnknownConnectorTypeError,
)
from tests._fakes import FakeCatalogAdapter

pytestmark = pytest.mark.asyncio


_WORKSPACE = "ws-1"
_INSTANCE_ID = "inst-1"
_TYPE = "oci-registry"
_VERSION = "1.0.0"


# ---------------------------------------------------------------------------
# Test doubles
# ---------------------------------------------------------------------------


class _FakeScheduler:
    """Minimal :class:`PullLoopScheduler`-shaped double for manager tests.

    Implements ``register`` + ``unregister`` since those are the only
    methods :class:`ListenManager` calls. Records every call so tests
    can assert on the registry mutations directly.
    """

    def __init__(self) -> None:
        self.registered: list[tuple[str, str]] = []
        self.unregistered: list[tuple[str, str]] = []
        self._live: set[tuple[str, str]] = set()

    def register(
        self,
        workspace_id: str,
        instance_id: str,
        *,
        interval_seconds: int | None = None,
    ) -> object:
        self.registered.append((workspace_id, instance_id))
        self._live.add((workspace_id, instance_id))
        return object()

    def unregister(self, workspace_id: str, instance_id: str) -> bool:
        self.unregistered.append((workspace_id, instance_id))
        key = (workspace_id, instance_id)
        was_live = key in self._live
        self._live.discard(key)
        return was_live


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _instance() -> ConnectorInstance:
    return ConnectorInstance(
        workspace_id=WorkspaceId(_WORKSPACE),
        instance_id=ConnectorInstanceId(_INSTANCE_ID),
        type=_TYPE,
        version=_VERSION,
        name="acme",
        lease_ttl_seconds=300,
        enabled=True,
        status="enabled",
        health_status="healthy",
        target_config={},
        credentials_authentication={},
        used_capabilities=(),
        created_at=datetime(2026, 1, 1, tzinfo=UTC),
        updated_at=datetime(2026, 1, 1, tzinfo=UTC),
    )


async def _seed_type(
    catalog: FakeCatalogAdapter,
    *,
    events_block: dict[str, Any] | None,
) -> None:
    """Seed a connector-type-version row with the provided events block."""
    spec: dict[str, Any] = {
        "description": "Test connector",
        "capabilities": ["c.read"],
        "target": {"kind": "oci-registry", "endpoint": "https://example.com", "config": {}},
        "credentials": {"authenticationType": "none"},
    }
    if events_block is not None:
        spec["events"] = events_block
    await catalog.put_connector_type_version(
        type=_TYPE,
        version=_VERSION,
        digest="sha256:deadbeef",
        image_ref="ghcr.io/acme/connector@sha256:deadbeef",
        normalized_manifest={"spec": spec},
    )


def _manager(catalog: FakeCatalogAdapter, scheduler: _FakeScheduler) -> ListenManager:
    return ListenManager(
        catalog_store=catalog,  # type: ignore[arg-type]
        scheduler=scheduler,  # type: ignore[arg-type]
        default_signature_verifier=AllowAllSignatureVerifier(test_only=True),
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


async def test_activate_pull_only_registers_with_scheduler() -> None:
    catalog = FakeCatalogAdapter()
    scheduler = _FakeScheduler()
    await _seed_type(
        catalog,
        events_block={
            "delivery": ["pull"],
            "produced": ["oci.image.pushed"],
            "pull": {"cursorEncoding": "my-enc", "initialCursorBehavior": "now"},
        },
    )
    manager = _manager(catalog, scheduler)

    result = await manager.activate(_instance())

    assert result.skipped is False
    assert result.modes == (DELIVERY_MODE_PULL,)
    assert scheduler.registered == [(_WORKSPACE, _INSTANCE_ID)]
    assert manager.get_push_registration(_WORKSPACE, _INSTANCE_ID) is None


async def test_activate_push_only_registers_in_push_registry() -> None:
    catalog = FakeCatalogAdapter()
    scheduler = _FakeScheduler()
    await _seed_type(
        catalog,
        events_block={
            "delivery": ["push"],
            "produced": ["oci.image.pushed"],
        },
    )
    manager = _manager(catalog, scheduler)

    result = await manager.activate(_instance())

    assert result.skipped is False
    assert result.modes == (DELIVERY_MODE_PUSH,)
    assert scheduler.registered == []
    push = manager.get_push_registration(_WORKSPACE, _INSTANCE_ID)
    assert push is not None
    assert push.active is True
    assert push.produced_event_types == ("oci.image.pushed",)


async def test_activate_both_modes_registers_in_both() -> None:
    catalog = FakeCatalogAdapter()
    scheduler = _FakeScheduler()
    await _seed_type(
        catalog,
        events_block={
            "delivery": ["pull", "push"],
            "produced": ["oci.image.pushed"],
            "pull": {"cursorEncoding": "my-enc", "initialCursorBehavior": "now"},
        },
    )
    manager = _manager(catalog, scheduler)

    result = await manager.activate(_instance())

    assert set(result.modes) == {DELIVERY_MODE_PULL, DELIVERY_MODE_PUSH}
    assert scheduler.registered == [(_WORKSPACE, _INSTANCE_ID)]
    assert manager.get_push_registration(_WORKSPACE, _INSTANCE_ID) is not None


async def test_activate_sink_connector_skips() -> None:
    """Issue #308 acceptance criterion: sink connector activation is a no-op."""
    catalog = FakeCatalogAdapter()
    scheduler = _FakeScheduler()
    # No events block at all.
    await _seed_type(catalog, events_block=None)
    manager = _manager(catalog, scheduler)

    result = await manager.activate(_instance())

    assert result.skipped is True
    assert result.reason == REASON_SINK_CONNECTOR
    assert result.modes == ()
    assert scheduler.registered == []
    assert manager.get_push_registration(_WORKSPACE, _INSTANCE_ID) is None


async def test_activate_is_idempotent() -> None:
    catalog = FakeCatalogAdapter()
    scheduler = _FakeScheduler()
    await _seed_type(
        catalog,
        events_block={
            "delivery": ["pull"],
            "produced": ["oci.image.pushed"],
            "pull": {"cursorEncoding": "my-enc", "initialCursorBehavior": "now"},
        },
    )
    manager = _manager(catalog, scheduler)

    first = await manager.activate(_instance())
    second = await manager.activate(_instance())

    assert first == second
    # Scheduler was only touched once.
    assert scheduler.registered == [(_WORKSPACE, _INSTANCE_ID)]


async def test_activate_unknown_connector_type_raises() -> None:
    catalog = FakeCatalogAdapter()
    scheduler = _FakeScheduler()
    manager = _manager(catalog, scheduler)
    with pytest.raises(UnknownConnectorTypeError):
        await manager.activate(_instance())


async def test_deactivate_reverses_both_registrations() -> None:
    catalog = FakeCatalogAdapter()
    scheduler = _FakeScheduler()
    await _seed_type(
        catalog,
        events_block={
            "delivery": ["pull", "push"],
            "produced": ["oci.image.pushed"],
            "pull": {"cursorEncoding": "my-enc", "initialCursorBehavior": "now"},
        },
    )
    manager = _manager(catalog, scheduler)
    await manager.activate(_instance())

    unwound = await manager.deactivate(_WORKSPACE, _INSTANCE_ID)

    assert unwound is True
    assert scheduler.unregistered == [(_WORKSPACE, _INSTANCE_ID)]
    assert manager.get_push_registration(_WORKSPACE, _INSTANCE_ID) is None
    # Activation cache is cleared, so re-activation goes through again.
    await manager.activate(_instance())
    assert scheduler.registered == [
        (_WORKSPACE, _INSTANCE_ID),
        (_WORKSPACE, _INSTANCE_ID),
    ]


async def test_deactivate_unknown_instance_is_idempotent() -> None:
    catalog = FakeCatalogAdapter()
    scheduler = _FakeScheduler()
    manager = _manager(catalog, scheduler)
    assert await manager.deactivate("nope", "nope") is False
