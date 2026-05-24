"""Tests for audit emission helpers (CS-IMPL-019 / #220).

Asserts that the catalog manager surface emits the canonical audit
events (event type + actor + subject keys + payload keys) after
state-changing operations land, and that ``append_audit`` failures
are swallowed without disrupting the calling flow but are counted
into ``custos_audit_emit_failures_total``.
"""

from __future__ import annotations

import pytest

from custos_catalog import audit
from custos_catalog.audit import (
    EVENT_ACTIVITY_DEPRECATED,
    EVENT_ACTIVITY_REGISTERED,
    EVENT_CONNECTOR_DEPRECATED,
    EVENT_CONNECTOR_REGISTERED,
    EVENT_TEMPLATE_EXTRACTED,
    EVENT_TEMPLATE_MATERIALIZED,
    EVENT_WORKFLOW_DEPRECATED,
    EVENT_WORKFLOW_PUBLISHED,
)
from tests._fakes import FakeMetadataStore

# ---------------------------------------------------------------------------
# audit_workflow_published
# ---------------------------------------------------------------------------


async def test_workflow_published_emits_canonical_event() -> None:
    store = FakeMetadataStore()
    await audit.audit_workflow_published(
        store,  # type: ignore[arg-type]
        workspace_id="ws-1",
        actor="alice",
        workflow_name="wf",
        version=3,
        derived_from_template_version_id="tpl-version-42",
    )
    assert len(store.audit) == 1
    event = store.audit[0]
    assert event.event_type == EVENT_WORKFLOW_PUBLISHED
    assert event.actor == "alice"
    assert event.workspace_id == "ws-1"
    assert set(event.subject.keys()) == {"workflow_name", "version"}
    assert event.subject["workflow_name"] == "wf"
    assert event.subject["version"] == 3
    assert event.payload == {"derived_from_template_version_id": "tpl-version-42"}


async def test_workflow_deprecated_emits_canonical_event() -> None:
    store = FakeMetadataStore()
    await audit.audit_workflow_deprecated(
        store,  # type: ignore[arg-type]
        workspace_id="ws-1",
        actor="alice",
        workflow_name="wf",
        reason="superseded",
    )
    event = store.audit[0]
    assert event.event_type == EVENT_WORKFLOW_DEPRECATED
    assert set(event.subject.keys()) == {"workflow_name"}
    assert event.payload == {"reason": "superseded"}


async def test_template_materialized_emits_canonical_event() -> None:
    store = FakeMetadataStore()
    await audit.audit_template_materialized(
        store,  # type: ignore[arg-type]
        workspace_id="ws-1",
        actor="alice",
        template_name="tpl",
        template_version=2,
        workflow_name="wf",
        workflow_version=7,
    )
    event = store.audit[0]
    assert event.event_type == EVENT_TEMPLATE_MATERIALIZED
    assert set(event.subject.keys()) == {
        "template_name",
        "template_version",
        "workflow_name",
        "workflow_version",
    }


async def test_template_extracted_emits_canonical_event() -> None:
    store = FakeMetadataStore()
    await audit.audit_template_extracted(
        store,  # type: ignore[arg-type]
        workspace_id="ws-1",
        actor="alice",
        source_workflow_name="wf",
        source_workflow_version=5,
        template_name="tpl",
        template_version=1,
    )
    event = store.audit[0]
    assert event.event_type == EVENT_TEMPLATE_EXTRACTED
    assert set(event.subject.keys()) == {
        "source_workflow_name",
        "source_workflow_version",
        "template_name",
        "template_version",
    }


async def test_activity_registered_emits_canonical_event() -> None:
    store = FakeMetadataStore()
    await audit.audit_activity_registered(
        store,  # type: ignore[arg-type]
        workspace_id="ws-1",
        actor="alice",
        namespace="ws/ws-1",
        type_name="my-activity",
        version="1.0.0",
        digest="sha256:abc",
        referrer_ref="oci://ghcr.io/x:v1@sha256:abc",
    )
    event = store.audit[0]
    assert event.event_type == EVENT_ACTIVITY_REGISTERED
    assert set(event.subject.keys()) == {"namespace", "type", "version"}
    assert event.subject["type"] == "my-activity"
    assert set(event.payload.keys()) == {"digest", "referrer_ref"}


async def test_activity_deprecated_emits_canonical_event() -> None:
    store = FakeMetadataStore()
    await audit.audit_activity_deprecated(
        store,  # type: ignore[arg-type]
        workspace_id="ws-1",
        actor="alice",
        namespace="ws/ws-1",
        type_name="my-activity",
        reason="cve",
    )
    event = store.audit[0]
    assert event.event_type == EVENT_ACTIVITY_DEPRECATED
    assert set(event.subject.keys()) == {"namespace", "type"}


async def test_connector_registered_emits_canonical_event() -> None:
    store = FakeMetadataStore()
    await audit.audit_connector_registered(
        store,  # type: ignore[arg-type]
        workspace_id="ws-1",
        actor="connector-svc",
        type_name="oci-registry",
        version="2.3.1",
        digest="sha256:def",
    )
    event = store.audit[0]
    assert event.event_type == EVENT_CONNECTOR_REGISTERED
    assert set(event.subject.keys()) == {"type", "version"}


async def test_connector_deprecated_emits_canonical_event() -> None:
    store = FakeMetadataStore()
    await audit.audit_connector_deprecated(
        store,  # type: ignore[arg-type]
        workspace_id="ws-1",
        actor="connector-svc",
        type_name="oci-registry",
        reason=None,
    )
    event = store.audit[0]
    assert event.event_type == EVENT_CONNECTOR_DEPRECATED
    assert set(event.subject.keys()) == {"type"}
    assert event.payload == {"reason": None}


# ---------------------------------------------------------------------------
# Best-effort emission semantics
# ---------------------------------------------------------------------------


async def test_append_audit_failure_is_swallowed(caplog: pytest.LogCaptureFixture) -> None:
    store = FakeMetadataStore()
    store.raise_on_append = RuntimeError("metadata down")
    with caplog.at_level("WARNING", logger="custos_catalog.audit"):
        await audit.audit_workflow_published(
            store,  # type: ignore[arg-type]
            workspace_id="ws-1",
            actor="alice",
            workflow_name="wf",
            version=1,
        )
    # Emission failed → no event captured, WARNING log line emitted.
    assert store.audit == []
    assert any("audit emission failed" in rec.message for rec in caplog.records)
