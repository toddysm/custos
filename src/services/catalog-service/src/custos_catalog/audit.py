"""Audit emission for catalog-service (CS-IMPL-019).

Replaces the original stdlib-logger stub with typed helpers that
write to the SPL ``MetadataStoreProvider`` audit outbox via
``append_audit``. The audit events flow through the SPL outbox into
the Observability Service audit pipeline (COMP-009).

Canonical event names
---------------------

Eight emission helpers correspond to the canonical event names listed
in design § Dependencies:

* :func:`audit_workflow_published`     → ``workflow.version.published``
* :func:`audit_workflow_deprecated`    → ``workflow.deprecated``
* :func:`audit_template_materialized`  → ``template.materialized``
* :func:`audit_template_extracted`     → ``template.extracted``
* :func:`audit_activity_registered`    → ``activity.type.registered``
* :func:`audit_activity_deprecated`    → ``activity.type.deprecated``
* :func:`audit_connector_registered`   → ``connector.type.registered``
* :func:`audit_connector_deprecated`   → ``connector.type.deprecated``

Atomicity
---------

The SPL ``with_transaction`` primitive is intra-provider; the catalog
+ definition stores are separate providers from the metadata store, so
the catalog data write and the audit write cannot literally share a
transaction. The contract this module enforces is **best-effort
post-commit emission**: callers run the audit emission after the
catalog mutation has committed, and a failure to write the audit row
is logged at WARNING but does **not** roll back the state. The
Observability Service detects emission gaps through the
``custos_audit_emit_failures_total`` counter incremented below and via
its own outbox-lag metric.

Dev-shim audit
--------------

The middleware-level ``auth.callctx.shim_used`` event is still emitted
through the legacy log-only :func:`emit_event` hook. CS-IMPL-024 will
rewire the dev-shim middleware to the real audit pipeline; until then
the shim is dev-only and the warning log is sufficient operator
signal.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Final
from uuid import uuid4

from custos_spl import AuditEvent
from custos_spl.ids import WorkspaceId
from opentelemetry import metrics

if TYPE_CHECKING:
    from custos_spl import MetadataStoreProvider

_LOGGER = logging.getLogger("custos_catalog.audit")
_AUDIT_LOGGER = logging.getLogger("custos_catalog.audit.event")
# Backwards-compat alias: existing modules and tests imported
# ``custos_catalog.audit.logger`` from the original stub.
logger = _AUDIT_LOGGER

_INSTRUMENTATION_NAME: Final[str] = "custos_catalog"
_INSTRUMENTATION_VERSION: Final[str] = "0.1.0"

_meter = metrics.get_meter(_INSTRUMENTATION_NAME, _INSTRUMENTATION_VERSION)

#: Counter incremented when ``append_audit`` raises and the emission
#: is dropped. The Observability Service alert rules treat any
#: non-zero rate on this counter as page-worthy because every drop is
#: an audit-trail hole.
EMIT_FAILURES_TOTAL = _meter.create_counter(
    name="custos_audit_emit_failures_total",
    description=(
        "Count of catalog-service audit emissions that failed to reach the "
        "SPL audit outbox. Labelled by event_type."
    ),
)


# ---------------------------------------------------------------------------
# Canonical event names
# ---------------------------------------------------------------------------


EVENT_WORKFLOW_PUBLISHED: Final[str] = "workflow.version.published"
EVENT_WORKFLOW_DEPRECATED: Final[str] = "workflow.deprecated"
EVENT_TEMPLATE_MATERIALIZED: Final[str] = "template.materialized"
EVENT_TEMPLATE_EXTRACTED: Final[str] = "template.extracted"
EVENT_ACTIVITY_REGISTERED: Final[str] = "activity.type.registered"
EVENT_ACTIVITY_DEPRECATED: Final[str] = "activity.type.deprecated"
EVENT_CONNECTOR_REGISTERED: Final[str] = "connector.type.registered"
EVENT_CONNECTOR_DEPRECATED: Final[str] = "connector.type.deprecated"


# ---------------------------------------------------------------------------
# Core emission
# ---------------------------------------------------------------------------


async def _emit(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    event_type: str,
    actor: str,
    subject: Mapping[str, Any],
    payload: Mapping[str, Any],
) -> None:
    """Append a catalog audit event to the SPL outbox.

    Best-effort: any failure here is logged at WARNING + bumps
    :data:`EMIT_FAILURES_TOTAL` but is otherwise swallowed so the
    state mutation that triggered the emission stays committed.
    """
    ws_id = WorkspaceId(workspace_id)
    event = AuditEvent(
        workspace_id=ws_id,
        event_id=str(uuid4()),
        event_type=event_type,
        actor=actor,
        subject=dict(subject),
        payload=dict(payload),
        occurred_at=datetime.now(UTC),
    )
    try:
        await metadata_store.append_audit(ws_id, event)
    except (KeyboardInterrupt, SystemExit, asyncio.CancelledError):
        # Cancellation / process-control signals must propagate so the
        # caller's task or process can unwind. They are not operational
        # emission failures and must not be counted as such.
        raise
    except Exception:
        EMIT_FAILURES_TOTAL.add(1, {"event_type": event_type})
        _LOGGER.warning(
            "audit emission failed event_type=%s workspace=%s actor=%s subject=%s",
            event_type,
            workspace_id,
            actor,
            json.dumps(dict(subject), default=str, sort_keys=True),
            exc_info=True,
        )
        return
    _AUDIT_LOGGER.info(
        "audit_event event_type=%s workspace=%s actor=%s subject=%s",
        event_type,
        workspace_id,
        actor,
        json.dumps(dict(subject), default=str, sort_keys=True),
    )


# ---------------------------------------------------------------------------
# Typed helpers — one per canonical event
# ---------------------------------------------------------------------------


async def audit_workflow_published(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    workflow_name: str,
    version: int,
    derived_from_template_version_id: str | None = None,
) -> None:
    """Emit ``workflow.version.published`` after a successful publish."""
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_WORKFLOW_PUBLISHED,
        actor=actor,
        subject={"workflow_name": workflow_name, "version": version},
        payload={
            "derived_from_template_version_id": derived_from_template_version_id,
        },
    )


async def audit_workflow_deprecated(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    workflow_name: str,
    reason: str | None = None,
) -> None:
    """Emit ``workflow.deprecated`` after the parent-row toggle."""
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_WORKFLOW_DEPRECATED,
        actor=actor,
        subject={"workflow_name": workflow_name},
        payload={"reason": reason},
    )


async def audit_template_materialized(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    template_name: str,
    template_version: int,
    workflow_name: str,
    workflow_version: int,
) -> None:
    """Emit ``template.materialized`` after a successful materialize."""
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_TEMPLATE_MATERIALIZED,
        actor=actor,
        subject={
            "template_name": template_name,
            "template_version": template_version,
            "workflow_name": workflow_name,
            "workflow_version": workflow_version,
        },
        payload={},
    )


async def audit_template_extracted(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    source_workflow_name: str,
    source_workflow_version: int,
    template_name: str,
    template_version: int,
) -> None:
    """Emit ``template.extracted`` after a successful extract."""
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_TEMPLATE_EXTRACTED,
        actor=actor,
        subject={
            "source_workflow_name": source_workflow_name,
            "source_workflow_version": source_workflow_version,
            "template_name": template_name,
            "template_version": template_version,
        },
        payload={},
    )


async def audit_activity_registered(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    namespace: str,
    type_name: str,
    version: str,
    digest: str,
    referrer_ref: str | None = None,
) -> None:
    """Emit ``activity.type.registered`` after a successful register."""
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_ACTIVITY_REGISTERED,
        actor=actor,
        subject={"namespace": namespace, "type": type_name, "version": version},
        payload={"digest": digest, "referrer_ref": referrer_ref},
    )


async def audit_activity_deprecated(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    namespace: str,
    type_name: str,
    reason: str | None = None,
) -> None:
    """Emit ``activity.type.deprecated`` after the parent-row toggle."""
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_ACTIVITY_DEPRECATED,
        actor=actor,
        subject={"namespace": namespace, "type": type_name},
        payload={"reason": reason},
    )


async def audit_connector_registered(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    type_name: str,
    version: str,
    digest: str,
) -> None:
    """Emit ``connector.type.registered`` after a successful register.

    Connector types are globally addressable (the SPL row is not
    workspace-scoped), but every audit event must carry a workspace id
    for the SPL audit-partition index. Callers pass the workspace of
    the call-context that triggered the registration.
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_CONNECTOR_REGISTERED,
        actor=actor,
        subject={"type": type_name, "version": version},
        payload={"digest": digest},
    )


async def audit_connector_deprecated(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    type_name: str,
    reason: str | None = None,
) -> None:
    """Emit ``connector.type.deprecated`` after the parent-row toggle."""
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_CONNECTOR_DEPRECATED,
        actor=actor,
        subject={"type": type_name},
        payload={"reason": reason},
    )


# ---------------------------------------------------------------------------
# Legacy log-only hook (retained for the dev-shim middleware)
# ---------------------------------------------------------------------------


def emit_event(name: str, payload: Mapping[str, Any]) -> None:
    """Emit a structured audit-style log line.

    Retained for the call-context dev-shim hook that fires before any
    DI machinery is in scope (the middleware runs ahead of FastAPI's
    request handler and therefore cannot reach the configured
    :class:`~custos_spl.MetadataStoreProvider`). CS-IMPL-024 will
    replace the dev shim itself with the real auth + audit path.

    All in-process emission for catalog mutations now flows through
    the typed helpers above; new call sites SHOULD NOT use this
    function.
    """
    try:
        body = json.dumps(dict(payload), default=str, sort_keys=True)
    except (TypeError, ValueError):
        body = repr(dict(payload))
    _AUDIT_LOGGER.info("audit_event name=%s payload=%s", name, body)


__all__ = [
    "EMIT_FAILURES_TOTAL",
    "EVENT_ACTIVITY_DEPRECATED",
    "EVENT_ACTIVITY_REGISTERED",
    "EVENT_CONNECTOR_DEPRECATED",
    "EVENT_CONNECTOR_REGISTERED",
    "EVENT_TEMPLATE_EXTRACTED",
    "EVENT_TEMPLATE_MATERIALIZED",
    "EVENT_WORKFLOW_DEPRECATED",
    "EVENT_WORKFLOW_PUBLISHED",
    "audit_activity_deprecated",
    "audit_activity_registered",
    "audit_connector_deprecated",
    "audit_connector_registered",
    "audit_template_extracted",
    "audit_template_materialized",
    "audit_workflow_deprecated",
    "audit_workflow_published",
    "emit_event",
    "logger",
]
