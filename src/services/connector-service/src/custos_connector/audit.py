"""Audit emission helpers for connector-service.

Two emission paths live here:

1. **Typed audit pipeline** (used by :class:`InstanceService` and
   future managers). Each domain event has a typed helper such as
   :func:`audit_instance_created`. Helpers call :func:`_emit` which
   builds an :class:`custos_spl.AuditEvent` and writes it through
   :meth:`custos_spl.MetadataStoreProvider.append_audit`. Failures
   are best-effort: they are logged at WARNING + counted on
   :data:`EMIT_FAILURES_TOTAL` but never roll back the state
   mutation that triggered the emission. This mirrors the
   catalog-service post-CS-IMPL-019 pattern.

2. **Legacy log-only shim** :func:`emit_event` for events fired
   before the FastAPI DI machinery has yielded a configured
   metadata store: the call-context dev-shim hook
   (``auth.callctx.shim_used``) and the FastAPI authorization
   decision hook (``authz.decision``). Both fire from middleware /
   dependency layers where the SPL provider is not yet available.
   CONN-IMPL-029 (Phase K) will replace these with proper
   audit-pipeline emissions; until then the warning log is the
   operator signal.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Mapping, Sequence
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, Final
from uuid import uuid4

from custos_spl import AuditEvent
from custos_spl.ids import WorkspaceId
from opentelemetry import metrics

if TYPE_CHECKING:
    from custos_spl import MetadataStoreProvider

_LOGGER = logging.getLogger("custos_connector.audit")
_AUDIT_LOGGER = logging.getLogger("custos_connector.audit.event")
# Back-compat alias retained for the legacy stub callers.
logger = _AUDIT_LOGGER

_INSTRUMENTATION_NAME: Final[str] = "custos_connector"
_INSTRUMENTATION_VERSION: Final[str] = "0.1.0"

_meter = metrics.get_meter(_INSTRUMENTATION_NAME, _INSTRUMENTATION_VERSION)

#: Counter incremented when ``append_audit`` raises and the emission
#: is dropped. The Observability Service alert rules treat any
#: non-zero rate on this counter as page-worthy because every drop
#: is an audit-trail hole.
EMIT_FAILURES_TOTAL = _meter.create_counter(
    name="custos_audit_emit_failures_total",
    description=(
        "Count of connector-service audit emissions that failed to reach the "
        "SPL audit outbox. Labelled by event_type."
    ),
)


# ---------------------------------------------------------------------------
# Canonical event names
# ---------------------------------------------------------------------------


EVENT_INSTANCE_CREATED: Final[str] = "connector.instance.created"
EVENT_INSTANCE_UPDATED: Final[str] = "connector.instance.updated"
EVENT_INSTANCE_ENABLED: Final[str] = "connector.instance.enabled"
EVENT_INSTANCE_DISABLED: Final[str] = "connector.instance.disabled"
EVENT_HEALTH_CHECK_INVOKED: Final[str] = "connector.health-check.invoked"
EVENT_HEALTH_CHECK_COMPLETED: Final[str] = "connector.health-check.completed"
#: CONN-IMPL-015 (Phase F). Emitted by
#: :class:`IdentityResolverRegistry` whenever a resolver mints fresh
#: credential material. Rate-limited per (workspace, instance) so a
#: noisy bind loop does not flood the audit outbox.
EVENT_IDENTITY_RESOLVED: Final[str] = "connector.identity.resolved"
#: CONN-IMPL-015 (Phase F). Emitted by
#: :class:`IdentityResolverRegistry` on any resolver failure. Not
#: rate-limited; every failure is operationally significant.
EVENT_IDENTITY_FAILED: Final[str] = "connector.identity.failed"
#: CONN-IMPL-016 (Phase G). Emitted by
#: :class:`custos_connector.binding.BindForStepService` on a successful
#: ``BindForStep`` round-trip. Subject carries the run/step coordinates;
#: payload carries the resolved slot → instance map (no secret material).
EVENT_BINDING_CREATED: Final[str] = "connector.binding.created"
#: CONN-IMPL-016 (Phase G). Emitted by the binder when a ``BindForStep``
#: call is rejected (workspace mismatch, instance disabled or unhealthy,
#: capability shortfall, upstream identity failure). Carries the reason
#: code and the offending slot so operators can diagnose the failure.
EVENT_BINDING_REJECTED: Final[str] = "connector.binding.rejected"
#: CONN-IMPL-016 (Phase G). Emitted once per deprecated capability
#: consumed by a successful ``BindForStep`` call. The connector type
#: version still satisfies the request, but operators see one event
#: per deprecated capability so they can plan migration.
EVENT_CAPABILITY_DEPRECATED: Final[str] = "connector.capability.deprecated"


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
    """Append a connector-service audit event to the SPL outbox.

    Best-effort: any failure here is logged at WARNING + bumps
    :data:`EMIT_FAILURES_TOTAL` but is otherwise swallowed so the
    state mutation that triggered the emission stays committed.
    Cancellation / process-control signals propagate.
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


async def audit_instance_created(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    instance_id: str,
    type_name: str,
    version: str,
    name: str | None,
    enabled: bool,
    lease_ttl_seconds: int | None,
) -> None:
    """Emit ``connector.instance.created`` after a successful PUT."""
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_INSTANCE_CREATED,
        actor=actor,
        subject={"instance_id": instance_id, "type": type_name, "version": version},
        payload={
            "name": name,
            "enabled": enabled,
            "lease_ttl_seconds": lease_ttl_seconds,
        },
    )


async def audit_instance_updated(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    instance_id: str,
    changes: Mapping[str, Mapping[str, Any]],
) -> None:
    """Emit ``connector.instance.updated`` after a successful PATCH.

    ``changes`` maps each mutated field to a ``{"from": ..., "to": ...}``
    pair so the audit trail can answer "who changed what to what".
    The service layer is responsible for building the diff; this
    helper is dumb passthrough so the emission contract stays cheap.
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_INSTANCE_UPDATED,
        actor=actor,
        subject={"instance_id": instance_id},
        payload={"changes": {k: dict(v) for k, v in changes.items()}},
    )


async def audit_instance_enabled(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    instance_id: str,
    health_status: str | None,
) -> None:
    """Emit ``connector.instance.enabled`` after enable transition succeeds."""
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_INSTANCE_ENABLED,
        actor=actor,
        subject={"instance_id": instance_id},
        payload={"health_status": health_status},
    )


async def audit_instance_disabled(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    instance_id: str,
) -> None:
    """Emit ``connector.instance.disabled`` after disable transition succeeds."""
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_INSTANCE_DISABLED,
        actor=actor,
        subject={"instance_id": instance_id},
        payload={},
    )


async def audit_health_check_invoked(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    instance_id: str,
    healthy: bool,
    detail: str | None,
) -> None:
    """Emit ``connector.health-check.invoked`` for operator force-check calls."""
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_HEALTH_CHECK_INVOKED,
        actor=actor,
        subject={"instance_id": instance_id},
        payload={"healthy": healthy, "detail": detail},
    )


async def audit_health_check_completed(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    instance_id: str,
    healthy: bool,
    detail: str | None,
) -> None:
    """Emit ``connector.health-check.completed`` for every probe completion."""
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_HEALTH_CHECK_COMPLETED,
        actor=actor,
        subject={"instance_id": instance_id},
        payload={"healthy": healthy, "detail": detail},
    )


async def audit_identity_resolved(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    instance_id: str,
    authentication_type: str,
    category: str,
    descriptor: str,
    material_keys: Sequence[str],
    expires_at: datetime | None,
    issued_at: datetime,
) -> None:
    """Emit ``connector.identity.resolved`` for a fresh resolution.

    The :class:`IdentityResolverRegistry` rate-limits this emission
    per ``(workspace_id, instance_id)``: the helper itself is not
    rate-limited so direct callers (tests, integration probes) see
    every emission.

    ``descriptor`` is the *non-secret* source identifier the resolver
    returned (e.g. ``"azure-key-vault:https://vault/secrets/foo"``);
    ``material_keys`` is the set of envelope keys that flow into the
    plugin. Neither field leaks the secret material itself.
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_IDENTITY_RESOLVED,
        actor=actor,
        subject={
            "instance_id": instance_id,
            "authentication_type": authentication_type,
            "category": category,
        },
        payload={
            "descriptor": descriptor,
            "material_keys": list(material_keys),
            "issued_at": issued_at.isoformat(),
            "expires_at": expires_at.isoformat() if expires_at else None,
        },
    )


async def audit_identity_failed(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    instance_id: str,
    authentication_type: str,
    category: str,
    error_code: str,
    error_detail: str,
    error_data: Mapping[str, Any],
) -> None:
    """Emit ``connector.identity.failed`` for any resolver-side failure.

    The taxonomy carried on ``error_code`` is the stable
    :class:`~custos_connector.identity.IdentityResolverErrorCode` set;
    the audit consumer treats it as an opaque string.

    ``category`` is the resolved
    :class:`~custos_connector.loader.identity.IdentityCategory` value
    (e.g. ``"kms"``, ``"workload"``, ``"federated"``, ``"vendor"``) when
    the registry got far enough to derive it. For failures raised
    *before* category derivation — currently only
    :class:`~custos_connector.identity.IdentityResolverErrorCode.UNKNOWN_AUTHENTICATION_TYPE`,
    where there is no resolver and therefore no category — the registry
    passes the sentinel string ``"unknown"`` so the audit subject shape
    remains stable.
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_IDENTITY_FAILED,
        actor=actor,
        subject={
            "instance_id": instance_id,
            "authentication_type": authentication_type,
            "category": category,
        },
        payload={
            "error_code": error_code,
            "error_detail": error_detail,
            "error_data": dict(error_data),
        },
    )


async def audit_binding_created(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    run_id: str,
    step_id: str,
    attempt: int,
    step_key: str,
    slots: Mapping[str, str],
) -> None:
    """Emit ``connector.binding.created`` for a successful ``BindForStep``.

    ``slots`` is the resolved slot-name → ``instance_id`` map. The
    payload deliberately carries only opaque identifiers; the actual
    :class:`ConnectorContext` payload (endpoint, handle, extras) flows
    over the RPC and never lands in the audit outbox.
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_BINDING_CREATED,
        actor=actor,
        subject={
            "run_id": run_id,
            "step_id": step_id,
            "attempt": attempt,
            "step_key": step_key,
        },
        payload={
            "slots": dict(slots),
        },
    )


async def audit_binding_rejected(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    run_id: str,
    step_id: str,
    attempt: int,
    step_key: str,
    slot: str | None,
    instance_id: str | None,
    reason_code: str,
    reason_detail: str,
) -> None:
    """Emit ``connector.binding.rejected`` for a failed ``BindForStep``.

    ``slot`` and ``instance_id`` are the first slot whose validation
    failed; both can be ``None`` for request-level rejections (e.g.
    empty slots array, malformed request) where no specific slot is
    implicated. ``reason_code`` is the stable taxonomy string from
    :class:`custos_connector.binding.errors.BindErrorCode`.
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_BINDING_REJECTED,
        actor=actor,
        subject={
            "run_id": run_id,
            "step_id": step_id,
            "attempt": attempt,
            "step_key": step_key,
        },
        payload={
            "slot": slot,
            "instance_id": instance_id,
            "reason_code": reason_code,
            "reason_detail": reason_detail,
        },
    )


async def audit_capability_deprecated(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    instance_id: str,
    type_name: str,
    version: str,
    capability: str,
    run_id: str,
    step_id: str,
    attempt: int,
) -> None:
    """Emit ``connector.capability.deprecated`` for a deprecated bind.

    Fired once per deprecated capability consumed by a successful
    ``BindForStep``. The connector type version *can* still satisfy
    the request — deprecation is an advisory signal, not a hard
    failure — so this event accompanies (not replaces)
    ``connector.binding.created``.
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_CAPABILITY_DEPRECATED,
        actor=actor,
        subject={
            "instance_id": instance_id,
            "type": type_name,
            "version": version,
            "capability": capability,
        },
        payload={
            "run_id": run_id,
            "step_id": step_id,
            "attempt": attempt,
        },
    )


# ---------------------------------------------------------------------------
# Legacy log-only shim (call-context + authz decision events)
# ---------------------------------------------------------------------------


def emit_event(name: str, payload: Mapping[str, Any]) -> None:
    """Emit a structured audit-style log line.

    Retained for the call-context dev-shim hook + the
    authorization-decision hook fired from
    :func:`custos_connector.middleware.require_permission`. Both fire
    before the FastAPI DI machinery has yielded a configured
    :class:`~custos_spl.MetadataStoreProvider`, so the legacy log-only
    hook is the right tool until CONN-IMPL-029 rewires them onto the
    real audit pipeline.

    Args:
        name: Canonical event name (e.g. ``auth.callctx.shim_used``,
            ``authz.decision``).
        payload: Per-event attributes. Values that aren't directly
            JSON-serialisable are coerced via ``str`` during JSON
            encoding so the audit log line is never lost; if JSON
            serialisation still fails, the whole payload falls back
            to ``repr(dict(payload))``.
    """
    try:
        body = json.dumps(dict(payload), default=str, sort_keys=True)
    except (TypeError, ValueError):
        body = repr(dict(payload))
    _AUDIT_LOGGER.info("audit_event name=%s payload=%s", name, body)


__all__ = [
    "EMIT_FAILURES_TOTAL",
    "EVENT_BINDING_CREATED",
    "EVENT_BINDING_REJECTED",
    "EVENT_CAPABILITY_DEPRECATED",
    "EVENT_HEALTH_CHECK_COMPLETED",
    "EVENT_HEALTH_CHECK_INVOKED",
    "EVENT_IDENTITY_FAILED",
    "EVENT_IDENTITY_RESOLVED",
    "EVENT_INSTANCE_CREATED",
    "EVENT_INSTANCE_DISABLED",
    "EVENT_INSTANCE_ENABLED",
    "EVENT_INSTANCE_UPDATED",
    "audit_binding_created",
    "audit_binding_rejected",
    "audit_capability_deprecated",
    "audit_health_check_completed",
    "audit_health_check_invoked",
    "audit_identity_failed",
    "audit_identity_resolved",
    "audit_instance_created",
    "audit_instance_disabled",
    "audit_instance_enabled",
    "audit_instance_updated",
    "emit_event",
    "logger",
]
