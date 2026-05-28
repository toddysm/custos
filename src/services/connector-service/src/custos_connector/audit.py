"""Audit emission helpers for connector-service.

Two emission paths live here:

1. **Typed audit pipeline** (used by :class:`InstanceService`, the
   Loader, the manifest discovery flow, the authorization decision
   hook, and every future manager). Each domain event has a typed
   helper such as :func:`audit_instance_created`. Helpers call
   :func:`_emit` which builds an :class:`custos_spl.AuditEvent` and
   writes it through
   :meth:`custos_spl.MetadataStoreProvider.append_audit`. Failures
   are best-effort: they are logged at WARNING + counted on
   :data:`EMIT_FAILURES_TOTAL` but never roll back the state
   mutation that triggered the emission. This mirrors the
   catalog-service post-CS-IMPL-019 pattern.

2. **Legacy log-only shim** :func:`emit_event` retained for the
   call-context dev-shim hook (``auth.callctx.shim_used``) which
   fires from middleware that runs *before* the FastAPI DI machinery
   has yielded a configured metadata store. CONN-IMPL-029 (Phase K)
   promoted every other previously-log-only event to the typed
   pipeline; the shim hangs on solely for the dev-shim warning.

Platform-scope events
---------------------

Some events are logically platform-global: connector-type
registration / deprecation / discovery flows mutate the platform
catalog rather than a single workspace, but
:meth:`MetadataStoreProvider.append_audit` is workspace-keyed.
Following the auth-service convention (AS-IMPL-006) we write those
rows under the sentinel workspace id :data:`PLATFORM_WORKSPACE_ID`
(``"__platform__"``); the Observability Service treats that id as the
control-plane bucket.
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


#: Sentinel workspace id used for platform-scope audit rows
#: (connector-type registration, deprecation, manifest-fallback
#: discovery, and any future platform-global event). The
#: Observability Service indexes the literal ``__platform__`` workspace
#: as the control-plane bucket; see auth-service for the prior art
#: (``custos_auth.audit.PLATFORM_WORKSPACE_ID``).
PLATFORM_WORKSPACE_ID: Final[str] = "__platform__"


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

EVENT_LEASE_ISSUED: Final[str] = "lease.issued"
EVENT_LEASE_REFRESHED: Final[str] = "lease.refreshed"
EVENT_LEASE_RELEASED: Final[str] = "lease.released"
#: CONN-IMPL-018 (Phase G/3). Emitted when a lease reaches its
#: ``expires_at`` deadline without an intervening refresh or release.
#: Carries the same identifier tuple as :data:`EVENT_LEASE_ISSUED`
#: plus the ``expired_at`` wall clock and a short ``reason`` tag
#: ("ttl-reached" from the sweeper, "sidecar-shutdown" from the
#: per-step sidecar shutdown hook).
EVENT_LEASE_EXPIRED: Final[str] = "lease.expired"
#: CONN-IMPL-018 (Phase G/3). Emitted by the operator revoke endpoint
#: before any individual lease revocation begins. Carries the
#: selector (lease | instance | run), the resolved lease IDs, the
#: operator identity, and a free-form reason so the audit trail
#: ties every subsequent ``lease.revoked`` back to a single
#: operator action.
EVENT_LEASE_REVOKE_REQUESTED: Final[str] = "lease.revoke-requested"
#: CONN-IMPL-018 (Phase G/3). Emitted per lease after the SPL
#: ``revoke_lease`` write commits (and, for sidecar-routed
#: revocations, after the sidecar control-channel acks). Carries
#: the full identifier tuple plus ``revoked_at`` and
#: ``revoke_reason`` for forensic correlation with the matching
#: ``lease.revoke-requested`` event.
EVENT_LEASE_REVOKED: Final[str] = "lease.revoked"
#: CONN-IMPL-018 (Phase G/3). Emitted whenever a lease operation is
#: rejected. Auto-fired by the Lease Manager when ``issue`` /
#: ``refresh`` / ``release`` raise :class:`LeaseError`, and fired
#: directly by the future REST handler when authorization or
#: capability checks bounce a request before it reaches the manager.
EVENT_LEASE_DENIED: Final[str] = "lease.denied"

#: CONN-IMPL-022 (Phase I). Emitted by :class:`CursorService` after
#: every successful pull tick that publishes a batch and commits the
#: cursor. Carries audit envelopes (``encoding`` + ``valueFingerprint``
#: + optional ``valueLength``; never the raw ``value``) for both the
#: pre-tick and post-tick cursor positions plus ``reason="tick"`` and
#: ``eventCount``.
EVENT_CURSOR_ADVANCED: Final[str] = "cursor.advanced"
#: CONN-IMPL-022 (Phase I). Emitted by :class:`CursorService` when a
#: plugin returns :class:`CursorExpired` from ``listen(mode=pull)``.
#: Carries the last-known cursor in audit-envelope form (no raw
#: value) plus the upstream error detail. Ticks for the instance
#: halt pending operator action.
EVENT_CURSOR_EXPIRED: Final[str] = "cursor.expired"
#: CONN-IMPL-022 (Phase I). Emitted by :class:`CursorService` when a
#: plugin returns :class:`CursorEncodingMismatch` from
#: ``listen(mode=pull)``. Carries the persisted (manifest-declared)
#: encoding and the plugin-declared encoding so operators can correlate
#: a connector-type upgrade with the migration that needs a rewind.
EVENT_CURSOR_ENCODING_MISMATCH: Final[str] = "cursor.encoding_mismatch"
#: CONN-IMPL-024 (Phase I). Emitted by the cursor admin router on a
#: successful ``POST /v1/workspaces/{ws}/connectors/{id}/cursor:rewind``.
#: Per design § Pull Cursor Model the SPL ``rewind_cursor`` adapter is
#: the eventual audit emitter (tracked under #129); until that lands
#: the connector-service admin router emits the event itself so the
#: operator surface ships with the canonical audit trail.
EVENT_CURSOR_REWOUND: Final[str] = "cursor.rewound"
#: CONN-IMPL-024 (Phase I). Emitted by the pull-loop admin router on a
#: successful ``POST /v1/workspaces/{ws}/connectors/{id}/pull-loop:pause``.
EVENT_PULL_LOOP_PAUSED: Final[str] = "connector.pull-loop.paused"
#: CONN-IMPL-024 (Phase I). Emitted by the pull-loop admin router on a
#: successful ``POST /v1/workspaces/{ws}/connectors/{id}/pull-loop:resume``.
EVENT_PULL_LOOP_RESUMED: Final[str] = "connector.pull-loop.resumed"

#: CONN-IMPL-025 (Phase I). Emitted by the push receiver
#: (:func:`custos_connector.listen.router.post_events`) after a
#: well-formed ``POST /v1/connectors/{instance_id}/events`` body has
#: passed signature verification and basic JSON parsing. Carries the
#: ``delivery_mode`` (always ``"push"`` here, retained for symmetry
#: with the pull path), the total ``event_count`` in the batch, and a
#: ``source`` hint identifying the request as webhook-borne.
#: Per-event ``event.normalized`` / ``event.rejected`` rows still fire
#: from the shared publisher bridge after this one ``event.received``.
EVENT_RECEIVED: Final[str] = "event.received"
#: CONN-IMPL-025 (Phase I). Emitted by the shared publisher bridge
#: (:func:`custos_connector.listen.publisher.process_batch`) once per
#: plugin event that passes :class:`EventNormalizer` validation and
#: was successfully forwarded to the wired
#: :class:`EventPublisher`. Carries ``event_id``, ``event_type``,
#: ``delivery_mode`` (``"pull"`` | ``"push"``), and the ``batch_index``
#: of the event inside its origin batch so operators can reconstruct
#: per-batch ordering even after fan-out.
EVENT_NORMALIZED: Final[str] = "event.normalized"
#: CONN-IMPL-025 (Phase I). Emitted by the shared publisher bridge
#: whenever :class:`EventNormalizer` raises (missing/empty
#: ``eventId``, missing/empty ``eventType``, ``eventType`` not in the
#: connector type's ``events.produced`` catalog, or malformed event
#: object). Carries a stable ``reason`` code
#: (``missing-event-id`` | ``missing-event-type`` |
#: ``unknown-event-type`` | ``malformed``) plus the ``batch_index`` and
#: the ``event_type`` / partial-``event_id`` if they were
#: present-but-rejected. The cursor still advances (rejections are
#: poison-pill quarantined per design §22.4 "Push receiver and pull
#: fan-out"); operators reading the audit log can choose to halt the
#: connector via ``pull-loop:pause`` if reject rate spikes.
EVENT_REJECTED: Final[str] = "event.rejected"


# ---------------------------------------------------------------------------
# CONN-IMPL-029 (Phase K) — platform-scope + middleware events
# ---------------------------------------------------------------------------


#: Emitted by the Plugin Loader when a connector-type registration
#: succeeds. Platform-scope; written under :data:`PLATFORM_WORKSPACE_ID`.
EVENT_REGISTRATION_ACCEPTED: Final[str] = "connector.registration.accepted"
#: Emitted by the Plugin Loader when a connector-type registration
#: fails. Platform-scope; written under :data:`PLATFORM_WORKSPACE_ID`.
EVENT_REGISTRATION_REJECTED: Final[str] = "connector.registration.rejected"
#: Emitted by the Plugin Loader on a successful deprecation toggle.
#: Platform-scope; written under :data:`PLATFORM_WORKSPACE_ID`.
EVENT_DEPRECATION_TOGGLED: Final[str] = "connector.deprecation.toggled"
#: Emitted by :func:`discover_manifest` when the deterministic fallback
#: tag resolved authoritatively. Platform-scope.
EVENT_MANIFEST_FALLBACK_USED: Final[str] = "connector.manifest.fallback-used"
#: Emitted by :func:`discover_manifest` when the Referrers API
#: resolved and the deterministic fallback tag was deliberately not
#: consulted. Platform-scope.
EVENT_MANIFEST_FALLBACK_IGNORED: Final[str] = "connector.manifest.fallback-ignored"
#: Emitted by :func:`discover_manifest` when discovery rejected on the
#: fallback path or during final resolution (ambiguous, unknown digest
#: algorithm, tag too long, no manifest found). Platform-scope.
EVENT_MANIFEST_FALLBACK_REJECTED: Final[str] = "connector.manifest.fallback-rejected"
#: Emitted by :func:`require_permission` in the middleware layer.
#: Carries the request path, method, permission, decision, and
#: principal so the audit log answers "who was allowed/denied what
#: when". Workspace-scoped via the verified call context.
EVENT_AUTHZ_DECISION: Final[str] = "authz.decision"


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
# Lease lifecycle events (CONN-IMPL-017 + CONN-IMPL-018)
# ---------------------------------------------------------------------------


async def audit_lease_issued(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    lease_id: str,
    run_id: str,
    step_id: str,
    attempt: int,
    slot: str,
    capability: str,
    connector_instance_id: str,
    token_type: str,
    issued_at: datetime,
    expires_at: datetime,
) -> None:
    """Emit ``lease.issued`` after the Lease Manager mints a lease.

    Fired after a successful :meth:`LeaseManager.issue` — both the
    cap check and the SPL ``put_lease`` write have already committed.
    ``lease_id`` is the freshly minted ULID and is the join key for
    the matching ``lease.refreshed`` / ``lease.released`` /
    ``lease.revoked`` events.
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_LEASE_ISSUED,
        actor=actor,
        subject={"lease_id": lease_id, "connector_instance_id": connector_instance_id},
        payload={
            "run_id": run_id,
            "step_id": step_id,
            "attempt": attempt,
            "slot": slot,
            "capability": capability,
            "token_type": token_type,
            "issued_at": issued_at.isoformat(),
            "expires_at": expires_at.isoformat(),
        },
    )


async def audit_lease_refreshed(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    lease_id: str,
    run_id: str,
    step_id: str,
    attempt: int,
    slot: str,
    capability: str,
    connector_instance_id: str,
    token_type: str,
    previous_expires_at: datetime,
    new_expires_at: datetime,
) -> None:
    """Emit ``lease.refreshed`` after :meth:`LeaseManager.refresh`.

    ``previous_expires_at`` is the pre-refresh deadline so the audit
    trail can answer "by how much was this lease extended" without
    needing to correlate against ``lease.issued``. The ``lease_id``
    remains stable across refreshes.

    The full identifier tuple (``run_id``, ``step_id``, ``attempt``,
    ``slot``, ``capability``, ``token_type``) is repeated on every
    lease event per the audit-table contract in
    ``design/components/connector-service/design.md`` so downstream
    consumers can filter or group without first joining against
    ``lease.issued``.
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_LEASE_REFRESHED,
        actor=actor,
        subject={"lease_id": lease_id, "connector_instance_id": connector_instance_id},
        payload={
            "run_id": run_id,
            "step_id": step_id,
            "attempt": attempt,
            "slot": slot,
            "capability": capability,
            "token_type": token_type,
            "previous_expires_at": previous_expires_at.isoformat(),
            "new_expires_at": new_expires_at.isoformat(),
        },
    )


async def audit_lease_released(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    lease_id: str,
    run_id: str,
    step_id: str,
    attempt: int,
    slot: str,
    capability: str,
    connector_instance_id: str,
    token_type: str,
    released_at: datetime,
) -> None:
    """Emit ``lease.released`` after :meth:`LeaseManager.release`.

    Release is idempotent at the adapter level; emission is best-effort
    but the Lease Manager fires it on every successful release call
    (including no-op repeats) so the audit trail records each request.

    Carries the full identifier tuple for the same join-free
    consumer experience documented on :func:`audit_lease_refreshed`.
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_LEASE_RELEASED,
        actor=actor,
        subject={"lease_id": lease_id, "connector_instance_id": connector_instance_id},
        payload={
            "run_id": run_id,
            "step_id": step_id,
            "attempt": attempt,
            "slot": slot,
            "capability": capability,
            "token_type": token_type,
            "released_at": released_at.isoformat(),
        },
    )


async def audit_lease_expired(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    lease_id: str,
    run_id: str,
    step_id: str,
    attempt: int,
    slot: str,
    capability: str,
    connector_instance_id: str,
    token_type: str,
    expires_at: datetime,
    expired_at: datetime,
    reason: str,
) -> None:
    """Emit ``lease.expired`` for a lease that reached its TTL.

    ``expires_at`` is the originally scheduled deadline; ``expired_at``
    is the wall clock at which the expiry was detected (sweeper tick
    or sidecar shutdown). ``reason`` is a short tag — currently
    ``"ttl-reached"`` (sweeper) or ``"sidecar-shutdown"`` (per-step
    sidecar) — that lets operators distinguish routine TTL churn from
    pod-lifecycle-driven mass expiries.

    Distinct from :func:`audit_lease_released` because release is a
    voluntary client-driven action whereas expiry is an
    infrastructure-driven cleanup. Both close the slot for the
    cap-check primitive, but only ``lease.released`` implies the
    client knows the lease is gone.
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_LEASE_EXPIRED,
        actor=actor,
        subject={"lease_id": lease_id, "connector_instance_id": connector_instance_id},
        payload={
            "run_id": run_id,
            "step_id": step_id,
            "attempt": attempt,
            "slot": slot,
            "capability": capability,
            "token_type": token_type,
            "expires_at": expires_at.isoformat(),
            "expired_at": expired_at.isoformat(),
            "reason": reason,
        },
    )


async def audit_lease_revoke_requested(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    selector_type: str,
    selector_value: str,
    lease_ids: Sequence[str],
    reason: str,
    operator: str,
) -> None:
    """Emit ``lease.revoke-requested`` for an operator-initiated revoke.

    Fired once per operator action, *before* any individual
    :meth:`LeaseManager.revoke` call begins. ``selector_type`` is
    one of ``"lease"`` (single lease ID), ``"instance"`` (all
    active leases for a connector instance), or ``"run"`` (all
    active leases for a workflow run). ``lease_ids`` is the
    fully-resolved list at request time — the audit consumer can
    correlate each subsequent ``lease.revoked`` to this single
    request without re-resolving the selector.

    ``operator`` is the human (or system principal) requesting the
    revocation, distinct from ``actor`` which is the service that
    actually emitted the event. ``reason`` is the free-form
    operator-supplied justification.
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_LEASE_REVOKE_REQUESTED,
        actor=actor,
        subject={"selector_type": selector_type, "selector_value": selector_value},
        payload={
            "lease_ids": list(lease_ids),
            "reason": reason,
            "operator": operator,
        },
    )


async def audit_lease_revoked(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    lease_id: str,
    run_id: str,
    step_id: str,
    attempt: int,
    slot: str,
    capability: str,
    connector_instance_id: str,
    token_type: str,
    revoked_at: datetime,
    revoke_reason: str,
) -> None:
    """Emit ``lease.revoked`` after :meth:`LeaseManager.revoke`.

    Fired per lease, after the SPL ``revoke_lease`` write commits.
    Carries the full identifier tuple plus ``revoked_at`` and the
    forensic ``revoke_reason`` so the audit consumer can correlate
    each revocation back to its initiating
    ``lease.revoke-requested`` event without join. Idempotent at the
    adapter level; emission fires on every successful revoke call.
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_LEASE_REVOKED,
        actor=actor,
        subject={"lease_id": lease_id, "connector_instance_id": connector_instance_id},
        payload={
            "run_id": run_id,
            "step_id": step_id,
            "attempt": attempt,
            "slot": slot,
            "capability": capability,
            "token_type": token_type,
            "revoked_at": revoked_at.isoformat(),
            "revoke_reason": revoke_reason,
        },
    )


async def audit_lease_denied(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    lease_id: str | None,
    connector_instance_id: str | None,
    op: str,
    reason_code: str,
    reason_detail: str,
    http_status: int,
) -> None:
    """Emit ``lease.denied`` whenever a lease request is rejected.

    Auto-fired by the Lease Manager when ``issue`` / ``refresh`` /
    ``release`` raise :class:`LeaseError`, and fired directly by the
    future REST handler when authorization or capability checks
    bounce a request before it reaches the manager.

    ``op`` is ``"issue"``, ``"refresh"``, or ``"release"``.
    ``reason_code`` is the stable taxonomy string from
    :class:`custos_connector.lease.errors.LeaseErrorCode` (e.g.
    ``"CAPACITY_EXCEEDED"``, ``"NOT_FOUND"``,
    ``"ALREADY_RELEASED"``, ``"INVALID_REQUEST"``). ``lease_id`` and
    ``connector_instance_id`` are nullable so request-level
    rejections that never resolved an instance (e.g. capacity check
    on issue, malformed request) still emit a well-formed event.
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_LEASE_DENIED,
        actor=actor,
        subject={"lease_id": lease_id, "connector_instance_id": connector_instance_id},
        payload={
            "op": op,
            "reason_code": reason_code,
            "reason_detail": reason_detail,
            "http_status": http_status,
        },
    )


# ---------------------------------------------------------------------------
# Cursor lifecycle events (CONN-IMPL-022)
# ---------------------------------------------------------------------------


def _cursor_envelope_audit(
    encoding: str,
    value_fingerprint: str | None,
    value_length: int | None,
) -> Mapping[str, Any]:
    """Build the non-sensitive audit envelope for a cursor position.

    Per ``design/components/connector-service/design.md`` § Pull Cursor
    Model → Cursor audit events, audit consumers MUST receive only
    ``encoding`` + a non-reversible fingerprint of ``value`` plus the
    optional ``value`` length, never the raw ``value`` (cursor values
    are opaque and MUST NOT embed secrets).

    ``value_fingerprint`` / ``value_length`` are both ``None`` for the
    uninitialized cursor sentinel so consumers can distinguish a
    first-tick cursor from a committed-then-rewound-to-empty cursor.
    """
    return {
        "encoding": encoding,
        "valueFingerprint": value_fingerprint,
        "valueLength": value_length,
    }


async def audit_cursor_advanced(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    instance_id: str,
    from_encoding: str,
    from_value_fingerprint: str | None,
    from_value_length: int | None,
    to_encoding: str,
    to_value_fingerprint: str | None,
    to_value_length: int | None,
    event_count: int,
    reason: str = "tick",
) -> None:
    """Emit ``cursor.advanced`` after a successful pull tick commit.

    Carries ``from``/``to`` audit envelopes (`encoding`,
    `valueFingerprint`, `valueLength`; never raw `value`), ``reason``
    (currently always ``"tick"`` from :class:`CursorService`; operator
    rewinds emit ``cursor.rewound`` via SPL's ``rewind_cursor`` instead),
    and ``eventCount`` so operators can correlate cursor advance with
    the size of the batch that was published.
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_CURSOR_ADVANCED,
        actor=actor,
        subject={"instance_id": instance_id},
        payload={
            "from": _cursor_envelope_audit(
                from_encoding, from_value_fingerprint, from_value_length
            ),
            "to": _cursor_envelope_audit(to_encoding, to_value_fingerprint, to_value_length),
            "reason": reason,
            "eventCount": event_count,
        },
    )


async def audit_cursor_expired(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    instance_id: str,
    encoding: str,
    value_fingerprint: str | None,
    value_length: int | None,
    error_detail: str,
) -> None:
    """Emit ``cursor.expired`` when the plugin returns ``CursorExpired``.

    Carries the last-known cursor in the same envelope form as
    :func:`audit_cursor_advanced` plus the plugin-supplied
    ``error_detail`` so the audit trail records why the upstream
    rejected the persisted position. Ticks for the instance halt
    pending operator action (status flipped to ``cursor_expired`` by
    the :class:`CursorService` caller).
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_CURSOR_EXPIRED,
        actor=actor,
        subject={"instance_id": instance_id},
        payload={
            "lastKnown": _cursor_envelope_audit(encoding, value_fingerprint, value_length),
            "errorDetail": error_detail,
        },
    )


async def audit_cursor_encoding_mismatch(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    instance_id: str,
    persisted_encoding: str | None,
    plugin_encoding: str | None,
    error_detail: str,
) -> None:
    """Emit ``cursor.encoding_mismatch`` on a connector-type encoding bump.

    Carries the persisted (manifest-declared) and plugin-declared
    encodings so operators can correlate a connector-type upgrade with
    the migration that needs a rewind. Either side may be ``None`` when
    the plugin's error payload omits the corresponding hint. Ticks for
    the instance halt pending operator action (status flipped to
    ``cursor_migration_required`` by the :class:`CursorService` caller).
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_CURSOR_ENCODING_MISMATCH,
        actor=actor,
        subject={"instance_id": instance_id},
        payload={
            "persistedEncoding": persisted_encoding,
            "pluginEncoding": plugin_encoding,
            "errorDetail": error_detail,
        },
    )


async def audit_cursor_rewound(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    instance_id: str,
    from_encoding: str,
    from_value_fingerprint: str | None,
    from_value_length: int | None,
    to_encoding: str,
    to_value_fingerprint: str | None,
    to_value_length: int | None,
    reason: str,
) -> None:
    """Emit ``cursor.rewound`` after an operator-initiated rewind.

    Per ``design/components/connector-service/design.md`` § Pull Cursor
    Model → Admin rewind / replay, the SPL ``rewind_cursor`` adapter
    is the documented audit emitter for this event. Until that wiring
    lands (tracked as ``TODO(#129)`` in ``custos_pg``'s metadata
    adapter) the connector-service admin router emits the event
    itself, using the same audit-envelope shape as
    :func:`audit_cursor_advanced` so consumers do not need to special-case
    the operator path.

    Carries ``from``/``to`` audit envelopes (``encoding``,
    ``valueFingerprint``, ``valueLength``; never raw ``value``), the
    operator ``actor`` (call-context ``principal_id``), and the
    mandatory operator-supplied ``reason``.
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_CURSOR_REWOUND,
        actor=actor,
        subject={"instance_id": instance_id},
        payload={
            "from": _cursor_envelope_audit(
                from_encoding, from_value_fingerprint, from_value_length
            ),
            "to": _cursor_envelope_audit(to_encoding, to_value_fingerprint, to_value_length),
            "reason": reason,
        },
    )


async def audit_pull_loop_paused(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    instance_id: str,
    reason: str | None,
) -> None:
    """Emit ``connector.pull-loop.paused`` after a successful pause.

    Per design § Operator Admin Surface → Pull-loop lifecycle
    operations, an operator-initiated ``pull-loop:pause`` produces
    one ``connector.pull-loop.paused`` audit event carrying the
    operator identity and the optional free-form reason.
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_PULL_LOOP_PAUSED,
        actor=actor,
        subject={"instance_id": instance_id},
        payload={"reason": reason},
    )


async def audit_pull_loop_resumed(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    instance_id: str,
) -> None:
    """Emit ``connector.pull-loop.resumed`` after a successful resume.

    Per design § Operator Admin Surface → Pull-loop lifecycle
    operations, an operator-initiated ``pull-loop:resume`` produces
    one ``connector.pull-loop.resumed`` audit event carrying the
    operator identity.
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_PULL_LOOP_RESUMED,
        actor=actor,
        subject={"instance_id": instance_id},
        payload={},
    )


async def audit_event_received(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    instance_id: str,
    delivery_mode: str,
    event_count: int,
    source: str,
) -> None:
    """Emit ``event.received`` after a push webhook batch arrives.

    Fired by the push receiver
    (:func:`custos_connector.listen.router.post_events`) once per
    accepted POST after signature verification and JSON parsing
    succeed but before any per-event normalization runs. Per-event
    rows (``event.normalized`` / ``event.rejected``) follow from the
    shared publisher bridge.

    Args:
        workspace_id: Workspace owning the connector instance.
        actor: Authenticated principal (call-context ``principal_id``)
            credited with the receive — typically a service identity
            on the inbound webhook path.
        instance_id: Connector instance the events are routed to.
        delivery_mode: Always ``"push"`` from the router today;
            retained as a parameter so future delivery channels (e.g.
            internal RPC ingest) reuse the same audit shape.
        event_count: Number of event objects in the parsed batch
            before any normalization filtering.
        source: Free-form provenance hint
            (``"webhook"`` for the push router). Provides a
            machine-readable discriminator for operators reading audit
            logs across delivery channels.
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_RECEIVED,
        actor=actor,
        subject={"instance_id": instance_id},
        payload={
            "deliveryMode": delivery_mode,
            "eventCount": event_count,
            "source": source,
        },
    )


async def audit_event_normalized(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    instance_id: str,
    event_id: str,
    event_type: str,
    delivery_mode: str,
    batch_index: int,
) -> None:
    """Emit ``event.normalized`` for a single successfully normalized event.

    Fired by the shared publisher bridge
    (:func:`custos_connector.listen.publisher.process_batch`) once per
    plugin event that passes :class:`EventNormalizer` validation and
    was forwarded to the wired :class:`EventPublisher`.

    Args:
        workspace_id: Workspace owning the connector instance.
        actor: Authenticated principal credited with the publish.
        instance_id: Connector instance that produced the event.
        event_id: Plugin-supplied stable identifier (the same value
            that gates duplicate suppression downstream in the Trigger
            Service).
        event_type: Connector-type catalog token (e.g.
            ``"oci.image.pushed"``).
        delivery_mode: ``"pull"`` | ``"push"``.
        batch_index: Zero-based position of the event inside its
            origin batch so operators can reconstruct per-batch
            ordering even after fan-out.
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_NORMALIZED,
        actor=actor,
        subject={"instance_id": instance_id},
        payload={
            "eventId": event_id,
            "eventType": event_type,
            "deliveryMode": delivery_mode,
            "batchIndex": batch_index,
        },
    )


async def audit_event_rejected(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    instance_id: str,
    delivery_mode: str,
    batch_index: int,
    reason: str,
    event_id: str | None = None,
    event_type: str | None = None,
    detail: str | None = None,
) -> None:
    """Emit ``event.rejected`` for a single plugin event that failed normalization.

    Fired by the shared publisher bridge when
    :class:`EventNormalizer` raises. The cursor still advances
    (rejections are poison-pill quarantined per design § 22.4
    "Push receiver and pull fan-out"); operators reading the audit
    log can choose to halt the connector via ``pull-loop:pause`` if
    reject rate spikes.

    Args:
        reason: Stable machine-readable code: ``"missing-event-id"``,
            ``"missing-event-type"``, ``"unknown-event-type"``, or
            ``"malformed"``. The Trigger Service alerting consumes
            this code, so do not localise.
        event_id: The plugin-supplied identifier if present-but-other
            validation failed; ``None`` for the missing-eventId case.
        event_type: The plugin-supplied event type if present-but-not
            in the connector type's ``events.produced`` catalog;
            ``None`` for the missing-eventType case.
        detail: Optional free-form detail string carrying the
            exception message for the ``malformed`` case. Never
            carries raw payload bytes — only the normalizer error
            description.
    """
    payload: dict[str, Any] = {
        "deliveryMode": delivery_mode,
        "batchIndex": batch_index,
        "reason": reason,
    }
    if event_id is not None:
        payload["eventId"] = event_id
    if event_type is not None:
        payload["eventType"] = event_type
    if detail is not None:
        payload["detail"] = detail
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_REJECTED,
        actor=actor,
        subject={"instance_id": instance_id},
        payload=payload,
    )


# ---------------------------------------------------------------------------
# CONN-IMPL-029 (Phase K) — registration / discovery / authz helpers
# ---------------------------------------------------------------------------


_REGISTRATION_ACTOR_DEFAULT: Final[str] = "connector-loader"
_DISCOVERY_ACTOR_DEFAULT: Final[str] = "connector-loader"


async def audit_registration_accepted(
    metadata_store: MetadataStoreProvider,
    *,
    type_name: str,
    version: str,
    image_ref: str,
    manifest_digest: str,
    actor: str = _REGISTRATION_ACTOR_DEFAULT,
) -> None:
    """Emit ``connector.registration.accepted`` (platform-scope).

    Fired by :class:`Loader` after the connector-type registration
    pipeline succeeds (manifest fetched, validated, normalized,
    digested, persisted). Subject pins the (type, version) tuple;
    payload carries the image reference + manifest digest so the
    audit trail ties the catalog row to the OCI artifact bytes.
    """
    await _emit(
        metadata_store,
        workspace_id=PLATFORM_WORKSPACE_ID,
        event_type=EVENT_REGISTRATION_ACCEPTED,
        actor=actor,
        subject={"type": type_name, "version": version},
        payload={
            "image_ref": image_ref,
            "manifest_digest": manifest_digest,
        },
    )


async def audit_registration_rejected(
    metadata_store: MetadataStoreProvider,
    *,
    image_ref: str,
    code: str,
    detail: str,
    type_name: str | None = None,
    version: str | None = None,
    actor: str = _REGISTRATION_ACTOR_DEFAULT,
) -> None:
    """Emit ``connector.registration.rejected`` (platform-scope).

    Fired by :class:`Loader` whenever the registration pipeline
    raises. ``type_name`` and ``version`` are best-effort: they may be
    ``None`` when the rejection happens before manifest parse
    (e.g. ``image_ref`` unparseable, fetch failed). The subject
    always carries at least the image reference so operators can
    correlate the row with the failing source artifact.
    """
    subject: dict[str, Any] = {"image_ref": image_ref}
    if type_name is not None:
        subject["type"] = type_name
    if version is not None:
        subject["version"] = version
    await _emit(
        metadata_store,
        workspace_id=PLATFORM_WORKSPACE_ID,
        event_type=EVENT_REGISTRATION_REJECTED,
        actor=actor,
        subject=subject,
        payload={"code": code, "detail": detail},
    )


async def audit_deprecation_toggled(
    metadata_store: MetadataStoreProvider,
    *,
    type_name: str,
    version: str,
    deprecated: bool,
    actor: str = _REGISTRATION_ACTOR_DEFAULT,
) -> None:
    """Emit ``connector.deprecation.toggled`` (platform-scope).

    Fired by :class:`Loader.set_deprecated` after a successful flip of
    the catalog row's ``deprecated`` flag.
    """
    await _emit(
        metadata_store,
        workspace_id=PLATFORM_WORKSPACE_ID,
        event_type=EVENT_DEPRECATION_TOGGLED,
        actor=actor,
        subject={"type": type_name, "version": version},
        payload={"deprecated": deprecated},
    )


async def audit_manifest_fallback_used(
    metadata_store: MetadataStoreProvider,
    *,
    repository: str,
    subject_digest: str,
    fallback_tag: str,
    actor: str = _DISCOVERY_ACTOR_DEFAULT,
) -> None:
    """Emit ``connector.manifest.fallback-used`` (platform-scope)."""
    await _emit(
        metadata_store,
        workspace_id=PLATFORM_WORKSPACE_ID,
        event_type=EVENT_MANIFEST_FALLBACK_USED,
        actor=actor,
        subject={"repository": repository, "subject_digest": subject_digest},
        payload={
            "fallback_tag": fallback_tag,
            "resolved_via": "fallback-tag",
        },
    )


async def audit_manifest_fallback_ignored(
    metadata_store: MetadataStoreProvider,
    *,
    repository: str,
    subject_digest: str,
    fallback_tag: str,
    actor: str = _DISCOVERY_ACTOR_DEFAULT,
) -> None:
    """Emit ``connector.manifest.fallback-ignored`` (platform-scope)."""
    await _emit(
        metadata_store,
        workspace_id=PLATFORM_WORKSPACE_ID,
        event_type=EVENT_MANIFEST_FALLBACK_IGNORED,
        actor=actor,
        subject={"repository": repository, "subject_digest": subject_digest},
        payload={
            "fallback_tag": fallback_tag,
            "resolved_via": "referrers",
        },
    )


async def audit_manifest_fallback_rejected(
    metadata_store: MetadataStoreProvider,
    *,
    repository: str,
    subject_digest: str,
    code: str,
    detail: str,
    actor: str = _DISCOVERY_ACTOR_DEFAULT,
) -> None:
    """Emit ``connector.manifest.fallback-rejected`` (platform-scope)."""
    await _emit(
        metadata_store,
        workspace_id=PLATFORM_WORKSPACE_ID,
        event_type=EVENT_MANIFEST_FALLBACK_REJECTED,
        actor=actor,
        subject={"repository": repository, "subject_digest": subject_digest},
        payload={"code": code, "detail": detail},
    )


async def audit_authz_decision(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    actor: str,
    principal_id: str,
    path: str,
    method: str,
    permission: str,
    allowed: bool,
) -> None:
    """Emit ``authz.decision`` from the middleware permission gate.

    Workspace-scoped: the audit row lands in the workspace the call
    context carries. Subject pins the requesting principal; payload
    carries the request shape (path/method/permission) and the
    boolean decision.
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_AUTHZ_DECISION,
        actor=actor,
        subject={"principal_id": principal_id},
        payload={
            "path": path,
            "method": method,
            "permission": permission,
            "allowed": allowed,
        },
    )


# ---------------------------------------------------------------------------
# Legacy log-only shim (call-context dev-shim hook only post-Phase K)
# ---------------------------------------------------------------------------


def emit_event(name: str, payload: Mapping[str, Any]) -> None:
    """Emit a structured audit-style log line.

    Retained for the call-context dev-shim hook
    (``auth.callctx.shim_used``) which fires from middleware that runs
    *before* the FastAPI DI machinery has yielded a configured
    :class:`~custos_spl.MetadataStoreProvider`. Every other previously
    log-only event was promoted to the typed audit pipeline under
    CONN-IMPL-029 (Phase K).

    Args:
        name: Canonical event name.
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
    "EVENT_AUTHZ_DECISION",
    "EVENT_BINDING_CREATED",
    "EVENT_BINDING_REJECTED",
    "EVENT_CAPABILITY_DEPRECATED",
    "EVENT_CURSOR_ADVANCED",
    "EVENT_CURSOR_ENCODING_MISMATCH",
    "EVENT_CURSOR_EXPIRED",
    "EVENT_CURSOR_REWOUND",
    "EVENT_DEPRECATION_TOGGLED",
    "EVENT_HEALTH_CHECK_COMPLETED",
    "EVENT_HEALTH_CHECK_INVOKED",
    "EVENT_IDENTITY_FAILED",
    "EVENT_IDENTITY_RESOLVED",
    "EVENT_INSTANCE_CREATED",
    "EVENT_INSTANCE_DISABLED",
    "EVENT_INSTANCE_ENABLED",
    "EVENT_INSTANCE_UPDATED",
    "EVENT_LEASE_DENIED",
    "EVENT_LEASE_EXPIRED",
    "EVENT_LEASE_ISSUED",
    "EVENT_LEASE_REFRESHED",
    "EVENT_LEASE_RELEASED",
    "EVENT_LEASE_REVOKED",
    "EVENT_LEASE_REVOKE_REQUESTED",
    "EVENT_MANIFEST_FALLBACK_IGNORED",
    "EVENT_MANIFEST_FALLBACK_REJECTED",
    "EVENT_MANIFEST_FALLBACK_USED",
    "EVENT_PULL_LOOP_PAUSED",
    "EVENT_PULL_LOOP_RESUMED",
    "EVENT_REGISTRATION_ACCEPTED",
    "EVENT_REGISTRATION_REJECTED",
    "PLATFORM_WORKSPACE_ID",
    "audit_authz_decision",
    "audit_binding_created",
    "audit_binding_rejected",
    "audit_capability_deprecated",
    "audit_cursor_advanced",
    "audit_cursor_encoding_mismatch",
    "audit_cursor_expired",
    "audit_cursor_rewound",
    "audit_deprecation_toggled",
    "audit_health_check_completed",
    "audit_health_check_invoked",
    "audit_identity_failed",
    "audit_identity_resolved",
    "audit_instance_created",
    "audit_instance_disabled",
    "audit_instance_enabled",
    "audit_instance_updated",
    "audit_lease_denied",
    "audit_lease_expired",
    "audit_lease_issued",
    "audit_lease_refreshed",
    "audit_lease_released",
    "audit_lease_revoke_requested",
    "audit_lease_revoked",
    "audit_manifest_fallback_ignored",
    "audit_manifest_fallback_rejected",
    "audit_manifest_fallback_used",
    "audit_pull_loop_paused",
    "audit_pull_loop_resumed",
    "audit_registration_accepted",
    "audit_registration_rejected",
    "emit_event",
    "logger",
]
