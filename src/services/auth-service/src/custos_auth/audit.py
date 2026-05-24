"""Audit emission for auth-service (AS-IMPL-005/006/007 onwards).

Cloned from :mod:`custos_catalog.audit` (CS-IMPL-019). The shape is
intentionally identical so the SPL audit outbox sees one consistent
schema across services and the Observability audit pipeline can dedupe
on ``event_id``.

Canonical events emitted from Phase C
-------------------------------------

* :func:`audit_tenant_created`         → ``tenant.created``
* :func:`audit_workspace_created`      → ``workspace.created``
* :func:`audit_principal_created`      → ``principal.created``
* :func:`audit_principal_disabled`     → ``principal.disabled``
* :func:`audit_oidc_identity_linked`   → ``oidc.identity-linked``

Additional canonical events (``authz.decision``, ``token.*``,
``role-binding.*``) are emitted by subsequent AS-IMPL-* phases and will
reuse the same :func:`_emit` core.

Platform-scope events
---------------------

``tenant.created`` is logically a platform-global event (it precedes
the existence of any workspace), but the SPL ``append_audit`` API is
workspace-keyed. To keep the contract one-shaped, platform-level events
are written under a sentinel workspace id
:data:`PLATFORM_WORKSPACE_ID` (``"__platform__"``); the Observability
service treats that id as a logical "control plane" bucket. Workspace-
scoped events (``workspace.created`` keyed under the new workspace,
``principal.created`` keyed under the workspace the principal was
created in, etc.) use their natural workspace.

Atomicity
---------

Same best-effort post-commit emission as catalog: a failed audit write
is logged at WARNING and bumps :data:`EMIT_FAILURES_TOTAL` but does
**not** roll back the underlying state mutation. The Observability
service alerts on any non-zero rate of that counter.
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

_LOGGER = logging.getLogger("custos_auth.audit")
_AUDIT_LOGGER = logging.getLogger("custos_auth.audit.event")

_INSTRUMENTATION_NAME: Final[str] = "custos_auth"
_INSTRUMENTATION_VERSION: Final[str] = "0.1.0"

_meter = metrics.get_meter(_INSTRUMENTATION_NAME, _INSTRUMENTATION_VERSION)

#: Counter incremented when an audit emission fails to reach the SPL
#: outbox. Any non-zero rate is page-worthy because every drop is an
#: audit-trail hole.
EMIT_FAILURES_TOTAL = _meter.create_counter(
    name="custos_audit_emit_failures_total",
    description=(
        "Count of auth-service audit emissions that failed to reach the "
        "SPL audit outbox. Labelled by event_type."
    ),
)

#: Sentinel workspace id used for platform-scope audit rows
#: (``tenant.created`` and any future platform-global events).
PLATFORM_WORKSPACE_ID: Final[str] = "__platform__"


# ---------------------------------------------------------------------------
# Canonical event names
# ---------------------------------------------------------------------------


EVENT_TENANT_CREATED: Final[str] = "tenant.created"
EVENT_WORKSPACE_CREATED: Final[str] = "workspace.created"
EVENT_PRINCIPAL_CREATED: Final[str] = "principal.created"
EVENT_PRINCIPAL_DISABLED: Final[str] = "principal.disabled"
EVENT_OIDC_IDENTITY_LINKED: Final[str] = "oidc.identity-linked"


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
    """Append an auth-service audit event to the SPL outbox.

    Best-effort: any failure here is logged at WARNING and bumps
    :data:`EMIT_FAILURES_TOTAL` but is otherwise swallowed so the state
    mutation that triggered the emission stays committed.
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
        # Cancellation and process-control signals must propagate so the
        # caller's task or process can unwind cleanly.
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


async def audit_tenant_created(
    metadata_store: MetadataStoreProvider,
    *,
    actor: str,
    tenant_id: str,
    name: str,
) -> None:
    """Emit ``tenant.created`` against the platform sentinel workspace."""
    await _emit(
        metadata_store,
        workspace_id=PLATFORM_WORKSPACE_ID,
        event_type=EVENT_TENANT_CREATED,
        actor=actor,
        subject={"tenant_id": tenant_id},
        payload={"name": name},
    )


async def audit_workspace_created(
    metadata_store: MetadataStoreProvider,
    *,
    actor: str,
    tenant_id: str,
    workspace_id: str,
    name: str,
) -> None:
    """Emit ``workspace.created`` keyed under the new workspace itself."""
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_WORKSPACE_CREATED,
        actor=actor,
        subject={"workspace_id": workspace_id},
        payload={"tenant_id": tenant_id, "name": name},
    )


async def audit_principal_created(
    metadata_store: MetadataStoreProvider,
    *,
    actor: str,
    workspace_id: str,
    principal_id: str,
    kind: str,
    display_name: str | None = None,
) -> None:
    """Emit ``principal.created``.

    ``workspace_id`` is the workspace under which the principal was
    minted (typically the actor's current workspace for service
    accounts; for users it is the workspace the OIDC bind / invitation
    completed in).
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_PRINCIPAL_CREATED,
        actor=actor,
        subject={"principal_id": principal_id},
        payload={"kind": kind, "display_name": display_name},
    )


async def audit_principal_disabled(
    metadata_store: MetadataStoreProvider,
    *,
    actor: str,
    workspace_id: str,
    principal_id: str,
    reason: str | None = None,
) -> None:
    """Emit ``principal.disabled``."""
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_PRINCIPAL_DISABLED,
        actor=actor,
        subject={"principal_id": principal_id},
        payload={"reason": reason},
    )


async def audit_oidc_identity_linked(
    metadata_store: MetadataStoreProvider,
    *,
    actor: str,
    workspace_id: str,
    user_id: str,
    issuer: str,
    subject: str,
) -> None:
    """Emit ``oidc.identity-linked`` after a successful OIDC bind."""
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_OIDC_IDENTITY_LINKED,
        actor=actor,
        subject={"user_id": user_id, "issuer": issuer, "oidc_subject": subject},
        payload={},
    )


__all__ = [
    "EMIT_FAILURES_TOTAL",
    "EVENT_OIDC_IDENTITY_LINKED",
    "EVENT_PRINCIPAL_CREATED",
    "EVENT_PRINCIPAL_DISABLED",
    "EVENT_TENANT_CREATED",
    "EVENT_WORKSPACE_CREATED",
    "PLATFORM_WORKSPACE_ID",
    "audit_oidc_identity_linked",
    "audit_principal_created",
    "audit_principal_disabled",
    "audit_tenant_created",
    "audit_workspace_created",
]
