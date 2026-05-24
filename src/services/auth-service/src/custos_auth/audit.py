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

Canonical events emitted from Phase D / E
-----------------------------------------

* :func:`audit_role_binding_granted`   → ``role-binding.granted``
* :func:`audit_role_binding_revoked`   → ``role-binding.revoked``
* :func:`audit_authz_decision`         → ``authz.decision``

Additional canonical events (``token.*``) are emitted by subsequent
AS-IMPL-* phases and will reuse the same :func:`_emit` core.

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
EVENT_ROLE_BINDING_GRANTED: Final[str] = "role-binding.granted"
EVENT_ROLE_BINDING_REVOKED: Final[str] = "role-binding.revoked"
EVENT_AUTHZ_DECISION: Final[str] = "authz.decision"
EVENT_TOKEN_ISSUED: Final[str] = "token.issued"
EVENT_TOKEN_USED: Final[str] = "token.used"
EVENT_TOKEN_REVOKED: Final[str] = "token.revoked"
EVENT_TOKEN_EXPIRED: Final[str] = "token.expired"
EVENT_AUTHN_SUCCESS: Final[str] = "authn.success"
EVENT_AUTHN_FAILURE: Final[str] = "authn.failure"


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
) -> str:
    """Append an auth-service audit event to the SPL outbox.

    Returns the generated ``event_id`` so the caller can correlate the
    audit row with its own observability or response envelope (the
    authorization engine in particular surfaces the id on its
    :class:`Decision` return value).

    Best-effort: any failure here is logged at WARNING and bumps
    :data:`EMIT_FAILURES_TOTAL` but is otherwise swallowed so the state
    mutation that triggered the emission stays committed. The
    generated ``event_id`` is still returned (and logged on the
    failure path) so observability can search both sides of the gap
    when a drop occurs.
    """
    ws_id = WorkspaceId(workspace_id)
    event_id = str(uuid4())
    event = AuditEvent(
        workspace_id=ws_id,
        event_id=event_id,
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
            "audit emission failed event_type=%s event_id=%s workspace=%s actor=%s subject=%s",
            event_type,
            event_id,
            workspace_id,
            actor,
            json.dumps(dict(subject), default=str, sort_keys=True),
            exc_info=True,
        )
        return event_id
    _AUDIT_LOGGER.info(
        "audit_event event_type=%s event_id=%s workspace=%s actor=%s subject=%s",
        event_type,
        event_id,
        workspace_id,
        actor,
        json.dumps(dict(subject), default=str, sort_keys=True),
    )
    return event_id


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


# ---------------------------------------------------------------------------
# Role-binding events (Phase D / AS-IMPL-010)
# ---------------------------------------------------------------------------
#
# The SPL ``with_transaction`` primitive is intra-provider — a handle
# issued by :class:`AuthStoreProvider` cannot be replayed against
# :class:`MetadataStoreProvider.append_audit`. Role-binding handlers
# therefore commit the binding write first and follow up with a
# best-effort audit emission; any drop bumps
# :data:`EMIT_FAILURES_TOTAL` and pages Observability. See
# :mod:`custos_auth.api.routes.role_bindings` for the full rationale.


def _audit_workspace_for_scope(scope_kind: str, scope_id: str | None) -> str:
    """Pick the audit-bucket workspace id for a role-binding event.

    Workspace-scope bindings record under the affected workspace; all
    other scopes record under the platform sentinel so the
    Observability pipeline can still index them by bucket.
    """
    if scope_kind == "workspace" and scope_id is not None:
        return scope_id
    return PLATFORM_WORKSPACE_ID


async def audit_role_binding_granted(
    metadata_store: MetadataStoreProvider,
    *,
    actor: str,
    binding_id: str,
    principal_id: str,
    role_id: str,
    scope_kind: str,
    scope_id: str | None,
) -> None:
    """Emit ``role-binding.granted`` (best-effort, post-commit)."""
    await _emit(
        metadata_store,
        workspace_id=_audit_workspace_for_scope(scope_kind, scope_id),
        event_type=EVENT_ROLE_BINDING_GRANTED,
        actor=actor,
        subject={"binding_id": binding_id, "principal_id": principal_id},
        payload={
            "role_id": role_id,
            "scope_kind": scope_kind,
            "scope_id": scope_id,
        },
    )


async def audit_role_binding_revoked(
    metadata_store: MetadataStoreProvider,
    *,
    actor: str,
    binding_id: str,
    principal_id: str,
    role_id: str,
    scope_kind: str,
    scope_id: str | None,
    reason: str,
) -> None:
    """Emit ``role-binding.revoked`` (best-effort, post-commit)."""
    await _emit(
        metadata_store,
        workspace_id=_audit_workspace_for_scope(scope_kind, scope_id),
        event_type=EVENT_ROLE_BINDING_REVOKED,
        actor=actor,
        subject={"binding_id": binding_id, "principal_id": principal_id},
        payload={
            "role_id": role_id,
            "scope_kind": scope_kind,
            "scope_id": scope_id,
            "reason": reason,
        },
    )


# ---------------------------------------------------------------------------
# Service-token lifecycle events (Phase F / AS-IMPL-013+)
# ---------------------------------------------------------------------------


async def audit_token_issued(
    metadata_store: MetadataStoreProvider,
    *,
    actor: str,
    workspace_id: str,
    token_id: str,
    service_account_id: str,
    issued_at: datetime,
    expires_at: datetime,
) -> None:
    """Emit ``token.issued`` keyed to the service-account's workspace.

    ``actor`` is the call-context principal that performed the mint
    (the operator holding ``admin:service-account``);
    ``service_account_id`` is the SA the token was issued to. The
    payload deliberately omits the token plaintext **and** the
    storage hash — the plaintext leaks the credential, and the hash
    is a deterministic function of the plaintext so it would let
    anyone with audit-read access correlate the row back to the same
    hash an attacker might intercept on the wire. ``token_id`` is
    sufficient for forensic correlation against the
    :class:`~custos_spl.interfaces.auth_store.ServiceToken` row.
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_TOKEN_ISSUED,
        actor=actor,
        subject={
            "token_id": token_id,
            "service_account_id": service_account_id,
        },
        payload={
            "issued_at": issued_at.isoformat(),
            "expires_at": expires_at.isoformat(),
        },
    )


# ---------------------------------------------------------------------------
# Service-token lifecycle events (Phase F / AS-IMPL-014)
# ---------------------------------------------------------------------------


async def audit_token_used(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    token_id: str,
    service_account_id: str,
) -> None:
    """Emit ``token.used`` keyed to the SA's workspace.

    AS-IMPL-014 spec: "``token.used`` audit event on first use after
    rotation (not on every request)." First-use detection is the
    verify-path responsibility: a cache miss on the authn cache
    triggers the emission, a cache hit does not. The 30 s cache TTL
    naturally rate-limits the row to at most ~one per 30 s per
    token, which is the design's intent — operators see the token
    leaving the gate after a rotation without drowning the audit
    pipeline with a row per HTTP request.

    ``actor`` is fixed to the bearer (the SA itself) because the
    verify path runs before the call-context middleware has
    established a calling identity.
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_TOKEN_USED,
        actor=service_account_id,
        subject={
            "token_id": token_id,
            "service_account_id": service_account_id,
        },
        payload={},
    )


async def audit_authn_success(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    token_id: str,
    service_account_id: str,
    cache_hit: bool,
) -> None:
    """Emit ``authn.success`` keyed to the SA's workspace.

    Per AS-IMPL-014: emitted at the gateway entry path on every
    successful verify call. The ``cache_hit`` payload field lets
    operators differentiate cache-served verifies from store
    lookups; cache pressure correlates with low verify latency and
    is useful diagnostic context.
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_AUTHN_SUCCESS,
        actor=service_account_id,
        subject={
            "token_id": token_id,
            "service_account_id": service_account_id,
        },
        payload={"cache_hit": cache_hit},
    )


async def audit_authn_failure(
    metadata_store: MetadataStoreProvider,
    *,
    reason: str,
    workspace_id: str = PLATFORM_WORKSPACE_ID,
    token_id: str | None = None,
    service_account_id: str | None = None,
) -> None:
    """Emit ``authn.failure`` for a verify call that did not return a Principal.

    Failure rows are written under the SA's workspace when a row
    was located (so workspace operators can see authentication
    failures against their SAs) and under the platform sentinel
    when no row was located (so an unknown-token probe surfaces in
    the platform audit bucket, not arbitrarily in some workspace).
    The ``reason`` carries one of:

    * ``"unknown-token"`` — no SPL row matched the input hash.
    * ``"malformed-token"`` — the input did not look like a custos
      bearer (failed :func:`looks_like_custos_token`).
    * ``"revoked"`` — the row was found but its ``revoked_at`` was
      non-null.
    * ``"expired"`` — the row was found but its ``expires_at`` was
      in the past.
    * ``"sa-disabled"`` — the row was found but the owning SA's
      ``disabled_at`` was non-null.
    * ``"sa-missing"`` — the row was found but the owning SA had
      been hard-deleted (defensive; the design forbids hard-delete
      of SAs but the failure mode is worth distinguishing).

    The payload never carries the plaintext or the hash; the
    ``token_id`` is the operator-facing identifier and is safe to
    log.
    """
    subject: dict[str, Any] = {}
    if token_id is not None:
        subject["token_id"] = token_id
    if service_account_id is not None:
        subject["service_account_id"] = service_account_id
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_AUTHN_FAILURE,
        actor=service_account_id or "anonymous",
        subject=subject,
        payload={"reason": reason},
    )


# ---------------------------------------------------------------------------
# Service-token lifecycle events (Phase F / AS-IMPL-015)
# ---------------------------------------------------------------------------


async def audit_token_revoked(
    metadata_store: MetadataStoreProvider,
    *,
    actor: str,
    workspace_id: str,
    token_id: str,
    service_account_id: str,
    reason: str,
) -> None:
    """Emit ``token.revoked`` keyed to the SA's workspace.

    The single-revoke endpoint emits one row per revoked token; the
    bulk-revoke endpoint emits one row per revoked-now token (rows
    that were already revoked are no-ops and emit nothing because
    the AS-IMPL-015 idempotency contract is "second revoke is a
    silent 204"). The reason carried on the row is the human-
    readable reason the operator supplied on the request body, not
    the SPL ``revoked_reason`` column which carries the same string
    — that's just an internal-vs-external naming thing.

    The payload deliberately does **not** carry the hash; ``token_id``
    is the operator-facing identifier and is what the audit pipeline
    keys on.
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_TOKEN_REVOKED,
        actor=actor,
        subject={
            "token_id": token_id,
            "service_account_id": service_account_id,
        },
        payload={"reason": reason},
    )


# ---------------------------------------------------------------------------
# Service-token expiry events (Phase F / AS-IMPL-016)
# ---------------------------------------------------------------------------


async def audit_token_expired(
    metadata_store: MetadataStoreProvider,
    *,
    workspace_id: str,
    token_id: str,
    service_account_id: str,
    expires_at: datetime,
) -> None:
    """Emit ``token.expired`` keyed to the SA's workspace.

    The sweeper emits one row per token it is about to physically
    delete. The actor is the SA itself — the sweep is an internal
    platform process, not an operator action, so attributing the
    row to the SA keeps the workspace-scoped audit feed coherent
    (every row in the feed has an actor that lives in the same
    workspace). The payload carries the original ``expires_at`` so
    operators can reconstruct the rotation cadence from the audit
    feed without joining against the (now-deleted) SPL row.
    """
    await _emit(
        metadata_store,
        workspace_id=workspace_id,
        event_type=EVENT_TOKEN_EXPIRED,
        actor=service_account_id,
        subject={
            "token_id": token_id,
            "service_account_id": service_account_id,
        },
        payload={"expires_at": expires_at.isoformat()},
    )


# ---------------------------------------------------------------------------
# Authorization decision events (Phase E / AS-IMPL-011)
# ---------------------------------------------------------------------------
#
# Every ``authorize`` call — allow *and* deny — emits exactly one
# ``authz.decision`` row so the audit pipeline records the full
# decision history. The id is returned to the caller so the HTTP/RPC
# response envelope can surface ``auditEventId`` per design §
# "Authorization decision".


async def audit_authz_decision(
    metadata_store: MetadataStoreProvider,
    *,
    actor: str,
    workspace_id: str | None,
    principal_id: str,
    permission: str,
    decision: str,
    reason: str,
    caller_component: str,
) -> str:
    """Emit ``authz.decision`` and return the generated ``event_id``.

    ``workspace_id`` is the authorize-call target workspace. When the
    workspace is unknown (the ``workspace-not-found`` deny path) or
    the caller targeted platform scope, the row is filed under the
    platform sentinel bucket so observability still gets the trail.
    ``actor`` is the call-context actor — typically the same string as
    ``principal_id`` for user-initiated requests, but distinct when an
    internal component re-checks on behalf of a user.
    """
    bucket = workspace_id if workspace_id else PLATFORM_WORKSPACE_ID
    return await _emit(
        metadata_store,
        workspace_id=bucket,
        event_type=EVENT_AUTHZ_DECISION,
        actor=actor,
        subject={
            "principal_id": principal_id,
            "permission": permission,
            "workspace_id": workspace_id,
        },
        payload={
            "decision": decision,
            "reason": reason,
            "caller_component": caller_component,
        },
    )


__all__ = [
    "EMIT_FAILURES_TOTAL",
    "EVENT_AUTHN_FAILURE",
    "EVENT_AUTHN_SUCCESS",
    "EVENT_AUTHZ_DECISION",
    "EVENT_OIDC_IDENTITY_LINKED",
    "EVENT_PRINCIPAL_CREATED",
    "EVENT_PRINCIPAL_DISABLED",
    "EVENT_ROLE_BINDING_GRANTED",
    "EVENT_ROLE_BINDING_REVOKED",
    "EVENT_TENANT_CREATED",
    "EVENT_TOKEN_EXPIRED",
    "EVENT_TOKEN_ISSUED",
    "EVENT_TOKEN_REVOKED",
    "EVENT_TOKEN_USED",
    "EVENT_WORKSPACE_CREATED",
    "PLATFORM_WORKSPACE_ID",
    "audit_authn_failure",
    "audit_authn_success",
    "audit_authz_decision",
    "audit_oidc_identity_linked",
    "audit_principal_created",
    "audit_principal_disabled",
    "audit_role_binding_granted",
    "audit_role_binding_revoked",
    "audit_tenant_created",
    "audit_token_expired",
    "audit_token_issued",
    "audit_token_revoked",
    "audit_token_used",
    "audit_workspace_created",
]
