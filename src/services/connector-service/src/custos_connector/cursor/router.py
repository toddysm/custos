"""Cursor admin REST surface (CONN-IMPL-024, Phase I, #307).

The operator admin surface for pull-cursor + pull-loop lifecycle
operations described in ``design/components/connector-service/design.md``
§ Pull Cursor Model → Admin rewind / replay and § Operator Admin Surface
→ Pull-loop lifecycle operations.

Four endpoints, all under ``/v1/workspaces/{ws}/connectors/{instanceId}``:

* ``GET    /cursor``               → :data:`CONNECTOR_READ`
* ``POST   /cursor:rewind``        → :data:`ADMIN_CONNECTOR`
* ``POST   /pull-loop:pause``      → :data:`ADMIN_CONNECTOR`
* ``POST   /pull-loop:resume``     → :data:`ADMIN_CONNECTOR`

The ``{ws}`` path segment MUST match the call-context's
``workspace_id`` — otherwise the handler returns 403
``connector.workspace_mismatch`` (mirroring the catalog-service
contract). This is enforced in addition to the call-context
permission check so an operator with a token issued for a
different workspace cannot reach the admin surface for the wrong
tenant even if they hold the permission globally.

Cursor values are opaque to the platform and MUST NOT be exposed
on this surface or in audit events. The GET handler redacts the
envelope to ``{encoding, valueFingerprint, valueLength,
advancedAt}``; the rewind handler accepts a ``value`` in the
request body (operators MUST know what they're rewinding to) but
audit events only carry the fingerprint via
:func:`audit_cursor_rewound`.

The :data:`CursorService` and :data:`PullLoopScheduler` are pulled
off ``app.state.providers`` at request time. Both are typed as
``Optional`` on :class:`~custos_connector.providers.Providers`
because lifespan-side wiring lands as a follow-up
(CONN-IMPL-024 wires the surface; CONN-IMPL-025 wires the Listen
Manager that drives registrations). A missing wiring surfaces as a
startup :class:`RuntimeError` from the resolver, not a 500 with no
context, matching the lease router's pattern.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Annotated, Any, Final, Literal, cast

from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.connector_instance_store import ConnectorInstanceStoreProvider
from custos_spl.interfaces.metadata_store import MetadataStoreProvider
from fastapi import APIRouter, Depends, Path, Request
from pydantic import BaseModel, ConfigDict, Field
from starlette.responses import JSONResponse, Response

from custos_connector.audit import (
    audit_cursor_rewound,
    audit_pull_loop_paused,
    audit_pull_loop_resumed,
)
from custos_connector.cursor.service import (
    UNINITIALIZED_PERSISTED_CURSOR,
    CursorEnvelopeRecord,
    CursorInstanceUnavailable,
    CursorService,
    encode_envelope_value,
    fingerprint_persisted_value,
    persisted_value_byte_length,
)
from custos_connector.middleware import CallContext, require_permission
from custos_connector.permissions import ADMIN_CONNECTOR, CONNECTOR_READ
from custos_connector.scheduler import PullLoopScheduler

if TYPE_CHECKING:
    from custos_connector.providers import Providers

#: Common path prefix. Mounted in :func:`custos_connector.create_app`.
router = APIRouter(prefix="/v1", tags=["cursor-admin"])


#: The HTTP error code returned by every admin handler when the
#: ``{ws}`` path segment does not match the call-context's workspace.
#: Matches the catalog-service convention (``catalog.workspace_mismatch``)
#: so cross-service operator tooling has one error vocabulary.
_WORKSPACE_MISMATCH_CODE: Final[str] = "connector.workspace_mismatch"


# ---------------------------------------------------------------------------
# Wire models
# ---------------------------------------------------------------------------


class _RewindExplicit(BaseModel):
    """Wire shape of an explicit-envelope rewind request.

    ``encoding`` MUST match the connector-type's current
    ``cursorEncoding`` — otherwise the handler rejects the request
    with 400 ``connector.cursor.encoding_mismatch``. The value is
    JSON-encoded with :func:`encode_envelope_value` and persisted
    as-is; the plugin sees it on the next tick.
    """

    model_config = ConfigDict(extra="forbid")

    encoding: str = Field(..., min_length=1)
    value: Any


class _RewindWire(BaseModel):
    """Wire shape of the ``cursor:rewind`` request body.

    ``to`` is one of:

    * ``"beginning"`` (string shorthand)
    * ``"now"`` (string shorthand)
    * ``{"encoding": "...", "value": <any JSON>}`` (explicit envelope)

    ``reason`` is mandatory and free-form; it lands verbatim in the
    ``cursor.rewound`` audit event so the audit trail records *why*
    the operator rewound (e.g. ``"replay after upstream backfill"``,
    ``"unblock cursor_migration_required after type bump"``).
    """

    model_config = ConfigDict(extra="forbid")

    to: Literal["beginning", "now"] | _RewindExplicit = Field(...)
    reason: str = Field(..., min_length=1)


class _PullLoopPauseWire(BaseModel):
    """Wire shape of the ``pull-loop:pause`` request body.

    ``reason`` is optional but recorded verbatim in the
    ``connector.pull-loop.paused`` audit event when present.
    """

    model_config = ConfigDict(extra="forbid")

    reason: str | None = Field(default=None, min_length=1)


# ---------------------------------------------------------------------------
# Dependency wiring
# ---------------------------------------------------------------------------


def _error_response(*, status_code: int, code: str, detail: str) -> JSONResponse:
    """Render the canonical connector-service error envelope.

    All endpoints under this router share the ``{"error": {"code",
    "detail"}}`` shape used by the binding and lease routers (the
    same envelope the sidecar's ``LeaseGateway`` unwraps), so
    clients never need to special-case cursor admin errors.
    """
    return JSONResponse(
        status_code=status_code,
        content={"error": {"code": code, "detail": detail}},
    )


def _workspace_mismatch_response(ctx: CallContext, ws: str) -> JSONResponse | None:
    """Return a 403 envelope when the URL workspace does not match the call-context.

    Returns ``None`` on a successful check; the caller treats a
    non-``None`` return as the response to short-circuit on. The
    call-context's ``workspace_id`` is the source of truth — the
    URL path segment is an additional guard for log / tooling
    clarity.
    """
    if ctx.workspace_id != ws:
        return _error_response(
            status_code=403,
            code=_WORKSPACE_MISMATCH_CODE,
            detail=(
                f"call context workspace {ctx.workspace_id!r} does not match URL workspace {ws!r}"
            ),
        )
    return None


async def _instance_not_found_response(
    instance_store: ConnectorInstanceStoreProvider,
    ws: str,
    instance_id: str,
) -> JSONResponse | None:
    """Return a 404 envelope when the connector instance row is missing.

    Distinguishes "instance does not exist" (returns
    ``connector.instance_not_found``) from "instance exists but has
    never ticked" (handled by the per-endpoint cursor-row check,
    which returns ``connector.cursor.not_found``). Without this
    disambiguation both cases would surface as the same
    ``connector.cursor.not_found`` 404 with a "connector has never
    ticked" detail, which is misleading for a genuinely unknown
    instance.
    """
    instance = await instance_store.get_connector_instance(
        WorkspaceId(ws), ConnectorInstanceId(instance_id)
    )
    if instance is None:
        return _error_response(
            status_code=404,
            code="connector.instance_not_found",
            detail=f"connector instance {instance_id!r} not found in workspace {ws!r}",
        )
    return None


def _resolve_instance_store(request: Request) -> ConnectorInstanceStoreProvider:
    """Pull :class:`ConnectorInstanceStoreProvider` off ``app.state.providers``."""
    providers = cast("Providers", request.app.state.providers)
    return providers.instance_store


def _resolve_metadata_store(request: Request) -> MetadataStoreProvider:
    """Pull :class:`MetadataStoreProvider` off ``app.state.providers``.

    The admin router needs the metadata store directly for two
    reasons:

    * ``cursor:rewind`` calls
      :meth:`MetadataStoreProvider.rewind_cursor` (the SPL primitive
      that owns the cursor row).
    * All four endpoints emit audit events through the same
      ``append_audit`` provider.

    The store is always wired (it is part of the SPL-required core
    providers), so the resolver does not check for ``None`` — a
    missing field is a startup wiring bug and ``Providers`` will
    have raised long before the request reached this handler.
    """
    providers = cast("Providers", request.app.state.providers)
    return providers.metadata_store


def _resolve_cursor_service(request: Request) -> CursorService:
    """Pull :class:`CursorService` off ``app.state.providers``.

    Surfaces a startup-wiring bug as a :class:`RuntimeError` (rather
    than a generic 500) so the test suite + operator dashboards can
    distinguish "wired wrong on startup" from "request failed at
    runtime". Mirrors :func:`_resolve_lease_manager` in
    :mod:`custos_connector.lease.router`.
    """
    providers = request.app.state.providers
    svc = getattr(providers, "cursor_service", None)
    if svc is None:
        raise RuntimeError(
            "cursor_service is not wired on app.state.providers; this is a startup wiring bug"
        )
    assert isinstance(svc, CursorService)
    return svc


def _resolve_pull_loop_scheduler(request: Request) -> PullLoopScheduler:
    """Pull :class:`PullLoopScheduler` off ``app.state.providers``."""
    providers = request.app.state.providers
    sched = getattr(providers, "pull_loop_scheduler", None)
    if sched is None:
        raise RuntimeError(
            "pull_loop_scheduler is not wired on app.state.providers; this is a startup wiring bug"
        )
    assert isinstance(sched, PullLoopScheduler)
    return sched


# ---------------------------------------------------------------------------
# Serialization
# ---------------------------------------------------------------------------


def _envelope_to_redacted_wire(envelope: CursorEnvelopeRecord) -> dict[str, Any]:
    """Render a :class:`CursorEnvelopeRecord` as the redacted GET response.

    Per design § Pull Cursor Model, the admin surface MUST NOT
    expose the raw ``value``. The response carries the
    ``encoding`` + a SHA-256 fingerprint of the persisted form +
    the byte-length so operators can size dashboards / detect a
    cursor that has stopped advancing without ever seeing the
    opaque payload.
    """
    persisted = (
        encode_envelope_value(envelope.value)
        if envelope.value is not None
        else (UNINITIALIZED_PERSISTED_CURSOR)
    )
    return {
        "workspaceId": envelope.workspace_id,
        "instanceId": envelope.instance_id,
        "encoding": envelope.encoding,
        "valueFingerprint": fingerprint_persisted_value(persisted),
        "valueLength": persisted_value_byte_length(persisted),
        "advancedAt": envelope.advanced_at.isoformat(),
        "leaseHolder": envelope.lease_holder,
        "leaseExpiresAt": (
            envelope.lease_expires_at.isoformat() if envelope.lease_expires_at else None
        ),
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("/workspaces/{ws}/connectors/{instance_id}/cursor")
async def read_cursor(
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(CONNECTOR_READ))],
    ws: Annotated[str, Path(min_length=1)],
    instance_id: Annotated[str, Path(min_length=1)],
) -> Response:
    """Read the current cursor envelope (redacted).

    Returns 200 with the redacted envelope; 404
    ``connector.instance_not_found`` if the instance row does not
    exist; 404 ``connector.cursor.not_found`` if the instance
    exists but the connector has never ticked. The ``encoding``
    field is sourced from the connector type's current manifest,
    not from the persisted row — a connector-type that has bumped
    its ``cursorEncoding`` will report the new encoding even though
    the persisted value is still in the old format. The next tick
    will surface the mismatch.
    """
    mismatch = _workspace_mismatch_response(ctx, ws)
    if mismatch is not None:
        return mismatch
    instance_store = _resolve_instance_store(request)
    missing_instance = await _instance_not_found_response(instance_store, ws, instance_id)
    if missing_instance is not None:
        return missing_instance
    cursor_service = _resolve_cursor_service(request)
    try:
        envelope = await cursor_service.read_envelope(ws, instance_id)
    except CursorInstanceUnavailable as exc:
        # Instance row exists but the catalog row for its type/version
        # is missing or the manifest has no pull encoding — neither
        # surfaces as a 404 because the instance _does_ exist; this
        # is an internal misconfiguration the operator needs to fix
        # in the catalog. Return 409 with the diagnostic message.
        return _error_response(
            status_code=409,
            code="connector.cursor.unavailable",
            detail=str(exc),
        )
    if envelope is None:
        return _error_response(
            status_code=404,
            code="connector.cursor.not_found",
            detail=(
                f"no cursor row for workspace {ws!r} instance {instance_id!r} "
                "(connector has never ticked)"
            ),
        )
    return JSONResponse(status_code=200, content=_envelope_to_redacted_wire(envelope))


@router.post("/workspaces/{ws}/connectors/{instance_id}/cursor:rewind")
async def rewind_cursor(
    body: _RewindWire,
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(ADMIN_CONNECTOR))],
    ws: Annotated[str, Path(min_length=1)],
    instance_id: Annotated[str, Path(min_length=1)],
) -> Response:
    """Rewind the cursor to a new position.

    Three rewind targets:

    * ``"beginning"`` writes the empty-string sentinel so the next
      tick passes ``cursor=None`` to the plugin (replay from the
      producer's oldest available position).
    * ``"now"`` writes a JSON-encoded UTC RFC 3339 timestamp string
      captured at request handling time. Whether the plugin accepts
      this position depends on the connector-type — pull sources
      that expect, say, a Kafka offset will reject it on the next
      tick with :class:`CursorEncodingMismatch`.
    * ``{"encoding": "...", "value": <any JSON>}`` writes an
      explicit envelope. ``encoding`` MUST match the connector-type's
      current ``cursorEncoding``.

    On success returns 200 with the new redacted envelope and emits a
    ``cursor.rewound`` audit event carrying the operator identity,
    free-form ``reason``, and ``from``/``to`` audit envelopes
    (encoding + value fingerprint + value length — never the raw
    value). Audit emission is best-effort: the rewind has already
    committed to SPL by the time the audit emit runs, so an audit
    drop is logged + counted on ``custos_audit_emit_failures_total``
    but does not fail the request.
    """
    mismatch = _workspace_mismatch_response(ctx, ws)
    if mismatch is not None:
        return mismatch
    instance_store = _resolve_instance_store(request)
    missing_instance = await _instance_not_found_response(instance_store, ws, instance_id)
    if missing_instance is not None:
        return missing_instance
    cursor_service = _resolve_cursor_service(request)
    metadata_store = _resolve_metadata_store(request)

    # Read the current envelope so the audit event carries a
    # ``from`` fingerprint. ``None`` means the instance has no
    # cursor row yet — the rewind is rejected because there is
    # nothing to rewind, and the operator needs to wait for the
    # first tick (or rewind to "beginning" once the tick has run,
    # which seeds the row).
    try:
        before = await cursor_service.read_envelope(ws, instance_id)
    except CursorInstanceUnavailable as exc:
        return _error_response(
            status_code=409,
            code="connector.cursor.unavailable",
            detail=str(exc),
        )
    if before is None:
        return _error_response(
            status_code=404,
            code="connector.cursor.not_found",
            detail=(
                f"no cursor row for workspace {ws!r} instance {instance_id!r}; "
                "wait for the first tick before issuing an admin rewind"
            ),
        )

    current_encoding = before.encoding

    new_persisted, target_encoding = _resolve_rewind_target(body.to, current_encoding)
    if target_encoding != current_encoding:
        return _error_response(
            status_code=400,
            code="connector.cursor.encoding_mismatch",
            detail=(
                f"rewind envelope encoding {target_encoding!r} does not "
                f"match connector-type encoding {current_encoding!r}"
            ),
        )

    # SPL ``rewind_cursor`` writes the new value, bumps
    # ``advanced_at`` to ``now()``, and clears any cursor lease.
    # Audit emission for ``cursor.rewound`` is documented as the SPL
    # adapter's job in design but the PG adapter has a TODO(#129)
    # placeholder — connector-service emits the event here so the
    # operator surface ships with the canonical audit trail. When
    # #129 lands the duplicate emit will be deduped by the audit
    # outbox (event ids carry the correlation id + timestamp; the
    # SPL helper emits inside the same txn).
    await metadata_store.rewind_cursor(
        WorkspaceId(ws),
        ConnectorInstanceId(instance_id),
        new_persisted,
        actor=ctx.principal_id,
        reason=body.reason,
    )

    # Re-read the envelope so the response + audit carry the
    # post-rewind ``advanced_at`` from the SPL row.
    after = await cursor_service.read_envelope(ws, instance_id)
    assert after is not None, "rewind_cursor returned without writing a row"

    before_persisted = (
        encode_envelope_value(before.value)
        if before.value is not None
        else UNINITIALIZED_PERSISTED_CURSOR
    )
    await audit_cursor_rewound(
        metadata_store,
        workspace_id=ws,
        actor=ctx.principal_id,
        instance_id=instance_id,
        from_encoding=before.encoding,
        from_value_fingerprint=fingerprint_persisted_value(before_persisted),
        from_value_length=persisted_value_byte_length(before_persisted),
        to_encoding=after.encoding,
        to_value_fingerprint=fingerprint_persisted_value(new_persisted),
        to_value_length=persisted_value_byte_length(new_persisted),
        reason=body.reason,
    )
    return JSONResponse(status_code=200, content=_envelope_to_redacted_wire(after))


@router.post("/workspaces/{ws}/connectors/{instance_id}/pull-loop:pause")
async def pause_pull_loop(
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(ADMIN_CONNECTOR))],
    ws: Annotated[str, Path(min_length=1)],
    instance_id: Annotated[str, Path(min_length=1)],
    body: _PullLoopPauseWire | None = None,
) -> Response:
    """Pause the pull-loop for a connector instance.

    Idempotent: pausing an already-paused (or never-registered)
    instance still emits a fresh ``connector.pull-loop.paused``
    audit event so the operator action is recorded. Any in-flight
    tick completes; the scheduler refuses to dispatch a new tick
    until :func:`resume_pull_loop` is called.

    Cursor state and consecutive-failure count are preserved across
    the pause/resume cycle so the loop picks up where it left off.
    """
    mismatch = _workspace_mismatch_response(ctx, ws)
    if mismatch is not None:
        return mismatch
    scheduler = _resolve_pull_loop_scheduler(request)
    scheduler.pause(ws, instance_id)
    metadata_store = _resolve_metadata_store(request)
    reason = body.reason if body is not None else None
    await audit_pull_loop_paused(
        metadata_store,
        workspace_id=ws,
        actor=ctx.principal_id,
        instance_id=instance_id,
        reason=reason,
    )
    return JSONResponse(status_code=200, content={"status": "paused"})


@router.post("/workspaces/{ws}/connectors/{instance_id}/pull-loop:resume")
async def resume_pull_loop(
    request: Request,
    ctx: Annotated[CallContext, Depends(require_permission(ADMIN_CONNECTOR))],
    ws: Annotated[str, Path(min_length=1)],
    instance_id: Annotated[str, Path(min_length=1)],
) -> Response:
    """Resume the pull-loop for a connector instance.

    Idempotent: resuming an already-running (or never-registered)
    instance still emits a fresh ``connector.pull-loop.resumed``
    audit event. Clears the in-memory ``halted`` flag — operators
    are expected to also clear the durable ``status`` flip via
    :func:`rewind_cursor` when the halt was triggered by a cursor
    migration. If ``status`` is still a halt sentinel on the next
    tick, the cursor service will refuse the tick with
    :class:`CursorInstanceUnavailable` and the scheduler will drop
    the instance from the rotation again.
    """
    mismatch = _workspace_mismatch_response(ctx, ws)
    if mismatch is not None:
        return mismatch
    scheduler = _resolve_pull_loop_scheduler(request)
    scheduler.resume(ws, instance_id)
    metadata_store = _resolve_metadata_store(request)
    await audit_pull_loop_resumed(
        metadata_store,
        workspace_id=ws,
        actor=ctx.principal_id,
        instance_id=instance_id,
    )
    return JSONResponse(status_code=200, content={"status": "resumed"})


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _resolve_rewind_target(
    to: Literal["beginning", "now"] | _RewindExplicit,
    current_encoding: str,
) -> tuple[str, str]:
    """Translate a wire-shape ``to`` field into a ``(persisted, encoding)``.

    Returns the persisted string ready for
    :meth:`MetadataStoreProvider.rewind_cursor` and the encoding
    label so the caller can compare against the connector-type's
    current encoding. For shorthand forms the returned encoding is
    the current encoding (the shorthand always targets the same
    encoding the plugin is currently on).
    """
    if isinstance(to, str):
        if to == "beginning":
            return UNINITIALIZED_PERSISTED_CURSOR, current_encoding
        # ``"now"`` — capture a UTC RFC 3339 timestamp at request
        # handling time. The lazy ``datetime`` import keeps the
        # module-import cost low for the GET-only path.
        from datetime import UTC, datetime

        now_iso = datetime.now(UTC).isoformat()
        return encode_envelope_value(now_iso), current_encoding
    # Explicit envelope.
    persisted = encode_envelope_value(to.value)
    return persisted, to.encoding


__all__ = ["router"]
