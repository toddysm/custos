"""Sidecar UDS HTTP router (CONN-IMPL-019).

Three endpoints, per the design \u00a7 Secret and Token Flow to Activities:

* ``GET  /v1/token?slot=<name>&purpose=<verb>``
* ``POST /v1/token/refresh`` body ``{leaseId}``
* ``POST /v1/token/release`` body ``{leaseId}`` \u2014 best-effort, 204

Every request must carry ``Custos-Sidecar-Token: <bootstrap>``; the
verifier rejects bad tokens with a 401 ``bootstrap-invalid`` problem
document before the handler runs. Domain failures (slot not found,
capability mismatch, capacity exceeded, etc.) raise
:class:`SidecarError` and the per-handler ``try/except`` renders them
through :func:`problem_response`.

The token envelope returned on success matches the design's locked
shape::

    {
        "leaseId": ...,
        "tokenType": "Bearer" | ...,
        "token": <opaque>,
        "expiresAt": <RFC3339>,
        "scope": {
            "connectorSlot": <name>,
            "connectorInstanceId": <id>,
            "capability": <purpose>,
            "runId": ..., "stepId": ..., "attempt": ...
        },
        "endpoint": <upstream url>,
        "extras": { ... per connector type ... }
    }

The router itself never logs the token bytes (the auditor in the CS
audit pipeline is the canonical record); the body never reaches a
log line because FastAPI's structured logger does not auto-dump
response bodies.
"""

from __future__ import annotations

import contextlib
from typing import Annotated, Any

from fastapi import APIRouter, Depends, Header, Query, Request
from pydantic import BaseModel, ConfigDict, Field
from starlette.responses import JSONResponse, Response

from custos_sidecar.auth import BootstrapTokenVerifier
from custos_sidecar.context_registry import ContextRegistry, SlotContext
from custos_sidecar.credential_minter import (
    CredentialMinter,
    InstanceUnavailable,
    UpstreamMintFailure,
)
from custos_sidecar.errors import SidecarError, SidecarErrorCode, problem_response
from custos_sidecar.lease_gateway import (
    GatewayLeaseError,
    GatewayTransportError,
    LeaseGateway,
    LeaseRecord,
)

router = APIRouter(prefix="/v1", tags=["sidecar"])

#: Header name the activity carries on every request.
BOOTSTRAP_HEADER = "Custos-Sidecar-Token"

#: Map CS lease error codes (returned by the Lease Gateway) to the
#: sidecar's wire taxonomy. Keeps the router free of string-soup
#: ``if`` chains.
_LEASE_CODE_TO_SIDECAR: dict[str, SidecarErrorCode] = {
    "CAPACITY_EXCEEDED": SidecarErrorCode.CAPACITY_EXCEEDED,
    "NOT_FOUND": SidecarErrorCode.LEASE_NOT_FOUND,
    "ALREADY_RELEASED": SidecarErrorCode.LEASE_REVOKED,
    "INVALID_REQUEST": SidecarErrorCode.INVALID_REQUEST,
}


# ---------------------------------------------------------------------------
# Wire models
# ---------------------------------------------------------------------------


class _RefreshBody(BaseModel):
    """Wire shape of ``POST /v1/token/refresh``."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    lease_id: str = Field(..., min_length=1, alias="leaseId")


class _ReleaseBody(BaseModel):
    """Wire shape of ``POST /v1/token/release``."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    lease_id: str = Field(..., min_length=1, alias="leaseId")


# ---------------------------------------------------------------------------
# Per-request dependencies
# ---------------------------------------------------------------------------


def _verify_bootstrap(
    request: Request,
    custos_sidecar_token: Annotated[str | None, Header(alias=BOOTSTRAP_HEADER)] = None,
) -> None:
    """Bootstrap-token verification dependency.

    Raises :class:`SidecarError(BOOTSTRAP_INVALID)` on every failure
    mode (missing header, signature mismatch, bound-triple mismatch,
    expired). The router-wide exception handler renders it as a 401
    problem document.
    """
    verifier: BootstrapTokenVerifier = request.app.state.bootstrap_verifier
    verifier.verify(custos_sidecar_token)


def _resolve_registry(request: Request) -> ContextRegistry:
    """Pull the :class:`ContextRegistry` off ``app.state``."""
    registry: ContextRegistry = request.app.state.context_registry
    return registry


def _resolve_gateway(request: Request) -> LeaseGateway:
    """Pull the :class:`LeaseGateway` off ``app.state``.

    Typed as :class:`LeaseGateway` for static analysis; at runtime the
    test suite injects an in-memory fake that quacks like the
    gateway (same async ``issue`` / ``refresh`` / ``release`` shape),
    so no ``isinstance`` check is enforced here.
    """
    gateway: LeaseGateway = request.app.state.lease_gateway
    return gateway


def _resolve_minter(request: Request) -> CredentialMinter:
    """Pull the :class:`CredentialMinter` off ``app.state``."""
    minter: CredentialMinter = request.app.state.credential_minter
    return minter


def _bound_triple(request: Request) -> tuple[str, str, int]:
    """Return the sidecar's bound ``(runId, stepId, attempt)``."""
    triple: tuple[str, str, int] = request.app.state.bound_triple
    return triple


# ---------------------------------------------------------------------------
# Envelope construction
# ---------------------------------------------------------------------------


def _token_envelope(
    *,
    lease: LeaseRecord,
    ctx: SlotContext,
    token: str,
    triple: tuple[str, str, int],
) -> dict[str, Any]:
    """Build the response envelope per the design's locked schema."""
    return {
        "leaseId": lease.lease_id,
        "tokenType": ctx.token_type,
        "token": token,
        "expiresAt": lease.expires_at.isoformat(),
        "scope": {
            "connectorSlot": ctx.slot,
            "connectorInstanceId": ctx.connector_instance_id,
            "capability": lease.capability,
            "runId": triple[0],
            "stepId": triple[1],
            "attempt": triple[2],
        },
        "endpoint": ctx.endpoint,
        "extras": dict(ctx.extras),
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("/token")
async def get_token(
    request: Request,
    _: Annotated[None, Depends(_verify_bootstrap)],
    slot: Annotated[str, Query(min_length=1)],
    purpose: Annotated[str, Query(min_length=1)],
) -> Response:
    """Mint a lease + upstream credential for ``(slot, purpose)``."""
    try:
        registry = _resolve_registry(request)
        gateway = _resolve_gateway(request)
        minter = _resolve_minter(request)
        triple = _bound_triple(request)

        slot_ctx = registry.resolve(slot, purpose=purpose)
        lease = await _issue_lease(
            gateway=gateway,
            slot_ctx=slot_ctx,
            purpose=purpose,
            triple=triple,
        )
        try:
            credential = await minter.mint(
                lease_id=lease.lease_id,
                slot_ctx=slot_ctx,
                ttl_hint=lease.expires_at,
            )
        except InstanceUnavailable as exc:
            await _best_effort_release(gateway, lease.lease_id)
            raise SidecarError(
                SidecarErrorCode.CONNECTOR_UNAVAILABLE,
                str(exc),
            ) from exc
        except UpstreamMintFailure as exc:
            await _best_effort_release(gateway, lease.lease_id)
            raise SidecarError(
                SidecarErrorCode.UPSTREAM_FAILED,
                str(exc),
            ) from exc
        envelope = _token_envelope(
            lease=lease,
            ctx=slot_ctx,
            token=credential.token,
            triple=triple,
        )
        return JSONResponse(status_code=200, content=envelope)
    except SidecarError as exc:
        return problem_response(exc, instance=str(request.url.path))


@router.post("/token/refresh")
async def refresh_token(
    body: _RefreshBody,
    request: Request,
    _: Annotated[None, Depends(_verify_bootstrap)],
) -> Response:
    """Re-mint the credential for an existing lease (same id, new token)."""
    try:
        gateway = _resolve_gateway(request)
        minter = _resolve_minter(request)
        registry = _resolve_registry(request)
        triple = _bound_triple(request)

        try:
            lease = await gateway.refresh({"leaseId": body.lease_id})
        except GatewayLeaseError as exc:
            raise _translate_lease_error(exc) from exc
        except GatewayTransportError as exc:
            raise SidecarError(
                SidecarErrorCode.CONNECTOR_UNAVAILABLE,
                str(exc),
            ) from exc
        # Resolve the slot the refreshed lease belongs to so the
        # response envelope's ``endpoint`` + ``extras`` are consistent
        # with the originally-issued envelope. The lease record carries
        # ``slot``; the registry resolves it without re-validating
        # purpose (refresh inherits the original purpose).
        slot_ctx = registry.resolve(lease.slot, purpose=lease.capability)
        try:
            credential = await minter.mint(
                lease_id=lease.lease_id,
                slot_ctx=slot_ctx,
                ttl_hint=lease.expires_at,
            )
        except InstanceUnavailable as exc:
            raise SidecarError(
                SidecarErrorCode.CONNECTOR_UNAVAILABLE,
                str(exc),
            ) from exc
        except UpstreamMintFailure as exc:
            raise SidecarError(
                SidecarErrorCode.UPSTREAM_FAILED,
                str(exc),
            ) from exc
        envelope = _token_envelope(lease=lease, ctx=slot_ctx, token=credential.token, triple=triple)
        return JSONResponse(status_code=200, content=envelope)
    except SidecarError as exc:
        return problem_response(exc, instance=str(request.url.path))


@router.post("/token/release")
async def release_token(
    body: _ReleaseBody,
    request: Request,
    _: Annotated[None, Depends(_verify_bootstrap)],
) -> Response:
    """Best-effort release. Always 204; transport errors are swallowed.

    A failed release leaves the lease to expire by TTL; the design's
    contract is that the activity must not block on a successful
    release acknowledgement, so the sidecar absorbs any CS error.
    """
    gateway = _resolve_gateway(request)
    with contextlib.suppress(GatewayTransportError):
        # Swallow: best-effort contract. The lease will expire via TTL.
        await gateway.release(body.lease_id)
    return Response(status_code=204)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def _issue_lease(
    *,
    gateway: LeaseGateway,
    slot_ctx: SlotContext,
    purpose: str,
    triple: tuple[str, str, int],
) -> LeaseRecord:
    """Call the Lease Gateway and translate errors to :class:`SidecarError`."""
    body = {
        "runId": triple[0],
        "stepId": triple[1],
        "attempt": triple[2],
        "slot": slot_ctx.slot,
        "capability": purpose,
        "connectorInstanceId": slot_ctx.connector_instance_id,
        "tokenType": slot_ctx.token_type,
    }
    try:
        return await gateway.issue(body)
    except GatewayLeaseError as exc:
        raise _translate_lease_error(exc) from exc
    except GatewayTransportError as exc:
        raise SidecarError(
            SidecarErrorCode.CONNECTOR_UNAVAILABLE,
            str(exc),
        ) from exc


async def _best_effort_release(gateway: LeaseGateway, lease_id: str) -> None:
    """Best-effort release used when a downstream step fails post-issue.

    If lease minting succeeds but credential minting fails, we must
    return the lease so the cap counter is not over-debited. Swallow
    any error \u2014 the activity will see the original failure code.
    """
    with contextlib.suppress(GatewayLeaseError, GatewayTransportError):
        await gateway.release(lease_id)


def _translate_lease_error(exc: GatewayLeaseError) -> SidecarError:
    """Map :class:`GatewayLeaseError` to a :class:`SidecarError`."""
    sidecar_code = _LEASE_CODE_TO_SIDECAR.get(exc.code, SidecarErrorCode.INVALID_REQUEST)
    retry_after = (
        exc.retry_after_sec if sidecar_code == SidecarErrorCode.CAPACITY_EXCEEDED else None
    )
    return SidecarError(sidecar_code, exc.detail, retry_after_sec=retry_after)


__all__ = ["BOOTSTRAP_HEADER", "router"]
