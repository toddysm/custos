"""Control-channel HTTPS router (CONN-IMPL-020).

Mounts ``POST /sidecar-admin/v1/revoke`` on the control HTTPS server
the sidecar starts alongside its UDS surface. The control server is
gated by mTLS at the TLS layer (uvicorn ``ssl_cert_reqs=CERT_REQUIRED``)
so any request reaching a handler has already presented a CA-signed
client certificate; the handler does not re-check the identity.

The revoke endpoint translates the operator/ARM-supplied
``{leaseIds, reason}`` body into a per-lease ack list. For each lease
id:

1. Consult the local :class:`RevocationRegistry`. A registry hit means
   the sidecar has already acked a revoke for this id; return
   ``already-revoked`` without touching CS so the audit pipeline does
   not record a duplicate emission.
2. Otherwise call the CS internal RPC
   ``POST /internal/v1/leases:revoke`` (batched as a single-id call to
   keep the per-lease ack mapping straight). CS does the discrimination
   (``revoked`` / ``already-revoked`` / ``already-expired`` /
   ``not-found``) and is the canonical source of the audit emission.
3. On CS ack ``revoked``, mark the lease in the local registry so
   subsequent UDS ``refresh`` / ``release`` requests serve a 410
   ``lease-revoked`` problem document with the recorded reason.

The endpoint always returns 200 with the full per-lease list; transport
errors against CS surface as :class:`SidecarError(CONNECTOR_UNAVAILABLE)`
which the control-app exception handler renders as a 503 problem
document.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, Request
from pydantic import BaseModel, ConfigDict, Field
from starlette.responses import JSONResponse, Response

from custos_sidecar.errors import SidecarError, SidecarErrorCode, problem_response
from custos_sidecar.lease_gateway import GatewayTransportError, LeaseGateway
from custos_sidecar.revocation import RevocationRegistry, RevokeMarkStatus

control_router = APIRouter(prefix="/sidecar-admin/v1", tags=["sidecar-control"])

#: Status string the registry returns on a fresh local revoke.
_STATUS_REVOKED = "revoked"
#: Statuses reported back from CS that indicate a successful revoke.
#: Only ``revoked`` causes a local registry entry; ``already-revoked``
#: hits should never reach CS (registry pre-check intercepts them) but
#: are still treated as terminal for safety.
_TERMINAL_LOCAL_STATUSES = frozenset({_STATUS_REVOKED, "already-revoked"})


class _RevokeBody(BaseModel):
    """Wire shape of ``POST /sidecar-admin/v1/revoke``."""

    model_config = ConfigDict(extra="forbid", populate_by_name=True)
    lease_ids: list[str] = Field(..., min_length=1, alias="leaseIds")
    reason: str = Field(..., min_length=1)


def _resolve_registry(request: Request) -> RevocationRegistry:
    registry: RevocationRegistry = request.app.state.revocation_registry
    return registry


def _resolve_gateway(request: Request) -> LeaseGateway:
    gateway: LeaseGateway = request.app.state.lease_gateway
    return gateway


@control_router.post("/revoke")
async def revoke_leases(body: _RevokeBody, request: Request) -> Response:
    """Revoke a batch of lease ids. Returns per-lease ack list.

    Response shape::

        {"results": [{"leaseId": "...", "status": "revoked"}, ...]}

    ``status`` is one of ``revoked`` / ``already-revoked`` /
    ``already-expired`` / ``not-found``. Always 200 unless the
    sidecar cannot reach Connector Service, in which case the
    handler raises :class:`SidecarError(CONNECTOR_UNAVAILABLE)` and
    the app-level exception handler renders a 503 problem document.
    """
    registry = _resolve_registry(request)
    gateway = _resolve_gateway(request)
    results: list[dict[str, Any]] = []

    # Partition input into "already locally revoked" (immediate ack)
    # and "needs CS round-trip". Preserve input order in the response.
    needs_cs: list[str] = []
    needs_cs_indices: list[int] = []
    for idx, lease_id in enumerate(body.lease_ids):
        if registry.is_revoked(lease_id):
            results.append({"leaseId": lease_id, "status": "already-revoked"})
        else:
            results.append({"leaseId": lease_id, "status": ""})  # placeholder
            needs_cs.append(lease_id)
            needs_cs_indices.append(idx)

    if needs_cs:
        try:
            acks = await gateway.revoke_many(needs_cs, body.reason)
        except GatewayTransportError as exc:
            raise SidecarError(SidecarErrorCode.CONNECTOR_UNAVAILABLE, str(exc)) from exc
        if len(acks) != len(needs_cs):
            raise SidecarError(
                SidecarErrorCode.UPSTREAM_FAILED,
                f"connector-service returned {len(acks)} acks for {len(needs_cs)} requested leases",
            )
        for idx, ack, lease_id in zip(needs_cs_indices, acks, needs_cs, strict=True):
            status = ack["status"]
            results[idx] = {"leaseId": lease_id, "status": status}
            if status in _TERMINAL_LOCAL_STATUSES:
                mark = await registry.mark_revoked(lease_id, body.reason)
                # On a race (concurrent control calls for the same id),
                # CS will have returned "revoked" to one caller and the
                # registry mark will be ALREADY_REVOKED for the loser.
                # Either way the lease is locally enforced; downgrade the
                # ack so the caller sees a consistent state.
                if status == _STATUS_REVOKED and mark is RevokeMarkStatus.ALREADY_REVOKED:
                    results[idx] = {"leaseId": lease_id, "status": "already-revoked"}

    return JSONResponse(status_code=200, content={"results": results})


def control_error_handler(request: Request, exc: SidecarError) -> Response:
    """Render :class:`SidecarError` as RFC 7807 on the control surface.

    Mirrors the UDS handler in :mod:`custos_sidecar.__init__` so
    callers see the same problem-document envelope on either surface.
    """
    return problem_response(exc, instance=str(request.url.path))


__all__ = [
    "control_error_handler",
    "control_router",
]
