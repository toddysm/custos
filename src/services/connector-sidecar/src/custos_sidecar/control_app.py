"""Control-channel FastAPI app factory (CONN-IMPL-020).

Builds a separate :class:`FastAPI` app for the control HTTPS server
(the one bound to ``${CUSTOS_SIDECAR_CONTROL_PORT}`` with mTLS). The
control app is intentionally kept distinct from the UDS app so the
two surfaces share no router state and a routing bug on one cannot
leak the other surface's URLs.

Both apps share the same :class:`LeaseGateway` and
:class:`RevocationRegistry` instances via their respective
``app.state`` slots so a revoke acknowledged on the control surface
is immediately observable to the UDS surface's
:func:`~custos_sidecar.router._verify_bootstrap` -> handler path.
"""

from __future__ import annotations

from fastapi import FastAPI, Request
from starlette.responses import Response

from custos_sidecar.control_router import control_error_handler, control_router
from custos_sidecar.errors import SidecarError
from custos_sidecar.lease_gateway import LeaseGateway
from custos_sidecar.revocation import RevocationRegistry

__all__ = ["create_control_app"]


def create_control_app(
    *,
    revocation_registry: RevocationRegistry,
    lease_gateway: LeaseGateway,
) -> FastAPI:
    """Build the control-channel FastAPI app.

    Args:
        revocation_registry: Shared registry of locally-revoked
            lease ids. The UDS app must hold the same instance so
            revokes observed on this surface are enforced on the
            data path.
        lease_gateway: Shared :class:`LeaseGateway` for forwarding
            ``:revoke`` calls to Connector Service (CS owns the
            audit emission per CONN-IMPL-018; the sidecar is a
            thin shim).

    The factory is intentionally minimal — no openapi/docs surface,
    a single ``/healthz`` for the operator's readiness probe, and
    the ``/sidecar-admin/v1/revoke`` router.
    """
    app = FastAPI(
        title="Custos Connector Sidecar (control)",
        version="0.1.0",
        openapi_url=None,
        docs_url=None,
        redoc_url=None,
    )
    app.state.revocation_registry = revocation_registry
    app.state.lease_gateway = lease_gateway

    @app.exception_handler(SidecarError)
    async def _sidecar_error_handler(request: Request, exc: SidecarError) -> Response:
        return control_error_handler(request, exc)

    @app.get("/healthz", include_in_schema=False)
    async def _healthz() -> dict[str, str]:
        return {"status": "ok"}

    app.include_router(control_router)
    return app
