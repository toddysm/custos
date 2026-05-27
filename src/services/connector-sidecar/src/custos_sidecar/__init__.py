"""Custos connector sidecar package (CONN-IMPL-019).

Exposes :func:`create_app` \u2014 the FastAPI factory the production
``__main__`` and unit tests both go through. The factory builds a
fully-wired app from explicit collaborators (verifier / registry /
gateway / minter) and stashes them on ``app.state`` so the per-request
dependencies can fish them out. This is the same pattern used by
``custos_connector`` (the production Connector Service) so tests do
not need to learn a second wiring convention.

The factory does *not* read settings or env vars \u2014 that is
:mod:`custos_sidecar.__main__`'s job. The factory accepts ready-made
collaborators so unit tests can substitute fakes without any env
plumbing.
"""

from __future__ import annotations

from fastapi import FastAPI, Request
from starlette.responses import Response

from custos_sidecar.auth import BootstrapTokenVerifier
from custos_sidecar.context_registry import ContextRegistry
from custos_sidecar.credential_minter import CredentialMinter
from custos_sidecar.errors import SidecarError, problem_response
from custos_sidecar.lease_gateway import LeaseGateway
from custos_sidecar.revocation import RevocationRegistry
from custos_sidecar.router import router as sidecar_router

__all__ = ["create_app"]


def create_app(
    *,
    bootstrap_verifier: BootstrapTokenVerifier,
    context_registry: ContextRegistry,
    lease_gateway: LeaseGateway,
    credential_minter: CredentialMinter,
    bound_triple: tuple[str, str, int],
    revocation_registry: RevocationRegistry | None = None,
) -> FastAPI:
    """Build the sidecar FastAPI app from its collaborators.

    All collaborators are required so the wiring is explicit —
    tests cannot accidentally use the real Lease Gateway, and
    production cannot accidentally use a stub. The factory:

    * Pins every collaborator on ``app.state`` so the per-request
      dependencies in :mod:`custos_sidecar.router` can fish them out
      without re-reading env vars.
    * Registers a single exception handler that renders any
      uncaught :class:`SidecarError` through :func:`problem_response`.
      Each handler also catches :class:`SidecarError` locally to add
      the request path as ``instance``, but the handler here is the
      backstop.
    * Adds a small ``/healthz`` endpoint with no auth; ARM uses it to
      gate the readiness probe.

    The ``revocation_registry`` argument is optional for backward
    compatibility with tests that pre-date CONN-IMPL-020. When omitted
    the factory builds a fresh empty :class:`RevocationRegistry` so
    the UDS router's 410 check always has something to consult; the
    registry stays empty unless the control surface (built separately
    via :func:`custos_sidecar.control_app.create_control_app`) shares
    the same instance.
    """
    app = FastAPI(
        title="Custos Connector Sidecar",
        version="0.1.0",
        openapi_url=None,  # internal-only surface; no public openapi
        docs_url=None,
        redoc_url=None,
    )
    app.state.bootstrap_verifier = bootstrap_verifier
    app.state.context_registry = context_registry
    app.state.lease_gateway = lease_gateway
    app.state.credential_minter = credential_minter
    app.state.bound_triple = bound_triple
    app.state.revocation_registry = (
        revocation_registry if revocation_registry is not None else RevocationRegistry()
    )

    @app.exception_handler(SidecarError)
    async def _sidecar_error_handler(request: Request, exc: SidecarError) -> Response:
        return problem_response(exc, instance=str(request.url.path))

    @app.get("/healthz", include_in_schema=False)
    async def _healthz() -> dict[str, str]:
        return {"status": "ok"}

    app.include_router(sidecar_router)
    return app
