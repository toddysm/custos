"""Health endpoints (AS-IMPL-004 ``/readyz`` lifespan-gated behavior).

The ``/healthz`` probe is a flat liveness signal; the FastAPI app being able
to respond at all means the process is up and the middleware chain is sane.

The ``/readyz`` probe reports the lifespan readiness flag set by
:func:`custos_auth.create_app`. When the SPL schema-revision gate fails,
:func:`custos_auth.providers.verify_schema_revisions` raises
:class:`custos_spl.MigrationRequired` from inside the lifespan, which
aborts startup with a non-zero exit before this router ever serves a
request (Kubernetes then crash-loops the pod). The 503 branch below is
therefore reached only during the brief window when lifespan startup is
still in progress — before ``app.state.ready`` flips to ``True``.
AS-IMPL-018 will additionally factor in JWKS-rotation health.
"""

from __future__ import annotations

from fastapi import APIRouter
from starlette.requests import Request
from starlette.responses import JSONResponse

router = APIRouter()


@router.get("/healthz", include_in_schema=False)
async def healthz() -> dict[str, str]:
    """Liveness probe — always 200 OK if the process is serving."""
    return {"status": "ok"}


@router.get("/readyz", include_in_schema=False)
async def readyz(request: Request) -> JSONResponse:
    """Readiness probe — 503 until lifespan startup completes successfully."""
    if getattr(request.app.state, "ready", False):
        return JSONResponse({"status": "ready"})
    return JSONResponse(
        status_code=503,
        content={"status": "not_ready", "detail": "auth-service has not finished startup"},
    )


__all__ = ["router"]
