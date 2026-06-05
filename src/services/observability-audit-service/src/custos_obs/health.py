"""Health probes for the Observability and Audit Service (OBS-IMPL-001).

The ``/healthz`` probe is a flat liveness signal: the FastAPI app being able to
respond at all means the process is up and the middleware chain is sane.

The ``/readyz`` probe reports startup readiness. The service has no provider /
schema-revision startup gate yet — the SPL provider wiring lands in
OBS-IMPL-004 — so the lifespan hook flips ``app.state.ready`` to ``True`` once
the application has finished booting. The probe is written to surface a 503
whenever ``app.state.ready`` is falsy so the readiness contract can grow a real
gate (provider connectivity, schema-revision check, audit-drain liveness) in a
later phase without changing the wire shape the deployment probes consume.
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
    """Readiness probe — 503 until the lifespan hook marks the app ready."""
    if getattr(request.app.state, "ready", False):
        return JSONResponse({"status": "ready"})
    return JSONResponse(
        status_code=503,
        content={
            "status": "not_ready",
            "detail": "observability-audit-service has not finished startup",
        },
    )


__all__ = ["router"]
