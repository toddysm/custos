"""Health endpoints for the Activity Runtime Manager (ARM-IMPL-001).

- ``GET /healthz`` is a flat liveness signal — the process can serve at
  all. Always returns ``200 OK``.
- ``GET /readyz`` reports the readiness gate: ``200`` once the lifespan
  startup hook has flipped ``app.state.ready`` to ``True``; ``503``
  before that. ARM-IMPL-001 wires only the gate plumbing — later phases
  (resolver warm-up, runtime-driver probe, Dapr worker readiness) will
  populate ``app.state.ready_detail`` with operator-actionable text when
  they fail.

Both routes are excluded from the OpenAPI schema so the Kubernetes probes
do not bloat the public API surface.
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
    """Readiness probe — 503 until ``app.state.ready`` is True."""
    if getattr(request.app.state, "ready", False):
        return JSONResponse({"status": "ready"})
    detail = getattr(
        request.app.state,
        "ready_detail",
        "activity-runtime-manager has not finished startup",
    )
    return JSONResponse(
        status_code=503,
        content={"status": "not_ready", "detail": detail},
    )


__all__ = ["router"]
