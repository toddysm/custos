"""Health probes for the API Gateway (AGW-IMPL-002).

The ``/healthz`` probe is a flat liveness signal: the FastAPI app being able to
respond at all means the process is up and the middleware chain is sane.

The ``/readyz`` probe reports startup readiness. The lifespan hook flips
``app.state.ready`` to ``True`` once the application has finished booting. The
probe surfaces a 503 whenever ``app.state.ready`` is falsy so the readiness
contract can grow a real gate (Auth Service reachability, downstream route
registry validation) in a later phase without changing the wire shape the
deployment probes consume.
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
    detail = getattr(request.app.state, "ready_detail", None) or (
        "api-gateway has not finished startup"
    )
    return JSONResponse(
        status_code=503,
        content={
            "status": "not_ready",
            "detail": detail,
        },
    )


__all__ = ["router"]
