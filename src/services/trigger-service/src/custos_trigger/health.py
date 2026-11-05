"""Liveness and fail-closed startup/receiver readiness without backend diagnostics."""

from __future__ import annotations

from fastapi import APIRouter
from starlette.requests import Request
from starlette.responses import JSONResponse

from custos_trigger.receiver_runtime import ReceiverRuntime

router = APIRouter()


@router.get("/healthz", include_in_schema=False)
async def healthz() -> dict[str, str]:
    """Liveness probe — always 200 OK if the process is serving."""
    return {"status": "ok"}


@router.get("/readyz", include_in_schema=False)
async def readyz(request: Request) -> JSONResponse:
    """Report startup and owned receiver health without exposing backend diagnostics."""
    runtime = getattr(request.app.state, "receiver_runtime", None)
    if isinstance(runtime, ReceiverRuntime) and not runtime.healthy:
        return JSONResponse(
            status_code=503,
            content={"status": "not_ready", "detail": "trigger receivers are not healthy"},
        )
    if getattr(request.app.state, "ready", False):
        return JSONResponse({"status": "ready"})
    return JSONResponse(
        status_code=503,
        content={
            "status": "not_ready",
            "detail": "trigger-service has not finished startup",
        },
    )


__all__ = ["router"]
