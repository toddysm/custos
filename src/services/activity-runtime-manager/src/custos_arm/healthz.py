"""Health endpoints for the Activity Runtime Manager (ARM-IMPL-001).

- ``GET /healthz`` is a flat liveness signal — the process can serve at
  all. Always returns ``200 OK``.
- ``GET /readyz`` requires the lifespan readiness gate and a configured
  scheduler. Missing or lost scheduling capability returns ``503``, even if
  a stale gate still says ready. Dependency failures retain their diagnostic
  detail; liveness remains independent.

Both routes are excluded from the OpenAPI schema so the Kubernetes probes
do not bloat the public API surface.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Final

import httpx
from custos_spl.errors import SPLError
from fastapi import APIRouter
from starlette.requests import Request
from starlette.responses import JSONResponse

from custos_arm._readiness_diagnostics import log_readiness_failure, readiness_cycle
from custos_arm.scheduler import ActivityScheduler

router = APIRouter()
logger = logging.getLogger(__name__)
SCHEDULER_NOT_CONFIGURED: Final[str] = "activity scheduler is not configured"


@router.get("/healthz", include_in_schema=False)
async def healthz() -> dict[str, str]:
    """Liveness probe — always 200 OK if the process is serving."""
    return {"status": "ok"}


@router.get("/readyz", include_in_schema=False)
async def readyz(request: Request) -> JSONResponse:
    """Retain a closed readiness-state reason; preserve the existing 503 body and timeout."""
    with readiness_cycle("readiness"):
        return await _readiness_response(request)


async def _readiness_response(request: Request) -> JSONResponse:
    """Share cycle identity with probes; log local timeouts, not already-diagnosed group errors."""
    if getattr(request.app.state, "ready", False):
        if isinstance(getattr(request.app.state, "scheduler", None), ActivityScheduler):
            probe = getattr(request.app.state, "readiness_probe", None)
            if probe is not None:
                try:
                    await asyncio.wait_for(probe(), timeout=8)
                except (TimeoutError, RuntimeError, SPLError, httpx.HTTPError) as error:
                    request.app.state.dependencies_ready = False
                    logger.warning("Activity runtime dependency readiness failed")
                    if isinstance(error, TimeoutError):
                        log_readiness_failure("dependencies", "error", error)
                    return JSONResponse(
                        {"status": "not_ready", "detail": "runtime dependency unavailable"},
                        status_code=503,
                    )
                request.app.state.dependencies_ready = True
            return JSONResponse({"status": "ready"})
        detail = SCHEDULER_NOT_CONFIGURED
        log_readiness_failure("scheduler", "scheduler_missing")
    else:
        log_readiness_failure("lifecycle", "gate_closed")
        detail = getattr(
            request.app.state,
            "ready_detail",
            "activity-runtime-manager has not finished startup",
        )
    return JSONResponse(
        status_code=503,
        content={"status": "not_ready", "detail": detail},
    )


__all__ = ["SCHEDULER_NOT_CONFIGURED", "router"]
