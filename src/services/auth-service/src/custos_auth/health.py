"""Health endpoints for the Auth Service scaffold (AS-IMPL-001).

The ``/healthz`` probe is a flat liveness signal; the FastAPI app being able
to respond at all means the process is up and the middleware chain is sane.

The ``/readyz`` probe currently returns 200 unconditionally. AS-IMPL-004 will
introduce the SPL schema-revision startup gate and ``/readyz`` will then
surface a 503 with operator-actionable text while migrations are behind.
AS-IMPL-018 will additionally report JWKS-rotation health.
"""

from __future__ import annotations

from fastapi import APIRouter

router = APIRouter()


@router.get("/healthz", include_in_schema=False)
async def healthz() -> dict[str, str]:
    """Liveness probe — always 200 OK if the process is serving."""
    return {"status": "ok"}


@router.get("/readyz", include_in_schema=False)
async def readyz() -> dict[str, str]:
    """Readiness probe — 200 OK in the AS-IMPL-001 scaffold.

    AS-IMPL-004 will gate this on the SPL schema-revision check. AS-IMPL-018
    will additionally include JWKS-rotation health.
    """
    return {"status": "ready"}


__all__ = ["router"]
