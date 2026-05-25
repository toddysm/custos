"""Health endpoints (CONN-IMPL-003 ``/readyz`` schema-gate behavior).

The ``/healthz`` probe is a flat liveness signal; the FastAPI app being
able to respond at all means the process is up and the middleware chain
is sane.

The ``/readyz`` probe additionally reports the schema-revision startup
gate: when :func:`custos_connector.providers.verify_schema_revisions`
raises :class:`custos_spl.MigrationRequired` during lifespan startup, the
app holds the exception on ``app.state.schema_gate_error`` and surfaces a
503 with the same operator-actionable text the startup log carried (see
:func:`custos_connector.providers.schema_gate_explainer`).
"""

from __future__ import annotations

from fastapi import APIRouter
from starlette.requests import Request
from starlette.responses import JSONResponse

from custos_connector.providers import MigrationRequired, schema_gate_explainer

router = APIRouter()


@router.get("/healthz", include_in_schema=False)
async def healthz() -> dict[str, str]:
    """Liveness probe — always 200 OK if the process is serving."""
    return {"status": "ok"}


@router.get("/readyz", include_in_schema=False)
async def readyz(request: Request) -> JSONResponse:
    """Readiness probe — 503 while the schema gate is failing."""
    if getattr(request.app.state, "ready", False):
        return JSONResponse({"status": "ready"})
    error = getattr(request.app.state, "schema_gate_error", None)
    if isinstance(error, MigrationRequired):
        detail = schema_gate_explainer(error)
    else:
        detail = "connector-service has not finished startup"
    return JSONResponse(
        status_code=503,
        content={"status": "not_ready", "detail": detail},
    )


__all__ = ["router"]
