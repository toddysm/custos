"""Health endpoints (AS-IMPL-004 ``/readyz`` lifespan-gated behavior).

The ``/healthz`` probe is a flat liveness signal; the FastAPI app being able
to respond at all means the process is up and the middleware chain is sane.

The ``/readyz`` probe reports the lifespan readiness flag set by
:func:`custos_auth.create_app` **plus** the per-subsystem dependency map
(``postgres`` / ``jwks_rotation`` / ``pubsub``) added in AS-IMPL-024 so
operators can tell *which* dependency is keeping the pod out of ready.
When the SPL schema-revision gate fails,
:func:`custos_auth.providers.verify_schema_revisions` raises
:class:`custos_spl.MigrationRequired` from inside the lifespan, which
aborts startup with a non-zero exit before this router ever serves a
request (Kubernetes then crash-loops the pod). The 503 branch below is
therefore reached only during the brief window when lifespan startup is
still in progress — before ``app.state.ready`` flips to ``True``.
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from starlette.requests import Request
from starlette.responses import JSONResponse

router = APIRouter()


def _dependency_snapshot(request: Request) -> dict[str, str]:
    """Return a shallow copy of the per-subsystem readiness map.

    Defaults to an all-``unknown`` map when the lifespan has not yet
    initialised ``app.state.dependency_status`` (the brief window
    before the lifespan body runs). Returning a copy means the
    response body can never be mutated by a concurrent lifespan
    progressing the map.
    """
    raw = getattr(request.app.state, "dependency_status", None)
    if not isinstance(raw, dict):
        return {"postgres": "unknown", "jwks_rotation": "unknown", "pubsub": "unknown"}
    snapshot: dict[str, str] = {}
    for key, value in raw.items():
        if isinstance(key, str) and isinstance(value, str):
            snapshot[key] = value
    return snapshot


@router.get("/healthz", include_in_schema=False)
async def healthz() -> dict[str, str]:
    """Liveness probe — always 200 OK if the process is serving."""
    return {"status": "ok"}


@router.get("/readyz", include_in_schema=False)
async def readyz(request: Request) -> JSONResponse:
    """Readiness probe — 503 until lifespan startup completes successfully.

    The 200 and 503 response bodies both carry the same shape:

    .. code-block:: json

        {
          "status": "ready",
          "dependencies": {
            "postgres": "ok",
            "jwks_rotation": "ok",
            "pubsub": "ok"
          }
        }

    Values are closed-set strings:

    * ``"ok"`` — subsystem wired and healthy
    * ``"unknown"`` — lifespan has not yet touched this subsystem
    * ``"schema_gap"`` — SPL schema-revision gate failed (postgres only)
    * ``"static"`` — JWKS rotation disabled (dev mode without rotation
      loop); JWKS feed is a single-entry set

    Mirrors the design's "Healthz/readyz expose dependency state
    (Postgres, JWKS rotation status, Pub/Sub)" acceptance criterion.
    """
    dependencies = _dependency_snapshot(request)
    if getattr(request.app.state, "ready", False):
        body: dict[str, Any] = {"status": "ready", "dependencies": dependencies}
        return JSONResponse(body)
    return JSONResponse(
        status_code=503,
        content={
            "status": "not_ready",
            "detail": "auth-service has not finished startup",
            "dependencies": dependencies,
        },
    )


__all__ = ["router"]
