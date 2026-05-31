"""Router registry for the workflow-service REST surface.

Each module under :mod:`custos_workflow.api.routes` exposes a
``router`` :class:`fastapi.APIRouter`. The :data:`all_routers`
tuple is the single import point :func:`create_app` (WF-IMPL-069 /
-070) uses to mount the public surface; keeping the registry here
means a new resource only has to land its own module + add itself
to this tuple, never touch the bootstrap.

Mirrors the catalog-service convention
(:mod:`custos_catalog.api.routes`) so future workflow-service
resource routers (steps, audit, …) drop into the same shape.
"""

from __future__ import annotations

from fastapi import APIRouter

from custos_workflow.api.routes.runs import router as runs_router

all_routers: tuple[APIRouter, ...] = (runs_router,)

__all__ = ["all_routers", "runs_router"]
