"""Router registry for the Catalog REST surface.

Each module under :mod:`custos_catalog.api.routes` exposes a
``router`` :class:`fastapi.APIRouter`. The :data:`all_routers` tuple is
the single import point :func:`create_app` uses to mount them.
"""

from __future__ import annotations

from fastapi import APIRouter

from custos_catalog.api.routes.activity_types import router as activity_types_router
from custos_catalog.api.routes.connector_types import router as connector_types_router
from custos_catalog.api.routes.templates import router as templates_router
from custos_catalog.api.routes.workflows import router as workflows_router

all_routers: tuple[APIRouter, ...] = (
    workflows_router,
    templates_router,
    activity_types_router,
    connector_types_router,
)

__all__ = ["all_routers"]
