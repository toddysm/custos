"""FastAPI application factory.

This module holds the ``create_app`` factory that assembles the gateway's
middleware stack, routes, and lifespan. AGW-IMPL-001 ships a minimal
placeholder so the package is importable and runnable end to end; AGW-IMPL-002
adds settings + health probes, and AGW-IMPL-016 wires the full middleware
ordering + lifespan-owned collaborators.
"""

from __future__ import annotations

from fastapi import FastAPI

from custos_gateway import __version__


def create_app() -> FastAPI:
    """Build and return the gateway FastAPI application.

    The placeholder app exposes only its metadata for now. Subsequent tasks
    layer in settings, probes, middleware, and the downstream route registry.
    """
    return FastAPI(
        title="Custos API Gateway",
        version=__version__,
        description="Single uniform HTTPS entrypoint for Custos (COMP-001).",
    )
