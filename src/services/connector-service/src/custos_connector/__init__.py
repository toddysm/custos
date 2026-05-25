"""Custos Connector Service (COMP-005).

This package hosts the Connector Service runtime: the connector type
registry, the connector instance lifecycle (configure / validate / activate
/ disable), capability matching at workflow publish, context issuance for
running activities, and the trigger listen / pull streams that feed the
Trigger Service.

See the design at:
https://github.com/toddysm/custos/blob/main/design/components/connector-service/design.md

CONN-IMPL-001 (Phase A) ships the package skeleton, the ``create_app()``
factory exposing ``/healthz`` and ``/readyz`` (so the IMPL-002 Helm chart
can pass its liveness / readiness gates), and the
``python -m custos_connector`` entry point. Phase B (CONN-IMPL-003 +
CONN-IMPL-004) will wire the SPL provider bundle (``CatalogStoreProvider``
+ ``MetadataStoreProvider``), the schema-revision startup gate, and the
call-context middleware (via ``custos-callctx``). REST routes land in
CONN-IMPL-026; the secret-bridge sidecar lands in Phase H
(CONN-IMPL-019..021).
"""

from __future__ import annotations

from fastapi import FastAPI

__all__ = ["__version__", "create_app"]

__version__ = "0.1.0"


def create_app() -> FastAPI:
    """Construct the Connector Service ASGI application.

    Phase A ships a minimal FastAPI application that only exposes the
    ``/healthz`` and ``/readyz`` probes so the Helm chart can deploy.
    Phase B (CONN-IMPL-003 + CONN-IMPL-004) wires providers and middleware;
    Phase J (CONN-IMPL-026) adds the REST surface.
    """
    app = FastAPI(title="Custos Connector Service", version=__version__)

    @app.get("/healthz")
    def healthz() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/readyz")
    def readyz() -> dict[str, str]:
        return {"status": "ok"}

    return app
