"""Custos Trigger Service (COMP-004).

This package hosts the Trigger Service runtime: the event ingestion and
dispatch broker that receives signals (manual, scheduled, webhook,
vendor-push, pull, internal), normalizes them into a `NormalizedEvent`
envelope, classifies them as workflow-start or step-resume, matches them to
`Subscription` rows via CEL selectors, deduplicates them, and dispatches to
the Workflow Service (`StartRun` / `RaiseExternalEvent`).

See the design at:
https://github.com/toddysm/custos/blob/main/design/components/trigger-service/design.md

This module is the scaffold entry point (TS-IMPL-001). The application
factory :func:`create_app` is a stub until the FastAPI skeleton lands in
TS-IMPL-003; the pipeline, receivers, and RPC surface are wired across the
subsequent TS-IMPL phases.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fastapi import FastAPI

__all__ = ["__version__", "create_app"]

__version__ = "0.1.0"


def create_app() -> FastAPI:
    """Construct the Trigger Service FastAPI application.

    Scaffold stub (TS-IMPL-001). The real factory — routers, Dapr
    subscription, lifespan-owned stores/clients, and the ``trigger.*``
    exception handlers — lands in TS-IMPL-003 (skeleton + probes) and is
    completed by TS-IMPL-018 (full app wiring). It is referenced here so
    the ``python -m custos_trigger`` / ``custos-trigger-service`` entry
    point resolves the ``custos_trigger:create_app`` factory target uvicorn
    imports, surfacing a clear error rather than an ``AttributeError`` until
    the skeleton lands.

    Raises:
        NotImplementedError: Always, until TS-IMPL-003 lands the FastAPI
            skeleton.
    """
    raise NotImplementedError(
        "custos_trigger.create_app is a scaffold stub; the FastAPI skeleton lands in TS-IMPL-003."
    )
