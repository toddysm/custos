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

__all__ = ["__version__"]

__version__ = "0.1.0"
