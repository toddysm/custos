"""Custos Trigger Service (COMP-004).

This package hosts the Trigger Service runtime: the event ingestion and
dispatch broker that receives signals (manual, scheduled, webhook,
vendor-push, pull, internal), normalizes them into a `NormalizedEvent`
envelope, classifies them as workflow-start or step-resume, matches them to
`Subscription` rows via CEL selectors, deduplicates them, and dispatches to
the Workflow Service (`StartRun` / `RaiseExternalEvent`).

See the design at:
https://github.com/toddysm/custos/blob/main/design/components/trigger-service/design.md

The FastAPI application factory :func:`create_app` (TS-IMPL-003) ships the
``/healthz`` + ``/readyz`` probes and the call-context middleware (with dev
shim). The pipeline, receivers, persistence, and RPC surface are wired across
the subsequent TS-IMPL phases.
"""

from __future__ import annotations

from custos_trigger._version import __version__
from custos_trigger.app import create_app

__all__ = ["__version__", "create_app"]
