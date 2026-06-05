"""Custos Observability and Audit Service (COMP-009).

This package hosts the Observability and Audit Service runtime: the platform's
single observer-side surface. It drains the SPL audit outbox into the durable
audit store, enforces audit retention, manages the OTel Collector exporter
bundle (the External Exporter Loader), dispatches alerts, and serves the
inbound read-back APIs (per-run log tail, audit query, run-scoped metrics).

See the design at:
https://github.com/toddysm/custos/blob/main/design/components/observability-audit-service/design.md

The FastAPI application factory :func:`create_app` (OBS-IMPL-001) ships the
``/healthz`` + ``/readyz`` probes. The settings loader, error taxonomy, SPL
provider wiring, audit pipeline, alerting, External Exporter Loader, and
read-back API surface are wired across the subsequent OBS-IMPL phases.
"""

from __future__ import annotations

from custos_obs._version import __version__
from custos_obs.app import create_app

__all__ = ["__version__", "create_app"]
