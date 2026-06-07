"""Custos migration Job (``custos-migrate-job``).

The Helm ``pre-install`` / ``pre-upgrade`` hook that runs the Storage Provider
Layer's strict, forward-only ``migrate up`` before any platform component
starts. The job is a thin wrapper around :mod:`custos_spl.migrations.cli`; the
strict migration policy lives in SPL, so on a remaining revision gap the process
exits non-zero and the Helm release aborts rather than letting components run
against an unmigrated database.

Design: ``design/architecture/reference-deployment.md`` § Migration job.
"""

from __future__ import annotations

from custos_migrate.__main__ import DSN_ENV_VAR, main, resolve_dsn

__all__ = ["DSN_ENV_VAR", "main", "resolve_dsn"]
__version__ = "0.1.0"
