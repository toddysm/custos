"""Read-back Query API route modules.

Mounted incrementally: the log routes land in OBS-IMPL-013; the metrics + audit
routes follow in OBS-IMPL-014.
"""

from __future__ import annotations

from custos_obs.api.routes.audit import router as audit_router
from custos_obs.api.routes.logs import router as logs_router
from custos_obs.api.routes.metrics import router as metrics_router

__all__ = ["audit_router", "logs_router", "metrics_router"]
