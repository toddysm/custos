"""Read-back Query API route modules.

Mounted incrementally: the log routes land in OBS-IMPL-013; the metrics + audit
routes follow in OBS-IMPL-014.
"""

from __future__ import annotations

from custos_obs.api.routes.logs import router as logs_router

__all__ = ["logs_router"]
