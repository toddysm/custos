"""Read-back Query API surface for the Observability and Audit Service.

Wire models live in :mod:`custos_obs.api.models`; FastAPI dependency factories in
:mod:`custos_obs.api.dependencies`. The route modules are mounted incrementally
across OBS-IMPL-013 (logs) and OBS-IMPL-014 (metrics + audit).
"""

from __future__ import annotations

from custos_obs.api.dependencies import (
    get_log_query_provider,
    get_metadata_store,
    get_metrics_query_provider,
    get_providers,
    get_settings,
)
from custos_obs.api.errors import obs_error_handler
from custos_obs.api.models import (
    AuditEventModel,
    AuditEventPageModel,
    LogPageModel,
    LogRecordModel,
    MetricSampleModel,
    MetricSeriesModel,
)

__all__ = [
    "AuditEventModel",
    "AuditEventPageModel",
    "LogPageModel",
    "LogRecordModel",
    "MetricSampleModel",
    "MetricSeriesModel",
    "get_log_query_provider",
    "get_metadata_store",
    "get_metrics_query_provider",
    "get_providers",
    "get_settings",
    "obs_error_handler",
]
