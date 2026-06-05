"""Metrics read-back route (Concern B, metrics side) — OBS-IMPL-014.

One route over the SPL :class:`MetricsQueryProvider`:

* ``GET /v1/workspaces/{ws}/runs/{runId}/metrics`` — a time-bucketed range query
  for one run, returning a :class:`MetricSeriesModel`.

The caller selects the metric by ``metric`` (name) and the window by ``from`` /
``to`` (inclusive / exclusive, ISO-8601, timezone-aware) plus a ``step`` bucket
width in seconds. The adapter adds the workspace + run filters on top — callers
never encode workspace/run into the selector.

When the configured provider is ``noop`` (or the backend is unreachable) the SPL
layer raises :class:`QueryUnsupported` / :class:`BackendUnavailable`; both map to
``503 MetricsQueryUnavailable`` (RFC 7807) carrying the configured
``CUSTOS_METRICS_EXTERNAL_URL`` pointer so the UI can link out to the external
metrics system. A run that resolves to a different workspace surfaces as ``404``.
"""

from __future__ import annotations

from typing import Annotated

from custos_spl import RunId, WorkspaceId
from custos_spl.errors import BackendUnavailable, QueryUnsupported, WorkspaceMismatch
from custos_spl.interfaces.metrics_query import (
    MetricRange,
    MetricSelector,
    MetricsQueryProvider,
)
from fastapi import APIRouter, Depends, Query

from custos_obs.api.dependencies import get_metrics_query_provider, get_settings
from custos_obs.api.models import MetricSeriesModel
from custos_obs.api.routes._common import (
    RunPath,
    WorkspacePath,
    ensure_workspace,
    require_iso_datetime,
    run_not_found,
)
from custos_obs.errors import MetricsQueryUnavailable
from custos_obs.middleware import CallContext, require_permission
from custos_obs.settings import Settings

__all__ = ["router"]

router = APIRouter(prefix="/v1/workspaces/{workspace_id}/runs/{run_id}", tags=["metrics"])

#: Permission scope the call-context middleware enforces for metric read-back.
PERM_READ = "metrics:read"

#: Default bucket width when the caller omits ``step``.
DEFAULT_STEP_SECONDS = 60

MetricsProviderDep = Annotated[MetricsQueryProvider, Depends(get_metrics_query_provider)]
SettingsDep = Annotated[Settings, Depends(get_settings)]
CtxDep = Annotated[CallContext, Depends(require_permission(PERM_READ))]


def _metrics_unavailable(settings: Settings) -> MetricsQueryUnavailable:
    """Build the ``503`` Problem Details, carrying the external-URL pointer."""
    extensions = (
        {"externalUrl": settings.metrics_external_url}
        if settings.metrics_external_url is not None
        else None
    )
    return MetricsQueryUnavailable(
        "the metrics query backend is not available; use the external metrics system",
        extensions=extensions,
    )


@router.get("/metrics")
async def query_run_metrics(
    workspace_id: WorkspacePath,
    run_id: RunPath,
    provider: MetricsProviderDep,
    settings: SettingsDep,
    ctx: CtxDep,
    metric: Annotated[str, Query(min_length=1, description="Metric name to select.")],
    from_: Annotated[str, Query(alias="from", description="ISO-8601 window start (inclusive).")],
    to: Annotated[str, Query(alias="to", description="ISO-8601 window end (exclusive).")],
    step: Annotated[
        int, Query(ge=1, description="Bucket width in seconds.")
    ] = DEFAULT_STEP_SECONDS,
) -> MetricSeriesModel:
    """Time-bucketed range query of one metric scoped to a run.

    A ``noop``/unreachable backend returns ``503`` carrying the external metrics
    pointer.
    """
    ensure_workspace(ctx, workspace_id)
    selector = MetricSelector(name=metric)
    metric_range = MetricRange(
        start=require_iso_datetime(from_),
        end=require_iso_datetime(to),
        step_seconds=step,
    )
    try:
        series = await provider.query_run_metrics(
            WorkspaceId(workspace_id), RunId(run_id), selector, metric_range
        )
    except (QueryUnsupported, BackendUnavailable) as exc:
        raise _metrics_unavailable(settings) from exc
    except WorkspaceMismatch as exc:
        raise run_not_found(run_id) from exc
    return MetricSeriesModel.from_domain(series)
