"""Public query validation must retain the provider's QueryUnsupported contract."""

from datetime import UTC, datetime, timedelta
from unittest.mock import patch

import pytest
from custos_spl.errors import QueryUnsupported
from custos_spl.ids import RunId, WorkspaceId
from custos_spl.interfaces.metrics_query import MetricRange, MetricSelector

from custos_prometheus.adapters import PrometheusMetricsAdapter


@pytest.mark.asyncio
@pytest.mark.parametrize("scope", ["run", "workspace", "instant"])
@pytest.mark.parametrize("invalid", ["metric", "label"])
async def test_invalid_queries_fail_before_backend_access(scope: str, invalid: str) -> None:
    """Bad metric/label names must not become retryable backend failures."""
    selector = (
        MetricSelector(name="123invalid")
        if invalid == "metric"
        else MetricSelector(name="up", label_matchers={"bad-label": "value"})
    )
    now = datetime.now(UTC)
    query_range = MetricRange(start=now - timedelta(minutes=1), end=now, step_seconds=15)
    with patch("custos_prometheus.adapters.httpx.AsyncClient") as client:
        adapter = PrometheusMetricsAdapter("http://prometheus.test")
        with pytest.raises(QueryUnsupported, match="invalid"):
            if scope == "run":
                await adapter.query_run_metrics(
                    WorkspaceId("workspace"), RunId("run"), selector, query_range
                )
            elif scope == "workspace":
                await adapter.query_workspace_metrics(
                    WorkspaceId("workspace"), selector, query_range
                )
            else:
                await adapter.query_instant_metric(WorkspaceId("workspace"), selector, now)
        client.assert_not_called()
