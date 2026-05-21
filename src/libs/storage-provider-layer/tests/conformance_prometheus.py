"""Example conformance tests for Prometheus MetricsQuery adapter.

Demonstrates how to use the conformance test suite with a concrete adapter.
Run: pytest tests/conformance_prometheus.py -v
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from custos_prometheus.adapters import PrometheusMetricsAdapter, make_prometheus_adapter
from custos_spl.conformance import MetricsQueryConformanceTests
from custos_spl.errors import QueryUnsupported, WorkspaceMismatch
from custos_spl.ids import RunId, WorkspaceId
from custos_spl.interfaces.metrics_query import (
    MetricRange,
    MetricSample,
    MetricSelector,
)


class TestPrometheusMetricsConformance(MetricsQueryConformanceTests):
    """Prometheus adapter conformance tests."""

    @pytest.fixture
    def adapter(self) -> PrometheusMetricsAdapter:
        """Provide Prometheus adapter for tests."""
        return PrometheusMetricsAdapter(base_url="http://prometheus:9090")

    def test_metric_name_validation(self, adapter: PrometheusMetricsAdapter) -> None:
        """Metric names are validated against Prometheus regex."""
        # Valid names should not raise
        adapter._validate_metric_name("cpu_usage")
        adapter._validate_metric_name("http_requests_total")
        adapter._validate_metric_name("test:metric")

        # Invalid names should raise QueryUnsupported
        with pytest.raises(QueryUnsupported, match="invalid metric name"):
            adapter._validate_metric_name("123invalid")
        with pytest.raises(QueryUnsupported, match="invalid metric name"):
            adapter._validate_metric_name("cpu-usage")

    def test_label_name_validation(self, adapter: PrometheusMetricsAdapter) -> None:
        """Label names are validated against Prometheus regex."""
        # Valid names
        adapter._validate_label_name("pod")
        adapter._validate_label_name("job_name")

        # Invalid names
        with pytest.raises(QueryUnsupported, match="invalid label name"):
            adapter._validate_label_name("123invalid")
        with pytest.raises(QueryUnsupported, match="invalid label name"):
            adapter._validate_label_name("job-name")

    def test_workspace_scoping_parse_range_response(
        self, adapter: PrometheusMetricsAdapter
    ) -> None:
        """Parse range response validates workspace ownership."""
        workspace_id = WorkspaceId("ws-123")
        data = {
            "status": "success",
            "data": {
                "resultType": "matrix",
                "result": [
                    {
                        "metric": {
                            "__name__": "cpu_usage",
                            "workspace_id": "ws-999",  # Different workspace
                        },
                        "values": [["1147483647", "1"]],
                    }
                ],
            },
        }

        with pytest.raises(WorkspaceMismatch, match="belongs to workspace ws-999"):
            adapter._parse_range_response("cpu_usage", data, workspace_id)

    def test_workspace_scoping_parse_instant_response(
        self, adapter: PrometheusMetricsAdapter
    ) -> None:
        """Parse instant response validates workspace ownership."""
        workspace_id = WorkspaceId("ws-123")
        at = datetime.utcnow()
        data = {
            "status": "success",
            "data": {
                "resultType": "vector",
                "result": [
                    {
                        "metric": {
                            "__name__": "up",
                            "workspace_id": "ws-456",  # Different workspace
                        },
                        "value": ["1147483647", "1"],
                    }
                ],
            },
        }

        with pytest.raises(WorkspaceMismatch, match="belongs to workspace ws-456"):
            adapter._parse_instant_response(data, at, workspace_id)

    def test_query_validation_in_build_query(
        self, adapter: PrometheusMetricsAdapter
    ) -> None:
        """_build_query validates both metric and label names."""
        workspace_id = WorkspaceId("ws-123")

        # Invalid metric name
        selector = MetricSelector(name="123invalid")
        with pytest.raises(QueryUnsupported, match="invalid metric name"):
            adapter._build_query(selector, workspace_id)

        # Invalid label name
        selector = MetricSelector(
            name="cpu_usage",
            label_matchers={"job-name": "my-job"},
        )
        with pytest.raises(QueryUnsupported, match="invalid label name"):
            adapter._build_query(selector, workspace_id)
