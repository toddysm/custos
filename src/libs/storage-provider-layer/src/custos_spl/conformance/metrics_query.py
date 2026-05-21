"""Conformance tests for MetricsQueryProvider adapters.

Tests that any MetricsQuery implementation must pass:
- Workspace scoping enforcement
- Metric name and label name validation
- Range queries (time-bucketed)
- Instant queries
- Error classification
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

from custos_spl.errors import BackendUnavailable, QueryUnsupported, WorkspaceMismatch
from custos_spl.ids import RunId, WorkspaceId
from custos_spl.interfaces.metrics_query import MetricRange, MetricSample, MetricSelector

from .base import AdapterConformanceBase


class MetricsQueryConformanceTests(AdapterConformanceBase):
    """Base conformance tests for MetricsQueryProvider adapters.

    Subclasses MUST provide:
    - `adapter` fixture: MetricsQueryProvider instance
    - `workspace_id` fixture: test workspace ID
    - `run_id` fixture: test run ID

    Example:
        @pytest.fixture
        def adapter(self) -> MetricsQueryProvider:
            return MyPrometheusAdapter(base_url="http://prometheus:9090")
    """

    def test_metric_name_validation_rejects_invalid_names(self) -> None:
        """Invalid metric names are rejected.

        Names must match [a-zA-Z_:][a-zA-Z0-9_:]* regex.
        Prevents PromQL injection (e.g., 'cpu{1==1}', 'up or up').
        """
        pytest.skip(
            "Adapter must implement: test invalid metric names raise QueryUnsupported"
        )

    def test_label_name_validation_rejects_invalid_names(self) -> None:
        """Invalid label names are rejected.

        Names must match [a-zA-Z_][a-zA-Z0-9_]* regex.
        Prevents injection via label matchers.
        """
        pytest.skip(
            "Adapter must implement: test invalid label names raise QueryUnsupported"
        )

    def test_workspace_scoping_prevents_cross_workspace_access(self) -> None:
        """Cross-workspace queries are blocked or return empty.

        Adapter must prevent callers from accessing metrics from
        workspace B when querying as workspace A.
        """
        pytest.skip(
            "Adapter must implement: test cross-workspace access is blocked"
        )

    def test_workspace_validation_on_response_parsing(self) -> None:
        """Workspace ownership is validated on response parsing.

        If backend returns cross-workspace data (due to compromise or bug),
        adapter must raise WorkspaceMismatch before returning it.
        """
        pytest.skip(
            "Adapter must implement: test response parsing validates workspace_id label"
        )

    def test_empty_query_result_handling(self) -> None:
        """Queries with no matches return empty MetricSeries.

        No error raised; samples tuple is empty.
        """
        pytest.skip("Adapter must implement: test empty result handling")

    def test_range_query_time_bounds_respected(self) -> None:
        """Range query respects start (inclusive) and end (exclusive).

        All samples must satisfy: start <= timestamp < end.
        """
        pytest.skip(
            "Adapter must implement: test range query time bounds enforcement"
        )

    def test_range_query_step_alignment(self) -> None:
        """Range query samples are aligned to step_seconds boundaries.

        All sample timestamps must be multiples of step_seconds from range.start.
        """
        pytest.skip(
            "Adapter must implement: test range query step alignment"
        )

    def test_instant_query_returns_single_sample(self) -> None:
        """Instant queries return exactly one sample.

        Staleness window may apply (e.g., Prometheus 5m default).
        """
        pytest.skip(
            "Adapter must implement: test instant query returns one sample"
        )

    def test_error_classification_transient_failures(self) -> None:
        """Network/transient errors raise BackendUnavailable.

        Connection refused, timeout, HTTP 503 → BackendUnavailable.
        Caller retries with backoff.
        """
        pytest.skip(
            "Adapter must implement: test transient errors raise BackendUnavailable"
        )

    def test_noop_adapter_raises_query_unsupported(self) -> None:
        """Noop adapter raises QueryUnsupported on all query methods.

        Used when metrics are disabled; UI falls back to external URL.
        """
        pytest.skip(
            "Adapter must implement: test QueryUnsupported for noop adapter"
        )
