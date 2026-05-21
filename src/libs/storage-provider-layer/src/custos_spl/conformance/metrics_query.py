"""Conformance tests for MetricsQueryProvider adapters.

Tests that any MetricsQuery implementation must pass:
- Workspace scoping enforcement
- Metric name and label name validation
- Range queries (time-bucketed)
- Instant queries
- Error classification
"""

from __future__ import annotations

import pytest

from custos_spl.errors import BackendUnavailable, QueryUnsupported, WorkspaceMismatch

from .base import AdapterConformanceBase


class MetricsQueryConformanceTests(AdapterConformanceBase):
    """Base conformance tests for MetricsQueryProvider adapters.

    Subclasses MUST provide an 'adapter' fixture that returns a configured
    MetricsQueryProvider implementation ready for testing.

    Example:
        @pytest.fixture
        def adapter(self) -> MetricsQueryProvider:
            return MyPrometheusAdapter(base_url="http://prometheus:9090")
    """

    def test_workspace_scoping_query_run_metrics(self) -> None:
        """query_run_metrics() filters to workspace.

        Returns metrics labeled with requested workspace_id only.

        Subclasses MUST implement:
        1. Query metrics from workspace A
        2. Query same metric from workspace B
        3. Assert workspace A gets no cross-workspace data
        """
        pytest.skip("Adapter must implement workspace scoping for query_run_metrics test")

    def test_workspace_scoping_query_workspace_metrics(self) -> None:
        """query_workspace_metrics() filters to workspace.

        Returns metrics labeled with requested workspace_id only (no run filter).

        Subclasses MUST implement:
        1. Query metrics from workspace A
        2. Query from workspace B
        3. Assert workspace A gets no cross-workspace data
        """
        pytest.skip("Adapter must implement workspace scoping for query_workspace_metrics test")

    def test_workspace_scoping_query_instant_metric(self) -> None:
        """query_instant_metric() filters to workspace.

        Returns metric labeled with requested workspace_id only.

        Subclasses MUST implement:
        1. Query instant metric from workspace A
        2. Query from workspace B
        3. Assert workspace A gets no cross-workspace data
        """
        pytest.skip("Adapter must implement workspace scoping for query_instant_metric test")

    def test_metric_name_validation(self) -> None:
        """Invalid metric names raise QueryUnsupported.

        Names must match [a-zA-Z_:][a-zA-Z0-9_:]* regex.
        Prevents PromQL injection (e.g., 'cpu{1==1}', 'up or up').

        Subclasses MUST implement:
        1. Try to query with invalid metric name (e.g., '123invalid')
        2. Assert raises QueryUnsupported
        """
        pytest.skip("Adapter must implement metric name validation test")

    def test_label_name_validation(self) -> None:
        """Invalid label names raise QueryUnsupported.

        Names must match [a-zA-Z_][a-zA-Z0-9_]* regex.
        Prevents injection via label matchers.

        Subclasses MUST implement:
        1. Try to query with invalid label name (e.g., 'job-name')
        2. Assert raises QueryUnsupported
        """
        pytest.skip("Adapter must implement label name validation test")

    def test_range_query_time_bucketing(self) -> None:
        """Range query respects step_seconds bucket width.

        All samples aligned to multiples of step_seconds from range.start.

        Subclasses MUST implement:
        1. Query range with step_seconds=60
        2. Assert all sample timestamps are aligned to 60s boundaries
        """
        pytest.skip("Adapter must implement range query time bucketing test")

    def test_range_query_time_bounds(self) -> None:
        """Range query respects start (inclusive) and end (exclusive).

        start <= sample.timestamp < end for all returned samples.

        Subclasses MUST implement:
        1. Query range with specific start/end times
        2. Assert all samples fall within bounds
        """
        pytest.skip("Adapter must implement range query time bounds test")

    def test_instant_query_staleness_window(self) -> None:
        """Instant query uses backend staleness window for missing values.

        If no value at exact timestamp, may return most recent within
        backend-specific window (typically 5m for Prometheus).

        Subclasses MUST implement:
        1. Query instant with timestamp before any data
        2. Verify either no data or most recent value returned
        """
        pytest.skip("Adapter must implement instant query staleness window test")

    def test_empty_query_result(self) -> None:
        """Query with no matches returns empty MetricSeries.

        samples tuple is empty; no error raised.

        Subclasses MUST implement:
        1. Query with filter that matches no data
        2. Assert returns MetricSeries with empty samples tuple
        """
        pytest.skip("Adapter must implement empty query result test")

    def test_workspace_mismatch_detection(self) -> None:
        """Backend returning cross-workspace data raises WorkspaceMismatch.

        Validates workspace_id label in response; prevents data leakage
        if backend is compromised or query construction has injection bug.

        Subclasses MUST implement:
        1. Mock backend returning cross-workspace data
        2. Call adapter query method
        3. Assert raises WorkspaceMismatch (not returned silently)
        """
        pytest.skip("Adapter must implement workspace mismatch detection test")

    def test_error_classification(self) -> None:
        """Backend connection errors classified as BackendUnavailable.

        Transient failures raise BackendUnavailable; caller retries.

        Subclasses MUST implement:
        1. Simulate backend unavailable
        2. Call adapter query method
        3. Assert raises BackendUnavailable (not other exception type)
        """
        pytest.skip("Adapter must implement error classification test")

    def test_query_unsupported_noop_adapter(self) -> None:
        """Noop adapter raises QueryUnsupported on all query methods.

        UI navigates to CUSTOS_METRICS_EXTERNAL_URL instead.

        Subclasses MUST implement (if testing noop adapter):
        1. Call any query method
        2. Assert raises QueryUnsupported
        """
        pytest.skip("Adapter must implement QueryUnsupported test (if noop)")
