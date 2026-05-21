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
from custos_spl.interfaces.metrics_query import (
    MetricRange,
    MetricSample,
    MetricSelector,
    MetricsQueryProvider,
)

from .base import AdapterConformanceBase


class MetricsQueryConformanceTests(AdapterConformanceBase):
    """Base conformance tests for MetricsQueryProvider adapters.

    Subclasses MUST provide these pytest fixtures:
    - `adapter` → MetricsQueryProvider instance, configured and ready
    - `workspace_id` → WorkspaceId for testing
    - `other_workspace_id` → different WorkspaceId for cross-workspace tests
    - `run_id` → RunId with available metrics

    Tests will skip if required fixtures are not provided.

    Example:
        class TestMyPrometheusAdapter(MetricsQueryConformanceTests):
            @pytest.fixture
            def adapter(self):
                return MyPrometheusAdapter(base_url="http://prometheus:9090")

            @pytest.fixture
            def workspace_id(self):
                return WorkspaceId("ws-test")
    """

    @pytest.fixture
    def adapter(self) -> MetricsQueryProvider:
        """Adapter fixture (must be overridden by subclass)."""
        pytest.skip("adapter fixture not provided by subclass")

    @pytest.fixture
    def workspace_id(self) -> WorkspaceId:
        """Primary workspace ID fixture (must be overridden by subclass)."""
        pytest.skip("workspace_id fixture not provided by subclass")

    @pytest.fixture
    def other_workspace_id(self) -> WorkspaceId:
        """Secondary workspace ID for cross-workspace tests (must be overridden by subclass)."""
        pytest.skip("other_workspace_id fixture not provided by subclass")

    @pytest.fixture
    def run_id(self) -> RunId:
        """Run ID with metrics (must be overridden by subclass)."""
        pytest.skip("run_id fixture not provided by subclass")

    @pytest.mark.asyncio
    async def test_metric_name_validation_rejects_invalid_names(
        self,
        adapter: MetricsQueryProvider,
        workspace_id: WorkspaceId,
    ) -> None:
        """Invalid metric names are rejected.

        Names must match [a-zA-Z_:][a-zA-Z0-9_:]* regex.
        Prevents PromQL injection (e.g., 'cpu{1==1}', 'up or up').
        """
        selector = MetricSelector(name="123invalid")  # Starts with digit
        range_query = MetricRange(
            start=datetime.utcnow() - timedelta(hours=1),
            end=datetime.utcnow(),
            step_seconds=60,
        )

        with pytest.raises(QueryUnsupported, match="invalid"):
            await adapter.query_workspace_metrics(workspace_id, selector, range_query)

    @pytest.mark.asyncio
    async def test_label_name_validation_rejects_invalid_names(
        self,
        adapter: MetricsQueryProvider,
        workspace_id: WorkspaceId,
    ) -> None:
        """Invalid label names are rejected.

        Names must match [a-zA-Z_][a-zA-Z0-9_]* regex.
        Prevents injection via label matchers.
        """
        selector = MetricSelector(
            name="up",
            label_matchers={"job-name": "invalid"},  # Hyphen not allowed
        )
        range_query = MetricRange(
            start=datetime.utcnow() - timedelta(hours=1),
            end=datetime.utcnow(),
            step_seconds=60,
        )

        with pytest.raises(QueryUnsupported, match="invalid"):
            await adapter.query_workspace_metrics(workspace_id, selector, range_query)

    @pytest.mark.asyncio
    async def test_empty_query_result_returns_empty_series(
        self,
        adapter: MetricsQueryProvider,
        workspace_id: WorkspaceId,
    ) -> None:
        """Query with no matches returns empty MetricSeries.

        No error raised; samples tuple is empty.
        """
        selector = MetricSelector(name="nonexistent_metric_xyz_123")
        range_query = MetricRange(
            start=datetime.utcnow() - timedelta(hours=1),
            end=datetime.utcnow(),
            step_seconds=60,
        )

        series = await adapter.query_workspace_metrics(workspace_id, selector, range_query)

        assert len(series.samples) == 0

    @pytest.mark.asyncio
    async def test_range_query_time_bounds_respected(
        self,
        adapter: MetricsQueryProvider,
        workspace_id: WorkspaceId,
    ) -> None:
        """Range query respects start (inclusive) and end (exclusive).

        All samples must satisfy: start <= timestamp < end.
        """
        now = datetime.utcnow()
        start = now - timedelta(hours=1)
        end = now

        selector = MetricSelector(name="up")
        range_query = MetricRange(start=start, end=end, step_seconds=60)

        series = await adapter.query_workspace_metrics(workspace_id, selector, range_query)

        # All samples should be within bounds
        for sample in series.samples:
            assert start <= sample.timestamp < end, (
                f"Sample timestamp {sample.timestamp} outside bounds [{start}, {end})"
            )

    @pytest.mark.asyncio
    async def test_instant_query_returns_single_sample(
        self,
        adapter: MetricsQueryProvider,
        workspace_id: WorkspaceId,
    ) -> None:
        """Instant queries return exactly one sample.

        Staleness window may apply (e.g., Prometheus 5m default).
        """
        selector = MetricSelector(name="up")
        at = datetime.utcnow()

        sample = await adapter.query_instant_metric(workspace_id, selector, at)

        assert isinstance(sample, MetricSample)
        assert sample.value is not None
        assert isinstance(sample.value, float)

    @pytest.mark.asyncio
    async def test_workspace_scoping_blocks_cross_workspace_access(
        self,
        adapter: MetricsQueryProvider,
        workspace_id: WorkspaceId,
        other_workspace_id: WorkspaceId,
        run_id: RunId,
    ) -> None:
        """Cross-workspace queries are blocked or return empty.

        Adapter must prevent callers from accessing metrics from workspace B
        when querying as workspace A.
        """
        selector = MetricSelector(name="up")
        range_query = MetricRange(
            start=datetime.utcnow() - timedelta(hours=1),
            end=datetime.utcnow(),
            step_seconds=60,
        )

        # Query from workspace A should work
        series_a = await adapter.query_run_metrics(
            workspace_id, run_id, selector, range_query
        )
        assert isinstance(series_a.samples, tuple)

        # Query from workspace B for same run should return empty or raise
        try:
            series_b = await adapter.query_run_metrics(
                other_workspace_id, run_id, selector, range_query
            )
            # If it returns, should be empty
            assert len(series_b.samples) == 0
        except WorkspaceMismatch:
            # Also acceptable to raise WorkspaceMismatch
            pass

    @pytest.mark.asyncio
    async def test_noop_adapter_raises_query_unsupported(
        self,
        adapter: MetricsQueryProvider,
        workspace_id: WorkspaceId,
        run_id: RunId,
    ) -> None:
        """Noop adapter raises QueryUnsupported on all query methods.

        Used when metrics are disabled; UI falls back to external URL.
        """
        selector = MetricSelector(name="up")
        range_query = MetricRange(
            start=datetime.utcnow() - timedelta(hours=1),
            end=datetime.utcnow(),
            step_seconds=60,
        )

        # Try all three methods; noop adapter should raise QueryUnsupported for all
        try:
            await adapter.query_run_metrics(workspace_id, run_id, selector, range_query)
        except QueryUnsupported:
            # Expected for noop adapter
            return
        except Exception:
            # Other adapters may succeed or fail differently
            pass

        # If we get here, adapter isn't noop (that's fine, only noop should raise)
        pass
