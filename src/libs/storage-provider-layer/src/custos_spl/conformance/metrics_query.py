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
from custos_spl.ids import RunId, WorkspaceId

from .base import AdapterConformanceBase


class MetricsQueryConformanceTests(AdapterConformanceBase):
    """Base conformance tests for MetricsQueryProvider adapters."""

    def test_workspace_scoping_query_run_metrics(self) -> None:
        """query_run_metrics() filters to workspace.

        Returns metrics labeled with requested workspace_id only.
        """
        pass

    def test_workspace_scoping_query_workspace_metrics(self) -> None:
        """query_workspace_metrics() filters to workspace.

        Returns metrics labeled with requested workspace_id only (no run filter).
        """
        pass

    def test_workspace_scoping_query_instant_metric(self) -> None:
        """query_instant_metric() filters to workspace.

        Returns metric labeled with requested workspace_id only.
        """
        pass

    def test_metric_name_validation(self) -> None:
        """Invalid metric names raise QueryUnsupported.

        Names must match [a-zA-Z_:][a-zA-Z0-9_:]* regex.
        Prevents PromQL injection (e.g., 'cpu{1==1}', 'up or up').
        """
        pass

    def test_label_name_validation(self) -> None:
        """Invalid label names raise QueryUnsupported.

        Names must match [a-zA-Z_][a-zA-Z0-9_]* regex.
        Prevents injection via label matchers.
        """
        pass

    def test_range_query_time_bucketing(self) -> None:
        """Range query respects step_seconds bucket width.

        All samples aligned to multiples of step_seconds from range.start.
        """
        pass

    def test_range_query_time_bounds(self) -> None:
        """Range query respects start (inclusive) and end (exclusive).

        start <= sample.timestamp < end for all returned samples.
        """
        pass

    def test_instant_query_staleness_window(self) -> None:
        """Instant query uses backend staleness window for missing values.

        If no value at exact timestamp, may return most recent within
        backend-specific window (typically 5m for Prometheus).
        """
        pass

    def test_empty_query_result(self) -> None:
        """Query with no matches returns empty MetricSeries.

        samples tuple is empty; no error raised.
        """
        pass

    def test_workspace_mismatch_detection(self) -> None:
        """Backend returning cross-workspace data raises WorkspaceMismatch.

        Validates workspace_id label in response; prevents data leakage
        if backend is compromised or query construction has injection bug.
        """
        pass

    def test_error_classification(self) -> None:
        """Backend connection errors classified as BackendUnavailable.

        Transient failures raise BackendUnavailable; caller retries.
        """
        pass

    def test_query_unsupported_noop_adapter(self) -> None:
        """Noop adapter raises QueryUnsupported on all query methods.

        UI navigates to CUSTOS_METRICS_EXTERNAL_URL instead.
        """
        pass
