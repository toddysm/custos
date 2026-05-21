"""Conformance tests for Prometheus MetricsQuery adapter.

These tests verify that the Prometheus adapter satisfies the MetricsQueryProvider
conformance contract. They require:
- custos-spl[conformance] package installed
- Live Prometheus instance (via CUSTOS_PROMETHEUS_URL, defaults to http://localhost:9090)

Run with: pytest tests/test_conformance.py -v -m integration
Skip without Prometheus: pytest tests/test_conformance.py -v -m "not integration"
"""

from __future__ import annotations

import os

import httpx
import pytest

from custos_spl.conformance import MetricsQueryConformanceTests
from custos_spl.ids import RunId, WorkspaceId

# Require custos-prometheus and conformance support
pytest.importorskip("custos_prometheus")

from custos_prometheus.adapters import PrometheusMetricsAdapter


@pytest.mark.integration
class TestPrometheusMetricsConformance(MetricsQueryConformanceTests):
    """Prometheus adapter conformance tests.

    Skipped if Prometheus is unavailable or custos-prometheus not installed.
    """

    @pytest.fixture(scope="class", autouse=True)
    def _check_prometheus_available(self) -> None:
        """Skip entire test class if Prometheus is not reachable."""
        base_url = os.environ.get(
            "CUSTOS_PROMETHEUS_URL", "http://localhost:9090"
        )
        health_url = f"{base_url}/-/healthy"
        try:
            response = httpx.get(health_url, timeout=2.0)
            response.raise_for_status()
        except (httpx.ConnectError, httpx.TimeoutError, httpx.HTTPError):
            pytest.skip(f"Prometheus not available at {health_url}")

    @pytest.fixture
    def adapter(self) -> PrometheusMetricsAdapter:
        """Provide configured Prometheus adapter."""
        base_url = os.environ.get(
            "CUSTOS_PROMETHEUS_URL", "http://localhost:9090"
        )
        return PrometheusMetricsAdapter(base_url=base_url)

    @pytest.fixture
    def workspace_id(self) -> WorkspaceId:
        """Primary test workspace."""
        return WorkspaceId("ws-conformance-test-primary")

    @pytest.fixture
    def other_workspace_id(self) -> WorkspaceId:
        """Secondary workspace for cross-workspace tests."""
        return WorkspaceId("ws-conformance-test-secondary")

    @pytest.fixture
    def run_id(self) -> RunId:
        """Test run ID."""
        return RunId("run-conformance-test")

    @pytest.mark.asyncio
    async def test_metric_name_validation_rejects_invalid_names(
        self,
        adapter: PrometheusMetricsAdapter,
        workspace_id: WorkspaceId,
    ) -> None:
        """Metric names are validated against Prometheus regex."""
        # This test is already implemented in base class; verify it runs
        await super().test_metric_name_validation_rejects_invalid_names(
            adapter, workspace_id
        )

    @pytest.mark.asyncio
    async def test_label_name_validation_rejects_invalid_names(
        self,
        adapter: PrometheusMetricsAdapter,
        workspace_id: WorkspaceId,
    ) -> None:
        """Label names are validated against Prometheus regex."""
        # This test is already implemented in base class; verify it runs
        await super().test_label_name_validation_rejects_invalid_names(
            adapter, workspace_id
        )

    @pytest.fixture
    def workspace_id(self) -> WorkspaceId:
        """Primary test workspace."""
        return WorkspaceId("ws-conformance-test-primary")

    @pytest.fixture
    def other_workspace_id(self) -> WorkspaceId:
        """Secondary workspace for cross-workspace tests."""
        return WorkspaceId("ws-conformance-test-secondary")

    @pytest.fixture
    def run_id(self) -> RunId:
        """Test run ID."""
        return RunId("run-conformance-test")

    @pytest.mark.asyncio
    async def test_metric_name_validation_rejects_invalid_names(
        self,
        adapter: PrometheusMetricsAdapter,
        workspace_id: WorkspaceId,
    ) -> None:
        """Metric names are validated against Prometheus regex."""
        # This test is already implemented in base class; verify it runs
        await super().test_metric_name_validation_rejects_invalid_names(
            adapter, workspace_id
        )

    @pytest.mark.asyncio
    async def test_label_name_validation_rejects_invalid_names(
        self,
        adapter: PrometheusMetricsAdapter,
        workspace_id: WorkspaceId,
    ) -> None:
        """Label names are validated against Prometheus regex."""
        # This test is already implemented in base class; verify it runs
        await super().test_label_name_validation_rejects_invalid_names(
            adapter, workspace_id
        )
