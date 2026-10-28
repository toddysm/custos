"""Live Prometheus conformance; an explicitly configured unavailable backend fails."""

from __future__ import annotations

import os

import httpx
import pytest
from custos_spl.conformance import MetricsQueryConformanceTests
from custos_spl.ids import RunId, WorkspaceId

from custos_prometheus.adapters import PrometheusMetricsAdapter


@pytest.mark.integration
class TestPrometheusMetricsConformance(MetricsQueryConformanceTests):
    """Run the inherited contract against the workflow's labeled test metrics."""

    @pytest.fixture(scope="class", autouse=True)
    def _check_prometheus_available(self) -> None:
        """Skip optional local service absence, but never configured CI outages."""
        base_url = os.environ.get("CUSTOS_PROMETHEUS_URL", "http://localhost:9090")
        health_url = f"{base_url}/-/ready"
        try:
            response = httpx.get(health_url, timeout=2.0)
            response.raise_for_status()
        except httpx.HTTPError as exc:
            if "CUSTOS_PROMETHEUS_URL" in os.environ:
                pytest.fail(f"Configured Prometheus is unavailable at {health_url}: {exc}")
            pytest.skip(f"Prometheus not available at {health_url}")

    @pytest.fixture
    def adapter(self) -> PrometheusMetricsAdapter:
        """Provide the real query adapter for the configured backend."""
        return PrometheusMetricsAdapter(
            base_url=os.environ.get("CUSTOS_PROMETHEUS_URL", "http://localhost:9090")
        )

    @pytest.fixture
    def workspace_id(self) -> WorkspaceId:
        """Match the primary label configured in the workflow's scrape target."""
        return WorkspaceId("ws-conformance-test-primary")

    @pytest.fixture
    def other_workspace_id(self) -> WorkspaceId:
        """Return the unpopulated workspace used for isolation checks."""
        return WorkspaceId("ws-conformance-test-secondary")

    @pytest.fixture
    def run_id(self) -> RunId:
        """Match the run label configured in the workflow's scrape target."""
        return RunId("run-conformance-test")
