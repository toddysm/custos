"""Conformance tests for Loki LogQuery adapter.

These tests verify that the Loki adapter satisfies the LogQueryProvider
conformance contract. They require:
- custos-spl[conformance] package installed
- Live Loki instance at http://loki:3100 (or CUSTOS_LOKI_URL)

Run with: pytest tests/test_conformance.py -v -m integration
Skip without Loki: pytest tests/test_conformance.py -v -m "not integration"
"""

from __future__ import annotations

import httpx
import pytest

from custos_spl.conformance import LogQueryConformanceTests
from custos_spl.ids import RunId, WorkspaceId

pytest.importorskip("custos_loki")

from custos_loki.adapters import LokiLogQueryAdapter


@pytest.mark.integration
class TestLokiLogConformance(LogQueryConformanceTests):
    """Loki adapter conformance tests.

    Skipped if Loki is unavailable or custos-loki not installed.
    """

    @pytest.fixture(scope="class", autouse=True)
    def _check_loki_available(self) -> None:
        """Skip entire test class if Loki is not reachable."""
        try:
            response = httpx.get(
                "http://loki:3100/-/ready",
                timeout=2.0,
            )
            response.raise_for_status()
        except (httpx.ConnectError, httpx.TimeoutError, httpx.HTTPError):
            pytest.skip("Loki not available at http://loki:3100")

    @pytest.fixture
    def adapter(self) -> LokiLogQueryAdapter:
        """Provide configured Loki adapter."""
        return LokiLogQueryAdapter(base_url="http://loki:3100")

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
