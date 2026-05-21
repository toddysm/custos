"""Conformance tests for Loki LogQuery adapter.

These tests verify that the Loki adapter satisfies the LogQueryProvider
conformance contract. They require:
- custos-spl[conformance] package installed
- Live Loki instance (via CUSTOS_LOKI_URL, defaults to http://localhost:3100)

Run with: pytest tests/test_conformance.py -v -m integration
Skip without Loki: pytest tests/test_conformance.py -v -m "not integration"
"""

from __future__ import annotations

import os

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
        base_url = os.environ.get("CUSTOS_LOKI_URL", "http://localhost:3100")
        health_url = f"{base_url}/-/ready"
        try:
            response = httpx.get(health_url, timeout=2.0)
            response.raise_for_status()
        except (httpx.ConnectError, httpx.TimeoutError, httpx.HTTPError):
            pytest.skip(f"Loki not available at {health_url}")

    @pytest.fixture
    def adapter(self) -> LokiLogQueryAdapter:
        """Provide configured Loki adapter."""
        base_url = os.environ.get("CUSTOS_LOKI_URL", "http://localhost:3100")
        return LokiLogQueryAdapter(base_url=base_url)

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
