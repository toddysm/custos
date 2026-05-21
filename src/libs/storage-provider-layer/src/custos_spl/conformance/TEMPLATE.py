"""Conformance test template for adapter implementations.

This file shows how to implement conformance tests for a new adapter.
Use it as a template for your adapter's conformance test file.

Location: src/libs/<your-adapter>/tests/test_conformance.py

Key points:
1. Place conformance tests in the adapter's own tests/ directory
2. Mark tests with @pytest.mark.integration (they require live backends)
3. Use pytest.importorskip() to handle missing optional dependencies
4. Add health checks to gracefully skip when backends are unavailable
5. Provide required fixtures (adapter, workspace_id, run_id, etc.)
"""

from __future__ import annotations

from datetime import datetime, timedelta

import pytest

# ============================================================================
# STEP 1: Import the conformance test base class for your adapter type
# ============================================================================

from custos_spl.conformance import MetricsQueryConformanceTests
# OR use LogQueryConformanceTests or ArtifactStoreConformanceTests

from custos_spl.ids import RunId, WorkspaceId
from custos_spl.interfaces.metrics_query import MetricRange, MetricSelector

# ============================================================================
# STEP 2: Gracefully handle missing optional dependencies
# ============================================================================

# This ensures the test file can be collected without breaking if the adapter
# package is not installed
pytest.importorskip("custos_prometheus")

from custos_prometheus.adapters import PrometheusMetricsAdapter

# ============================================================================
# STEP 3: Mark test class as integration and add backend availability check
# ============================================================================


@pytest.mark.integration
class TestYourAdapterConformance(MetricsQueryConformanceTests):
    """Your adapter conformance tests.

    These tests verify that your adapter satisfies the MetricsQueryProvider
    (or LogQueryProvider/ArtifactStoreProvider) conformance contract.

    Requirements:
    - Live backend instance (Prometheus, Loki, S3, etc.)
    - Proper environment configuration (URLs, credentials)
    - custos-spl with conformance support installed

    Run with: pytest tests/test_conformance.py -v -m integration
    Skip without backend: pytest -v -m "not integration"
    """

    @pytest.fixture(scope="class", autouse=True)
    def _check_backend_available(self) -> None:
        """Skip entire test class if backend is not available.

        Implement a simple health check appropriate for your backend.
        Examples:
        - HTTP GET to health endpoint
        - Connection attempt to database
        - S3 bucket existence check
        """
        # Replace with your backend health check
        try:
            import httpx

            response = httpx.get(
                "http://prometheus:9090/-/healthy",
                timeout=2.0,
            )
            response.raise_for_status()
        except Exception:
            pytest.skip("Backend not available — skipping integration tests")

    # ========================================================================
    # STEP 4: Implement required fixtures
    # ========================================================================

    @pytest.fixture
    def adapter(self) -> PrometheusMetricsAdapter:
        """Return a fully-configured adapter instance.

        Required by all conformance test base classes.
        """
        return PrometheusMetricsAdapter(base_url="http://prometheus:9090")

    @pytest.fixture
    def workspace_id(self) -> WorkspaceId:
        """Return a test workspace ID for primary queries.

        Required by all conformance test base classes.
        """
        return WorkspaceId("ws-conformance-test-primary")

    @pytest.fixture
    def other_workspace_id(self) -> WorkspaceId:
        """Return a different workspace ID for cross-workspace tests.

        Required by all conformance test base classes.
        """
        return WorkspaceId("ws-conformance-test-secondary")

    @pytest.fixture
    def run_id(self) -> RunId:
        """Return a test run ID (if your adapter supports run scoping).

        Required by LogQueryProvider and MetricsQueryProvider conformance tests.
        May be a real run ID if your backend has test data, or a synthetic ID
        if you're mocking responses.
        """
        return RunId("run-conformance-test")

    @pytest.fixture
    async def sample_content(self) -> bytes:
        """Return sample content for artifact store tests.

        Required only by ArtifactStoreConformanceTests.
        Used by put() tests.
        """
        return b"sample content for conformance testing"

    # ========================================================================
    # STEP 5: (Optional) Override base tests if you need adapter-specific setup
    # ========================================================================

    # All base conformance tests are inherited and will run automatically.
    # Override a test only if:
    # 1. You need special setup/teardown for your adapter
    # 2. You need to skip a test that doesn't apply to your adapter
    # 3. You need to provide additional assertions specific to your adapter

    # Example: Override if your adapter needs special configuration
    # @pytest.mark.asyncio
    # async def test_metric_name_validation_rejects_invalid_names(self, ...):
    #     """Add adapter-specific validation logic if needed."""
    #     await super().test_metric_name_validation_rejects_invalid_names(...)
    #     # Add additional assertions here


# ============================================================================
# NOTES FOR ADAPTER AUTHORS
# ============================================================================

"""
1. LOCATION: Put this file in src/libs/<your-adapter>/tests/test_conformance.py
   NOT in src/libs/storage-provider-layer/tests/

2. DEPENDENCIES: Mark with @pytest.mark.integration and use pytest.importorskip()
   to gracefully handle missing dependencies or backends.

3. FIXTURES: You MUST provide all required fixtures. The base class will skip
   tests if fixtures are missing, but it's cleaner to provide them upfront.

4. BACKEND AVAILABILITY: Add a health check fixture to skip gracefully if the
   backend is not available. This prevents CI failures when Prometheus/Loki/etc.
   are not running.

5. TEST DATA: If you don't have real backend data, you can mock responses in
   your adapter fixture or override specific tests.

6. RUNNING TESTS:

   Run conformance tests only:
     pytest tests/test_conformance.py -v -m integration

   Run all tests except conformance:
     pytest tests/ -v -m "not integration"

   Run specific conformance test:
     pytest tests/test_conformance.py::TestYourAdapterConformance::test_metric_name_validation_rejects_invalid_names

7. CI/CD: In CI pipelines, either:
   - Skip integration tests if backends are not available
   - Provide containerized backends (testcontainers-python, Docker Compose, etc.)
   - Mark as optional and allow failures

8. DEBUGGING: If a conformance test fails:
   - Check that required fixtures are properly configured
   - Verify backend is reachable and healthy
   - Check adapter logs for error details
   - Run the test in isolation to debug: pytest -k test_name -vvs
"""
