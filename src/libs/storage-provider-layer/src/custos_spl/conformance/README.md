# SPL Adapter Conformance Test Suite

Defines the shared contract that all Storage Provider Layer adapters must satisfy.

## Purpose

This conformance suite ensures consistent, safe behavior across all adapter implementations.

### Currently Enforced (gated by tests)

- **Workspace scoping** — cross-workspace access is blocked with WorkspaceMismatch or returns empty
- **Sweeper-only deletion** — delete() requires is_sweeper=True flag; calling without it raises ValueError
- **Deletion idempotency** — delete() succeeds even if artifact/data already absent (safe for retries)
- **Cursor pagination idempotency** — same cursor yields same results on retry
- **Streaming for query results** — tail_run_logs() returns async generator (not coroutine)
- **Time range validation** — query results respect start/end time bounds
- **Error classification** — invalid metric/label names raise QueryUnsupported
- **Media type consistency** — put() stores effective media_type; head() returns same value
- **Content-addressability** — identical content produces identical digest and artifact_id

### Future Work (planned, not yet gated)

- **Immutability rules** — immutable fields raise ImmutableViolation on update attempts
- **Streaming O(1) memory** — put/get don't buffer entire content in memory (currently documented, not measured)
- **Lease primitive contract** — busy/expired/release state machine for cursor leases
- **Audit outbox semantics** — write and drain protocols for audit stream
- **Idempotency records** — state machine for dedup and device-code sessions
- **Transaction handle misuse** — detection of cross-provider transaction errors
- **Multi-tenant stress tests** — concurrent workspace queries under load
- **Chaos engineering** — backend failure injection and recovery

## Usage

Adapter developers inherit from base test classes and implement adapter fixtures:

### ArtifactStore Adapter Example

```python
from custos_spl.conformance import ArtifactStoreConformanceTests
from my_adapter import MyS3Adapter

class TestMyS3Adapter(ArtifactStoreConformanceTests):
    @pytest.fixture
    def adapter(self):
        return MyS3Adapter(bucket="conformance-test")

    @pytest.fixture
    def workspace_id(self):
        return WorkspaceId("ws-test-primary")

    @pytest.fixture
    def other_workspace_id(self):
        return WorkspaceId("ws-test-secondary")

    @pytest.fixture
    async def sample_content(self):
        return b"test content for conformance"
```

### LogQuery Adapter Example

```python
from custos_spl.conformance import LogQueryConformanceTests
from my_adapter import MyLokiAdapter

class TestMyLokiAdapter(LogQueryConformanceTests):
    @pytest.fixture
    def adapter(self):
        return MyLokiAdapter(base_url="http://loki:3100")

    @pytest.fixture
    def workspace_id(self):
        return WorkspaceId("ws-test")

    @pytest.fixture
    def run_id(self):
        return RunId("run-123")
```

## Implementing Conformance Tests

Complete template for a new adapter's conformance test file (`src/libs/<your-adapter>/tests/test_conformance.py`):

```python
"""Conformance tests for <Your Adapter> <Interface>Provider.

These tests verify that the adapter satisfies the conformance contract.
Requires: custos-spl[conformance] installed, live backend (or moto for S3).

Run with: pytest tests/test_conformance.py -v -m integration
Skip without backend: pytest -v -m "not integration"
"""

from __future__ import annotations

import pytest

# STEP 1: Import the conformance test base class for your adapter type
from custos_spl.conformance import (
    ArtifactStoreConformanceTests,
    LogQueryConformanceTests,
    MetricsQueryConformanceTests,
)
from custos_spl.ids import RunId, WorkspaceId

# STEP 2: Gracefully handle missing optional dependencies
pytest.importorskip("your_adapter_module")

from your_adapter_module.adapters import YourAdapter

# STEP 3: Mark test class as integration and add backend availability check
@pytest.mark.integration
class TestYourAdapterConformance(MetricsQueryConformanceTests):
    """Your adapter conformance tests."""

    @pytest.fixture(scope="class", autouse=True)
    def _check_backend_available(self) -> None:
        """Skip entire test class if backend is not available."""
        try:
            import httpx
            response = httpx.get(
                "http://your-backend:port/health",
                timeout=2.0,
            )
            response.raise_for_status()
        except Exception:
            pytest.skip("Backend not available — skipping integration tests")

    # STEP 4: Implement required fixtures
    @pytest.fixture
    def adapter(self) -> YourAdapter:
        """Return a fully-configured adapter instance."""
        return YourAdapter(base_url="http://your-backend:port")

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
        """Test run ID (for LogQuery and MetricsQuery adapters)."""
        return RunId("run-conformance-test")

    @pytest.fixture
    async def sample_content(self) -> bytes:
        """Sample content for ArtifactStore tests."""
        return b"test content"

    # STEP 5: (Optional) Override base tests if needed
    # All base conformance tests are inherited automatically.
    # Override only if you need special setup/teardown.
```

### Key Implementation Notes

1. **Location**: Place conformance tests in `src/libs/<your-adapter>/tests/test_conformance.py`, NOT in storage-provider-layer/tests
2. **Markers**: Use `@pytest.mark.integration` for all conformance test classes
3. **Dependency handling**: Use `pytest.importorskip("your_adapter_module")` to gracefully skip if adapter package not installed
4. **Backend checks**: Implement `_check_backend_available()` fixture with health check (HTTP GET, connection attempt, filesystem check, etc.)
5. **Required fixtures**: Provide all fixtures required by your conformance base class:
   - **ArtifactStore**: `adapter`, `workspace_id`, `other_workspace_id`, `sample_content`
   - **LogQuery**: `adapter`, `workspace_id`, `other_workspace_id`, `run_id`
   - **MetricsQuery**: `adapter`, `workspace_id`, `other_workspace_id`, `run_id`
6. **Test execution**:
   ```bash
   # Run conformance tests only (requires services available)
   pytest tests/test_conformance.py -v -m integration

   # Skip conformance tests (services not needed)
   pytest tests/ -v -m "not integration"
   ```
7. **CI/CD**: In CI pipelines, services have health checks; tests skip gracefully if services unavailable

## Test Organization

```
conformance/
├── __init__.py              # Exports test classes and fixtures
├── README.md               # This file
├── fixtures.py             # Shared pytest fixtures (Postgres container, URLs)
├── base.py                # AdapterConformanceBase with common patterns
├── artifact_store.py       # ArtifactStoreConformanceTests (9 tests)
├── log_query.py           # LogQueryConformanceTests (5 tests)
├── metrics_query.py       # MetricsQueryConformanceTests (7 tests)
```

## Fixture Requirements

Each conformance test class requires specific fixtures. If a subclass doesn't provide them, tests will skip with a clear message:

### ArtifactStore
- `adapter` — ArtifactStoreProvider instance, configured and ready
- `workspace_id` — primary WorkspaceId for testing
- `other_workspace_id` — secondary WorkspaceId for cross-workspace tests
- `sample_content` — async iterator of bytes (or bytes) for put() testing

### LogQuery
- `adapter` — LogQueryProvider instance, configured and ready
- `workspace_id` — primary WorkspaceId for testing
- `other_workspace_id` — secondary WorkspaceId for cross-workspace tests
- `run_id` — RunId with available logs for testing

### MetricsQuery
- `adapter` — MetricsQueryProvider instance, configured and ready
- `workspace_id` — primary WorkspaceId for testing
- `other_workspace_id` — secondary WorkspaceId for cross-workspace tests
- `run_id` — RunId with available metrics for testing

## What's Tested

### Workspace Scoping (all adapters)
- Cross-workspace reads raise WorkspaceMismatch or return empty
- Workspace ownership is validated
- No data leakage between workspaces

### ArtifactStore (9 tests)
- Sweeper-only deletion: delete() requires is_sweeper=True flag
- Deletion safety: delete() succeeds on missing artifacts (idempotent)
- Cross-workspace access blocked: get() raises WorkspaceMismatch
- Cross-workspace visibility hidden: head() returns None (not error)
- Content-addressability: identical content → identical digest
- Media type consistency: put() and head() agree on media_type

### LogQuery (5 tests)
- Empty results: no-match queries return empty page
- Cursor pagination: same cursor yields same results (idempotent)
- Cross-workspace access blocked: queries blocked or return empty
- Streaming interface: tail_run_logs() returns async generator
- Time bounds: all results within [start, end) window

### MetricsQuery (7 tests)
- Metric name validation: invalid names raise QueryUnsupported
- Label name validation: invalid names raise QueryUnsupported
- Empty results: no-match queries return empty series
- Time bounds: all samples within [start, end) window
- Instant queries: return exactly one sample
- Cross-workspace access blocked: queries blocked or return empty
- Noop adapter: raises QueryUnsupported when metrics disabled

## Test Invocation

Run conformance tests for an adapter:

```bash
# All tests for Prometheus adapter
pytest tests/conformance_prometheus.py -v

# Specific test
pytest tests/conformance_prometheus.py::TestPrometheusMetricsConformance::test_metric_name_validation_rejects_invalid_names -v

# With markers
pytest -m "not integration" tests/conformance_*.py -v
```

## Postgres Container Setup

Tests requiring Postgres use `testcontainers-python` to spin up an isolated container:

```python
@pytest.fixture(scope="session")
def postgres_container() -> Generator[PostgresContainer, None, None]:
    """Lifecycle: one container per test session, auto-cleanup."""
    with PostgresContainer(...) as container:
        yield container
```

## Adding New Conformance Tests

To add a new test that applies to all adapters of a type:

1. Add the test method to the appropriate conformance class (ArtifactStoreConformanceTests, etc.)
2. Use existing fixtures (adapter, workspace_id, etc.) or add new required fixtures
3. Implement the test logic with real assertions (not skip or pass)
4. Update this README to document what is being tested
5. Update the "Currently Enforced" section above

## Roadmap

**Phase 1 (v1.0)** — Core contracts gated ✅
- Workspace scoping
- Sweeper-only deletion
- Pagination idempotency
- Streaming interfaces
- Error classification

**Phase 2 (v1.1)** — Storage provider contracts
- Immutability rules
- Lease primitives (busy/expired/release)
- Transaction handle misuse detection

**Phase 3 (v1.2)** — Audit and observability
- Audit outbox write/drain semantics
- Idempotency record state machine

**Phase 4 (v1.3+)** — Advanced testing
- O(1) memory measurement
- Multi-tenant stress testing
- Chaos engineering (failure injection)
