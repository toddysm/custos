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
