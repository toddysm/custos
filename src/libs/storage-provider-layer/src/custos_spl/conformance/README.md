# SPL Adapter Conformance Test Suite

Defines the shared contract that all Storage Provider Layer adapters must satisfy.

## Purpose

This conformance suite ensures consistent, safe behavior across all adapter implementations:

- **Workspace scoping enforcement** — cross-workspace access is blocked
- **Immutability rules** — immutable fields cannot be updated
- **Error classification** — transient vs permanent failures are correctly mapped
- **Query validation** — metric/label names are validated to prevent injection
- **Streaming semantics** — put/get preserve O(1) memory contract
- **Pagination** — cursor-based pagination is idempotent
- **Audit/outbox contracts** — correct write and drain semantics

## Usage

Adapter developers inherit from base test classes and implement adapter fixtures:

### ArtifactStore Adapter Example

```python
from custos_spl.conformance import ArtifactStoreConformanceTests
from my_adapter import MyS3Adapter

class TestMyS3Adapter(ArtifactStoreConformanceTests):
    @pytest.fixture
    def adapter(self):
        # Provide your adapter instance
        return MyS3Adapter(bucket="test")

    def test_content_addressability(self):
        # Implement actual test using fixture
        ...
```

### LogQuery Adapter Example

```python
from custos_spl.conformance import LogQueryConformanceTests
from my_adapter import MyLokiAdapter

class TestMyLokiAdapter(LogQueryConformanceTests):
    @pytest.fixture
    def adapter(self):
        return MyLokiAdapter(base_url="http://loki:3100")

    async def test_workspace_scoping_query_run_logs(self):
        # Implement actual test
        ...
```

## Test Organization

```
conformance/
├── __init__.py           # Exports test classes and fixtures
├── fixtures.py           # Shared pytest fixtures (Postgres container, URLs)
├── base.py              # AdapterConformanceBase with common patterns
├── artifact_store.py    # ArtifactStoreConformanceTests
├── log_query.py         # LogQueryConformanceTests
├── metrics_query.py     # MetricsQueryConformanceTests
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

## What's Tested

### Workspace Scoping (all adapters)
- Cross-workspace reads raise `WorkspaceMismatch` → caller maps to 404
- Workspace label is validated on response parsing
- No data leakage even if backend is compromised

### ArtifactStore
- Content-addressability: identical content → identical digest
- Streaming: put/get never buffer entire artifact in memory
- Sweeper-only deletion with `is_sweeper=True` flag
- Media type consistency: stored type matches returned descriptor

### LogQuery
- Cursor pagination is idempotent
- Severity filtering (severity_at_least)
- Time range filtering (start inclusive, end exclusive)
- Message substring matching
- Async generator semantics for `tail_run_logs()`

### MetricsQuery
- Metric name validation: prevents PromQL injection
- Label name validation: prevents matcher injection
- Time bucketing: samples aligned to multiples of step_seconds
- Staleness window: instant queries return recent value if exact time unavailable
- QueryUnsupported for noop adapter

## Error Classification

All adapters must classify errors correctly:

- `BackendUnavailable` — transient; caller retries with backoff
  - Connection refused, timeout, HTTP 503, etc.
- `WorkspaceMismatch` — cross-workspace access; returns as 404
- `QueryUnsupported` — feature not available (noop adapter)
- `ImmutableViolation` — attempted update of immutable field (storage adapters)

## Future Work

This suite is the skeleton; future work includes:

- [ ] Lease primitive tests (busy/expired/release states)
- [ ] Audit outbox write and drain semantics
- [ ] Idempotency record state machine
- [ ] Transaction handle misuse detection
- [ ] Multi-tenant stress tests
- [ ] Chaos engineering (backend failure injection)
