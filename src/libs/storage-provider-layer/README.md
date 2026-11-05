# `custos-spl` — Storage Provider Layer

The Storage Provider Layer (SPL) defines seven small, stable interfaces and routes all
Custos platform persistence and observability-query access through them. The rest of
Custos has no compile-time or run-time dependency on any concrete backend.

| Interface | Default v1 adapter |
|---|---|
| `DefinitionStoreProvider` | Postgres |
| `CatalogStoreProvider` | Postgres |
| `MetadataStoreProvider` | Postgres |
| `ArtifactStoreProvider` | CSI/PVC (S3 optional) |
| `AuthStoreProvider` | Postgres |
| `LogQueryProvider` | Loki / OpenSearch / noop |
| `MetricsQueryProvider` | Prometheus / noop |

Full design: [`design/components/storage-provider-layer/design.md`](https://github.com/toddysm/custos/blob/main/design/components/storage-provider-layer/design.md).

## Layout

```
src/custos_spl/
├── interfaces/    # Protocols for the seven providers
├── adapters/      # Backend implementations (postgres, csi, loki, prometheus, ...)
├── middleware/    # Workspace-scoping middleware, audit partition enforcer
└── migrations/    # SQL migrations and the migration runner

tests/
└── conformance/   # Shared suite every adapter must pass
```

## Development

This package uses [hatchling](https://hatch.pypa.io/), [ruff](https://docs.astral.sh/ruff/),
[mypy](https://mypy-lang.org/) in strict mode, and [pytest](https://docs.pytest.org/).

```bash
cd src/libs/storage-provider-layer
pip install -e ".[dev,postgres]"

ruff check .
ruff format --check .
mypy src tests
pytest
```

The development dependencies include the PostgreSQL testcontainer fixture used
by the shared conformance helpers. Entering `PostgresContainer` waits for
readiness; no additional connection-client method is required.

## Adapter conformance in continuous integration

The [library workflow](../../../.github/workflows/python-libs.yml) runs each
adapter's suite in a separate pytest process from that package's directory.
Several adapters use the same `tests.test_conformance` module name; collecting
them together can reuse the wrong module even with importlib import mode.

PostgreSQL, Loki and Prometheus use the workflow's service containers. Readiness
is checked from the runner rather than assuming `curl` exists inside an image.
The [Prometheus test configuration](../../../.github/prometheus.yml) adds the
workspace/run labels required by the metric queries. Explicitly configured
unavailable backends fail instead of silently skipping.

CSI filesystem behavior is checked against an isolated temporary mount, not a
shared cluster volume. S3 uses a loopback Moto server so the real asynchronous
client receives HTTP responses rather than incompatible synchronous mock
responses. This does not certify a Kubernetes CSI driver or a live cloud account.
Artifact media types remain optional; conformance checks preserve explicit types
and require the returned descriptor to agree with stored metadata.

## Durable runtime snapshots

[WorkflowStart](src/custos_spl/interfaces/delivery.py) accepts legacy runtime
protocol 2 without an execution identity, or protocol 3 with a closed,
workspace-matching reference. Protocol 3 fingerprints include that reference;
credentials, extra identity fields and identity-bearing protocol-2 snapshots
are rejected. This is separate from the unchanged delivery HTTP protocol 2.
Providers retain the original snapshot for acknowledgement-loss recovery and
restart rather than choosing a new execution account.

## Receiver scan capabilities

[ReceiverWorkspaceProvider](src/custos_spl/interfaces/receiver_workspaces.py)
discovers bounded keyset pages of workspace IDs retaining intent or recovery
state, independent of the Auth directory. It does not authorize effects.
[ReceiverScanProgressProvider](src/custos_spl/interfaces/receiver_progress.py)
persists versioned opaque continuations with fenced discovery/operation claims.
Only acknowledged pages advance; failures retain prior progress. Scheduled
claims require live scheduler authority; all existing effect fences still apply.

[MemoryReceiverScanProgress](src/custos_spl/receiver_memory.py) supplies atomic
single-loop reference behavior and shareable state for replica tests, not
process-restart durability. The
[PostgreSQL implementation](../custos-postgres/README.md#optional-durable-receiver-scan-progress)
requires additive metadata revision 16. Test 51, 200 and 1,000 continuing scopes,
deep target positions, stale owners and restart using the actual provider APIs.

## Status

Implementation tracked under GitHub label
[`component:storage-provider-layer`](https://github.com/toddysm/custos/labels/component%3Astorage-provider-layer).
The TODO list lives in [`design/components/storage-provider-layer/todos.md`](https://github.com/toddysm/custos/blob/main/design/components/storage-provider-layer/todos.md).
