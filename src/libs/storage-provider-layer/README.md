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

Full design: [`design/components/storage-provider-layer/design.md`](../../../design/components/storage-provider-layer/design.md).

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

## Status

Implementation tracked under GitHub label
[`component:storage-provider-layer`](https://github.com/toddysm/custos/labels/component%3Astorage-provider-layer).
The TODO list lives in [`design/components/storage-provider-layer/todos.md`](../../../design/components/storage-provider-layer/todos.md).
