# custos-postgres

Postgres adapters for the Custos Storage Provider Layer. v1 implements
`DefinitionStoreProvider` and `CatalogStoreProvider` against Postgres 14+.

## Configuration

| Variable | Required | Description |
|---|---|---|
| `CUSTOS_PG_DSN` | Yes | libpq-style DSN, e.g. `postgresql://user:pw@host:5432/custos` |

Adapter factories read `CUSTOS_PG_DSN` once at startup. The platform
never auto-migrates; run `custos-migrate up` to apply pending revisions.

### Optional durable Connector polling

Metadata revision **10** adds workspace/instance-scoped pull controls without
raising the shared revision-4 minimum. The
[pull-control capability](../storage-provider-layer/src/custos_spl/interfaces/pull_control.py)
atomically couples operator pause with audit/outbox writes, and polling due time
with a fresh cursor ownership token. Commit and failure paths retain shared
interval/backoff reservations. Demand comes from current stored starts/waits and
explicit Connector registrations; reconciliation never clears operator pause.

Known legacy instances are migrated paused; absent policy also fails closed.
Drain old pollers, apply migrations, explicitly confirm operator intent, then
enable durable polling on all replicas. See the
[Connector operating contract](../../services/connector-service/README.md#durable-pull-controls-and-demand).

## Entry points

This package registers two adapters under the `custos_spl.adapters`
entry-point group:

- `postgres-definition` — `DefinitionStoreProvider`
- `postgres-catalog` — `CatalogStoreProvider`

See `design/components/storage-provider-layer/design.md` for the
contract surface.

## Tests

```bash
pytest                       # unit tests only
pytest -m integration        # requires docker (testcontainers) or CUSTOS_PG_DSN
```
