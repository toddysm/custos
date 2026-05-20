# custos-postgres

Postgres adapters for the Custos Storage Provider Layer. v1 implements
`DefinitionStoreProvider` and `CatalogStoreProvider` against Postgres 14+.

## Configuration

| Variable | Required | Description |
|---|---|---|
| `CUSTOS_PG_DSN` | Yes | libpq-style DSN, e.g. `postgresql://user:pw@host:5432/custos` |

Adapter factories read `CUSTOS_PG_DSN` once at startup. The platform
never auto-migrates; run `custos-migrate up` to apply pending revisions.

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
