# Custos bootstrap Job (`custos-bootstrap`)

The idempotent **post-install** Helm hook image that seeds a freshly migrated
Custos database into a *usable* platform. It runs after the migration hook
([`src/jobs/migrate`](../migrate/README.md)) and reuses the canonical auth
domain so permissions and roles have a single source of truth.

On every run it:

1. upserts every component's declared permission (the bundled auth-service
   permission registry),
2. seeds the canonical built-in roles,
3. creates the default tenant (`tenant-default`) and workspace
   (`workspace-default`),
4. binds the configured admin principal to the platform-admin role.

Each step is keyed on the entity's identity, so re-running (e.g. on
`helm upgrade`) converges without duplicating rows.

## Configuration

| Variable | Purpose |
| --- | --- |
| `CUSTOS_PG_DSN` | libpq DSN for the Custos Postgres cluster. Falls back to `DATABASE_URL` then `uri` (the CloudNativePG application secret exposes the connection string under `uri`). |
| `CUSTOS_ADMIN_PRINCIPAL_ID` | Principal id granted platform-admin. **Optional** — when unset the seeder still seeds permissions/roles/tenant/workspace and exits `0`. |

## Usage

```bash
# Seed the platform (the image default ENTRYPOINT).
custos-bootstrap

# Override the admin principal id (otherwise read from the env var).
custos-bootstrap --admin-principal-id <principal-id>
```

## Development

```bash
pip install -e src/libs/storage-provider-layer[dev]
pip install -e src/libs/custos-postgres[dev]
pip install -e src/services/auth-service
pip install -e src/jobs/bootstrap[dev]

ruff check . && ruff format --check . && mypy src tests && pytest -q
```
