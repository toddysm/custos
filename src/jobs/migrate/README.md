# Custos migration Job (`custos-migrate-job`)

The Helm **pre-install / pre-upgrade** hook image that runs the Storage Provider
Layer's strict, forward-only `migrate up` before any platform component starts.

It is a thin wrapper around the SPL migration CLI
(`custos_spl.migrations.cli`): the migration runner and the strict revision
policy live in SPL, while `custos-postgres` registers the Postgres adapter entry
points (group `custos_spl.adapters`) that the CLI discovers and applies the
pending migrations against. If required schema revisions remain missing after
applying, the process exits non-zero so the Helm release aborts rather than
letting components run against an unmigrated database.

## Database connection

The SPL Postgres adapters read the libpq DSN from `CUSTOS_PG_DSN`. When run as
the Helm hook, the Job inherits the CloudNativePG-generated application secret
via `envFrom` (keys `host` / `port` / `dbname` / `username` / `password` /
`uri`). The entrypoint resolves `CUSTOS_PG_DSN` from the first available of:

1. `CUSTOS_PG_DSN` — explicit override, used verbatim.
2. `DATABASE_URL` / `uri` — a ready-made libpq/postgres connection URL.

## Usage

```bash
# Apply pending migrations (the image default ENTRYPOINT).
custos-migrate-job

# Report whether a migration is needed without applying it.
custos-migrate-job --check

# Restrict to a single SPL adapter.
custos-migrate-job --adapter postgres-metadata
```

> Note: `custos-spl` already ships a `custos-migrate` console script. Because
> both distributions are installed in the same image, this job uses the
> distinct `custos-migrate-job` name to avoid a console-script collision.

## Development

```bash
pip install -e src/libs/storage-provider-layer[dev]
pip install -e src/libs/custos-postgres[dev]
pip install -e src/jobs/migrate[dev]

ruff check . && ruff format --check . && mypy src tests && pytest -q
```
