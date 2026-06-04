# Workflow Service — Durable Wiring & Catalog Configuration

Last Updated: 2026-06-05

> Audience: operators and contributors deploying or running the
> workflow-service. This guide explains how the service switches between
> the in-memory dev/test adapters and the durable production adapters,
> the configuration that drives that switch, and the fail-fast
> behaviour that prevents a production worker from silently losing
> state.

The workflow-service factory (`custos_workflow.create_app`) is
import-safe and builds **in-memory adapters everywhere by default** so
the sidecar-free dev/test path stays composable. Production deployments
opt into durable adapters by setting a small number of environment
variables that the lifespan reads at startup. The same env surface also
controls one safety rail: when `ENVIRONMENT=production`, the worker
**refuses to start** unless the durable upstreams are configured.

## Configuration surface

| Variable | Required | Default | Purpose |
|---|---|---|---|
| `WF_CATALOG_ENDPOINT` | Production | — | Catalog Service Dapr app-id. Activates the durable `DaprCatalogClient` that `StartRun` uses to fetch and compile workflow versions. |
| `WF_METADATA_STORE` | Production | — | libpq-style DSN (e.g. `postgresql://user:pw@host:5432/custos`) for the durable `custos_pg` `MetadataStoreProvider`. Backs the Run store and the idempotency ledger. |
| `WF_IDEMPOTENCY_KEY_TTL` | No | `PT24H` | ISO-8601 duration for the `(workspaceId, StartRun idempotencyKey)` dedup window. |
| `ENVIRONMENT` | No | — | Deployment label. When it equals `production` (case-insensitive), a missing `WF_CATALOG_ENDPOINT` or `WF_METADATA_STORE` is a fatal misconfiguration. |

> The full service env surface (Dapr workflow component, ARM / Trigger /
> Connector endpoints, pub/sub, timeouts) lives in the
> [service README § Configuration](../../src/services/workflow-service/README.md#configuration).

## The durable-vs-in-memory switch

Two independent adapters flip between an in-memory stub and a durable
backend. Each switch is driven by its own env var, and each one
escalates a missing value to a startup failure **only** when
`ENVIRONMENT=production`.

### Catalog client (`WF_CATALOG_ENDPOINT`)

| `WF_CATALOG_ENDPOINT` | `ENVIRONMENT` | Result |
|---|---|---|
| set | any | `DaprCatalogClient` over the shared `httpx.AsyncClient` (durable). |
| unset | not `production` | `_NotConfiguredCatalogClient` stub — the dev/test path. |
| unset | `production` | **Startup fails** with `RuntimeError`. |

The stub is safe in dev because the existing-run dedup branch of
`RunController.start_run` never touches the Catalog client; only a
genuinely new `StartRun` needs to fetch a workflow version.

### Metadata store (`WF_METADATA_STORE`)

| `WF_METADATA_STORE` | `ENVIRONMENT` | Result |
|---|---|---|
| set | any | One pooled `custos_pg.PgMetadataAdapter` shared by the Run store and the idempotency ledger (durable). The pool is opened **eagerly** so a bad DSN or unreachable database surfaces at startup and keeps `/readyz` at `503`. |
| unset | not `production` | In-process `_InProcessMetadataStoreProvider` — runs and idempotency records live only in the worker's memory and are lost on restart. |
| unset | `production` | **Startup fails** with `RuntimeError`. |

When the DSN is set, the same provider backs both the durable Run store
(so runs survive a restart / HA failover) and the durable idempotency
ledger (so `StartRun` dedup survives a restart). This end-to-end
durability is proven by the Postgres-backed integration suite at
[`tests/integration/test_durable_start_run_postgres.py`](../../src/services/workflow-service/tests/integration/test_durable_start_run_postgres.py).

### Idempotency TTL (`WF_IDEMPOTENCY_KEY_TTL`)

The durable ledger reaps expired `(workspaceId, idempotencyKey)`
reservations on a background sweep. The TTL window defaults to `PT24H`
and is overridden with any positive ISO-8601 duration — for example
`PT1H` (one hour) or `P7D` (seven days). Months and years are rejected
because they describe a calendar-dependent window the ledger contract
disallows. An invalid value fails fast at startup.

## Production-refusal semantics

The guard exists so a production worker never silently degrades to
non-durable behaviour:

- A worker started with `ENVIRONMENT=production` but **no**
  `WF_CATALOG_ENDPOINT` would otherwise serve `StartRun` off the
  not-configured stub and reject every new run.
- A worker started with `ENVIRONMENT=production` but **no**
  `WF_METADATA_STORE` would otherwise persist runs and idempotency
  records to process-local memory and lose them on restart.

In both cases the lifespan raises a `RuntimeError` with a message that
names the missing variable and the expected value, so the failure is
loud and actionable rather than a silent data-loss footgun. In any
non-production environment the same missing variables keep the in-memory
fallbacks so the dev / test path stays sidecar-free.

## Example: a durable production worker

```bash
export ENVIRONMENT=production
export WF_DAPR_WORKFLOW_COMPONENT=custos-workflow
export WF_CATALOG_ENDPOINT=catalog-service
export WF_METADATA_STORE='postgresql://custos:secret@postgres:5432/custos'
export WF_IDEMPOTENCY_KEY_TTL=PT24H   # optional; this is the default
python -m custos_workflow
```

Omitting `WF_CATALOG_ENDPOINT` or `WF_METADATA_STORE` from the block
above (with `ENVIRONMENT=production` still set) makes the worker refuse
to start.

## Example: the dev / test path

```bash
# No ENVIRONMENT, no Catalog endpoint, no DSN — all in-memory.
export WF_DAPR_WORKFLOW_COMPONENT=custos-workflow
python -m custos_workflow
```

This is the composable, sidecar-free path the unit suite exercises. Runs
and idempotency records live in process memory and are intentionally
discarded on restart.
