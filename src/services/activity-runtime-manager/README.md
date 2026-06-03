# custos-activity-runtime-manager

Custos Activity Runtime Manager (COMP-006).

The Activity Runtime Manager (ARM) is the execution plane for workflow
activities. It schedules activities requested by the Workflow Service over
Dapr, resolves and verifies activity manifests, isolates each run in an OCI
container under an operator-configured sandbox tier, brokers the activity's
typed inputs/outputs and artifacts through the Storage Provider Layer, injects
short-lived secret leases from the Connector Service, and maps the container's
outcome back to the locked `ActivityResultEnvelope` contract the Workflow
Service consumes.

## Status

**In progress** — the `ARM-IMPL-000-ACTIVITY-RUNTIME-MANAGER` milestone
([#591](https://github.com/toddysm/custos/issues/591)) is under active
implementation. This package currently provides the ARM-IMPL-001 scaffold:
the FastAPI application factory and the `/healthz` / `/readyz` probes, wired to
the ruff + mypy (strict) + pytest (≥90 % coverage) quality-gate toolchain.

Tracking issue: [#591](https://github.com/toddysm/custos/issues/591)
(ARM-IMPL-000).

Design reference:
[`design/components/activity-runtime-manager/design.md`](../../../design/components/activity-runtime-manager/design.md).
Implementation plan:
[`design/components/activity-runtime-manager/implementation-plan.md`](../../../design/components/activity-runtime-manager/implementation-plan.md).

## Layout

```
src/custos_arm/
  __init__.py     # re-exports create_app
  app.py          # FastAPI application factory + readiness lifespan
  __main__.py     # uvicorn entry point (HOST/PORT)
  config.py       # typed Settings over the ARM_* env table
  healthz.py      # /healthz + /readyz routes
  middleware/     # call-context middleware + AuthZ dev-shim
  _version.py     # package version string
tests/
  test_healthz.py # probe behaviour
  test_smoke.py   # import + factory smoke
  test_config.py  # Settings loader + ISO-8601 durations
  test_callctx.py # call-context dev-shim middleware
```

## Configuration

`Settings` ([`config.py`](src/custos_arm/config.py)) is loaded from the
environment at application startup; a missing required variable fails fast with
a clear message. ISO-8601 durations (`ARM_MAX_TIMEOUT`, `ARM_IDEMPOTENCY_TTL`)
are parsed and validated to positive `timedelta` values.

| Variable | Required | Default | Purpose |
| -------- | -------- | ------- | ------- |
| `ARM_ARTIFACT_STORE` | yes | — | `ArtifactStoreProvider` binding. |
| `ARM_METADATA_STORE` | yes | — | `MetadataStoreProvider` binding. |
| `ARM_CATALOG_ENDPOINT` | yes | — | Catalog Service endpoint for `activityRef` resolution. |
| `ARM_CONNECTOR_ENDPOINT` | yes | — | Connector Service endpoint for `RefreshLease`. |
| `ARM_AUTHZ_ENDPOINT` | prod | (empty) | Empty enables the dev-shim that trusts `x-custos-callctx`, warns per request, and refuses to start when `ENVIRONMENT=production`. |
| `ARM_SANDBOX_NAMESPACE` | yes | — | Kubernetes namespace for activity `Job`s. |
| `ARM_SIDECAR_IMAGE` | yes | — | Connector sidecar image injected into every activity Pod. |
| `ARM_DEFAULT_TIER` | no | `process` | Cluster-default isolation tier (`process`/`vm`/`microvm`). |
| `ARM_RUNTIME_CLASS_PROCESS` | no | (empty) | `RuntimeClass` for the `process` tier. |
| `ARM_RUNTIME_CLASS_VM` | no | (empty) | `RuntimeClass` for the `vm` tier (empty = unavailable). |
| `ARM_RUNTIME_CLASS_MICROVM` | no | (empty) | `RuntimeClass` for the `microvm` tier (empty = unavailable). |
| `ARM_DEFAULT_CPU_REQUEST` / `ARM_DEFAULT_CPU_LIMIT` | no | `250m` / `1` | Platform-default CPU. |
| `ARM_DEFAULT_MEMORY_REQUEST` / `ARM_DEFAULT_MEMORY_LIMIT` | no | `256Mi` / `1Gi` | Platform-default memory. |
| `ARM_DEFAULT_EPHEMERAL_STORAGE_LIMIT` | no | `2Gi` | Platform-default ephemeral storage. |
| `ARM_MAX_TIMEOUT` | no | `PT1H` | ISO-8601 ceiling clamping the step timeout. |
| `ARM_OUTPUT_MAX_BYTES` | no | `1048576` | Max `outputs.json` size. |
| `ARM_ARTIFACT_MAX_BYTES` | no | `5368709120` | Per-artifact upload ceiling. |
| `ARM_IDEMPOTENCY_TTL` | no | `PT24H` | ISO-8601 retention of terminal execution records. |
| `ENVIRONMENT` | no | `development` | Dev-shim refuses to start when `production`. |
| `HOST` | no | `0.0.0.0` | uvicorn bind host. |
| `PORT` | no | `8080` | uvicorn bind port. |

## Development

Install the path dependencies and this package, then run the quality gates from
this directory:

```bash
pip install -e ../../libs/custos-callctx[dev]
pip install -e ../../libs/storage-provider-layer[dev]
pip install -e ".[dev]"

ruff format . && ruff check . && mypy src tests && pytest -q
```

Run the service locally:

```bash
python -m custos_arm        # or: custos-activity-runtime-manager
```
