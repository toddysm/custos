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

**Feature-complete (M1)** — the `ARM-IMPL-000-ACTIVITY-RUNTIME-MANAGER`
milestone ([#591](https://github.com/toddysm/custos/issues/591)) has landed:
the attempt state machine (resolve → limit → materialize → inject → run →
finalize → map → persist), the OCI Container Driver (Job builder +
kind/k8s lifecycle monitor), the Activity Contract and Manifest v1 models, the
Resource Limiter and sandbox/isolation model, the I/O Broker (two-phase
artifact finalization), the Secret Injector, the Result Mapper, the Dapr RPC
adapter (`ScheduleActivity` / `CancelActivity`), cancel + deadline/timeout,
OpenTelemetry spans + metrics, and a kind/k8s integration suite — all under the
ruff + mypy (strict) + pytest (≥90 % coverage) quality-gate toolchain.

Deferred (tracked separately): the `http` / `wasm` runtime drivers (later
milestones).

The ARM↔pod I/O bridge ([#613](https://github.com/toddysm/custos/issues/613))
has landed — see § The ARM↔pod I/O bridge below.

Tracking issue: [#591](https://github.com/toddysm/custos/issues/591)
(ARM-IMPL-000).

Design reference:
[`design/components/activity-runtime-manager/design.md`](../../../design/components/activity-runtime-manager/design.md).
Implementation plan:
[`design/components/activity-runtime-manager/implementation-plan.md`](../../../design/components/activity-runtime-manager/implementation-plan.md).
Activity author guide:
[`docs/developers/activity-author.md`](../../../docs/developers/activity-author.md).

## Layout

```
src/custos_arm/
  __init__.py     # re-exports create_app
  app.py          # FastAPI application factory + readiness lifespan
  __main__.py     # uvicorn entry point (HOST/PORT)
  config.py       # typed Settings over the ARM_* env table
  healthz.py      # /healthz + /readyz routes
  _version.py     # package version string
  middleware/     # call-context middleware + AuthZ dev-shim
  contract/       # Activity Contract v1 envelopes, platform types, errors
  manifest/       # Activity Manifest v1 models + parser + semver
  store/          # ActivityExecution state machine + ArtifactRecord clients
  resolve/        # Activity Resolver (Catalog adapter) + ActivityTypeVersion
  limit/          # Resource Limiter + EffectiveResources + Quantity
  io/             # I/O Broker (two-phase output finalization)
  secrets/        # Secret Injector + sidecar token minter + lease client
  result/         # Result Mapper (exit code + outputs → ActivityResultEnvelope)
  logs/           # Log Streamer + audit sink
  runtime/        # RuntimeDriver Protocol + dispatcher + sandbox/isolation
    oci/          #   OCI Container Driver — Job builder + lifecycle monitor
  scheduler/      # Activity Scheduler (the attempt state machine) + fsio
  rpc/            # Dapr RPC adapter (ScheduleActivity / CancelActivity)
  observe/        # OpenTelemetry spans + metrics
tests/
  test_*.py             # unit suites for every package above
  integration/          # kind/k8s end-to-end (integration-marked)
    test_oci_lifecycle_integration.py
    test_scheduler_integration.py
  test_docs_examples.py # pins this README + the activity-author guide
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
| `ARM_IO_BRIDGE_IMAGE` | no | `busybox:1.37.0@sha256:9532d8c39891ca2ecde4d30d7710e01fb739c87a8b9299685c63704296b16028` | Image for the io-bridge input-injector init container and output-collector native sidecar; override to point at an internal mirror. |
| `ARM_ALLOW_UNPINNED_IMAGES` | no | `False` | Test/dev escape hatch: when `true`, a digest-less activity image renders tag-only with `imagePullPolicy: IfNotPresent` instead of being rejected. Leave `false` in production so every activity runs digest-pinned (content-addressed) bits. |
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

> **Cluster requirement:** the io-bridge output collector uses the Kubernetes
> **native sidecar** pattern (an `initContainers` entry with
> `restartPolicy: Always`), which requires **Kubernetes >= 1.28**. On older
> clusters the activity Pod will be rejected by the API server (or stick in
> `Init`), so deploy ARM only against a 1.28+ control plane.

## The ARM↔pod I/O bridge

ARM never shares a writable volume with the activity container. Instead it
brokers the activity's typed inputs, outputs, and artifacts across the pod
boundary with two short-lived helper containers built from `ARM_IO_BRIDGE_IMAGE`
(both mount the same emptyDir contract volumes the activity sees at `/custos/in`
and `/custos/out`):

- **Input injector** — an `initContainer` that blocks until ARM streams a tar of
  the materialized contract directory into it (`tar -x -C /custos/in`) and drops
  a readiness sentinel. The stream is recursive, so `inputs.json`, `ctx.json`,
  the secret tree, and any downstream-materialized artifacts under
  `/custos/in/artifacts/` all land before the activity starts.
- **Output collector** — a **native sidecar** (`initContainers` entry with
  `restartPolicy: Always`) that idles after the activity exits, ignoring
  `SIGTERM`, until ARM execs `tar -c -C /custos/out` to stream `outputs.json` and
  produced artifacts back, then drops the collected sentinel so the pod can drain.

On the host side the **I/O Broker** runs the two schema-validation boundaries of
an attempt: it validates the materialized `inputs` payload against the activity's
input JSON Schema before start, and after exit runs two-phase output
finalization — parse `outputs.json` (size-capped by `ARM_OUTPUT_MAX_BYTES`),
upload every declared `spec.outputs.artifacts[]` (capped by
`ARM_ARTIFACT_MAX_BYTES`), rewrite each `ArtifactRef` with its store-assigned
`id`/`digest`/`mediaType`/`size`, synthesize `produced[]`, and validate the
finalized `outputs` against the output JSON Schema. A downstream step's input
`ArtifactRef`s are fetched by `id` and materialized onto `/custos/in/artifacts/`
so the consuming activity reads a plain local file.

**Image pinning.** Production activities always run digest-pinned
(content-addressed) bits — a manifest without a runtime digest is rejected. The
`ARM_ALLOW_UNPINNED_IMAGES` escape hatch is a test/dev affordance only (for a
locally `kind load`ed image that has no registry digest to pin against); leave it
`false` in production.

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
