# workflow-service

Custos Workflow Service (COMP-003). Owns the orchestration state machine:
Run / Step / StepAttempt lifecycle over Dapr Workflow, the Definition
Compiler that turns a published `WorkflowVersion` into a runtime-ready
`ExecutionGraph`, the Expression Evaluator integration (`custos-cel`),
sub-orchestration management for dynamic loops and approval gates, resume
subscription lifecycle against the Trigger Service, and publication of
workflow lifecycle events to `custos.workflow.events`.

Design: [`design/components/workflow-service/design.md`](../../../design/components/workflow-service/design.md).

## Status

**Implemented** — every workflow-service sub-module is complete and merged; the
service runs end-to-end against durable Postgres state and live downstream
components. The sub-modules and their (closed) trackers:

- **Definition Compiler** ([#363](https://github.com/toddysm/custos/issues/363),
  WF-IMPL-001 … WF-IMPL-028) — `WorkflowDocument` model + YAML loader,
  `SchemaBindings` derivation, frozen `ExecutionGraph` with a byte-stable JSON
  serializer, topology + cycle detection, the CEL call-site collector, the
  six-stage `compile()` driver, the effective retry-policy resolver, the
  on-error route compiler, and the locked `compile.*` error taxonomy. (The
  Expression Evaluator ships separately in
  [`src/libs/custos-cel/`](../../libs/custos-cel), WF-IMPL-001 … WF-IMPL-012.)
- **Run Controller** ([#399](https://github.com/toddysm/custos/issues/399),
  WF-IMPL-029 … WF-IMPL-046) — Run / Step / StepAttempt lifecycle over Dapr
  Workflow (`WorkflowRuntime` / `WorkflowClient` adapters + `FakeWorkflowRuntime`
  test double), deterministic run-id derivation, the orchestrator function,
  lifecycle event publication, and replay reconciliation.
- **Step Coordinator** ([#432](https://github.com/toddysm/custos/issues/432),
  WF-IMPL-047 … WF-IMPL-060) — the `(runId, stepId, attempt)` idempotency triple,
  `with:` input resolution, the activity + let step handlers, the retry driver,
  step lifecycle events, and OTel instrumentation.
- **API Adapter + Validator**
  ([#459](https://github.com/toddysm/custos/issues/459), WF-IMPL-061 …
  WF-IMPL-072) — the REST run routes (`POST /v1/workspaces/{ws}/runs`, get,
  list, cancel), the Internal RPC surface (`StartRun` / `CancelRun` /
  `RaiseExternalEvent`), RFC 7807 problem envelopes, and the pre-execution
  validator (version exists, input schema match, workspace authorization,
  idempotency dedup).
- **Real ARM + Connector adapters**
  ([#495](https://github.com/toddysm/custos/issues/495), WF-IMPL-073 …
  WF-IMPL-083) — production `DaprActivityRuntimeClient` (`ScheduleActivity` /
  `CancelActivity`) and `DaprConnectorClient` (`BindForStep`) over Dapr Service
  Invocation behind the Step Coordinator Protocols.
- **Sub-Orchestration Manager**
  ([#522](https://github.com/toddysm/custos/issues/522), WF-IMPL-084 …
  WF-IMPL-098) — `for:` dynamic loops, `approval:` gates, and `workflow:`
  sub-calls; spawns child Dapr Workflow instances with deterministic ids and
  awaits via `when_all` / `when_any`.
- **Resume Subscription Manager**
  ([#552](https://github.com/toddysm/custos/issues/552), WF-IMPL-099 …
  WF-IMPL-112) — the `waitFor:` step kind, the `TriggerServiceClient` +
  `DaprTriggerServiceClient`, `ResumeSubscriptionMirror` persistence, idempotent
  re-registration on replay, terminal cancellation, and the TTL sweeper.
- **Durable Wiring** ([#623](https://github.com/toddysm/custos/issues/623),
  WF-IMPL-113 … WF-IMPL-119) — the `DaprCatalogClient` (`GetWorkflowVersion`),
  the lifespan-owned `custos_pg` `MetadataStoreProvider` behind `WF_METADATA_STORE`,
  the durable Run store, and the `MetadataStoreProvider`-backed idempotency
  ledger, replacing the in-memory deployed-build stubs.

All sub-modules are backed by unit + integration suites at the package's
`--cov-fail-under=90` floor (current coverage ≈ 99 %). Deferred, non-blocking
follow-ups: full Observability Client integration (audit-event sink wiring +
log-stream delegation for `GET …/steps/{stepId}/logs`) and the cross-component
workflow event taxonomy (TODO-001).

## Configuration

Per [`design/components/workflow-service/design.md`](../../../design/components/workflow-service/design.md) § Configuration:

| Variable | Required | Default | Description |
|---|---|---|---|
| `WF_DAPR_WORKFLOW_COMPONENT` | Yes | — | Name of the Dapr Workflow component to bind. |
| `WF_PUBLISH_TOPIC` | No | `custos.workflow.events` | Dapr Pub/Sub topic for lifecycle event publication. |
| `WF_ARM_ENDPOINT` | Yes | — | Activity Runtime Manager service endpoint. |
| `WF_TS_ENDPOINT` | Yes | — | Trigger Service service endpoint. |
| `WF_CONNECTOR_ENDPOINT` | Yes | — | Connector Service endpoint. |
| `WF_CATALOG_ENDPOINT` | Production | — | Catalog Service Dapr app-id (read-only `WorkflowVersion`). Activates the durable `DaprCatalogClient`; unset keeps the not-configured stub outside `ENVIRONMENT=production`. See [Durable Wiring](../../../docs/developers/workflow-durable-wiring.md). |
| `WF_METADATA_STORE` | Production | — | libpq DSN for the durable `custos_pg` metadata store (backs the Run store + idempotency ledger). Unset keeps the in-memory store outside `ENVIRONMENT=production`. See [Durable Wiring](../../../docs/developers/workflow-durable-wiring.md). |
| `WF_RUN_HISTORY_RETENTION` | No | `90d` | How long to keep terminal-run metadata before archival. |
| `WF_RESUME_SUB_DEFAULT_TTL` | No | `PT24H` | Default TTL for `RegisterResumeSubscription` when caller does not specify. |
| `WF_REGISTER_SUB_MAX_RETRIES` | No | `5` | Max retries when registering a resume subscription with TS before failing the wait step. |
| `WF_RESUME_SUB_SWEEP_INTERVAL` | No | `300` | Seconds between background sweeps that reap TTL-expired resume-subscription mirror rows. |
| `WF_EXPR_TIMEOUT_MS` | No | `100` | Per-expression evaluation timeout. |
| `WF_IDEMPOTENCY_KEY_TTL` | No | `PT24H` | Window for `(workspaceId, StartRun idempotencyKey)` dedup. |

Process bind:

| Variable | Default | Purpose |
|---|---|---|
| `HOST` | `0.0.0.0` | Address the uvicorn process binds to. |
| `PORT` | `8080` | Port the uvicorn process listens on. |
| `WF_REQUIRE_CALL_CONTEXT` | `""` (dev shim) | Set to the exact literal `"1"` to enforce that every non-probe request carries `X-Custos-Workspace` and `X-Custos-Principal` headers (returns `401` `callctx_missing` otherwise). Any other value — including `"true"`, `"yes"`, `"TRUE"` — leaves the dev shim active. |

## Local development

```bash
cd src/services/workflow-service
pip install -e "../../libs/custos-cel[dev]"
pip install -e ".[dev]"
ruff check .
ruff format --check .
mypy src tests
pytest -q
```

`python -m custos_workflow` starts uvicorn against
`custos_workflow:create_app` (`factory=True`), honouring `HOST` /
`PORT`. The lifespan flips `app.state.ready` immediately because Phase A
has no startup dependencies; WF-IMPL-016+ gates the readiness flip on
the Definition Compiler bootstrap and the Catalog client warm-up.

Probe quick-check:

```bash
curl -fsS http://localhost:8080/healthz
curl -fsS http://localhost:8080/readyz
```

## Layout

```
src/services/workflow-service/
├── pyproject.toml
├── README.md
├── src/
│   └── custos_workflow/
│       ├── __init__.py        # re-exports __version__ + create_app
│       ├── __main__.py        # python -m custos_workflow entry point
│       ├── _version.py        # package version
│       ├── app.py             # FastAPI factory + lifespan
│       ├── call_context.py    # CallContext + middleware shim
│       ├── healthz.py         # /healthz + /readyz routes
│       ├── document/          # WorkflowDocument Pydantic models + YAML loader
│       │   ├── __init__.py    # public re-exports
│       │   ├── models.py      # WorkflowDocument + Step union + RetryPolicy …
│       │   └── loader.py      # parse_document() + DocumentParseError
│       ├── bindings/          # per-step SchemaBindings + ActivityTypeRegistry
│       │   ├── __init__.py    # public re-exports
│       │   ├── registry.py    # ActivityTypeRegistry Protocol + in-memory impl
│       │   └── derive.py      # derive_bindings(doc, registry) → {step_id: bindings}
│       ├── graph/             # ExecutionGraph dataclasses + byte-stable JSON + topology
│       │   ├── __init__.py    # public re-exports
│       │   ├── model.py       # frozen dataclasses + enum tags
│       │   ├── serialize.py   # to_json / from_json + GraphSerializationError
│       │   └── topology.py    # explicit/implicit edges + cycle detection + stable sort
│       ├── callsites/         # CEL call-site collector (WF-IMPL-020)
│       │   ├── __init__.py    # public re-exports
│       │   ├── model.py       # CallSite + SourcePosition dataclasses
│       │   ├── placeholders.py # ${{ ... }} segment extractor
│       │   └── collect.py     # collect_call_sites(doc) -> {step_id: [CallSite]}
│       ├── compiler.py        # Definition Compiler driver (WF-IMPL-021)
│       ├── retry/             # Effective retry-policy resolver (WF-IMPL-022)
│       │   ├── __init__.py    # public re-exports
│       │   ├── defaults.py    # PLATFORM_RETRY_DEFAULTS (layer 4)
│       │   └── resolve.py     # resolve_step_retry + resolve_arm_retry overlays
│       ├── on_error/          # On-error route compiler (WF-IMPL-023)
│       │   ├── __init__.py    # public re-exports
│       │   └── compile.py     # compile_on_error — implicit policy + cancelled short-circuit
│       ├── errors.py          # Locked compile-time error taxonomy (WF-IMPL-024)
│       ├── _telemetry.py      # OpenTelemetry instrumentation (WF-IMPL-027)
│       ├── runtime/           # Dapr Workflow runtime + client adapters (WF-IMPL-029)
│       │   ├── __init__.py    # public re-exports
│       │   ├── _common.py     # RunStatus, RunState, request dataclasses
│       │   ├── dapr.py        # WorkflowRuntime + WorkflowClient (real adapter)
│       │   └── fake.py        # FakeWorkflowRuntime + FakeWorkflowClient test substitute
│       └── py.typed
└── tests/
    ├── test_smoke.py             # package import + factory smoke
    ├── test_app.py               # factory shape + lifespan + env flag
    ├── test_call_context.py      # header presence/absence + dev/prod mode
    ├── test_healthz.py           # liveness/readiness status codes
    ├── test_document_models.py   # WorkflowDocument + Step union + retry
    ├── test_document_loader.py   # YAML → WorkflowDocument + error wrapping
    ├── test_bindings_registry.py # InMemoryActivityTypeRegistry contract
    ├── test_bindings_derive.py   # per-step ordering + activity / let / sub-wf
    ├── test_graph_model.py       # frozen invariants + __post_init__ checks
    ├── test_graph_serialize.py   # round-trip + byte-stability + schema guards
    ├── test_graph_topology.py    # explicit/implicit edges + cycles + stable sort
    ├── test_callsites.py         # placeholder scanner + call-site collector
    ├── test_compiler.py          # compile() driver: happy path, errors, stubs
    ├── test_retry_resolver.py    # resolve_step_retry + resolve_arm_retry overlays
    ├── test_on_error_compile.py  # compile_on_error: implicit + rejections
    ├── test_errors.py            # CompileError taxonomy: kinds, to_dict, hash, repr
    ├── test_kind_grid.py         # Parametrized kind grids + exhaustiveness guards (WF-IMPL-025)
    ├── test_determinism_property.py  # Hypothesis property-based determinism tests (WF-IMPL-026)
    ├── test_observability.py     # OTel span / metric instrumentation tests (WF-IMPL-027)
    ├── test_docs_examples.py     # Doc-block round-trip smoke test (WF-IMPL-028)
    └── runtime/                  # Dapr runtime + client adapter tests (WF-IMPL-029)
        ├── test_fake.py          # FakeWorkflowRuntime + FakeWorkflowClient behaviour
        └── test_dapr_adapter_shape.py # Real adapter import-safety + delegation shape
```

The WF-IMPL-000-COMPILER milestone is complete.
