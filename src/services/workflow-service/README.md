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

**Phase A scaffold + FastAPI surface + WorkflowDocument models + SchemaBindings derivation + ExecutionGraph data model + call-site collector + Definition Compiler driver + effective retry-policy resolver + on-error route compiler + locked compile-time error taxonomy + exhaustive kind-grid test suite + Hypothesis-driven determinism property tests + OpenTelemetry observability hooks + developer-facing documentation (≥90 % coverage gate)** —
WF-IMPL-013 ([#347](https://github.com/toddysm/custos/issues/347)),
WF-IMPL-014 ([#348](https://github.com/toddysm/custos/issues/348)),
WF-IMPL-015 ([#349](https://github.com/toddysm/custos/issues/349)),
WF-IMPL-016 ([#350](https://github.com/toddysm/custos/issues/350)),
WF-IMPL-017 ([#351](https://github.com/toddysm/custos/issues/351)),
WF-IMPL-018 ([#352](https://github.com/toddysm/custos/issues/352)),
WF-IMPL-019 ([#353](https://github.com/toddysm/custos/issues/353)),
WF-IMPL-020 ([#354](https://github.com/toddysm/custos/issues/354)),
WF-IMPL-021 ([#355](https://github.com/toddysm/custos/issues/355)),
WF-IMPL-022 ([#356](https://github.com/toddysm/custos/issues/356)),
WF-IMPL-023 ([#357](https://github.com/toddysm/custos/issues/357)),
WF-IMPL-024 ([#358](https://github.com/toddysm/custos/issues/358)),
WF-IMPL-025 ([#359](https://github.com/toddysm/custos/issues/359)),
WF-IMPL-026 ([#360](https://github.com/toddysm/custos/issues/360)),
WF-IMPL-027 ([#361](https://github.com/toddysm/custos/issues/361)), and
WF-IMPL-028 ([#362](https://github.com/toddysm/custos/issues/362)). The
package skeleton, the runnable `create_app()` factory, the
`python -m custos_workflow` entry point, the `/healthz` and `/readyz`
probes, the call-context middleware shim, the Helm subchart, the
CI gate (`.github/workflows/python-services.yml`), the typed
`WorkflowDocument` model + YAML loader, the per-step
`SchemaBindings` derivation (with an `ActivityTypeRegistry`
Protocol), the frozen `ExecutionGraph` dataclasses with a
byte-stable JSON serializer, the topology layer (explicit edges,
data-dependency edges, cycle detection, stable topological sort),
the CEL call-site collector, the Definition Compiler driver
(`compile(document, run_meta, registry) -> ExecutionGraph` —
wiring stages 1–6 of the pipeline), the effective retry-policy
resolver (`resolve_step_retry` / `resolve_arm_retry` — per-match →
step → `spec.defaults` → platform overlay, field-by-field), and
the on-error route compiler (`compile_on_error` — implicit-policy
synthesis, prepended `cancelled` short-circuit, disallowed-kind
rejection), and the locked public compiler error taxonomy
(`custos_workflow.errors` — `CompileError` base + four canonical
subclasses `CompileParseError` / `CompileTypeError` /
`CompileTopologyError` / `CompileRetryPolicyError`, each pinning
a stable `compile.*` `kind` string with JSON-safe `to_dict()`),
and the exhaustive kind-grid test suite
(`tests/test_kind_grid.py` — parametrized matrices that enumerate
every documented `StepKind` / `CallSiteKind` / `EdgeKind` /
`PrimitiveHandler` / `BackoffStrategy` / `JitterStrategy` /
`OnErrorAction` member and every locked `compile.*` `kind`,
guarded by `set(observed) == set(EnumClass)` so adding an enum
member without adding a grid row fails the build), and the
Hypothesis-driven determinism property tests
(`tests/test_determinism_property.py` — four properties locking
the replay-safety contract: (1) byte-equal `to_json(compile())`
across 100 repeats, (2) topological-order stability under
`spec.steps` shuffle, (3) `from_json(to_json(g)) == g` JSON
round-trip, (4) per-step `ResolvedRetryPolicy` stability; PR/push
CI runs with `--hypothesis-seed=0` for reproducibility while the
broader random-seed exploration is run nightly by
`.github/workflows/workflow-service-nightly.yml`), and the
OpenTelemetry observability hooks
(`custos_workflow._telemetry` — single `custos_workflow` tracer +
meter, one duration histogram per pipeline stage
(`custos_workflow_compile_{parse,topology,type_check,retry_policy,total}_duration_ms`,
all labelled by `outcome`), and a per-`kind` error counter
(`custos_workflow_compile_errors_total`) keyed to the WF-IMPL-024
taxonomy plus `compile.bindings_error`; spans
`custos_workflow.compile` (outer) and
`custos_workflow.compile.{parse,topology,type_check,retry_policy}`
carry `step_count` / `edge_count` / `call_site_count`
attributes; instrumentation is no-op when no OTel SDK is
installed because only `opentelemetry-api` is a runtime
dependency — the SDK is dev-only and only the test harness wires
in-memory exporters), and the developer-facing documentation
([`docs/developers/workflow-compilation.md`](../../../docs/developers/workflow-compilation.md)
— pipeline overview with Mermaid sequence, `WorkflowDocument`
input contract, `ExecutionGraph` output contract, full error
taxonomy table (every `compile.*` `kind`), retry-policy resolution
worked example, and three end-to-end YAML examples; backed by
`tests/test_docs_examples.py` which parses every fenced ```yaml```
block in the doc, runs it through `compile()`, and asserts the
resulting graph shape so the doc cannot drift away from the code)
backed by the
`--cov-fail-under=90` floor wired into the package's pytest
defaults (current coverage ≈ 99 %, `_telemetry.py`, `errors.py`,
and `graph/serialize.py` at 100 %)
are real. The WF-IMPL-000-COMPILER milestone is complete; tracker
[#363](https://github.com/toddysm/custos/issues/363) closes with
this task.

**WF-IMPL-000-RUN-CONTROLLER** is now in progress. WF-IMPL-029
([#381](https://github.com/toddysm/custos/issues/381)) lands the
thin `WorkflowRuntime` + `WorkflowClient` async adapters around
`dapr-ext-workflow>=1.17,<2` plus the in-memory
`FakeWorkflowRuntime` + `FakeWorkflowClient` test substitute.
Every subsequent Run Controller task (WF-IMPL-030 +) consumes
only these adapters; no other module imports
`dapr.ext.workflow`. WF-IMPL-046
([#398](https://github.com/toddysm/custos/issues/398)) ships the
Run Controller developer documentation at
[`docs/developers/workflow-run-controller.md`](../../../docs/developers/workflow-run-controller.md)
(lifecycle state machine, `RunController` public API, `StepHandler`
Protocol, Dapr Workflow primitive mapping, replay determinism
contract, `run.*` error taxonomy, worked examples — pinned to the
running code by `tests/test_docs_examples_run_controller.py`).
Tracker: [#399](https://github.com/toddysm/custos/issues/399).

**WF-IMPL-000-STEP-COORDINATOR** is now in progress. WF-IMPL-047
([#418](https://github.com/toddysm/custos/issues/418)) lands the
foundation for the fourth workflow-service sub-module: the
deterministic `(runId, stepId, attempt)` idempotency triple at
`custos_workflow.steps.IdempotencyTriple`, with canonical wire
form `f"{run_id}|{step_id}|{attempt}"` and round-trip
`IdempotencyTriple.from_str()`. The triple becomes the shared
scheduling key for the Activity Runtime Manager
(`ScheduleActivity`), the Connector Service lease key, and the
audit-event correlation key across replays. WF-IMPL-048
([#419](https://github.com/toddysm/custos/issues/419)) lands the
public Step Coordinator error taxonomy at
`custos_workflow.steps`: a frozen `StepCoordinatorError` hierarchy
with the five locked `step.*` `kind` strings pinned on
`LOCKED_STEP_KINDS` — `step.kind_not_implemented`,
`step.with_input_resolution_error`, `step.connector_bind_error`,
`step.activity_schedule_error`, and `step.retry_budget_exhausted` —
mirroring the Run Controller pattern from WF-IMPL-031 and
becoming the closed label set the WF-IMPL-058 OTel counter and
downstream audit consumers will rely on. WF-IMPL-049
([#420](https://github.com/toddysm/custos/issues/420)) lands the
first outbound client boundary at `custos_workflow.clients`: the
runtime-checkable `ActivityRuntimeClient` Protocol with frozen
`ScheduleActivityRequest` / `ActivityResultEnvelope` envelopes
(four-value `ActivityResultClass` Literal pinned on
`ACTIVITY_RESULT_CLASSES`) plus `NoopActivityRuntimeClient`
(refuses every call) and `FakeActivityRuntimeClient` (returns
canned envelopes, records calls + cancellations) test doubles —
the Step Coordinator's `ActivityStepHandler` (WF-IMPL-054) and
retry decision driver (WF-IMPL-053) consume this surface, and
the production Dapr-Workflow adapter plugs in behind the same
Protocol via the deferred *Real ARM Client* sub-module. WF-IMPL-050
([#421](https://github.com/toddysm/custos/issues/421)) extends
`custos_workflow.clients` with the matching outbound boundary to
Connector Service: the runtime-checkable `ConnectorClient`
Protocol with frozen `BindForStepRequest` (carrying a tuple of
`SlotSpec(name, connector_ref, capabilities)`) and
`BindForStepResponse` (whose `contexts` is always a
`MappingProxyType` snapshot of `slot_name → ConnectorContext`)
envelopes, the hashable `ConnectorContext` slot-handle dataclass,
plus `NoopConnectorClient` and `FakeConnectorClient` test
doubles — the Step Coordinator's `ActivityStepHandler`
(WF-IMPL-054) binds every slot through this surface before
calling `ScheduleActivity`, and the production Dapr Service
Invocation adapter plugs in behind the same Protocol via the
deferred *Real Connector Client* sub-module. Tracker:
[#432](https://github.com/toddysm/custos/issues/432).


The Expression Evaluator (the first sub-module) is already in
[`src/libs/custos-cel/`](../../libs/custos-cel) and shipped via
WF-IMPL-001 through WF-IMPL-012 (#176–#187).

## Configuration

Per [`design/components/workflow-service/design.md`](../../../design/components/workflow-service/design.md) § Configuration:

| Variable | Required | Default | Description |
|---|---|---|---|
| `WF_DAPR_WORKFLOW_COMPONENT` | Yes | — | Name of the Dapr Workflow component to bind. |
| `WF_PUBLISH_TOPIC` | No | `custos.workflow.events` | Dapr Pub/Sub topic for lifecycle event publication. |
| `WF_ARM_ENDPOINT` | Yes | — | Activity Runtime Manager service endpoint. |
| `WF_TS_ENDPOINT` | Yes | — | Trigger Service service endpoint. |
| `WF_CONNECTOR_ENDPOINT` | Yes | — | Connector Service endpoint. |
| `WF_CATALOG_ENDPOINT` | Yes | — | Catalog Service endpoint (read-only `WorkflowVersion`). |
| `WF_RUN_HISTORY_RETENTION` | No | `90d` | How long to keep terminal-run metadata before archival. |
| `WF_RESUME_SUB_DEFAULT_TTL` | No | `PT24H` | Default TTL for `RegisterResumeSubscription` when caller does not specify. |
| `WF_REGISTER_SUB_MAX_RETRIES` | No | `5` | Max retries when registering a resume subscription with TS before failing the wait step. |
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
