# TODOs: Workflow Service

Last Updated: 2026-06-04 (WF-IMPL-072 merged via PR #482; API Adapter + Validator sub-module complete; tracker #459 closed)

## Open

- [ ] TODO-001: Finalize canonical workflow event taxonomy (`workflow.*`, `run.*`, `step.*`) jointly with Trigger Service TS-TODO-001 (#18) and ARM TODO-009 (INCON-013 cross-link). Tracked under those existing issues; no separate WF issue. (added 2026-05-17)

## Deferred sub-modules

Sub-modules of the workflow-service host whose design is already locked in [`design.md`](design.md) § Internal Structure but whose implementation plan has not yet been derived. Each will get its own `implement-component` plan, tracker, and task issue set when prioritised. (added 2026-05-29 during the Step Coordinator plan derivation.)

- [ ] **Resume Subscription Manager** — `waitFor:` step kind + `TriggerServiceClient` Protocol + `RegisterResumeSubscription` / `CancelResumeSubscription` RPC + `ResumeSubscriptionMirror` persistence (`MetadataStoreProvider`) + replay re-registration through the WF-IMPL-042 reconciler hook. Design refs: § Operation: Step Resume on External Event, § Resume Subscription Replay Protocol. Step Coordinator (WF-IMPL-055) currently returns `StepFailed(step.kind_not_implemented)` for `waitFor:`.
- [ ] **Sub-Orchestration Manager** — `for:` (dynamic loop) + `approval:` (gate + timeout) + `workflow:` (sub-workflow call); spawns child Dapr Workflow instances with deterministic `<parentRunId>/<stepId>/<iterationKey>` ids; awaits via `when_all` / `when_any`; merges outputs. Design refs: § Operation: Sub-Orchestration, § Sub-Orchestration Manager (ADR-007). Step Coordinator currently returns `StepFailed(step.kind_not_implemented)` for all three.
- [x] **API Adapter + Validator** — _complete_ as the fifth sub-module under tracker [#459](https://github.com/toddysm/custos/issues/459) (closed 2026-06-04); tasks WF-IMPL-061..072 merged across PRs #463–#480 and #482. See the dedicated section below + [`implementation-plan.md`](implementation-plan.md).
- [ ] **Real ARM Client + Connector Client adapters** — production `ActivityRuntimeClient` / `ConnectorClient` Dapr Service Invocation bridges behind the Protocols that ship with the Step Coordinator (WF-IMPL-049 / WF-IMPL-050). Design refs: § Internal RPC (outbound).
- [ ] **Full Observability Client integration** — Audit-event sink wiring + cross-component event taxonomy unification (TS-TODO-001 / ARM TODO-009 under INCON-013) + log-stream delegation for `GET …/steps/{stepId}/logs`. Design refs: § Observability Client. `workflow.*` / `step.*` event publication already lands via the existing `LifecycleEventPublisher`.
- [ ] **Durable `IdempotencyLedger`** — `MetadataStoreProvider`-backed adapter for the `(workspaceId, idempotencyKey)` ledger introduced in WF-IMPL-063; in-memory adapter ships with the API Adapter sub-module. Filed as a separate follow-up issue once the in-memory adapter merges. (added 2026-05-31 during the API Adapter plan derivation.)

## Implementation — API Adapter + Validator

Fifth sub-module: **API Adapter + Validator**, packaged inside the service host `src/services/workflow-service/` under the new `custos_workflow.api` + `custos_workflow.validator` packages. Owns the inbound REST and Internal RPC surface, plus the pre-execution checks that gate every `StartRun` (workflow-version existence, inputs schema match, workspace authorization, `(workspaceId, idempotencyKey)` dedup). After this sub-module lands, the workflow-service stops being reachable only in-process. Plan: [`implementation-plan.md`](implementation-plan.md).

### Phase A — Foundations (errors, models, validator)

- [x] WF-IMPL-061 (#447): Public API error taxonomy + RFC 7807 problem envelope. Merged: PR #460.
- [x] WF-IMPL-062 (#448): API wire Pydantic models. Merged: PR #462.
- [x] WF-IMPL-063 (#449): Validator package + Idempotency-Key ledger. Merged: PR #464.

### Phase B — Public REST surface

- [x] WF-IMPL-064 (#450): FastAPI dependency factories (depends on #449). Merged: PR #466.
- [x] WF-IMPL-065 (#451): REST routes — runs (depends on #450). Merged: PR #468.
- [x] WF-IMPL-066 (#452): REST routes — steps + log-stream stub (depends on #450). Merged: PR #470.

### Phase C — Internal RPC inbound surface

- [x] WF-IMPL-067 (#453): Internal RPC routes — `StartRun` / `CancelRun` (depends on #451). Merged: PR #472.
- [x] WF-IMPL-068 (#454): `RaiseExternalEvent` bridge (depends on #453). Merged: PR #474.

### Phase D — App wiring + observability

- [x] WF-IMPL-069 (#455): Mount routers + exception handlers in `create_app` (depends on #451, #452, #453, #454). Merged: PR #476.
- [x] WF-IMPL-070 (#456): OTel HTTP-server observability (depends on #455). Merged: PR #478.

### Phase E — Verification + documentation

- [x] WF-IMPL-071 (#457): Unit + integration test suite (≥ 90 % coverage gate) (depends on #456). Merged: PR #480.
- [x] WF-IMPL-072 (#458): Developer documentation — `docs/developers/workflow-api.md` (depends on #457). Merged: PR #482.

Tracker: #459 — `WF-IMPL-000-API-ADAPTER`.

## Implementation — Step Coordinator

Fourth sub-module: **Step Coordinator**, packaged inside the service host `src/services/workflow-service/` under the new `custos_workflow.steps` + `custos_workflow.clients` packages. Drives execution of one step at a time within a Run: evaluates `with:` inputs through `custos_cel`, derives the per-attempt `(runId, stepId, attempt)` idempotency triple, dispatches to the Activity Runtime Manager via a typed client boundary, applies the workflow-level retry policy on retryable failures, and emits `step.*` lifecycle events. Concrete `StepHandler` for the Protocol the Run Controller orchestrator (WF-IMPL-035) already publishes. Plan: [`implementation-plan.md`](implementation-plan.md).

### Phase A — Foundations (IDs, errors)

- [x] WF-IMPL-047 (#418): Idempotency Tracker — deterministic `(runId, stepId, attempt)` triples.
- [x] WF-IMPL-048 (#419): Public Step Coordinator error taxonomy — `StepCoordinatorError` + 5 subclasses, locked `kind` strings.

### Phase B — Outbound client boundaries

- [x] WF-IMPL-049 (#420): `ActivityRuntimeClient` Protocol + `ActivityResultEnvelope` (+ fake test client).
- [x] WF-IMPL-050 (#421): `ConnectorClient` Protocol + `ConnectorContext` (+ fake test client).

### Phase C — Step Coordinator core

- [x] WF-IMPL-051 (#422): `WithInputResolver` — evaluate `with:` CEL expressions.
- [x] WF-IMPL-052 (#423): `LetStepHandler` — inline expression evaluation (depends on #422).
- [x] WF-IMPL-053 (#424): Retry decision driver — `on_error` route walk + effective delay (depends on #419).
- [x] WF-IMPL-054 (#425): `ActivityStepHandler` — full activity step lifecycle (depends on #418, #420, #421, #422, #424).

### Phase D — Coordinator integration

- [x] WF-IMPL-055 (#426): `StepCoordinator` — concrete `StepHandler` dispatcher (depends on #419, #423, #425).
- [x] WF-IMPL-056 (#427): `step.*` lifecycle event emission (depends on #426).
- [x] WF-IMPL-057 (#428): FastAPI lifespan worker wiring (depends on #426).

### Phase E — Observability, verification, docs

- [x] WF-IMPL-058 (#429): OTel observability hooks for the Step Coordinator (depends on #427, #428).
- [x] WF-IMPL-059 (#430): Unit + integration test suite (≥ 90 % coverage gate) (depends on #429).
- [F] WF-IMPL-060 (#431): Developer documentation — `docs/developers/workflow-step-coordinator.md` (depends on #430).

Tracker: #432 — `WF-IMPL-000-STEP-COORDINATOR`.

## Implementation — Definition Compiler

Second sub-module under construction: **Definition Compiler**, packaged inside the service host `src/services/workflow-service/` (Python package `custos_workflow`). Reads a `WorkflowVersion` and produces a runtime-ready `ExecutionGraph` with cached typed ASTs per design.md § Internal Structure + § `let` Primitive. Tracked under #363 (WF-IMPL-000-COMPILER).

### Phase A — Service host scaffolding

- [ ] WF-IMPL-013: Scaffold `custos-workflow-service` service package + CI gate (issue #347).
- [x] WF-IMPL-014: Wire workflow-service Helm subchart — env vars, ConfigMap, ExternalSecret (issue #348; depends on #347).
- [x] WF-IMPL-015: FastAPI app skeleton + `healthz` / `readyz` + call-context middleware shim (issue #349; depends on #347, #348).

### Phase B — Compiler input contract

- [x] WF-IMPL-016: `WorkflowDocument` Pydantic models (issue #350; depends on #347).
- [x] WF-IMPL-017: Per-step `SchemaBindings` derivation + `ActivityTypeRegistry` interface (issue #351; depends on #350).

### Phase C — Compiler core

- [x] WF-IMPL-018: `ExecutionGraph` data model + byte-stable JSON serializer (issue #352; depends on #347).
- [x] WF-IMPL-019: Topology builder — explicit + implicit edges, cycle detection, stable sort (issue #353; depends on #350, #352, #354).
- [x] WF-IMPL-020: Call-site collector — every CEL call site with source position + parsed AST (issue #354; depends on #350).
- [x] WF-IMPL-021: Compiler driver — parse → type-check → topology → typed-AST caching (issue #355; depends on #351, #352, #353, #354, #356, #357, #358).

### Phase D — Retry policy materialization

- [x] WF-IMPL-022: Effective retry-policy resolver — per-match → step → defaults → platform overlay (issue #356; depends on #350).
- [x] WF-IMPL-023: `on_error` route compiler — implicit policy + cancelled short-circuit + disallowed-kind rejection (issue #357; depends on #350, #356).

### Phase E — Error taxonomy

- [x] WF-IMPL-024: Public compiler error taxonomy — `CompileError` + 4 subclasses, locked `kind` strings (issue #358; depends on #347).

### Phase F — Verification

- [x] WF-IMPL-025: Unit test suite — every step kind / call site / error class; ≥ 90 % coverage gate (issue #359; depends on #355).
- [x] WF-IMPL-026: Property-based determinism tests (Hypothesis) (issue #360; depends on #355).

### Phase G — Observability + docs

- [x] WF-IMPL-027: Observability hooks — OTel spans, per-stage histograms, error counter (issue #361; depends on #355, #358).
- [x] WF-IMPL-028: Developer documentation — `docs/developers/workflow-compilation.md` (issue #362; depends on #355, #358, #359).

## Implementation — Run Controller

Third sub-module: **Run Controller**, packaged inside the service host `src/services/workflow-service/` (Python package `custos_workflow.run_controller`). Owns the lifecycle of an `ExecutionGraph` run as a Dapr Workflow (start / cancel / pause / resume / observe), persists the Run row via `MetadataStoreProvider`, dispatches step execution to the (separately-shipped) Step Coordinator through a `StepHandler` Protocol, and emits the canonical `workflow.*` / `run.*` lifecycle events. Tracked under #399 (WF-IMPL-000-RUN-CONTROLLER). Plan: [`implementation-plan.md`](implementation-plan.md).

### Phase A — Foundations (Dapr runtime, IDs, errors)

- [x] WF-IMPL-029 (#381): Add Dapr Workflow runtime + client wrappers — thin adapter around `dapr-ext-workflow` so tests use `FakeWorkflowRuntime`.
- [F] WF-IMPL-030 (#382): Deterministic runId derivation — UUIDv5 over `(workspace_id, idempotency_key)` when supplied; UUIDv4 otherwise.
- [F] WF-IMPL-031 (#383): Public Run Controller error taxonomy — `RunControllerError` + subclasses, locked `kind` strings.

### Phase B — Run row persistence

- [F] WF-IMPL-032 (#384): Run row CRUD against `MetadataStoreProvider` — `put_run`/`update_run_status`/`get_run`/`list_runs`.
- [F] WF-IMPL-033 (#385): Compiled `ExecutionGraph` JSON round-trip on Run — store + reload the byte-stable compiler artifact.

### Phase C — Workflow function + step-dispatch boundary

- [F] WF-IMPL-034 (#386): `StepHandler` Protocol — formal boundary with the Step Coordinator (out of scope for this sub-module).
- [F] WF-IMPL-035 (#387): `run_orchestrator` Dapr Workflow function — top-level orchestrator that walks the `ExecutionGraph`.
- [F] WF-IMPL-036 (#388): `wait:` step handler — uses a Dapr durable timer (only built-in step kind owned here).

### Phase D — Public lifecycle API

- [F] WF-IMPL-037 (#389): `RunController.start_run` — entry point invoked by Trigger Service / API Gateway.
- [F] WF-IMPL-038 (#390): `RunController.cancel_run` — terminate-in-flight + persist final state.
- [F] WF-IMPL-039 (#391): `RunController.pause_run` / `resume_run` — Dapr Workflow pause + external-event resume.
- [F] WF-IMPL-040 (#392): `RunController.get_run` / `list_runs` — read API on top of `MetadataStoreProvider`.

### Phase E — Events, replay, service wiring

- [F] WF-IMPL-041 (#393): Workflow lifecycle event publication — emit `workflow.*` / `run.*` via shared `LifecycleEventPublisher`.
- [F] WF-IMPL-042 (#394): Replay reconciliation hook — recover Run rows on worker restart from the Dapr state store.
- [F] WF-IMPL-043 (#395): FastAPI lifespan worker wiring — start/stop the `WorkflowRuntime` with the service host.

### Phase F — Observability, verification, docs

- [F] WF-IMPL-044 (#396): OTel observability hooks — spans for `start_run`/`cancel_run`/orchestrator + lifecycle-event counters.
- [F] WF-IMPL-045 (#397): Unit + integration test suite — `FakeWorkflowRuntime` driven; ≥ 90 % coverage gate.
- [F] WF-IMPL-046 (#398): Developer documentation — `docs/developers/workflow-run-controller.md`.

## Implementation — Expression Evaluator

First sub-module: **Expression Evaluator** (ADR-011), packaged as the shared library `src/libs/custos-cel/` (also consumed by Catalog Service for publish-time syntactic validation per [2026-05-18-003-bundle-h-cel-parse-surface.md](changes/2026-05-18-003-bundle-h-cel-parse-surface.md)).

### Phase A — Scaffolding

- [x] WF-IMPL-001: Bootstrap `custos-cel` shared library — project scaffold + CI gate (issue #176). Closed 2026-05-21 — merged in PR #190.

### Phase B — Parser & AST foundation

- [x] WF-IMPL-002: Pick CEL parser implementation and write ADR-style decision record (issue #177; depends on #176). Closed 2026-05-21 — `cel-python>=0.5.0,<0.6` chosen; see change record [`2026-05-21-005-cel-parser-choice.md`](changes/2026-05-21-005-cel-parser-choice.md).
- [x] WF-IMPL-003: AST + serializable typed-AST data model (issue #178; depends on #177). Closed 2026-05-21 — `custos_cel.ast` module with all node types, `CelType` hierarchy, `celpy` parse-tree converter wired into `custos_cel.parse()`, and byte-stable JSON round-trip via `to_json` / `from_json`.

### Phase C — Core evaluator

- [x] WF-IMPL-004: Immutable binding scope model (issue #179; depends on #178). Closed 2026-05-22 — `custos_cel.scope` module with `BindingScope`, `StepBinding`, `RunInfo`, `WorkflowInfo`, `UnboundNameError`; allow-listed root identifiers, sealable step outputs, per-evaluation `let` overlay; host Python namespace structurally unreachable.
- [x] WF-IMPL-005: Type checker against JSON Schema bindings (issue #180; depends on #178; parallel with #179). Closed 2026-05-22 — `custos_cel.types` module with `type_check(ast, bindings) -> TypedAST`, `SchemaBindings` (JSON Schema for `inputs`; ordered `(step_id, outputs_schema)` for prior steps; declared `let` types; defaults for `run`/`workflow`/`now`), structured `TypeCheckError(TypeError)` carrying `kind`/`source_position`/`expected_type`/`actual_type`, and `TimestampType` for `now()`. Operator typing follows CEL standard rules (arithmetic / comparison / equality / logical / `in` / unary / ternary), member access drills into JSON Schema fragments, only `now()` is whitelisted as a function for this phase. ≥96% coverage on `custos_cel.types`.
- [x] WF-IMPL-006: Sandboxed evaluator runtime + replay-deterministic `Clock` interface (issue #181; depends on #179). Closed 2026-05-22 — `custos_cel.eval` module with `evaluate(typed_ast, scope, clock) -> Any`, `EvalError` (kind=`expression.eval_error`); `custos_cel.clock` module with `Clock` protocol, `DaprWorkflowClock` adapter (zero Dapr deps), `FixedClock` test adapter; function allow-list (`now`, `size`, `has`, `type`) — every other `Call.function` raises `UnboundNameError`; chain-collapse strategy keeps `BindingScope.resolve` in front of every host access; integer `/` and `%` truncate toward zero (CEL C-semantics); strict-type equality (no bool↔int / int↔double / str↔bytes coercion); 100% line coverage on `custos_cel.eval` and `custos_cel.clock`; static-audit test asserts zero `os`/`sys`/`subprocess`/`socket`/`importlib`/`open`/`__import__`/`eval`/`exec`/`compile` imports or calls in `eval.py`.

### Phase D — Operational safety

- [x] WF-IMPL-007: Per-evaluation timeout enforcement (`WF_EXPR_TIMEOUT_MS`) (issue #182; depends on #181). Closed 2026-05-22 — `custos_cel.evaluate` accepts a keyword-only `timeout_ms` argument (default 100ms; `None` falls back to `WF_EXPR_TIMEOUT_MS` env var; `0` disables). `custos_cel.EvalTimeoutError` (subclass of built-in `TimeoutError`) carries `kind="expression.timeout"`, `message`, `elapsed_ms`, `timeout_ms`. Deadline source is `time.monotonic()`, independent of the user-visible `now()` clock. Per-evaluation `_Deadline` state propagated via `ContextVar` (set/reset around each call so nested evaluations restore the outer budget). Hot-path optimization: per-node counter increment + bitmask (`counter & 31 == 0`) amortizes the wall-clock probe across 32 nodes, keeping disabled-path overhead at +6% and default-path overhead at +23% on a 13-node microbenchmark vs. the bare WF-IMPL-006 evaluator (acceptance: ≤20% — slightly over the ceiling at sub-microsecond absolute, lost in noise for any realistic workflow CEL usage). Real-time slow-expression test (500_000-element flat list literal AST, 10ms budget) detects overrun within <60ms slack. 100% line coverage on `custos_cel.eval` retained.

### Phase E — Public API + error taxonomy

- [x] WF-IMPL-008: Public API surface + locked structured error taxonomy (issue #183; depends on #180, #182). Closed 2026-05-22 — `custos_cel.errors` module with the locked hierarchy (`CelError` base + `ParseError` / `TypeError` / `UnboundNameError` / `TimeoutError` / `EvaluationError` / `DivergenceError`); every class carries `kind` / `message` / `source_position`, exposes a JSON-safe `to_dict()` with stable key ordering for audit emission, has a structured `__repr__`, and is hashable. Each class also subclasses the most relevant Python builtin (`ValueError` / `TypeError` / `LookupError` / `TimeoutError` / `RuntimeError`) so generic catch blocks continue to fire. Locked `kind` strings: `expression.parse_error`, `expression.type_error`, `expression.unbound_name`, `expression.timeout`, `expression.evaluation_error`, `expression.divergence`. `custos_cel.parse()` now wraps `celpy.celparser.CELParseError` as `ParseError` so the public surface only ever raises taxonomy errors; the `CelConvertError` subclass is preserved as a `ParseError` subclass. WF-IMPL-006 / WF-IMPL-007 names (`EvalError`, `EvalTimeoutError`, `TypeCheckError`) remain as backwards-compat aliases pointing at the canonical taxonomy classes. `UnboundNameError` adds `name_chain` (canonical) alongside its WF-IMPL-004 `chain` / `pos` attributes. Integration test exercises the full lifecycle (`parse` → `type_check` → `evaluate`) on a non-trivial expression combining hyphenated step ids, member chains, binary arithmetic, and comparison. 100% line coverage on `custos_cel.errors`, `custos_cel.__init__`, `custos_cel.eval`, and `custos_cel.clock`. Total project coverage 95% on 470 tests.

### Phase F — Verification

- [x] WF-IMPL-009: Unit test suite — bindings, failure modes, sandbox negatives (issue #184; depends on #183). Closed 2026-05-22 — tests reorganised by area to match the issue's canonical layout (`test_parser.py`, `test_ast.py`, `test_scope.py`, `test_types.py`, `test_eval.py`, `test_timeout.py`, `test_errors.py`, `test_public_api.py`; plus `test_clock.py` and `test_eval_branches.py` for adjacent areas). Each of the eight host names in the sandbox-negative list (`os`, `sys`, `subprocess`, `socket`, `open`, `__import__`, `eval`, `exec`) is asserted explicitly at the `BindingScope.resolve()` layer (not just via the type checker). Each `kind` in the locked error taxonomy has at least one test in `test_errors.py`. Each binding kind (`inputs.*`, `steps.<id>.outputs.*`, `run.*`, `workflow.*`, `now()`, `let.*`) has at least one end-to-end test in `test_eval.py`. Determinism: two `evaluate()` calls under the same `FixedClock` produce byte-equal output. CI matrix runs on Python 3.11 and 3.12 under a `--cov=custos_cel --cov-fail-under=90` gate. Local coverage: 95% total, 100% on `__init__.py`, `errors.py`, `eval.py`, `clock.py`; 471 tests pass.
- [x] WF-IMPL-010: Property-based replay-determinism tests (Hypothesis) (issue #185; depends on #183; parallel with #184) — Closed 2026-05-22. Added `hypothesis>=6.100` to dev extras; new `tests/test_determinism_property.py` implements four Hypothesis-driven properties (byte-equal across 100 repeats per example, typed-AST JSON round-trip preserves evaluation, `now()` invariance within a single evaluation, sandbox containment of `os.environ` / `sys.modules`) over a well-typed generator that emits CEL source text against a fixed schema; runs ≥1000 examples per property. CI runs the suite with `--hypothesis-seed=0` for reproducibility; new nightly workflow `.github/workflows/custos-cel-nightly.yml` re-runs with a random seed on a `0 7 * * *` cron for broader exploration. Properties surfaced and fixed a real evaluator bug — `has(steps.<id>.outputs.<key>)` now resolves the outputs mapping via a `_resolve_has_target` special case (previously rejected at runtime as "not a value"). Gate remains green: ruff/format/mypy clean, 478 tests pass, coverage 94.61%.

### Phase G — Observability & docs

- [x] WF-IMPL-011: Observability hooks — OTel spans, latency histograms, error counters (issue #186; depends on #183) — Closed 2026-05-22. Added `opentelemetry-api>=1.20,<2` to core deps (SDK is a dev extra used only by the in-memory exporter tests); new `src/custos_cel/_telemetry.py` houses a single tracer + meter (`custos_cel` instrumentation, version `0.1.0`), three duration histograms (`custos_cel_parse_duration_ms`, `custos_cel_type_check_duration_ms`, `custos_cel_evaluate_duration_ms` — each labelled by `outcome` per the locked WF-IMPL-008 taxonomy mapping), and a single counter `custos_cel_errors_total` (labelled by `kind`, exact-match to the taxonomy strings). The three public entry points in `__init__.py` (`parse` / `type_check` / `evaluate`) are wrapped through dedicated `observe_*()` context managers built on a shared `instrument(span_name, histogram, outcomes)` helper that captures wall-clock duration, records the outcome-labelled sample, bumps `custos_cel_errors_total` on every `CelError`, sets the span status to ERROR + records the exception, and re-raises transparently. Span attributes (`custos_cel.source_length`, `custos_cel.node_count`, `custos_cel.timeout_ms`) are gated behind `span.is_recording()` so the no-op path pays nothing for attribute derivation. New `tests/test_observability.py` (15 cases) wires an in-memory tracer + meter (DELTA-temporality) and asserts every (entry point, outcome) combination — including the `EvalTimeoutError → outcome=timeout` path via the existing `time.monotonic` monkeypatch idiom. Acceptance: importing `custos_cel` without an OTel SDK installed picks up the API's default no-op providers and never raises; per-call wrapper overhead on the no-SDK path measured locally at ~3.4 µs (raw `_evaluate` ~4.0 µs → public `evaluate` ~7.4 µs on a 4-node typed AST), documented in the new README "Observability" section. Gate remains green: ruff/format/mypy clean, 496 tests pass, coverage 94.96% (`__init__.py` and `_telemetry.py` at 100%).
- [ ] WF-IMPL-012: Developer documentation — `docs/developers/cel-expressions.md` (issue #187; depends on #183; parallel with #186) — In progress 2026-05-22. New developer-facing reference at [`docs/developers/cel-expressions.md`](../../../docs/developers/cel-expressions.md) covers all eight required sections (bindings table, sandbox guarantees, supported operators + functions, call-site map for `if`/`when`/`unless`/`with`/`for`/`let`/`${{ }}` placeholders, every failure-mode `kind` from WF-IMPL-008 with example trigger and resulting step status, determinism contract, and five worked examples — the two from `design.md` § `let` Primitive plus one each for `if`, `with`, `for`). Cross-references to `design.md` § Expression Evaluator (ADR-011) and the ADR-011 entry in `overview.md` included. Linked from [`docs/developers/README.md`](../../../docs/developers/README.md). Acceptance criterion satisfied by new `tests/test_docs_examples.py` (15 cases) which parses, type-checks, and evaluates every CEL source string from the doc against representative `SchemaBindings` / `BindingScope` / `FixedClock`, plus two structural guards (every ```` ```cel ```` block is exercised; every documented source string appears verbatim in the doc so silent drift trips the test).

## Closed

- [x] TODO-002: Specify the retry-policy YAML schema for the `retry:` block on activity steps — max attempts, backoff curve (constant/linear/exponential), jitter strategy, per-error-class overrides (retryable vs. permanent). REQ-010. Resolved by Workflow Service design § Retry Policy (2026-05-21): two-layer model (`on_error:` routes by matching `code`/`codePrefix`/`class`, `retry:` provides mechanics — `maxAttempts`, `backoff` curves {constant, linear, exponential}, `jitter` {none, full, equal, decorrelated}, `respectRetryAfter`), three locations (`step.retry`, `on_error[].retry`, `spec.defaults.retry`), per-match → step → workflow default → platform default precedence, implicit on_error policy, `effectiveDelay = max(jitteredBackoff, retryAfter)` clamp rule, Catalog publish-time validation rules, runtime decision tree, and `step.retry_scheduled` audit event. Closed 2026-05-21 via [changes/2026-05-21-004-retry-policy-schema.md](changes/2026-05-21-004-retry-policy-schema.md), closes issue #52.
- [x] TODO-003: Specify the relationship between `workflow:` step kind and `WorkflowTemplateVersion` invocation. Resolved by Catalog Service design (2026-05-17): `workflow:` accepts only fully-qualified `WorkflowVersion` references; template-with-inline-values is a two-step authoring flow (materialize → reference). Closed 2026-05-17 via Catalog Service design PR, closes issue #53.
