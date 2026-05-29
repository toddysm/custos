# TODOs: Workflow Service

Last Updated: 2026-05-27

## Open

- [ ] TODO-001: Finalize canonical workflow event taxonomy (`workflow.*`, `run.*`, `step.*`) jointly with Trigger Service TS-TODO-001 (#18) and ARM TODO-009 (INCON-013 cross-link). Tracked under those existing issues; no separate WF issue. (added 2026-05-17)

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

- [ ] WF-IMPL-027: Observability hooks — OTel spans, per-stage histograms, error counter (issue #361; depends on #355, #358).
- [ ] WF-IMPL-028: Developer documentation — `docs/developers/workflow-compilation.md` (issue #362; depends on #355, #358, #359).

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
