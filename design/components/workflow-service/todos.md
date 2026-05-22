# TODOs: Workflow Service

Last Updated: 2026-05-21

## Open

- [ ] TODO-001: Finalize canonical workflow event taxonomy (`workflow.*`, `run.*`, `step.*`) jointly with Trigger Service TS-TODO-001 (#18) and ARM TODO-009 (INCON-013 cross-link). Tracked under those existing issues; no separate WF issue. (added 2026-05-17)

## Implementation

First sub-module under construction: **Expression Evaluator** (ADR-011), packaged as the shared library `src/libs/custos-cel/` (also consumed by Catalog Service for publish-time syntactic validation per [2026-05-18-003-bundle-h-cel-parse-surface.md](changes/2026-05-18-003-bundle-h-cel-parse-surface.md)).

### Phase A — Scaffolding

- [x] WF-IMPL-001: Bootstrap `custos-cel` shared library — project scaffold + CI gate (issue #176). Closed 2026-05-21 — merged in PR #190.

### Phase B — Parser & AST foundation

- [x] WF-IMPL-002: Pick CEL parser implementation and write ADR-style decision record (issue #177; depends on #176). Closed 2026-05-21 — `cel-python>=0.5.0,<0.6` chosen; see change record [`2026-05-21-005-cel-parser-choice.md`](changes/2026-05-21-005-cel-parser-choice.md).
- [x] WF-IMPL-003: AST + serializable typed-AST data model (issue #178; depends on #177). Closed 2026-05-21 — `custos_cel.ast` module with all node types, `CelType` hierarchy, `celpy` parse-tree converter wired into `custos_cel.parse()`, and byte-stable JSON round-trip via `to_json` / `from_json`.

### Phase C — Core evaluator

- [ ] WF-IMPL-004: Immutable binding scope model (issue #179; depends on #178)
- [ ] WF-IMPL-005: Type checker against JSON Schema bindings (issue #180; depends on #178; parallel with #179)
- [ ] WF-IMPL-006: Sandboxed evaluator runtime + replay-deterministic `Clock` interface (issue #181; depends on #179)

### Phase D — Operational safety

- [ ] WF-IMPL-007: Per-evaluation timeout enforcement (`WF_EXPR_TIMEOUT_MS`) (issue #182; depends on #181)

### Phase E — Public API + error taxonomy

- [ ] WF-IMPL-008: Public API surface + locked structured error taxonomy (issue #183; depends on #180, #182)

### Phase F — Verification

- [ ] WF-IMPL-009: Unit test suite — bindings, failure modes, sandbox negatives (issue #184; depends on #183)
- [ ] WF-IMPL-010: Property-based replay-determinism tests (Hypothesis) (issue #185; depends on #183; parallel with #184)

### Phase G — Observability & docs

- [ ] WF-IMPL-011: Observability hooks — OTel spans, latency histograms, error counters (issue #186; depends on #183)
- [ ] WF-IMPL-012: Developer documentation — `docs/developers/cel-expressions.md` (issue #187; depends on #183; parallel with #186)

## Closed

- [x] TODO-002: Specify the retry-policy YAML schema for the `retry:` block on activity steps — max attempts, backoff curve (constant/linear/exponential), jitter strategy, per-error-class overrides (retryable vs. permanent). REQ-010. Resolved by Workflow Service design § Retry Policy (2026-05-21): two-layer model (`on_error:` routes by matching `code`/`codePrefix`/`class`, `retry:` provides mechanics — `maxAttempts`, `backoff` curves {constant, linear, exponential}, `jitter` {none, full, equal, decorrelated}, `respectRetryAfter`), three locations (`step.retry`, `on_error[].retry`, `spec.defaults.retry`), per-match → step → workflow default → platform default precedence, implicit on_error policy, `effectiveDelay = max(jitteredBackoff, retryAfter)` clamp rule, Catalog publish-time validation rules, runtime decision tree, and `step.retry_scheduled` audit event. Closed 2026-05-21 via [changes/2026-05-21-004-retry-policy-schema.md](changes/2026-05-21-004-retry-policy-schema.md), closes issue #52.
- [x] TODO-003: Specify the relationship between `workflow:` step kind and `WorkflowTemplateVersion` invocation. Resolved by Catalog Service design (2026-05-17): `workflow:` accepts only fully-qualified `WorkflowVersion` references; template-with-inline-values is a two-step authoring flow (materialize → reference). Closed 2026-05-17 via Catalog Service design PR, closes issue #53.
