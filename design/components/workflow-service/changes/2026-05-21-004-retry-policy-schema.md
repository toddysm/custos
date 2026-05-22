# Change: retry-policy-schema

Date: 2026-05-21
Type: component-design
Component: workflow-service
Sequence: 004
GitHub Issue: #52
Status: closed

## Summary

Locks the workflow `retry:` block schema (TODO-002 / REQ-010) and resolves the latent conflict between the `retry:` block referenced in [design/components/workflow-service/design.md](../design.md) (line 144, original) and the `on_error: … do: retry, maxAttempts: N` shape used in the canonical workflow schema example in [design/architecture/overview.md](../../../architecture/overview.md) § Workflow and Template Schema.

Resolution is a **two-layer model**: `on_error:` is the routing layer (match an ARM error envelope → pick `skip` / `retry` / `fail`); `retry:` is the mechanics layer (when retrying, how many attempts, what backoff curve, what jitter, whether to honor `retryAfter`). They compose: a `do: retry` arm consumes the prevailing `retry:` policy, computed by overlaying per-match → step-level → `spec.defaults.retry:` → platform defaults, field by field.

## Before

Three independent statements that did not fit together:

1. WF design.md line 144:
   > Workflow-level retry policy lives here, applied by the Step Coordinator using the per-step `retry` block from the workflow definition.

   The `retry` block was named but never specified — fields, defaults, validation rules all undefined.

2. overview.md § Workflow and Template Schema example (line 218–222):
   ```yaml
   on_error:
     - match: { codePrefix: "registry." }
       do: skip
     - match: { class: "retryable" }
       do: retry
       maxAttempts: 5
   ```
   The canonical example used `on_error:` with an inline `maxAttempts:` shorthand — no `retry:` block in sight — and gave no defaults, no backoff curve, no jitter strategy, no precedence rules.

3. ARM design.md error envelope reference (line 320):
   > `code` ... Used by workflows for `on_error` matching ...
   > `retryAfter` ... Lower-bound hint to the retry scheduler; clamped by the workflow's backoff policy. Only meaningful when `class: retryable`.

   ARM described a hint flowing to a "workflow backoff policy" that did not exist on paper.

REQ-010 — "Execution: configurable retry and error-handling policies per step" — was Open. Issue #52 explicitly called out the resulting ambiguity in a follow-up comment from the author: "Please include the relationship between `retry:` and `on_error:` in the schema decision: whether `on_error` is deferred, aliases into retry policy, or is the general policy surface and `retry:` is shorthand."

## After

New normative § Retry Policy in [design/components/workflow-service/design.md](../design.md) defines:

- **Two-layer model.** `on_error:` routes; `retry:` provides mechanics. A `do: retry` arm consumes the prevailing `retry:` policy.
- **`retry:` schema.** `maxAttempts` (int ≥ 1), `backoff: { strategy ∈ {constant, linear, exponential}, initialDelay (ISO-8601), maxDelay (ISO-8601), multiplier (≥ 1.0; exponential only) }`, `jitter ∈ {none, full, equal, decorrelated}`, `respectRetryAfter` (bool).
- **Three locations `retry:` may appear**: per-step (`step.retry`), per-match override inside an `on_error[]` arm (`on_error[i].retry`), and workflow-wide default (`spec.defaults.retry`).
- **Precedence**: per-match → step → workflow default → platform default, applied field-by-field (partial overrides supported).
- **Platform defaults**: `maxAttempts: 3`, `backoff: { strategy: exponential, initialDelay: PT1S, maxDelay: PT5M, multiplier: 2.0 }`, `jitter: full`, `respectRetryAfter: true`.
- **Implicit `on_error` policy** (when no `on_error:` block is present): `retryable` → `do: retry`; `permanent` → `do: fail`; `cancelled` → `do: fail`.
- **Cancellation short-circuit**: `class: cancelled` is never matched by a `do: retry` arm; it always terminates as `fail`. Prevents a misconfigured workflow from looping on operator-initiated cancellation.
- **Backoff formulas** (1-indexed attempt counter, clamped by `maxDelay`):
  - `constant`: `D₀`
  - `linear`: `D₀ × n`
  - `exponential`: `D₀ × m^(n − 1)`
- **Jitter formulas** (AWS Architecture Blog naming):
  - `none`: `D`
  - `full`: `random(0, D)`
  - `equal`: `D/2 + random(0, D/2)`
  - `decorrelated`: `min(Dmax, random(D₀, prevDelay × 3))`
- **`retryAfter` interaction**: `effectiveDelay = max(jitteredBackoff, retryAfter)` when `respectRetryAfter: true`.
- **Where `retry:` is allowed**: activity steps, `on_error[]` arms, `spec.defaults.retry:`. Rejected on `let:`, `if:`-only, `forEach:` container, `wait:`/`waitFor:`/`approval:`, `workflow:` (sub-workflow invocation).
- **Publish-time validation rules** (enforced by Catalog Service): listed in full in § Retry Policy. Catalog rejects the publish; nothing surfaces at `StartRun`.
- **Runtime decision tree** (Mermaid flowchart) and the `step.retry_scheduled` audit event emission for observability.
- **Shorthand**: inline `maxAttempts: N` on a `do: retry` arm is retained as sugar for `retry: { maxAttempts: N }` (preserves backward compatibility with the existing overview.md example). Conflict between inline shorthand and an explicit `retry: { maxAttempts: ... }` on the same arm is a publish-time error.

[design/architecture/overview.md](../../../architecture/overview.md) § Workflow and Template Schema is updated with:

- A new `retry:` row in the Step forms table.
- A new cross-component implications bullet describing the two-layer model.
- A new `### Retry policy` subsection showing the schema with `spec.defaults.retry:`, the implicit policy table, and a pointer to the WF design as the normative spec.

[design/components/activity-runtime-manager/design.md](../../activity-runtime-manager/design.md) gets cross-references:

- The `retryAfter` row in the error-envelope table now points to WF § Retry Policy for the precise interaction.
- The locked-defaults bullet now reiterates ARM is the sole classifier and WF is the sole retry decision-maker, with the `effectiveDelay = max(jitteredBackoff, retryAfter)` formula spelled out by reference.

No data-model changes. No REST or RPC surface changes. The `(runId, stepId, attempt)` triple from the Idempotency Tracker continues to be the shared scheduling key; ARM continues to dedup on it.

## Impact

- **Workflow authors** can now write `retry:` blocks (per-step or workflow-default) and per-match overrides with a well-defined schema and predictable defaults. Unblocks meaningful retry behavior in M1 demo workflows.
- **Catalog Service** gains a concrete publish-time validation surface for the retry schema (listed rules in § Retry Policy). The validation rules are part of the publish-gate work but are scoped — they only require schema parsing, no expression evaluation.
- **Workflow Service Step Coordinator** implementation (future ticket) has a complete contract: precedence overlay algorithm, backoff and jitter formulas, `retryAfter` clamp rule, decision tree, and observability event (`step.retry_scheduled`). Closely related to WF-IMPL-006 (sandboxed evaluator) and the broader Step Coordinator slice.
- **ARM** is unchanged behaviorally. The boundary is reaffirmed: ARM classifies, WF decides.
- **Observability/Audit** gains a new event kind `step.retry_scheduled` that will fold into the canonical event taxonomy work (TODO-001 / #18).
- **Approval-gate timeouts** continue to be out of scope for retry policy (already locked in § Approval-gate timeout — a timed-out approval is a business decision, not a transient failure).
- **REQ-010** moves from Open → satisfied at the schema level. Implementation tracks separately under the Step Coordinator work.

## Files changed

- `design/components/workflow-service/design.md` — added § Retry Policy, updated the line-144 cross-reference paragraph, closed TODO-002 in § Open TODOs, added change-history row, header bumped Version 3 → 4.
- `design/architecture/overview.md` — added `retry:` row to Step forms table, added retry-mechanics bullet to cross-component implications, added § Retry policy subsection with example + implicit policy table + pointer to WF normative spec, added change-history row, header bumped Version 13 → 14.
- `design/components/activity-runtime-manager/design.md` — added cross-references on the `retryAfter` envelope row and the locked-defaults bullet, added change-history row, header bumped Version 4 → 5.
- `design/components/workflow-service/todos.md` — moved TODO-002 from Open to Closed with closure note referencing this change record and #52.
- `design/components/workflow-service/changes/2026-05-21-004-retry-policy-schema.md` — this file.

## Related Requirements

- REQ-010 (Execution: configurable retry and error-handling policies per step)
- REQ-028 (re-run / re-trigger) — unaffected; retry policy applies within a run, re-run is the inter-run mechanism.

## Related Issues

- Closes #52 (WF-TODO-002).
- Cross-references #18 (TODO-001, canonical event taxonomy) — the new `step.retry_scheduled` event kind folds into that work.
- Cross-references #38 (INCON-013, ARM/Trigger event-kind unification) for the same reason.
