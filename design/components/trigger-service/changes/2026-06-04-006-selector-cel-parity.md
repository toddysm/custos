# Change: selector-cel-parity

Date: 2026-06-04
Type: component-design
Component: trigger-service
Sequence: 006
GitHub Issue: #19
Status: closed

## Summary

Resolves TODO-002 (selector language). The Trigger Service selector language is
the **CEL expression language** evaluated by the shared `custos-cel` sandboxed
evaluator (ADR-011) — full parity with the `inputMapping` placeholders
(`${{ event.data.outputs.image }}`) the design already uses. A subscription
selector is a single CEL **boolean** expression evaluated against a new `event`
binding root that mirrors the `NormalizedEvent` envelope. One expression
language now spans the whole platform: workflow `if`/`when`/`with`/`for`/`let`
(WF), publish-time validation (Catalog), and trigger selectors + input mapping
(Trigger).

## Before

The design sketched selectors as `(fieldPath, matchType, value)` tuples with
`matchType ∈ {eq, prefix, regex, jsonpath}` (`SubscriptionSelector` ER entity),
and noted in TODO-002 that this "may need CEL parity with ADR-011." Two
expression dialects coexisted on the same trigger object: tuple selectors for
matching and CEL placeholders (`${{ … }}`) for input mapping. Connector authors
and workflow authors had to learn both.

## After

### Selector = CEL boolean expression over an `event` root

- A selector is one CEL boolean expression, e.g.
  `event.kind == "workflow.completed" && event.data.status == "succeeded"`.
- A new `event` binding root resolves the `NormalizedEvent` envelope:
  `event.schemaVersion`, `event.eventId`, `event.kind`, `event.subject`,
  `event.source.{type,connectorInstanceId,subscriptionId,vendor,occurredAt}`,
  `event.data.*`, `event.raw.{headers,body}`.

### Enabling change to `custos-cel` (ADR-011)

- Add `event` to `custos_cel.scope._ALLOWED_ROOTS` and a matching
  `SchemaBindings.event` JSON-Schema entry + `BindingScope.event` mapping. This
  single addition powers **both** trigger selectors and trigger `inputMapping`.
- The sandbox / determinism / timeout guarantees are inherited unchanged; the
  `event` root is just another allow-listed binding, structurally identical to
  `inputs`.

### Selector lifecycle

1. **Authoring (YAML).** The `selector:` block accepts either (a) a CEL string,
   or (b) the legacy `field: matchType:value` sugar (`repository:
   prefix:ghcr.io/acme/`) which **desugars** to equivalent CEL
   (`event.data.repository.startsWith("ghcr.io/acme/")`). CEL is the canonical
   persisted form; the four legacy match-types remain accepted at the API for
   convenience and are lowered to CEL before storage.
2. **Persistence.** Stored as a single `SubscriptionSelector` row with
   `matchType = "cel"`, `value = <cel expr>`, `fieldPath = ""`. The
   contract-locked SPL v1 schema is preserved — `cel` simply joins the existing
   `eq|prefix|regex|jsonpath` enum as the canonical value.
3. **Create / patch (fail-fast).** `parse()` + `type_check()` against the
   `event` `SchemaBindings`; invalid CEL is rejected with `trigger.selector_invalid`
   (HTTP 422) **before** the subscription is persisted. The typed AST is cached
   in-process keyed by `(subscriptionId, exprHash)`.
4. **Match (hot path).** Build `BindingScope(event=<normalized event>)`,
   `evaluate(typed_ast, scope, clock)` under the per-evaluation timeout budget.
   A non-bool result → `trigger.selector_type_error` (treated as no-match +
   audit); a timeout → no-match + audit.
5. **Resume selectors.** The WF `RegisterResumeSubscription(selector=…)` payload
   is likewise a CEL expression (or `None` = match on event key alone); same
   compile-at-register, evaluate-at-match path.

## Rationale

- **One language.** Eliminates the tuple/CEL split; selectors and input mapping
  share `event.*`.
- **Reuse.** `custos-cel` already ships the parser, type checker, sandbox,
  timeout budget, and determinism property tests — no new evaluator.
- **Fail-fast.** Type-checking at subscription create rejects malformed
  selectors before they can silently never match in production.
- **Backward-compatible storage.** The locked `SubscriptionSelector` row shape
  is untouched; `cel` is an additive `matchType`.

## Impact

- `custos-cel`: additive `event` binding root (`_ALLOWED_ROOTS`,
  `SchemaBindings.event`, `BindingScope.event`).
- Trigger Service: a selector-evaluator module (compile-at-create,
  evaluate-at-match, legacy desugar), new error kinds `trigger.selector_invalid`
  / `trigger.selector_type_error`.
- No change to the Internal RPC signatures — `selector` was already a string on
  `RegisterResumeSubscription`.
