# Change: bundle-h-cel-parse-surface

Date: 2026-05-18
Type: component-design
Component: workflow-service
Sequence: 003
GitHub Issue: #100
Status: open

## Summary

Bundle H, issue #100 half (WF side). Resolves the contradiction between Catalog Service (which already claims CEL syntactic validation is publish-time and that `WorkflowVersion.document` stores a normalized definition, not an AST) and WF's `let` compilation paragraph (which claimed parse errors fail at `StartRun`). After this change WF agrees that Catalog is the sole syntactic gate; the Definition Compiler at StartRun re-parses CEL source strings from `WorkflowVersion.document`, **type-checks** against the bound schemas, and caches the typed AST on `ExecutionGraph`. Type errors fail StartRun; a parse failure at this stage indicates a contract violation (Catalog gate bypassed or document tampered) and surfaces as a compile error, not as a user-facing parse error.

## Before

WF § "Compilation strategy" (lines 332–334) said:

> `let` expressions are parsed and type-checked at workflow **compile time** (Definition Compiler pass) and the compiled AST is cached on `ExecutionGraph` alongside the rest of the workflow's expressions. Parse errors fail the workflow at `StartRun` time (Validator rejects the request before a `runId` is issued). Evaluation errors fail the specific step at run time with status `permanent` — the AST is well-formed but a binding produced an incompatible value.
>
> This matches the compilation model for all other expressions (`if`, `when`, `with`, loop `for`): parse-once at compile, evaluate-many at step boundaries.

That contradicted Catalog Service § Responsibility (line 28: "Catalog only parses for syntactic validation at publish time"), § Source-of-truth split table, and § Publish-time validation table line 395 ("CEL expression parse … Reject with parse error and position. No evaluation"). It also left the AST-storage location ambiguous (some readers assumed `WorkflowVersion.document` held a pre-built AST).

## After

- WF § "Compilation strategy" now says: CEL syntactic validation is Catalog's responsibility at workflow publish time; WF never observes a syntactically invalid `WorkflowVersion`. `WorkflowVersion.document` stores the normalized definition with original CEL **source strings**, not a pre-built AST. The Definition Compiler runs at `StartRun`: it re-parses each CEL source string from `WorkflowVersion.document`, type-checks the AST against bound input/output schemas and the step graph, and caches the typed AST on `ExecutionGraph`. Type errors fail StartRun (Validator rejects before a `runId` is issued); defensive re-parse failures at this stage are a contract violation surfaced as compile errors, not parse errors. Evaluation errors at step boundaries still fail the specific step with status `permanent` (unchanged).
- "parse-once at compile" rewritten to "syntax gated once at publish (Catalog), type-checked and parsed-into-AST once per run at StartRun (WF Definition Compiler), evaluated many times at step boundaries".
- Header bumped: Version 2 → 3; Change History row added.

§ Validator (line 56) already said "Does not re-validate the workflow definition itself — Catalog already did that at publish time" — that line is consistent with the new wording and is unchanged.

## Impact

- WF, Catalog, and any future Definition Compiler implementer now read a single consistent story: publish parses (Catalog), StartRun type-checks (WF), step boundaries evaluate (WF).
- No REST surface changes. No audit-event taxonomy changes. No data-model changes — `WorkflowVersion.document` continues to store normalized source CEL.
- A type-check error at StartRun is a permanent (4xx) failure on the request, not a transient retryable condition.

## Files changed

- `design/components/workflow-service/design.md`
- `design/components/workflow-service/changes/2026-05-18-003-bundle-h-cel-parse-surface.md` (this file)

## Related Change Records

- `design/components/catalog-service/changes/2026-05-18-002-bundle-h-publish-ownership.md` (companion #105 work in the same bundle; Catalog's claims at lines 28, 29, 341, 395 are already correct and not modified)
