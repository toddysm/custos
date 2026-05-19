# Change: bundle-h-cel-parse-surface-catalog

Date: 2026-05-18
Type: component-design
Component: catalog-service
Sequence: 004
GitHub Issue: #100
Status: open

## Summary

Aligned Catalog Service's CEL parsing language with the Workflow Service Bundle H update (`2026-05-18-003-bundle-h-cel-parse-surface.md`). Catalog remains the sole syntactic gate at publish time, but it now explicitly states that the parsed AST is **discarded after validation** and that `WorkflowVersion.document` stores normalized CEL **source strings**, not a pre-built AST. The "publish-time validation is the only gate before runtime" claim is softened to exclude two unavoidably-deferred error classes: CEL type errors against bound activity/connector schemas (resolved only at StartRun) and runtime evaluation errors.

## Before

- Publish sequence diagram return: `CEL-->>DM: AST set (no evaluation)` — implied the AST was kept and stored.
- Paragraph: *"Publish-time validation is the **only** validation gate before runtime. Once a `WorkflowVersion` exists, Workflow Service trusts it: the Validator does not re-validate the document..."* — conflicted with WF's StartRun re-parse + type-check.
- Publish-time validation scope row for CEL parse said `**No evaluation**` only.
- Closing sentence: *"The publish-time validator is exhaustive: any failure that could be caught here must be caught here, not deferred to run time."* — too absolute; type errors against bound schemas can only be caught at StartRun.

## After

- Publish sequence diagram return: `CEL-->>DM: parse OK (AST discarded; not stored on WorkflowVersion)`.
- Paragraph rewritten: Catalog is the syntactic gate; WF re-parses + type-checks at StartRun; Catalog guarantees (a) CEL syntactic well-formedness, (b) reference resolution, (c) digest pins; type errors and runtime eval errors are observable only at StartRun and run time respectively.
- CEL parse row updated: `Reject with parse error and position. **No evaluation, AST not stored** — runtime bindings unknown; document stores normalized source strings; WF re-parses at StartRun.`
- Closing sentence loosened: Catalog is exhaustive for everything catchable without runtime bindings or external schemas resolved; CEL type errors against bound schemas and runtime evaluation errors are explicitly deferred.

## Impact

- Storage model now unambiguous: `WorkflowVersion.document` stores source strings, never AST. Schema and persistence design unchanged in practice — this was the existing implementation intent, just not documented.
- WF Definition Compiler can be implemented against source strings without contradicting Catalog's contract.
- Error classification for the API surface is now defined: parse → Catalog publish 4xx, type → WF StartRun 4xx, evaluation → step `permanent` failure.

## Files changed

- `design/components/catalog-service/design.md` v2 → v3 (Publish sequence diagram; publish-validation paragraph; validation-scope table CEL row; closing exhaustiveness sentence; Change History)

## Related Change Records

- Workflow Service: `2026-05-18-003-bundle-h-cel-parse-surface.md` (companion entry — defines the StartRun re-parse + type-check behavior)
- Catalog Service: `2026-05-18-002-bundle-h-publish-ownership.md` (the other Bundle H change on catalog — #105 publish ownership)
