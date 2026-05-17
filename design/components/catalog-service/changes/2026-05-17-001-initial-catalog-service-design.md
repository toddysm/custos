# Change: initial-catalog-service-design

Date: 2026-05-17
Type: component-design
Component: catalog-service
Sequence: 001
GitHub Issue: #55
Status: open

## Summary

Initial detailed design for COMP-007 Catalog Service. Catalog is the authoring-time and publish-time entry point for workflow definitions and templates, and the read-side metadata index for activity types and connector types. This design also resolves WF-TODO-003 (#53) by locking the `workflow:` step kind to fully-qualified `WorkflowVersion` references only — template-with-inline-values is a two-step authoring flow (materialize, then reference).

## Before

`design/architecture/components.md` COMP-007 row status: `Defined`. No `design/components/catalog-service/design.md` file existed. Cross-component dependencies and source-of-truth questions were referenced but unowned:

- ARM design § Activity Catalog described the activity-type registration flow into "the Catalog" without specifying what Catalog stores, validates, or returns.
- Connector Service design referenced `CatalogStoreProvider` and `events.produced` but the connector-type registry projection was unspecified.
- Workflow Service design § Validator referenced "Catalog already validated the document at publish time" without a home document specifying what publish-time validation covers.
- The two `Connector Type Registry` sub-modules (one in COMP-005, one in COMP-007) were not disambiguated.
- ADR-006 / ADR-009 (templates, ADR-009 round-trip) had no implementing-component design.
- REQ-024 (action catalog), REQ-025 (workflow versioning immutability), REQ-076 (templates) had no owning design.
- WF-TODO-003 (#53): `workflow:` step kind ↔ template instantiation relationship was blocked on this component.

## After

New file `design/components/catalog-service/design.md` (v1, Status: Draft) covering:

- **Responsibility and Boundaries** — what Catalog owns vs. what ARM, Connector Service, WF, and Storage own. Explicit **source-of-truth split table**: Catalog is writer for `WorkflowVersion` / `WorkflowTemplateVersion`; ARM is writer for activity-type metadata; Connector Service is writer for connector-type metadata; Catalog holds the read-side index for the latter two.
- **Internal Structure** — the eleven sub-modules from `components.md` COMP-007 unchanged, with explicit responsibility per sub-module. The two `Connector Type Registry` sub-modules are disambiguated: COMP-005's is the runtime plugin loader, COMP-007's is the metadata index.
- **Nine key operations with sequence diagrams**: Publish Workflow Version, Materialize Workflow from Template, Extract Template from Workflow, Register Activity Type Version, Resolve Activity Reference at Publish, Sub-Workflow Reference Resolution, Deprecate Activity Type Version, List Activity Types, Pod Restart / Recovery.
- **Semver resolution rules table** — `<ns>/<type>@<major>` (latest non-deprecated within major; digest pinned at publish); exact pin allowed; `<ns>/<type>@<major>.<minor>` rejected in M1; short-form (no namespace) rejected in M1.
- **`workflow:` step kind reference rules** (resolves WF-TODO-003 / #53): fully-qualified `workflowVersionId` only. Templates with inline values are a two-step authoring flow (materialize → reference). Rationale: alternative (inline placeholder values at call site) would produce a runtime `WorkflowVersion`-equivalent without an immutable record, violating REQ-025.
- **Publish-time validation scope** — exhaustive table covering schema, reference resolution, instance existence, CEL parse (not evaluation), expression name-binding, placeholder schema, trigger event-name validation against `events.produced`.
- **Compilation strategy**: **publish-time CEL parsing, not evaluation**. Runtime bindings are unknown at publish; Catalog parses with the same grammar WF Expression Evaluator uses and rejects syntactic errors. Name-binding errors (refs to non-existent steps or undeclared placeholders) are also caught at publish because both step graph and placeholder block are known.
- **Round-trip property** for template extraction (ADR-009): materializing an extracted template with the original concrete values reproduces the source workflow byte-for-byte after canonicalization. The Extractor enforces this self-check.
- **Public Interface** — REST API endpoints for workflow publish/get/deprecate, template publish/materialize/extract, activity-type and connector-type registration and resolution. Internal RPC inbound (`GetWorkflowVersion`, `ResolveActivityRef`, `ResolveConnectorTypeRef`) and outbound (`ExistsConnectorInstance`).
- **Data model** — `Workflow`, `WorkflowVersion`, `WorkflowTemplate`, `WorkflowTemplateVersion`, `ActivityTypeVersion`, `ConnectorTypeVersion` with lineage links (`derivedFromTemplateVersionId`, `derivedFromWorkflowVersionId`). All version rows are immutable per REQ-025.
- **Persistence split**: `DefinitionStoreProvider` for workflow/template versions; `CatalogStoreProvider` for activity-type/connector-type index rows. v1 = Postgres for both; OCI registry adapter for `DefinitionStoreProvider` deferred to M2+.
- **Configuration** and **Dependencies** tables.
- **Failure Modes** table covering dependency unavailability, version conflicts, validation failures, deprecation race, round-trip violation.

Also: `design/architecture/components.md` COMP-007 status changes `Defined` → `Designed`.

Companion edits to `design/components/workflow-service/`:

- `design.md` § Sub-Orchestration Manager already locked the `workflow:` rule (PR #54), but TODO-003 in `Open TODOs` is now satisfied.
- `todos.md`: TODO-003 moved from Open to Closed with reference to this change.

## Key Decisions Locked This Session

1. **Catalog is read-side index for activity/connector types; ARM and Connector Service are writers.** This is the disambiguation for the two `Connector Type Registry` sub-modules in `components.md`. The OCI registry (when used) remains source of truth for activity manifest blobs; Catalog stores normalized projections.
2. **`workflow:` step kind only accepts fully-qualified `WorkflowVersion` references** (WF-TODO-003 / #53 resolved). Template materialization is two-step at the authoring layer. Reasoning: REQ-025 immutability requires every executable definition to have a persistent immutable record; inline-placeholder invocation would violate this.
3. **Publish-time CEL parsing, not evaluation, in Catalog.** Catalog uses the same CEL grammar as WF Expression Evaluator. Parse errors caught at publish; evaluation deferred to WF at run time. Name-binding errors (non-existent step refs, undeclared placeholders) are caught at publish.
4. **v1 = Postgres for both `DefinitionStoreProvider` and `CatalogStoreProvider`.** OCI registry adapter for `DefinitionStoreProvider` deferred to M2+ per the requirements timeline.
5. **Sub-module list unchanged from `components.md` COMP-007.** No additions; responsibilities described in the design.
6. **Semver resolution rules locked**: `@<major>` resolves to latest non-deprecated within major and digest-pins at publish; exact pins allowed; `@<major>.<minor>` and short-form (no namespace) rejected in M1.
7. **Deprecation is forward-only.** Existing `WorkflowVersion` documents continue to resolve their digest-pinned activity references; new publishes that try to resolve to a deprecated version are rejected. No hard deletes in v1.
8. **Round-trip extraction self-check is mandatory**: extractor verifies that materializing the new template with the original values reproduces the source workflow byte-for-byte after canonicalization, else 500 with `roundtrip_violation`. This makes ADR-009 a runtime invariant, not just an aspirational property.

## Impact

- COMP-007 component design gap closed: the authoring-time gate now has a home document.
- WF-TODO-003 (#53) resolved: `workflow:` step kind locked to `WorkflowVersion` references. WF design's Sub-Orchestration Manager section already aligns; WF `todos.md` TODO-003 moves to Closed.
- ARM design § Activity Catalog now has a counterparty design: Catalog is the receiver of `POST /v1/catalog/activities` and the read-side resolver of `<ns>/<type>@<major>`. ARM's existing TODO-002 (manifest signing M2+) is unaffected.
- Connector Service connector-type registration path now has a counterparty design.
- ADR-006, ADR-009 gain implementing-component ownership.
- REQ-024 (catalog), REQ-025 (versioning immutability), REQ-076 (templates) now have a designed home component.

## Out of Scope (Deferred to Future Sessions)

- Cross-workspace sub-workflow references and their RBAC model — deferred to M3 multi-tenancy work.
- OCI registry adapter for `DefinitionStoreProvider` — deferred to M2+ per REQ-029, REQ-051.
- Activity manifest signing (cosign Referrer verification at Catalog ingest) — already deferred to M2+ in ARM TODO-002.
- Hard-delete / purge of deprecated activity-type and connector-type versions with no extant references — out of v1 scope.
- Short-form activity reference resolution (`vuln-scan@2` without a namespace) — already deferred post-M1 in architecture overview.
- `@<major>.<minor>` pin form — reserved; rejected in M1.

## Related Requirements

- REQ-024 (action catalog discoverable and versioned) — § Activity Type Registry + § Register Activity Type Version + § List Activity Types.
- REQ-025 (workflow versioning immutability) — § Data Models + § Publish Workflow Version (frozen=true) + § Sub-Workflow Reference Resolution.
- REQ-029 / REQ-051 (workflow definitions as OCI artifacts) — OCI adapter for `DefinitionStoreProvider` noted, deferred to M2+.
- REQ-048 (datastore-agnostic abstractions; Postgres v1) — § Persistence split.
- REQ-073 (workflow primitives) — publish-time validation parses CEL for `if`/`when`/`for`/`let`/`with`.
- REQ-076 (workflow templates with placeholders, round-trip extraction) — § Materialize Workflow from Template + § Extract Template from Workflow + round-trip property.
- ADR-006 (first-class templates) — implementing-component design.
- ADR-009 (workflow + template share one schema, placeholders declared against it) — § Template Engine + round-trip property.
- ADR-011 (CEL evaluator) — Catalog uses the same grammar for publish-time parsing.
- Issues: WF-TODO-003 (#53) closed by this change.
