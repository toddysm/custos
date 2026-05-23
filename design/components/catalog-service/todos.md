# TODOs: Catalog Service

Last Updated: 2026-05-22

## Open

(none — all design questions for the initial Catalog Service design were resolved in-session)

## Closed

(none yet)

## Implementation

Implementation plan for the Catalog Service (COMP-007) is tracked under meta-issue
**#226 (CS-IMPL-000)** with 24 child issues on the `design/impl-20260522-catalog-service`
branch. Issues are organised into ten phases. Within a phase, work can proceed in parallel;
cross-phase dependencies are noted explicitly. #226 stays open until every child below
— including the Phase J cross-component follow-ups — is closed.

### Phase A — Foundations

| ID         | Title                                                                                          | Issue | Depends on            |
|------------|------------------------------------------------------------------------------------------------|-------|-----------------------|
| CS-IMPL-001 | Scaffold catalog-service Python package + CI gate                                              | #202  | —                     |
| CS-IMPL-002 | Wire catalog-service Helm subchart (env vars, ConfigMap, ExternalSecret)                       | #203  | CS-IMPL-001           |

### Phase B — Persistence + identity

| ID         | Title                                                                                          | Issue | Depends on            |
|------------|------------------------------------------------------------------------------------------------|-------|-----------------------|
| CS-IMPL-003 | SPL provider wiring + schema-revision startup gate                                             | #204  | CS-IMPL-001           |
| CS-IMPL-004 | Call-context middleware (dev shim pending CS-IMPL-024)                                         | #205  | CS-IMPL-001           |

### Phase C — Workflow document validation

| ID         | Title                                                                                          | Issue | Depends on            |
|------------|------------------------------------------------------------------------------------------------|-------|-----------------------|
| CS-IMPL-005 | Workflow + Template JSON Schema validators                                                     | #206  | CS-IMPL-001           |
| CS-IMPL-006 | Workflow document normalizer (canonical key ordering, hashing)                                 | #207  | CS-IMPL-005           |
| CS-IMPL-007 | Publish-time CEL syntactic + name-binding validator                                            | #208  | CS-IMPL-005, 006      |
| CS-IMPL-008 | Reference resolver (activity / sub-workflow / connector — connector client stubbed)            | #209  | CS-IMPL-003, 006      |

### Phase D — Versioning + workflow publish

| ID         | Title                                                                                          | Issue | Depends on            |
|------------|------------------------------------------------------------------------------------------------|-------|-----------------------|
| CS-IMPL-009 | Versioning Manager + immutability enforcement                                                  | #210  | CS-IMPL-003           |
| CS-IMPL-010 | Definition Manager (workflow publish path)                                                     | #211  | CS-IMPL-003, 006, 007, 008, 009 |
| CS-IMPL-011 | Workflow list/get + deprecation                                                                | #212  | CS-IMPL-010           |

### Phase E — Templates

| ID         | Title                                                                                          | Issue | Depends on            |
|------------|------------------------------------------------------------------------------------------------|-------|-----------------------|
| CS-IMPL-012 | Placeholder Schema Validator + template publish path                                           | #213  | CS-IMPL-005, 009      |
| CS-IMPL-013 | Template Engine + `:materialize` operation                                                     | #214  | CS-IMPL-010, 012      |
| CS-IMPL-014 | Template-from-Workflow Extractor + round-trip property                                         | #215  | CS-IMPL-013           |

### Phase F — Catalog indexes

| ID         | Title                                                                                          | Issue | Depends on            |
|------------|------------------------------------------------------------------------------------------------|-------|-----------------------|
| CS-IMPL-015 | Activity Type Registry (namespace tier rules, register/list/get/deprecate)                     | #216  | CS-IMPL-003           |
| CS-IMPL-016 | Connector Type Registry (register/list/get/deprecate)                                          | #217  | CS-IMPL-003           |

### Phase G — Public surface

| ID         | Title                                                                                          | Issue | Depends on            |
|------------|------------------------------------------------------------------------------------------------|-------|-----------------------|
| CS-IMPL-017 | API Adapter (FastAPI REST surface, OpenAPI, healthz/readyz)                                    | #218  | CS-IMPL-004, 010, 011, 012, 013, 014, 015, 016 |
| CS-IMPL-018 | Internal RPC inbound (GetWorkflowVersion, ResolveConnectorTypeRef)                             | #219  | CS-IMPL-010, 016      |

### Phase H — Observability

| ID         | Title                                                                                          | Issue | Depends on            |
|------------|------------------------------------------------------------------------------------------------|-------|-----------------------|
| CS-IMPL-019 | Observability + audit event emission                                                           | #220  | CS-IMPL-010, 013, 014, 015, 016 |

### Phase I — Verification + docs

| ID         | Title                                                                                          | Issue | Depends on            |
|------------|------------------------------------------------------------------------------------------------|-------|-----------------------|
| CS-IMPL-020 | Unit test suite backfill + ≥90% coverage gate                                                  | #221  | CS-IMPL-017, 019      |
| CS-IMPL-021 | Integration test pass against Postgres                                                         | #222  | CS-IMPL-017, 020      |
| CS-IMPL-022 | Developer documentation — `docs/developers/catalog-api.md`                                     | #223  | CS-IMPL-017           |

### Phase J — Cross-component follow-ups

These two issues replace M1-era stubs with real cross-service integrations once the
sibling components are implemented. They are dual-labelled with the partner component.

| ID         | Title                                                                                          | Issue | Depends on            |
|------------|------------------------------------------------------------------------------------------------|-------|-----------------------|
| CS-IMPL-023 | Wire real Connector Service client (replace CS-IMPL-008 stub)                                  | #224  | CS-IMPL-008, Connector Service impl |
| CS-IMPL-024 | Wire real Auth Service integration (replace CS-IMPL-004 dev shim)                              | #225  | CS-IMPL-004, Auth Service impl |
