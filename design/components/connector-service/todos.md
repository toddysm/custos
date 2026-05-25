# Connector Service TODOs

Last Updated: 2026-05-25

## Open

- [ ] Define connector test harness and conformance criteria.
- [ ] Keep example manifests synchronized with schema updates.

## Closed

- [x] Define strict JSON schema for ConnectorManifest v1 fields and validation errors.
- [x] Define sidecar secret/token API contract (request/response, auth, lease binding, refresh). Closed 2026-05-17 — see `design.md` § Secret and Token Flow to Activities.
- [x] Define pull cursor model and dedup key strategy for trigger streams. Closed 2026-05-17 — see `design.md` § Pull Cursor Model. Dedup keys remain Trigger Service's responsibility; Connector Service contributes the normative `eventId` emission rule.
- [x] Specify capability namespace governance and compatibility policy. Closed 2026-05-17 — see `design.md` § Capabilities and Events → Namespace governance and `design/architecture/capabilities.md`.
- [x] Finalize fallback tag naming for manifest discovery and digest normalization algorithm. Closed 2026-05-17 — see `design.md` § Fallback tag naming. v1 locks sha256-only; tag format is algorithm-agnostic so sha512/others can be added in M2+ behind a scheme version bump if length budget requires it.
- [x] Define lease expiry and revocation behavior for running activities. Closed 2026-05-17 — see `design.md` § Operator Admin Surface and the expanded § Revocation with sidecar control-channel API. Operator surface covers single/instance/run revoke selectors, pause/resume of pull loops, live-state vs audit-history split, and permission model.

## Implementation

Implementation plan for the Connector Service (COMP-005) is tracked under meta-issue
**#318 (CONN-IMPL-000)** with 34 child issues on the `design/impl-20260525-connector-service`
branch. Issues are organised into thirteen phases. Within a phase, work can proceed in parallel;
cross-phase dependencies are noted explicitly. #318 stays open until every child below
— including the Phase M cross-component follow-up — is closed.

### Phase A — Foundations

| ID            | Title                                                                                          | Issue | Depends on            |
|---------------|------------------------------------------------------------------------------------------------|-------|-----------------------|
| CONN-IMPL-001 | Scaffold connector-service Python package + CI gate                                            | #284  | —                     |
| CONN-IMPL-002 | Wire connector-service Helm subchart (env vars, ConfigMap, ExternalSecret, Service, Deployment)| #285  | CONN-IMPL-001         |

### Phase B — Persistence + identity

| ID            | Title                                                                                          | Issue | Depends on            |
|---------------|------------------------------------------------------------------------------------------------|-------|-----------------------|
| CONN-IMPL-003 | SPL provider wiring (CatalogStoreProvider + MetadataStoreProvider) + schema-revision startup gate | #286  | CONN-IMPL-001         |
| CONN-IMPL-004 | Call-context middleware + permission enforcement                                               | #287  | CONN-IMPL-001         |

### Phase C — Manifest & plugin discovery

| ID            | Title                                                                                          | Issue | Depends on            |
|---------------|------------------------------------------------------------------------------------------------|-------|-----------------------|
| CONN-IMPL-005 | ConnectorManifest v1 JSON Schema validator                                                     | #288  | CONN-IMPL-001         |
| CONN-IMPL-006 | Manifest normalizer + digest computation                                                       | #289  | CONN-IMPL-005         |
| CONN-IMPL-007 | OCI Referrers API discovery + fallback tag resolver                                            | #290  | CONN-IMPL-005, 006    |

### Phase D — Connector Type Registry

| ID            | Title                                                                                          | Issue | Depends on            |
|---------------|------------------------------------------------------------------------------------------------|-------|-----------------------|
| CONN-IMPL-008 | Plugin Loader — register/list/get/deprecate ConnectorTypeVersion + identity-category derivation | #291  | CONN-IMPL-003, 006, 007 |
| CONN-IMPL-009 | Capability namespace governance + compatibility policy + deprecation flow                      | #292  | CONN-IMPL-008         |
| CONN-IMPL-010 | Event-block validation (`events.delivery`, `events.produced`, `events.pull` cursor encoding)   | #293  | CONN-IMPL-005, 008    |

### Phase E — Connector Instance lifecycle

| ID            | Title                                                                                          | Issue | Depends on            |
|---------------|------------------------------------------------------------------------------------------------|-------|-----------------------|
| CONN-IMPL-011 | ConnectorInstance store + workspace-scoped CRUD                                                | #294  | CONN-IMPL-003, 008    |
| CONN-IMPL-012 | Config Validator (per-kind `target.config` + credentials shape + capability availability)      | #295  | CONN-IMPL-008, 011    |
| CONN-IMPL-013 | Activation state (enable/disable) + health probing + `:force-health-check` admin               | #296  | CONN-IMPL-011, 012    |

### Phase F — Plugin Runtime + identity resolution

| ID            | Title                                                                                          | Issue | Depends on            |
|---------------|------------------------------------------------------------------------------------------------|-------|-----------------------|
| CONN-IMPL-014 | Plugin Runtime adapter (OCI-image plugin invocation, `bind/listen/health` hooks)               | #297  | CONN-IMPL-008         |
| CONN-IMPL-015 | Identity resolvers per category (KMS-backed, workload, federated)                              | #298  | CONN-IMPL-008, 011    |

### Phase G — Context Binder + Lease Manager

| ID            | Title                                                                                          | Issue | Depends on            |
|---------------|------------------------------------------------------------------------------------------------|-------|-----------------------|
| CONN-IMPL-016 | Context Binder — `BindForStep` RPC (capability coverage + identity resolution + ConnectorContexts) | #299  | CONN-IMPL-011, 012, 014, 015 |
| CONN-IMPL-017 | Lease Manager core (stable `leaseId`, TTL precedence, concurrent-lease cap=16)                 | #300  | CONN-IMPL-016         |
| CONN-IMPL-018 | Lease audit emission (issued/refreshed/released/expired/revoked/denied/revoke-requested)       | #301  | CONN-IMPL-017         |

### Phase H — Secret Bridge Sidecar

| ID            | Title                                                                                          | Issue | Depends on            |
|---------------|------------------------------------------------------------------------------------------------|-------|-----------------------|
| CONN-IMPL-019 | Sidecar UDS server (`/v1/token`, `/refresh`, `/release`) + bootstrap-token verification        | #302  | CONN-IMPL-017         |
| CONN-IMPL-020 | Sidecar control-channel HTTPS server (`/sidecar-admin/v1/revoke`) with mTLS                    | #303  | CONN-IMPL-019         |
| CONN-IMPL-021 | Sidecar container image + standalone integration test harness                                  | #304  | CONN-IMPL-019, 020    |

### Phase I — Pull Cursor + Listen Manager

| ID            | Title                                                                                          | Issue | Depends on            |
|---------------|------------------------------------------------------------------------------------------------|-------|-----------------------|
| CONN-IMPL-022 | Pull cursor lifecycle (initial write, at-least-once commit, lease claim, encoding-mismatch halt) | #305  | CONN-IMPL-010, 011    |
| CONN-IMPL-023 | Pull-loop scheduler (per-instance ticks, ≥10s interval, single-writer lease)                   | #306  | CONN-IMPL-022, 014    |
| CONN-IMPL-024 | Cursor admin operations (`GET /cursor`, `:rewind`, `pull-loop:pause`/`:resume`) + audit        | #307  | CONN-IMPL-022, 023    |
| CONN-IMPL-025 | Listen Manager (push receivers + pull fan-out) + Event Normalizer + `eventId` rule enforcement | #308  | CONN-IMPL-014, 022, 023 |

### Phase J — Public surface (REST + Internal RPCs)

| ID            | Title                                                                                          | Issue | Depends on            |
|---------------|------------------------------------------------------------------------------------------------|-------|-----------------------|
| CONN-IMPL-026 | API Adapter — FastAPI REST surface + OpenAPI + healthz/readyz                                  | #309  | CONN-IMPL-004, 008, 011, 013, 016, 017, 024 |
| CONN-IMPL-027 | Internal RPCs inbound (`BindForStep`, `ValidateConnector`, `SubscribeEvents`, `RefreshLease`)  | #310  | CONN-IMPL-016, 017, 025 |
| CONN-IMPL-028 | Operator revoke flows + live-state fan-out aggregation                                         | #311  | CONN-IMPL-017, 018, 020, 026 |

### Phase K — Observability + audit

| ID            | Title                                                                                          | Issue | Depends on            |
|---------------|------------------------------------------------------------------------------------------------|-------|-----------------------|
| CONN-IMPL-029 | Observability + audit emission for every event in design + OTel spans + Prometheus metrics    | #312  | CONN-IMPL-016, 017, 022, 025, 028 |

### Phase L — Verification + docs

| ID            | Title                                                                                          | Issue | Depends on            |
|---------------|------------------------------------------------------------------------------------------------|-------|-----------------------|
| CONN-IMPL-030 | Unit test suite + ≥90% coverage gate                                                           | #313  | CONN-IMPL-026, 027, 029 |
| CONN-IMPL-031 | Integration test pass against Postgres + OCI registry fixture + sample plugins                 | #314  | CONN-IMPL-026, 030, 032 |
| CONN-IMPL-032 | Sample reference plugins (`oci-registry` + `slack-notifier` sink) under `src/libs/connector-plugins/` | #315  | CONN-IMPL-014, 025    |
| CONN-IMPL-033 | Developer documentation — `docs/developers/connector-plugin-author.md` + update `connections-api.md` | #316  | CONN-IMPL-026         |

### Phase M — Cross-component follow-up

This issue replaces an M1-era stub with a real cross-service integration once Connector
Service lands. It is dual-labelled with `component:catalog-service`.

| ID            | Title                                                                                          | Issue | Depends on            |
|---------------|------------------------------------------------------------------------------------------------|-------|-----------------------|
| CONN-IMPL-034 | Wire real Connector Service client into Catalog Service (closes #224 / CS-IMPL-023)            | #317  | CONN-IMPL-026, 027, Catalog Service CS-IMPL-008 |
