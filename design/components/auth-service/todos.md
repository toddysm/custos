# Auth Service TODOs

Last Updated: 2026-05-23

## Open

- [ ] Define the exact JWT claim shape for signed call contexts (claim names, audience, signing algorithm — proposed EdDSA).
- [ ] Specify the OIDC issuer config schema for `CUSTOS_AUTH_OIDC_ISSUERS` (per-issuer provisioning policy options). _**Resolved** 2026-05-25 via Phase H (AS-IMPL-020 #255). Schema documented in `changes/2026-05-25-002-impl-phase-h-oidc.md`._
- [ ] Specify the **GitHub OIDC preset** (default issuer URL, JWKS endpoint, audience claim shape, GitHub Actions `aud`/`sub`/`repository` claim handling for workload tokens, human-login vs workload-token distinction — **M1, P0**). _**Resolved** 2026-05-25 via Phase H (AS-IMPL-021 #256)._
- [ ] Specify the **Azure Entra ID OIDC preset** (default authority URL, tenant-vs-multitenant audience handling, group-claim → role-binding mapping rules — **M1, P0**). _**Resolved** 2026-05-25 via Phase H (AS-IMPL-022 #257)._
- [ ] Cross-region replication strategy for Auth Service state (multi-region M2+).
- [ ] Custom role authoring API (M2+).
- [ ] SPIFFE/SPIRE cutover plan (M2/M3).

## Closed

_(none yet)_

## Implementation

Implementation plan for Auth Service (COMP-002) is tracked under meta-issue
**#267 (AS-IMPL-000)** with child issues on the `design/impl-20260524-auth-service`
branch. Issues are organised into eleven phases. Within a phase, work can proceed in parallel;
cross-phase dependencies are noted explicitly. The tracking issue stays open until every child
below — including the Phase K cross-component follow-ups — is closed.

The implementation order honours the design's **M1 implementation note**: M1 lands the
`AuthStoreProvider` migrations, the tenancy + principal data model, the permission/role
registry tables, service tokens (REQ-035), the signed-JWT internal call-context (used by
every other service), and the public surface. OIDC verifiers + presets + active RBAC
enforcement at the gateway land in **M3** per REQ-034 / REQ-056 / REQ-057 / REQ-058; their
phases below are scoped to M3 and explicitly marked.

### Phase A — Foundations (M1)

| ID          | Title                                                                                            | Issue | Depends on            |
|-------------|--------------------------------------------------------------------------------------------------|-------|-----------------------|
| AS-IMPL-001 | Scaffold auth-service Python package + CI gate                                                   | #236 | —                     |
| AS-IMPL-002 | Wire auth-service Helm subchart (env vars, ConfigMap, ExternalSecret for call-context key)       | #237 | AS-IMPL-001           |

### Phase B — SPL `AuthStoreProvider` (M1)

| ID          | Title                                                                                            | Issue | Depends on            |
|-------------|--------------------------------------------------------------------------------------------------|-------|-----------------------|
| AS-IMPL-003 | SPL `AuthStoreProvider` interface + Postgres provider migrations (Tenant/Workspace/Principal/OidcIdentity/ServiceToken/Role/Permission/RoleBinding) | #238 | AS-IMPL-001           |
| AS-IMPL-004 | SPL provider wiring + schema-revision startup gate                                               | #239 | AS-IMPL-003           |

### Phase C — Tenancy + principal model (M1)

| ID          | Title                                                                                            | Issue | Depends on            |
|-------------|--------------------------------------------------------------------------------------------------|-------|-----------------------|
| AS-IMPL-005 | Tenant + Workspace data model + management endpoints                                             | #240 | AS-IMPL-003           |
| AS-IMPL-006 | Principal model (User / ServiceAccount discriminator)                                            | #241 | AS-IMPL-003           |
| AS-IMPL-007 | OidcIdentity binding store (data model + admin endpoints; verifier wiring deferred to Phase H)   | #242 | AS-IMPL-006           |

### Phase D — Permission + role registry (M1)

| ID          | Title                                                                                            | Issue | Depends on            |
|-------------|--------------------------------------------------------------------------------------------------|-------|-----------------------|
| AS-IMPL-008 | `permissions.yaml` loader + Permission table upsert + startup refusal on undeclared references   | #243 | AS-IMPL-003           |
| AS-IMPL-009 | Built-in v1 roles (viewer/author/operator/admin + tenant.admin + platform.admin) hard-coded      | #244 | AS-IMPL-008           |
| AS-IMPL-010 | RoleBinding store + scope-rule enforcement (`InvalidRoleScope` on mismatch)                      | #245 | AS-IMPL-005, 006, 009 |

### Phase E — Authorization engine (M1)

| ID          | Title                                                                                            | Issue | Depends on            |
|-------------|--------------------------------------------------------------------------------------------------|-------|-----------------------|
| AS-IMPL-011 | `authorize(principalId, permission, workspaceId)` decision engine + `authz.decision` audit       | #246 | AS-IMPL-008, 009, 010 |
| AS-IMPL-012 | Authz decision cache (60s TTL) + `custos.auth.binding-changed` pub/sub invalidation              | #247 | AS-IMPL-011           |

### Phase F — Service tokens (M1, REQ-035 — the M1 P1 functionality)

| ID          | Title                                                                                            | Issue | Depends on            |
|-------------|--------------------------------------------------------------------------------------------------|-------|-----------------------|
| AS-IMPL-013 | Service-account CRUD + token mint (bcrypt-hash storage, plaintext returned once, default 90d TTL) | #248 | AS-IMPL-006           |
| AS-IMPL-014 | Token verify + 30s authn cache + `custos.auth.token-revoked` immediate eviction                  | #249 | AS-IMPL-013           |
| AS-IMPL-015 | Token revoke (single + all-for-SA) + `token.revoked` audit                                       | #250 | AS-IMPL-013           |
| AS-IMPL-016 | Token expiry housekeeping job + `token.expired` audit + `verifyAndAuthorize` gateway helper      | #251 | AS-IMPL-014, 011      |

### Phase G — Internal call-context (M1, used by every other service)

| ID          | Title                                                                                            | Issue | Depends on            |
|-------------|--------------------------------------------------------------------------------------------------|-------|-----------------------|
| AS-IMPL-017 | Call-context signer (EdDSA JWT) + Dapr Secrets key resolution                                    | #252 | AS-IMPL-002, 006      |
| AS-IMPL-018 | JWKS endpoint + 7-day rotation + 2× rotation overlap                                             | #253 | AS-IMPL-017           |
| AS-IMPL-019 | Verifier helper library shipped to every other component (`callctx.verify(metadata)`)            | #254 | AS-IMPL-018           |

### Phase H — OIDC (M3, REQ-034/056/057/058 — code paths present but disabled in M1 deployment)

| ID          | Title                                                                                            | Issue | Depends on            |
|-------------|--------------------------------------------------------------------------------------------------|-------|-----------------------|
| AS-IMPL-020 | Generic OIDC verifier (issuer config schema, JWKS fetch+cache, audience/iss claim enforcement)   | #255 | AS-IMPL-007           |
| AS-IMPL-021 | **GitHub OIDC preset** (human login + Actions workload tokens; `repository`/`sub` claim handling) | #256 | AS-IMPL-020           |
| AS-IMPL-022 | **Azure Entra ID OIDC preset** (tenant vs multi-tenant audience; group-claim → role-binding map) | #257 | AS-IMPL-020           |
| AS-IMPL-023 | OIDC provisioning policy (zero-binding user creation) + `oidc.identity-linked` audit             | #258 | AS-IMPL-020           |

### Phase I — Public surface (M1)

| ID          | Title                                                                                            | Issue | Depends on            |
|-------------|--------------------------------------------------------------------------------------------------|-------|-----------------------|
| AS-IMPL-024 | API Adapter (FastAPI REST surface, OpenAPI, healthz/readyz; OIDC endpoints stubbed until Phase H) | #259 | AS-IMPL-005, 010, 013, 016, 019 |
| AS-IMPL-025 | Internal RPC inbound (`authn.verifyToken`, `authz.authorize`, `authz.verifyAndAuthorize`, `callctx.sign`, `callctx.verify`) | #260 | AS-IMPL-016, 019 |

### Phase J — Observability + audit (M1)

| ID          | Title                                                                                            | Issue | Depends on            |
|-------------|--------------------------------------------------------------------------------------------------|-------|-----------------------|
| AS-IMPL-026 | Observability + audit event emission (`authz.decision`, `authn.*`, `token.*`, `principal.*`, `role-binding.*`, `tenant.*`, `workspace.*`, `call-context.invalid`) | #261 | AS-IMPL-011, 016, 018 |

### Phase K — Verification + docs (M1)

| ID          | Title                                                                                            | Issue | Depends on            |
|-------------|--------------------------------------------------------------------------------------------------|-------|-----------------------|
| AS-IMPL-027 | Unit test suite + ≥90% coverage gate                                                             | #262 | AS-IMPL-024, 026      |
| AS-IMPL-028 | Integration test pass against Postgres + signed call-context round-trip                          | #263 | AS-IMPL-024, 027      |
| AS-IMPL-029 | Developer documentation — `docs/developers/auth-api.md`                                          | #264 | AS-IMPL-024           |

### Phase L — Cross-component follow-ups

These issues retire dev-shim call-context middleware in sibling components once the verifier
library lands. They are dual-labelled with the partner component.

| ID          | Title                                                                                            | Issue | Depends on            |
|-------------|--------------------------------------------------------------------------------------------------|-------|-----------------------|
| AS-IMPL-030 | Replace catalog-service call-context dev shim with real verifier (closes #225)                   | #265 | AS-IMPL-019           |
| AS-IMPL-031 | SPIFFE/SPIRE workload identity cutover plan + `CUSTOS_AUTH_INTERNAL_IDENTITY_MODE=spiffe` (M3, REQ-059) | #266 | AS-IMPL-019      |
