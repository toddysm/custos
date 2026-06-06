# API Gateway TODOs

Last Updated: 2026-06-05 (AGW-IMPL implementation plan derived + 21 tasks filed under tracker [#732](https://github.com/toddysm/custos/issues/732))

## Open

- [ ] Specify the per-component thin client package layout (`custos-<service>-routes`) that the gateway imports for route + Pydantic model registration.
- [ ] Define the OpenAPI extension schema (`x-custos-required-permission`, `x-custos-idempotent`) and pin its semver.
- [ ] Decide whether the OIDC device-code landing page is server-rendered by the gateway or a redirect into the Web UI (M1 vs M2).
- [ ] Specify the coordinated rate-limiter design for M2 (Dapr state-backed or Redis).
- [ ] Specify the multi-region routing model (M2+).

## Implementation — API Gateway (COMP-001)

Full M1 gateway implementation, packaged at `src/services/api-gateway/` (Python package `custos_gateway`). Derived fresh from [`design.md`](design.md) into [`implementation-plan.md`](implementation-plan.md); filed as 21 tasks under tracker [#732](https://github.com/toddysm/custos/issues/732). Mirrors `trigger-service` conventions; no COMP-008 (SPL) delta required (`IdempotencyRecord` / `DeviceCodeSession` already exist). Device-code routes ship as a `503` stub per the design M1 note. The conformance suite TODO is absorbed by AGW-IMPL-020.

### Phase A — Scaffold & foundations

- [x] AGW-IMPL-001 (#711): Scaffold custos-gateway package + CI gate.
- [x] AGW-IMPL-002 (#712): Settings + create_app skeleton + health probes.
- [x] AGW-IMPL-003 (#713): Error envelope + taxonomy + correlation-id middleware.

### Phase B — Auth delegation & call-context minting

- [x] AGW-IMPL-004 (#714): AuthServiceClient over Dapr service invocation.
- [x] AGW-IMPL-005 (#715): AuthN/AuthZ middleware + per-route required permission.
- [x] AGW-IMPL-006 (#716): Workspace Resolver.
- [x] AGW-IMPL-007 (#717): Call-Context Minter.
- [x] AGW-IMPL-008 (#718): Startup permission validation.

### Phase C — Cross-cutting write-path middleware

- [x] AGW-IMPL-009 (#719): Idempotency Coordinator.
- [x] AGW-IMPL-010 (#720): Rate Limiter.
- [x] AGW-IMPL-011 (#721): Request Validator.

### Phase D — Routing, webhook & device-code

- [x] AGW-IMPL-012 (#722): Downstream Router + Response Shaper.
- [x] AGW-IMPL-013 (#723): Route registry — full M1 contract set.
- [x] AGW-IMPL-014 (#724): Webhook Pass-through.
- [F] AGW-IMPL-015 (#725): Device-Code Session Manager (M1 503 stub).

### Phase E — App wiring, OpenAPI, observability

- [F] AGW-IMPL-016 (#726): Full create_app wiring.
- [F] AGW-IMPL-017 (#727): OpenAPI 3.1 emission.
- [F] AGW-IMPL-018 (#728): OTel observability.

### Phase F — Verification & docs

- [F] AGW-IMPL-019 (#729): Helm subchart wiring.
- [F] AGW-IMPL-020 (#730): Unit + integration + conformance suite.
- [F] AGW-IMPL-021 (#731): Developer documentation.

Tracker: #732 — `AGW-IMPL-000-API-GATEWAY`.

## Closed

_(none yet)_
