# Change: impl-phase-j-observability

Date: 2026-05-25
Type: component-design
Component: auth-service
Sequence: 003
GitHub Issue: #261
Status: closed

## Summary

Phase J of the M3 auth-service implementation plan (AS-IMPL-026) landed. The `custos_auth` public surface is now fully instrumented for OpenTelemetry tracing and metrics, and the call-context verifier emits the security-relevant `call-context.invalid` audit event on every failed verification. Refs #267 (M3 auth-service track).

## Before

* No telemetry module on the auth-service surface — REST handlers and internal RPC methods emitted no spans, no duration samples, no error counts. Operators had no SLO signal for the authz / authn / callctx APIs.
* No `call-context.invalid` audit emission. The call-context RPC verifier (`POST /rpc/callctx.verify`) silently returned `valid=false` on every failure branch (malformed, unknown_kid, bad_signature, expired, wrong_audience, wrong_issuer). Incident response had no audit trail for failed call-context verifications.
* `audit.py` had no helper or event-type constant for the `call-context.invalid` event documented in the design's audit-event registry.

## After

### `custos_auth._telemetry` (new)

Public OTel surface mirroring catalog-service's pattern. Imports `opentelemetry-api` only — the default no-op providers mean importing `custos_auth` without an SDK installed is safe. Production deployments wire the Helm-managed OTel Collector sidecar; the in-memory SDK is dev-only and exists exclusively to drive the assertions in `tests/test_telemetry.py`.

| Symbol | Purpose |
|---|---|
| `OPERATION_DURATION_MS` | Histogram `custos_auth_operation_duration_ms` (unit `ms`), labels `operation` + `outcome`. |
| `ERRORS_TOTAL` | Counter `custos_auth_errors_total`, label `kind`. |
| `observe_operation(op, outcomes=None)` | Context manager wrapping a public REST handler or RPC method. Emits a span `custos_auth.<op>` and a duration sample. On exception: maps via `outcomes`, falls back to `internal_error`, bumps `ERRORS_TOTAL` with the structured `code` slug when present. Process-control exceptions (`KeyboardInterrupt`, `SystemExit`, `GeneratorExit`) propagate unrecorded. |
| `record_error_kind(kind)` | Out-of-band counter bump for failures caught *outside* an `observe_operation` block (middleware short-circuit paths). |
| `OP_*` constants (25) | Canonical operation labels, one per REST route and RPC method on the auth surface. |

Auth-service has no internal pipeline like catalog-service's publish-stages — every public operation is a single REST handler or RPC method, so a single operation histogram suffices. There is no `observe_stage`.

### Routes instrumented

Every REST handler in `custos_auth/api/routes/*` and every RPC handler in `custos_auth/api/routes/rpc.py` is now wrapped in `with telemetry.observe_operation(...)`. The `outcomes` map per route carries the structured exception → outcome slug bindings the route raises (`{NotFound: "not_found", Conflict: "conflict", ValidationFailure: "validation_failed", InvalidRoleScope: "invalid_role_scope", Unauthenticated: "unauthenticated"}` as applicable). Stub-only handlers (the 501 `create_role` placeholder) are intentionally not wrapped — they emit no real work.

### `call-context.invalid` audit (new)

`custos_auth.audit.audit_call_context_invalid(metadata_store, *, reason, actor="system", kid=None)` files the event against the platform sentinel workspace. The payload carries:

* `reason` — one of the closed-set codes the verifier already exposes (`malformed`, `unknown_kid`, `bad_signature`, `expired`, `wrong_audience`, `wrong_issuer`).
* `kid` (optional) — the `kid` header parsed off the failing token. Public via the JWKS endpoint, so safe to log for incident response (which signing key was the caller pointing at).

**Security invariant:** the helper takes no keyword that could carry the raw token. Auditing the *reason* is what makes the event security-relevant; echoing the JWT itself would defeat the security posture the call-context scheme establishes. A regression test asserts the payload keyset is exactly `{reason, kid}`.

### Wired into the RPC verifier

`POST /rpc/callctx.verify` now emits one `call-context.invalid` row per failed verification, with `record_error_kind(f"callctx.{reason}")` bumping `custos_auth_errors_total` alongside. The eight invalid return sites (malformed, unknown_kid, bad_signature, expired, wrong_audience, wrong_issuer, plus the two fall-throughs) all funnel through a single `_invalid(reason, *, kid=None)` closure so the audit-write and metric-bump are guaranteed to fire on every failure path.

### Closed-set operation labels (25)

REST (20): `auth.verify`, `authz.verify_and_authorize`, `jwks.get`, `oidc.callback`, `principal.get_me`, `principal.disable`, `role_binding.grant`, `role_binding.revoke`, `role.list`, `permission.list`, `service_account.create`, `service_token.issue`, `service_token.list`, `service_token.revoke`, `service_token.revoke_all`, `tenant.create`, `tenant.list`, `workspace.create`, `workspace.list`, `workspace.get`.

RPC (5): `rpc.authn_verify_token`, `rpc.authz_authorize`, `rpc.authz_verify_and_authorize`, `rpc.callctx_sign`, `rpc.callctx_verify`.

Adding a new public route or RPC method requires adding a matching `OP_*` constant so the histogram labelset stays bounded.

## Impact

* **Auth-service**: every public REST + RPC entry point now emits a span and a duration sample. Dashboards built against `custos_auth_operation_duration_ms{operation="<slug>", outcome="<slug>"}` and `custos_auth_errors_total{kind="<slug>"}` are unblocked.
* **Operability**: failed call-context verifications are now visible in the audit trail — incident response can scope the blast radius of a key compromise or a misconfigured caller without having to scrape application logs.
* **OTel runtime cost**: zero at import time when no SDK is configured (no-op providers); cost is paid only when a collector is wired up.
* **Downstream services**: the verifier helper library (AS-IMPL-019) consumed by every other component is *not* affected — it remains a thin client of `POST /rpc/callctx.verify`, and the new audit emission lives server-side.
* **Phase K (#262 / #263 / #264)** is unblocked: the coverage gate (#262) can now assert on `_telemetry.py`, and the developer-docs work (#264) can document the operation-label catalogue.

## Verified

* 550 / 550 tests pass (15 new tests across `tests/test_telemetry.py`, `tests/test_audit.py`, `tests/test_rpc.py`).
* 100.0 % coverage on `src/custos_auth/_telemetry.py` (gate: 90 %).
* `ruff check`, `ruff format --check`, and `mypy --python-version 3.11` clean.
* Importing `custos_auth` without `opentelemetry-sdk` installed yields no-op providers (covered by the `opentelemetry-api`-only default behaviour — no test boots an SDK without explicit opt-in).

## Related Requirements

REQ-051 (audit trail), REQ-052 (telemetry / SLO signals), REQ-059 (call-context integrity).

## Related Change Records

* `design/components/auth-service/changes/2026-05-25-002-impl-phase-h-oidc.md` — Phase H (OIDC) shipped the audit-event extensions this record builds on.
* `design/components/catalog-service/changes/` — catalog-service's `_telemetry.py` is the template this module mirrors.

