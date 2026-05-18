# Change: sidecar-secret-token-api-contract

Date: 2026-05-17
Type: component-design
Component: connector-service
Sequence: 007
GitHub Issue: #57
Status: open

## Summary

Locks the normative contract for the connector sidecar secret/token API — the runtime credential path activities use to obtain short-lived material from the sidecar over a pod-local Unix domain socket. Closes the corresponding open item in `connector-service/todos.md`.

## Before

`design/components/connector-service/design.md` § Secret and Token Flow to Activities contained an 11-line description naming the sidecar approach and the lease-scope intent, with no transport, no API surface, no auth model, no TTL or refresh semantics, no concurrent-lease bound, no revocation flow, and no audit event taxonomy. `todos.md` carried an open item "Define sidecar secret/token API contract (request/response, auth, lease binding, refresh)". Open Questions listed "Sidecar API details (path, protocol, token refresh semantics, cache policy)".

This left activities — the only consumers of the API — with no contract to code against, and made it impossible for ARM to write the sidecar pod template.

## After

§ Secret and Token Flow to Activities rewritten as a normative contract specification covering:

- **Transport**: Unix domain socket at `/custos/run/connector.sock`, HTTP/1.1 over UDS, no TCP, `0600` socket permissions.
- **Authentication**: ARM-issued bootstrap token written to `/custos/in/sidecar-token` (separate file, `0400`, tmpfs), bound to `(runId, stepId, attempt)`, sent via `Custos-Sidecar-Token` header. SPIFFE/SPIRE deferred to M3 per REQ-059.
- **API surface**: three endpoints — `GET /v1/token?slot&purpose`, `POST /v1/token/refresh`, `POST /v1/token/release`. RFC 7807 problem+json errors.
- **Response envelope**: `leaseId`, `tokenType`, `token`, `expiresAt`, `scope` (echoed for defensive checks), `endpoint`, `extras`.
- **Lease lifecycle**: 10-minute default TTL; 4-level precedence (sidecar default → connector-type max → instance config → step-deadline cap); pull-based explicit refresh; stable `leaseId` across refreshes.
- **Concurrent lease cap**: 16 per `(runId, stepId, attempt)`; 429 beyond; configurable per connector-type via `credentials.maxConcurrentLeasesPerStep`.
- **Revocation**: separate ARM → sidecar control channel (not the activity-facing UDS); 410 Gone on revoked leases.
- **Audit**: six structured event kinds (`lease.issued`, `lease.refreshed`, `lease.released`, `lease.revoked`, `lease.expired`, `lease.denied`) carrying `leaseId`, scope, and `tokenType` but never the token itself (REQ-038).
- **`extras` shape**: per-connector-type opaque JSON bag in v1; typed schemas deferred to M2+.
- **Failure modes**: full table mapping 401/403/404/410/429/502/503 to permanent vs retryable for ADR-008 exit-code mapping.
- **Sidecar internal lifecycle**: new Mermaid sequence diagram showing ARM → sidecar bootstrap, activity → sidecar token acquisition + refresh + release, and step-terminal revocation.

`todos.md`: closes the sidecar-API item. `design.md` Open Questions: removes the sidecar bullet. The lease-expiry-and-revocation item stays open as a follow-up to specify operator-facing controls (admin revoke, audit query UX) — the activity-facing contract for revocation is now complete.

## Key Decisions Locked This Session

1. **UDS transport, not loopback TCP.** Pod-local only, no risk of leaking to neighbor sidecars or metadata services. JSON-over-HTTP/1.1 keeps activity client code trivial in any language.
2. **Bootstrap token in a separate file**, not a `ctx.json` field. Activities don't need to parse JSON to authenticate; the file is read-once, tmpfs, `0400`.
3. **10-minute default TTL with step-deadline hard cap.** Activities do not refresh on a schedule — they refresh only if still using the token near expiry. Most steps run sub-minute and never refresh. TTL is configurable in a 4-level precedence stack; the step deadline is always the ceiling.
4. **Pull-based explicit refresh, stable `leaseId`.** Sidecar may transparently re-mint upstream credentials between refreshes; activity caches one `leaseId`. No server-push or callback into the activity — keeps the activity-facing surface a pure pull API.
5. **16 concurrent leases per step-attempt.** Covers realistic multi-connector workloads (image-promote source+dest with pull+push = 4) with 4× headroom; catches runaway loops early via 429 without spamming the audit pipeline. Configurable per connector-type if legitimately needed.
6. **Revocation on a separate control channel (ARM → sidecar), not the activity-facing UDS.** Activity-facing API stays pure pull; on revoke, subsequent requests return 410 Gone. Activity is expected to exit promptly on 410.
7. **`extras` is open-ended JSON in v1.** Activities are already coupled to their connector type via the activity manifest; locking shapes per connector type is an M2+ hardening item that does not change the wire format.
8. **`tokenType` is open-ended string.** `bearer`, `basic`, `aws-sigv4`, `azure-sas` etc. — connector type defines its own set; sidecar does not enumerate centrally.

## Impact

- ARM pod template can now specify the sidecar container, the UDS bind-mount, and the bootstrap-token file mount without further design input.
- Activity SDK can lock its sidecar client interface against this contract.
- REQ-037 (secrets management — pluggable, never in workflow defs) and REQ-038 (audit log for every secret access) both have a concrete delivery path documented.
- Connector Service `todos.md` reduced from 7 open items to 5; component approaches `Designed` status pending the four remaining design questions (capability namespace governance, fallback tag naming, lease expiry operator UX, pull cursor model). The status flip will happen after those items land or are explicitly deferred.

## Out of Scope (Deferred)

- Operator-facing admin API to revoke leases / cursor / instance state outside of cancel-run flow (`lease expiry and revocation behavior` TODO covers this).
- Typed `extras` schemas declared in the connector-type manifest with sidecar-side validation — M2+ hardening.
- SPIFFE/SPIRE workload identity for sidecar ↔ Connector Service authentication — deferred to M3 per REQ-059. v1 uses ARM-issued bootstrap tokens and Connector Service handshake at sidecar start.
- Sidecar caching policy for token material — current contract treats every lease as a fresh upstream mint; per-instance caching for high-throughput scenarios is future work.
- Connector test harness conformance criteria for sidecar API surface — covered by the separate test harness TODO.

## Related Requirements

- REQ-037 (secrets management; never stored in workflow defs) — § Authentication + § Lease lifecycle.
- REQ-038 (audit log for every secret access) — § Audit (six event kinds).
- REQ-059 (SPIFFE/SPIRE) — explicitly deferred to M3; v1 uses bootstrap-token model.
- ADR-008 (exit codes / failure classes) — § Activity-visible failure modes maps HTTP status to retryable vs permanent.
