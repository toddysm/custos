# TODOs: Activity Runtime Manager

Last Updated: 2026-06-03

## Implementation tasks (filed)

Tracker: ARM-IMPL-000 (#591). Plan: [implementation-plan.md](implementation-plan.md).

- [x] ARM-IMPL-001 (#569): Scaffold the service package
- [x] ARM-IMPL-002 (#570): Configuration & AuthZ dev-shim
- [x] ARM-IMPL-003 (#571): Activity Contract v1 types
- [x] ARM-IMPL-004 (#572): Activity Manifest v1 model + parser
- [x] ARM-IMPL-005 (#573): ActivityExecution store
- [x] ARM-IMPL-006 (#574): ArtifactRecord + Artifact Store Client
- [x] ARM-IMPL-007 (#575): Activity Resolver (real Dapr Catalog adapter)
- [x] ARM-IMPL-008 (#576): Resource Limiter
- [x] ARM-IMPL-009 (#577): I/O Broker (two-phase finalization)
- [x] ARM-IMPL-010 (#578): Secret Injector (real Connector RefreshLease adapter)
- [x] ARM-IMPL-011 (#579): Result Mapper
- [x] ARM-IMPL-012 (#580): Log Streamer
- [x] ARM-IMPL-013 (#581): RuntimeDriver Protocol + dispatcher
- [x] ARM-IMPL-014 (#582): Sandbox & isolation model
- [x] ARM-IMPL-015 (#583): OCI Container Driver — Job builder
- [F] ARM-IMPL-016 (#584): OCI Container Driver — lifecycle monitor (kind/k8s integration)
- [F] ARM-IMPL-017 (#585): Activity Scheduler
- [F] ARM-IMPL-018 (#586): RPC Adapter (real Dapr ScheduleActivity/CancelActivity)
- [F] ARM-IMPL-019 (#587): Cancel + deadline/timeout
- [F] ARM-IMPL-020 (#588): Observability
- [F] ARM-IMPL-021 (#589): Integration suite (kind/k8s)
- [F] ARM-IMPL-022 (#590): Developer docs + README

## Open

- [ ] TODO-002: Manifest signing (cosign-signed Referrer with Catalog verification) — deferred to M2+
- [ ] TODO-003: Per-artifact content schema validation (e.g. CycloneDX schema URL) — deferred to M2
- [ ] TODO-004: `spec.secrets[]` for standalone secret slots — deferred to M2 alongside REQ-019 attestation creation
- [ ] TODO-005: Short-form (non-fully-qualified) activity references — deferred to a later milestone
- [ ] TODO-009: Finalize platform event taxonomy mapping for activity lifecycle events with Observability
- [ ] TODO-010: Lock the canonical built-in `policy-eval@1` activity manifest (filter/gate modes) as the reference for the Layer-3 filter pattern

## Closed

- [x] TODO-006: Sandbox technology decided per REQ-039 — operator-configured tier→`RuntimeClass` mapping; `process`/runc guaranteed, `vm`/`microvm` Kata opt-in, gVisor omitted, no silent downgrade (closed 2026-06-02)
- [x] TODO-007: Runtime Driver dispatcher contract specified (`RuntimeDriver` Protocol selected by `runtime.kind`; OCI Container Driver for v1) (closed 2026-06-02)
- [x] TODO-008: Sub-module deep dive completed (Scheduler, Runtime Driver, OCI Container Driver, I/O Broker, Artifact Store Client, Log Streamer, Result Mapper, Resource Limiter, Secret Injector) (closed 2026-06-02)

- [x] TODO-001: Define error envelope details and exit-code mapping per ADR-008 (closed 2026-05-16)
- [x] TODO-002 (original): Define activity manifest v1 (schema, versioning, OCI Referrers-based discovery) — closed 2026-05-16
