# TODOs: Activity Runtime Manager

Last Updated: 2026-06-02

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
