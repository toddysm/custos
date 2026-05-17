# TODOs: Activity Runtime Manager

Last Updated: 2026-05-16

## Open

- [ ] TODO-002: Manifest signing (cosign-signed Referrer with Catalog verification) — deferred to M2+
- [ ] TODO-003: Per-artifact content schema validation (e.g. CycloneDX schema URL) — deferred to M2
- [ ] TODO-004: `spec.secrets[]` for standalone secret slots — deferred to M2 alongside REQ-019 attestation creation
- [ ] TODO-005: Short-form (non-fully-qualified) activity references — deferred to a later milestone
- [ ] TODO-006: Decide sandbox technology per REQ-039 (gVisor, Kata-CLH, Kata-MSHV, Kata-FC, runc+seccomp, or Kubernetes Jobs only) — manifest surface locked; concrete RuntimeClass set and cluster-default tier still pending
- [ ] TODO-007: Specify Runtime Driver dispatcher contract; OCI Container Driver for v1, HTTP/WASM/Hyperlight later
- [ ] TODO-008: Sub-module deep dive (Scheduler, I/O Broker, Artifact Store Client, Log Streamer, Result Mapper, Resource Limiter, Secret Injector)
- [ ] TODO-009: Finalize platform event taxonomy mapping for activity lifecycle events with Observability
- [ ] TODO-010: Lock the canonical built-in `policy-eval@1` activity manifest (filter/gate modes) as the reference for the Layer-3 filter pattern

## Closed

- [x] TODO-001: Define error envelope details and exit-code mapping per ADR-008 (closed 2026-05-16)
- [x] TODO-002 (original): Define activity manifest v1 (schema, versioning, OCI Referrers-based discovery) — closed 2026-05-16
