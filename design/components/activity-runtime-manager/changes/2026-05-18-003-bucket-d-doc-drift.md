# Change: bucket-d-doc-drift

Date: 2026-05-18
Type: component-design
Component: activity-runtime-manager
Sequence: 003
GitHub Issues: #85, #92, #93
Status: open

## Summary

Bucket D of the design-inconsistency cleanup. Three pre-existing decisions had drifted out of the ARM design:

1. **#85 (INCON-023)** — Connector Service change 007 (2026-05-17) introduced `/custos/in/sidecar-token` as the bootstrap token for sidecar API authentication, but the Activity Contract v1 filesystem layout table in the ARM design did not list it. Activity authors reading only the ARM design could not discover the path.
2. **#92 (INCON-030)** — REQ-015 (WebAssembly module actions) is M4+ in the requirements milestone table; the ARM design said `runtime.kind: wasm` was "deferred to M3/M4+", permitting M3 delivery.
3. **#93 (INCON-031)** — `runtime.kind: hyperlight` was reserved in the locked Activity Manifest v1 enum without any backing requirement (REQ-001 .. REQ-081 contain no Hyperlight / micro-VM entry).

This change is purely doc-side: it brings the ARM design into alignment with the requirements plan and the Connector Service sidecar-token contract. No new contract decisions are made.

## Before

- The Activity Contract v1 filesystem layout table (§ Activity Contract v1) listed `/custos/in/inputs.json`, `/custos/in/ctx.json`, and `/custos/in/secrets/<connector-name>/<key>` — but not `/custos/in/sidecar-token`. An activity author would not know how the sidecar token is delivered without cross-reading connector-service change 007.
- `spec.runtime.kind` (§ Activity Manifest v1) read: *"`oci-container` in v1. `http`, `wasm`, `hyperlight` reserved for later milestones."* The Deferred-to-later section read: *"`runtime.kind: http | wasm | hyperlight`: deferred to M3/M4+."* TODO-007 mentioned "HTTP/WASM/Hyperlight later".
- The intro prose for the file-based Activity Contract said "OCI container today; HTTP, WASM later" (no milestone, no Hyperlight — internally consistent but downstream of the conflicts above).

## After

- **`/custos/in/sidecar-token` row added** to the Activity Contract v1 filesystem layout table. Captures: ARM-written, dedicated tmpfs mount with permissions `0400`, scope `(runId, stepId, attempt)`, revoked on completion/retry/cancellation, required `Custos-Sidecar-Token` header on every sidecar API request. Cross-references connector-service change 007 as the authoritative contract.
- **WASM pinned to M4+.** `runtime.kind` reservation now reads `http` and `wasm` only (no `hyperlight`). The Deferred-to-later bullet now reads: *"`http` deferred to M3 (REQ-014); `wasm` deferred to M4+ (REQ-015)."* Aligned with the requirements milestone table.
- **`hyperlight` removed from the enum** and from TODO-007 and the Deferred-to-later bullet. The locked Activity Manifest v1 no longer reserves a runtime kind that has no backing requirement. If micro-VM isolation becomes a real requirement in a later cycle, a new requirement will be filed and the enum re-extended at that point.
- Header bumped: Version 1 → 2; Change History row added.

## Impact

- Activity authors can now build sidecar-API-calling activities purely from the ARM design without spelunking through connector-service change records.
- M3 implementation planning for ARM is unambiguous: HTTP runtime is M3 scope; WASM is not. Removes a planning-time foot-gun where someone could have started WASM in M3 citing the old "M3/M4+" phrasing.
- The Activity Manifest v1 enum no longer carries a speculative `hyperlight` value. Schema is tighter; a future Hyperlight introduction will go through requirements first.
- No code changes (no service implementation exists yet). No changes to any other component design.

## Files changed

- `design/components/activity-runtime-manager/design.md`
- `design/components/activity-runtime-manager/changes/2026-05-18-003-bucket-d-doc-drift.md` (this file)

## Related Change Records

- `design/components/connector-service/changes/2026-05-17-007-sidecar-secret-token-api-contract.md` — originating contract for `/custos/in/sidecar-token`.
