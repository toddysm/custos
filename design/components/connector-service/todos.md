# Connector Service TODOs

Last Updated: 2026-05-17

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
