# Connector Service TODOs

Last Updated: 2026-05-17

## Open

- [ ] Finalize fallback tag naming for manifest discovery and digest normalization algorithm.
- [ ] Define lease expiry and revocation behavior for running activities.
- [ ] Define connector test harness and conformance criteria.
- [ ] Keep example manifests synchronized with schema updates.

## Closed

- [x] Define strict JSON schema for ConnectorManifest v1 fields and validation errors.
- [x] Define sidecar secret/token API contract (request/response, auth, lease binding, refresh). Closed 2026-05-17 — see `design.md` § Secret and Token Flow to Activities.
- [x] Define pull cursor model and dedup key strategy for trigger streams. Closed 2026-05-17 — see `design.md` § Pull Cursor Model. Dedup keys remain Trigger Service's responsibility; Connector Service contributes the normative `eventId` emission rule.
- [x] Specify capability namespace governance and compatibility policy. Closed 2026-05-17 — see `design.md` § Capabilities and Events → Namespace governance and `design/architecture/capabilities.md`.
