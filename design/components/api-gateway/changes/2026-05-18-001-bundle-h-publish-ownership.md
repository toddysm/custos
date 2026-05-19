# Change: bundle-h-publish-ownership

Date: 2026-05-18
Type: component-design
Component: api-gateway
Sequence: 001
GitHub Issue: #105
Status: open

## Summary

Bundle H, issue #105 half (API Gateway side). The Catalog Service routing entry in the Route Registry already covered `/v1/workspaces/{ws}/activity-types/*` by wildcard, but the table's Notes column described that prefix as "activity/connector type registry **reads**" — implying writes went somewhere else. Catalog's `POST /v1/catalog/activities` path (the writer side) was therefore not represented in the gateway and appeared to bypass it. This change clarifies the Catalog routing entry so the `/v1/workspaces/{ws}/activity-types/*` prefix is explicitly understood to cover both reads **and** the Author CLI write path (`POST /v1/workspaces/{ws}/activity-types`), which is the canonical publish path agreed across Catalog, ARM, and CLI in the same bundle.

## Before

Route Registry, Catalog Service row (line 251):

```
| Catalog Service | /v1/workspaces/{ws}/workflows/*, /v1/workspaces/{ws}/templates/*, /v1/workspaces/{ws}/activity-types/*, /v1/workspaces/{ws}/connector-types/* | Workflow and template authoring; activity/connector type registry reads. |
```

"reads" framing was misleading once the activity-type writer flipped from ARM-direct to CLI-via-gateway.

## After

- Catalog Service row Notes column updated to: "Workflow and template authoring; activity-type registry reads and writes (`POST /v1/workspaces/{ws}/activity-types` is the Author CLI publishing path — see Catalog Service design § Operation: Register Activity Type); connector type registry reads."
- Prefix list itself is unchanged — the `/v1/workspaces/{ws}/activity-types/*` wildcard already covered the new write path; this change is documentation only.
- Header bumped: Version 1 → 2; Change History row added.

## Impact

- No prefix-list change; no new route to wire in. The clarification ensures that any future reader of the gateway design understands the Author CLI publish call traverses the gateway.
- Auth Service delegation, signed call-context minting, idempotency dedup, and rate-limiting behaviors apply to the write under this prefix exactly as they apply to every other Catalog write under `/v1/workspaces/{ws}/workflows/*` and `/v1/workspaces/{ws}/templates/*` — no special-case behavior.
- The OIDC-only gating on writes is unchanged: in M1 the Author CLI publishes using an API token (REQ-035); OIDC device-code flow becomes live in M3.

## Files changed

- `design/components/api-gateway/design.md`
- `design/components/api-gateway/changes/2026-05-18-001-bundle-h-publish-ownership.md` (this file)

## Related Change Records

- `design/components/catalog-service/changes/2026-05-18-002-bundle-h-publish-ownership.md`
- `design/components/activity-runtime-manager/changes/2026-05-18-005-bundle-h-publish-ownership.md`
