# Change: bundle-h-publish-ownership

Date: 2026-05-18
Type: component-design
Component: catalog-service
Sequence: 002
GitHub Issue: #105
Status: open

## Summary

Bundle H, issue #105 half (Catalog side). The Catalog design named **ARM** as the writer of activity-type metadata via `POST /v1/catalog/activities`, while the ARM design's publishing flow showed the **Author CLI** writing directly to Catalog, and the API Gateway's Catalog routing prefix `/v1/workspaces/{ws}/activity-types/*` covered only the read side. This change flips Catalog to the canonical model: the Author CLI is the writer, the write path is `POST /v1/workspaces/{ws}/activity-types` through the API Gateway, and all `/v1/catalog/activities*` paths are re-homed under `/v1/workspaces/{ws}/activity-types*`. ARM is runtime-only and does not write to or proxy Catalog.

## Before

Source-of-truth split table (line 40):

```
| Activity manifest (`ActivityManifestv1` document) | Activity Runtime Manager (`POST /catalog/activities`) | Read-side index; Catalog persists a normalized projection ... |
```

Activity Type Registry sub-module (line 75):

```
| Activity Type Registry | Read-side index of activity types and versions. **Writer is ARM** via `POST /catalog/activities`. ... |
```

REST API table (line 358):

```
| POST | `/v1/catalog/activities` | { manifest, referrerRef? } | ActivityTypeRef (201) | Register an activity type version. (Writer: ARM) |
| GET  | `/v1/catalog/activities` | filters | [ActivityTypeRef] | List activity types. |
| GET  | `/v1/catalog/activities/{namespace}/{type}@{version}` | — | ActivityTypeVersion | Fetch ... |
| POST | `/v1/catalog/activities/{namespace}/{type}@{version}:deprecate` | { reason } | 200 | Deprecate ... |
```

Operation: Register Activity Type sequence diagram showed `ARM->>API: POST /v1/catalog/activities` and prose at line 193 stated "ARM is the writer for activity-type metadata".

Operation: List Activity Types example showed `GET /v1/catalog/activities?...`.

## After

- Source-of-truth split table: writer changed to `Author CLI via API Gateway (POST /v1/workspaces/{ws}/activity-types)`.
- Activity Type Registry sub-module: "**Writer is the Author CLI** via API Gateway → Catalog (`POST /v1/workspaces/{ws}/activity-types`); ARM is runtime-only and neither writes nor proxies activity-type registrations."
- Register Activity Type sequence diagram now shows `CLI->>GW: POST /v1/workspaces/{ws}/activity-types ...`, `GW->>API: forward (signed call-context)`, etc.
- Prose at § Register Activity Type updated: "The Author CLI is the writer for activity-type metadata; Catalog receives the registration call through API Gateway and persists the normalized index entry. ARM is runtime-only and does not write to Catalog."
- Activity-type deprecate operation: path renamed to `POST /v1/workspaces/{ws}/activity-types/{ref}:deprecate`.
- List operation example: `GET /v1/workspaces/{ws}/activity-types?...`.
- REST API table: register/list/get/deprecate rows all re-homed under `/v1/workspaces/{ws}/activity-types*`; writer column now says "Author CLI via API Gateway".
- Header bumped: Version 1 → 2; Change History row added.

The connector-type registration row (`POST /v1/catalog/connector-types`) is untouched in this change — the issue is scoped to activity manifests, and Connector Service remains the writer of connector type metadata via the existing path.

## Impact

- Catalog, ARM, API Gateway, and the Author CLI now agree on a single publish path that traverses the gateway.
- API Gateway needs no new prefix entry (the `/v1/workspaces/{ws}/activity-types/*` prefix already covered both reads and writes by wildcard); the API Gateway design adds a clarifying note that the write under this prefix is the Author CLI publishing path. See the companion API Gateway change record.
- The CLI publish UX is unchanged from the user's perspective — only the upstream URL it talks to changes.
- ARM is now strictly a runtime consumer of `ActivityTypeVersion` records; its design diagram is reproduced for cross-component context but is no longer authoritative for the publish path.

## Files changed

- `design/components/catalog-service/design.md`
- `design/components/catalog-service/changes/2026-05-18-002-bundle-h-publish-ownership.md` (this file)

## Related Change Records

- `design/components/activity-runtime-manager/changes/2026-05-18-005-bundle-h-publish-ownership.md`
- `design/components/api-gateway/changes/2026-05-18-001-bundle-h-publish-ownership.md`
- `design/components/workflow-service/changes/2026-05-18-003-bundle-h-cel-parse-surface.md` (companion #100 work in the same bundle)
