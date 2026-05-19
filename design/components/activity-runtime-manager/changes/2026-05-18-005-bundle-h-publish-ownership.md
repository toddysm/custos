# Change: bundle-h-publish-ownership

Date: 2026-05-18
Type: component-design
Component: activity-runtime-manager
Sequence: 005
GitHub Issue: #105
Status: open

## Summary

Bundle H, issue #105 half (ARM side). The ARM design's § Publishing flow showed `CLI->>Cat: POST /catalog/activities { manifest, referrerRef }` — a direct CLI → Catalog call — while the Catalog design separately named ARM as the writer via the same path. Both versions sidestepped the API Gateway. This change makes the publishing flow consistent with the canonical model: the Author CLI publishes through the API Gateway via `POST /v1/workspaces/{ws}/activity-types`. ARM is runtime-only; it does not write to Catalog and does not proxy CLI registration calls. The diagram in the ARM design is now explicitly labeled as cross-component context — the authoritative version lives in the Catalog design.

## Before

§ Publishing flow:

```
sequenceDiagram
    participant Author as Activity Author
    participant CLI as custos CLI
    participant Reg as OCI Registry
    participant Cat as Catalog Service

    Author->>CLI: custos activity publish manifest.json
    ...
    CLI->>Cat: POST /catalog/activities { manifest, referrerRef }
    Cat->>Cat: validate, dedup by (namespace, type, version)
    Cat->>Reg: verify Referrer exists at digest (proof of publish)
    Cat-->>CLI: 201 Created
```

No prose disclaimer that ARM is not on this path.

## After

- § Publishing flow opens with a disclaimer: "The Activity Runtime Manager does **not** participate in the activity-publishing path. ARM is runtime-only — it consumes published `ActivityTypeVersion` records from Catalog at step-execution time. The authoritative publish flow is owned by the Author CLI and goes through the API Gateway to the Catalog Service. The diagram is reproduced here for cross-component context only; see `design/components/catalog-service/design.md` § Operation: Register Activity Type for the authoritative version."
- Diagram now shows an `API Gateway` participant between CLI and Catalog: `CLI->>GW: POST /v1/workspaces/{ws}/activity-types ...`, `GW->>Cat: forward (signed call-context)`, then Catalog's existing validation steps, with the 201 round-tripping through the gateway.
- Header bumped: Version 3 → 4; Change History row added.

The runtime-side ARM contracts (`ScheduleActivity`, `RefreshLease`, sidecar bootstrap token minting per § Secret and Token Flow to Activities) are not touched. The `Activity Catalog` resolver section (which ARM uses at runtime to fetch `ActivityTypeVersion` records by reference) remains the read-side counterpart of this publish path and is unchanged.

## Impact

- ARM, Catalog, API Gateway, and the Author CLI now agree on a single publish path.
- No ARM runtime behavior changes (no `ScheduleActivity` change, no sidecar contract change, no Connector Service interaction change).
- The "Activity Catalog" trust boundary for manifest signature verification remains deferred to M2+ per ARM TODO-002 — this change does not address signing, only writer identity and routing.

## Files changed

- `design/components/activity-runtime-manager/design.md`
- `design/components/activity-runtime-manager/changes/2026-05-18-005-bundle-h-publish-ownership.md` (this file)

## Related Change Records

- `design/components/catalog-service/changes/2026-05-18-002-bundle-h-publish-ownership.md`
- `design/components/api-gateway/changes/2026-05-18-001-bundle-h-publish-ownership.md`
