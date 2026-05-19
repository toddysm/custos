---
name: bucket-e-er-cleanup
description: Trigger Service ER diagram — remove cross-service relationship to Run; ResumeSubscription now carries scalar opaque runId/stepId refs
type: component-design
---

# Change: bucket-e-er-cleanup

Date: 2026-05-18
Type: component-design
Component: trigger-service
Sequence: 003
GitHub Issue: #86
Status: open

## Summary

Bucket E of the design-inconsistency cleanup (component-side half). The Trigger Service ER diagram drew a `ResumeSubscription ||--|| Run : "waits for"` relationship, but `Run` is owned by the Workflow Service (COMP-003) and is not part of the Trigger Service's data model. Drawing it as an ER participant implied a cross-service foreign-key relationship that does not — and must not — exist. This change removes the cross-boundary relationship and replaces it with scalar opaque-ID fields on `ResumeSubscription`, plus an explicit note that cross-service references travel as IDs only.

## Before

The ER diagram included:

```
ResumeSubscription ||--|| Run : "waits for"
```

`ResumeSubscription` carried only `subscriptionId`, `oneShot`, `registeredAt`, `expiresAt`. The `runId` / `stepId` / `eventKey` fields needed to identify the waiting step were declared on the parent `Subscription` row (as `kind=resume only` discriminator fields). The diagram suggested — incorrectly — that the Trigger Service held a foreign key into the Workflow Service `Run` table.

## After

- Removed the `ResumeSubscription ||--|| Run` line.
- Added `runId`, `stepId`, `eventKey` directly on `ResumeSubscription` as scalar fields, annotated as opaque references to Workflow Service-owned entities. The duplicated discriminator fields on `Subscription` are kept for now to avoid a wider schema rename in this purely doc-side cleanup; both surfaces describe the same in-memory shape.
- Added a `Subscription ||--o| ResumeSubscription : "specialized as (kind=resume)"` relationship to make the inheritance pattern explicit on the diagram itself, replacing the dropped cross-service edge.
- Added a paragraph after the diagram: cross-service references are scalar IDs, not ER relationships. The only interaction Trigger Service has with `Run` / `Step` is via `RegisterResumeSubscription` / `CancelResumeSubscription` Internal RPCs and dispatch back to `RaiseExternalEvent`. The Trigger Service never reads the Workflow Service tables.
- Header bumped: Version 3 → 4; Change History row added.

## Impact

- The ER diagram no longer suggests cross-service database coupling. Implementers reading only the Trigger Service design cannot misread `Run` as a foreign-key target the Trigger Service should join against.
- No RPC, REST, or audit-event changes. The Workflow Service's `RegisterResumeSubscription` / `CancelResumeSubscription` contract is unchanged.
- The companion architecture-side change (`design/architecture/changes/2026-05-18-012-bucket-e-domain-naming.md`, issue #88) reconciles the `Trigger` ⟷ `Subscription` naming mismatch in the overview Domain Model. The two changes ship together in one PR.

## Files changed

- `design/components/trigger-service/design.md`
- `design/components/trigger-service/changes/2026-05-18-003-bucket-e-er-cleanup.md` (this file)

## Related Change Records

- `design/architecture/changes/2026-05-18-012-bucket-e-domain-naming.md` — companion fix for #88 (architecture overview Domain Model renamed `Trigger` → `Subscription`).
- `design/components/trigger-service/changes/2026-05-17-001-incon-011-014-013-cursor-pubsub-taxonomy.md` — prior cleanup that established `Subscription` as the canonical entity name (removed `Cursor` to Connector Service).
