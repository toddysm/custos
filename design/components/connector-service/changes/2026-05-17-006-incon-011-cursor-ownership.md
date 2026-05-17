# Change: incon-011-cursor-ownership

Date: 2026-05-17
Type: component-design
Component: connector-service
Sequence: 006
GitHub Issue: #36
Status: open

## Summary

Add a § Cursor Ownership section to `design/components/connector-service/design.md` documenting that pull cursors are owned by the Connector Service, keyed per `ConnectorInstance`, with one pull loop per instance fanning normalized events out to all subscribing Trigger Service `Subscription`s. This locks the boundary that INCON-011 flagged as ambiguous: the existing `ConnectorCursor` entity and `Listen Manager → Cursor Store` flow are now the explicit, authoritative model, and the Trigger Service has been updated in the same PR to remove its competing `Cursor` entity.

## Before

The Connector Service design's data model already showed `ConnectorInstance ||--o{ ConnectorCursor : checkpoints` and the internal structure showed `Listen Manager → Cursor Store`, but no prose explained the per-instance granularity, the fan-out semantics for multiple subscriptions sharing an instance, or the boundary with Trigger Service. The architecture overview, Connector Service, and Trigger Service designs together produced three different cursor representations (architecture-level: "via MetadataStoreProvider"; Connector Service: per-instance; Trigger Service: per-subscription).

## After

New § Cursor Ownership states:

- **Owner**: Connector Service. **Key**: `ConnectorInstance`. **Persistence**: `MetadataStoreProvider`.
- One pull loop per active instance; events fan out to multiple Trigger Service subscriptions sharing that instance.
- Trigger Service holds no cursor state. Per-subscription dedup keys are an orthogonal concern.
- Reset/replay semantics: operator rewinds the per-instance cursor; Trigger Service dedup keys are independently cleared if a true re-dispatch is desired.

## Impact

- One cursor per upstream stream — eliminates the N×M cursor-write race that two competing owners would create.
- Avoids N parallel pull loops against the same upstream API when N subscriptions consume the same `ConnectorInstance`.
- Connector plugin implementations have an unambiguous contract for cursor reads and advances.
- Operators get a single, predictable admin surface for cursor rewind / event replay (per instance, not per subscription).

## Related Requirements

- `design/components/trigger-service/design.md` Boundaries / Data Models (updated in same PR — Cursor entity removed)
- REQ-079 (multi-source pull/push triggers)
- Issues: #36 (this change); related: TS-TODO-001 (#18), Connector Service open TODO "Define pull cursor model and dedup key strategy"
