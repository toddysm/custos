---
name: bucket-e-domain-naming
description: Architecture overview Domain Model renamed Trigger to Subscription to match the Trigger Service data model; REST resource stays /triggers
type: architecture
---

# Change: bucket-e-domain-naming

Date: 2026-05-18
Type: architecture
Sequence: 012
GitHub Issue: #88
Status: open

## Summary

Bucket E of the design-inconsistency cleanup (architecture-side half). The architecture overview's Domain Model used `Trigger` as an entity name; the Trigger Service component design (locked) uses `Subscription` with `kind = start | resume`, and the public REST surface is `/triggers`. The three-way naming split was never reconciled. Per Option B from issue #88, this change aligns the architecture overview on `Subscription` (the locked data-model name) and keeps `/triggers` as the public REST resource name.

## Before

The Domain Model ER diagram declared:

```
Workspace ||--o{ Trigger : contains
Trigger }o--|| WorkflowVersion : starts
Trigger }o--o| ConnectorInstance : sourcedFrom
Schedule }o--|| Trigger : drives
```

No `Trigger` entity exists in the Trigger Service data model. The mismatch forced anyone moving between the overview and the component design to mentally translate `Trigger ⟷ Subscription` with no documented mapping.

## After

- Domain Model ER renames `Trigger` → `Subscription` in the four relationship lines. The conceptual structure is unchanged.
- Added a short paragraph immediately after the Domain Model diagram: the data-model entity is `Subscription` (with `kind = start | resume`); the public REST resource is `/triggers` for ergonomic reasons; the Trigger Service design is authoritative. Pipeline prose and the Trigger Pipeline section can continue to use "trigger" colloquially for the user-facing concept — only the entity name is normative.
- Header bumped: Version 11 → 12; Change History row added.

## Impact

- Architecture-review readers and component-design implementers now see the same entity name in the ER diagrams.
- No contract changes. The REST API (`/triggers`), RPC names (`RegisterResumeSubscription`, etc.), `kind = start | resume` discriminator, and all audit-event names are unchanged.
- The companion component-side change (`design/components/trigger-service/changes/2026-05-18-003-bucket-e-er-cleanup.md`, issue #86) fixes the cross-boundary `Run` reference in the Trigger Service ER diagram. The two changes ship together in one PR but are recorded separately because they target different design surfaces.

## Files changed

- `design/architecture/overview.md`
- `design/architecture/changes/2026-05-18-012-bucket-e-domain-naming.md` (this file)

## Related Change Records

- `design/components/trigger-service/changes/2026-05-18-003-bucket-e-er-cleanup.md` — companion fix for #86 (ER diagram cross-boundary cleanup).
