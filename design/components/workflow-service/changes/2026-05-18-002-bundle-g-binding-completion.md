# Change: bundle-g-binding-completion

Date: 2026-05-18
Type: component-design
Component: workflow-service
Sequence: 002
GitHub Issue: #98, #101
Status: open

## Summary

Bundle G (component-side, Workflow Service half). Locks the "Workflow Service preflights, ARM consumes" pattern in the WF design: the Step Coordinator is the only caller of Connector Service for step binding, the outbound RPC is renamed `Resolve` → `BindForStep(stepKey, slots[])`, and `ScheduleActivity` carries pre-resolved `connectorContexts` plus a `sidecarBootstrapToken` instead of unresolved `connectorRefs`. Completion remains on the native Dapr activity-task return path (already documented; reinforced in the change history for issue #101).

## Before

Operation: Execute Step diagram and Internal RPC table named the outbound bind as `Resolve(connectorRef, runId, stepId)` and `ScheduleActivity` took `connectorRefs`. The companion Connector Service design labeled `BindForStep`'s caller as "Activity Runtime Manager", and the overview sequence diagram showed ARM calling CS — three conflicting versions of the same RPC.

## After

- Step Coordinator sequence diagram: `SC->>CC: BindForStep(stepKey, slots[])`, `CC->>Conn: BindForStep(stepKey, slots[])`, `Conn-->>CC: ConnectorContexts (named) + sidecarBootstrapToken`, `SC->>AC: ScheduleActivity(... connectorContexts, sidecarBootstrapToken ...)`.
- Internal RPC (outbound) table:
  - `Resolve(connectorRef, runId, stepId)` → `BindForStep(stepKey, slots[])` with purpose updated.
  - `ScheduleActivity(... connectorRefs ...)` → `ScheduleActivity(... connectorContexts, sidecarBootstrapToken ...)` with a note that ARM consumes the pre-resolved contexts.
- Failure Modes table: `Connector Service unreachable on Resolve` → `Connector Service unreachable on BindForStep`.
- Header bumped: Version 1 → 2; Change History row added covering both #98 (binding) and #101 (completion clarification).

The existing prose at § Operation: Execute Step that already named "native Dapr activity-task return path" as canonical is unchanged — issue #101 was a cross-doc consistency fix, and the WF design was the authoritative version it pointed at.

## Impact

- WF, CS, ARM, and the architecture overview now agree on the bind path. Implementers reading the WF design alone see a single outbound bind call and a single outbound activity schedule call.
- No REST surface changes. No audit-event taxonomy changes.
- Companion change records: see Related.

## Files changed

- `design/components/workflow-service/design.md`
- `design/components/workflow-service/changes/2026-05-18-002-bundle-g-binding-completion.md` (this file)

## Related Change Records

- `design/architecture/changes/2026-05-18-013-bundle-g-binding-completion.md`
- `design/components/connector-service/changes/2026-05-18-013-bundle-g-binding-completion.md`
- `design/components/activity-runtime-manager/changes/2026-05-18-004-bundle-g-binding-completion.md`
