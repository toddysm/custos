# Change: bundle-g-binding-completion

Date: 2026-05-18
Type: component-design
Component: activity-runtime-manager
Sequence: 004
GitHub Issue: #98, #101
Status: open

## Summary

Bundle G (component-side, Activity Runtime Manager half). Two related fixes:

- **#98 (binding):** ARM's Public Interface and Dependencies sections still implied that ARM resolves `connectorRefs` against the Connector Service at step start. The agreed pattern (locked in the Workflow Service design and confirmed across CS, WF, and the architecture overview) is that the Workflow Service's Step Coordinator calls Connector Service `BindForStep` and passes the resulting named `ConnectorContexts` (opaque slot handles) to ARM via `ScheduleActivity`. ARM only calls Connector Service directly for `RefreshLease` on long-running steps. The sidecar bootstrap token is unchanged: ARM still mints and signs it at sidecar start per the Connector Service § Secret and Token Flow to Activities contract — it is **not** a `BindForStep` response field or a `ScheduleActivity` parameter.
- **#101 (completion):** ARM's Public Interface section listed completion as a vague "callback delivery to Workflow Service". The canonical mechanism is the native Dapr activity-task return path documented in the Workflow Service design.

## Before

```
Internal RPC surface (Workflow Service ⇄ ARM):
- `ScheduleActivity(runId, stepId, attempt, activityRef, inputs, connectorRefs, deadline)`
- `CancelActivity(runId, stepId)`
- Activity completion callback delivery to Workflow Service.
```

```
| Connector Service | Runtime | Resolves `ConnectorRef` → `ConnectorContext` (handles, not credentials) and provides scoped sidecar/API access to resolved connector material for activities. |
```

## After

Public Interface (pending) section:

- `ScheduleActivity(... connectorContexts ...)` — ARM consumes the pre-resolved named `ConnectorContexts` produced by the Workflow Service's `BindForStep` call; it does not call Connector Service for the initial bind. ARM mints and writes the sidecar bootstrap token (`/custos/in/sidecar-token`) at sidecar start per the Connector Service § Secret and Token Flow to Activities contract — the bootstrap token is not a `ScheduleActivity` input.
- Activity completion: native Dapr activity-task return path (the orchestrator invokes ARM through Dapr Workflow's activity-task primitive; ARM's return value is the typed result envelope). No `custos.activity.events` topic in v1. Cross-link added to `design/components/workflow-service/design.md` § Operation: Execute Step as authoritative.

Dependencies table:

- Connector Service row rewritten: "ARM consumes pre-resolved named `ConnectorContexts` (opaque slot handles) produced by the Workflow Service's `BindForStep` call — ARM does not call CS for the initial bind. ARM mints the sidecar bootstrap token at sidecar start per the Connector Service § Secret and Token Flow to Activities contract. ARM calls Connector Service directly only for `RefreshLease` on long-running steps."

Header bumped: Version 2 → 3; Change History row added covering both issues.

## Impact

- ARM, WF, CS, and the architecture overview now agree on the bind path and the completion path.
- The Activity Contract v1 filesystem layout (including the `/custos/in/sidecar-token` row added in the 2026-05-18-003 Bucket D change) is unchanged — the bootstrap token still arrives via the sandbox tmpfs mount; only the upstream producer/forwarder relationship changes.
- No driver-level changes (OCI Container Driver, etc.). No retry-policy or error-envelope changes.

## Files changed

- `design/components/activity-runtime-manager/design.md`
- `design/components/activity-runtime-manager/changes/2026-05-18-004-bundle-g-binding-completion.md` (this file)

## Related Change Records

- `design/architecture/changes/2026-05-18-013-bundle-g-binding-completion.md`
- `design/components/workflow-service/changes/2026-05-18-002-bundle-g-binding-completion.md`
- `design/components/connector-service/changes/2026-05-18-013-bundle-g-binding-completion.md`
