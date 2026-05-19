# Change: bundle-g-binding-completion

Date: 2026-05-18
Type: architecture
Sequence: 013
GitHub Issue: #98, #101
Status: open

## Summary

Bundle G of the design-inconsistency cleanup (architecture-side half). The overview's Execution Model step-execution sequence diagram still showed ARM calling Connector Service to `resolve(connectorRef) -> ConnectorContext` after Dapr invoked it, and used a generic "typed result" return. That conflicted with the Workflow Service design (which makes the Step Coordinator the only caller of connector bind) and with the activity-completion path (canonical: native Dapr activity-task return). This change aligns the overview with the "Workflow Service preflights, ARM consumes" pattern from issue #98 and the activity-task return path from issue #101.

## Before

The step-execution sequence had:

```
WF->>Dapr: schedule activity(stepKey, activityRef, inputs)
Dapr->>ARM: invoke(stepKey, activityRef, inputs)
ARM->>Conn: resolve(connectorRef) -> ConnectorContext
ARM->>Driver: run(activity image/module, inputs, ctx)
...
ARM-->>Dapr: typed result
```

Two problems:

- ARM is shown as the caller of Connector Service for the initial bind. The Workflow Service design (locked) and the Connector Service design both place `BindForStep` as a WF-to-CS call, with the named `ConnectorContexts` passed through to ARM via `ScheduleActivity`.
- The completion edge `ARM-->>Dapr: typed result` was correct but didn't name the canonical mechanism. Issue #101 needs the diagram to identify it as the activity-task return path so downstream readers don't infer a separate completion topic.

## After

The diagram now shows:

```
WF->>Conn: BindForStep(stepKey, slots[])
Conn-->>WF: ConnectorContexts (named, opaque slot handles)
WF->>Dapr: schedule activity(stepKey, activityRef, inputs, connectorContexts)
Dapr->>ARM: invoke(stepKey, activityRef, inputs, connectorContexts)
ARM->>ARM: write/sign sidecarBootstrapToken at pod start
ARM->>Driver: run(activity image/module, inputs, ctx)
Driver-->>ARM: result (exitCode, outputs, artifacts)
ARM->>Audit: emit step events + artifacts
ARM-->>Dapr: typed result (activity-task return)
Dapr-->>WF: continue / branch / retry
```

The sidecar bootstrap token is **not** part of the `BindForStep` response or the `ScheduleActivity` inputs. ARM continues to mint and sign that token at sidecar start, as locked in the Connector Service § Secret and Token Flow to Activities. `BindForStep` returns only named `ConnectorContexts` — opaque slot handles — and those are what flow from WF to ARM via `ScheduleActivity`.

Header bumped: Version 12 → 13; Change History row added.

## Impact

- Architecture overview, Workflow Service design, Connector Service design, and Activity Runtime Manager design now agree on a single bind path (WF calls CS) and a single completion path (Dapr activity-task return).
- No RPC, REST, or audit-event surface area changes beyond renames already captured in the component designs.
- Companion component-side changes ship in the same PR:
  - `design/components/workflow-service/changes/2026-05-18-002-bundle-g-binding-completion.md`
  - `design/components/connector-service/changes/2026-05-18-013-bundle-g-binding-completion.md`
  - `design/components/activity-runtime-manager/changes/2026-05-18-004-bundle-g-binding-completion.md`

## Files changed

- `design/architecture/overview.md`
- `design/architecture/changes/2026-05-18-013-bundle-g-binding-completion.md` (this file)

## Related Change Records

- `design/components/workflow-service/changes/2026-05-18-002-bundle-g-binding-completion.md`
- `design/components/connector-service/changes/2026-05-18-013-bundle-g-binding-completion.md`
- `design/components/activity-runtime-manager/changes/2026-05-18-004-bundle-g-binding-completion.md`
