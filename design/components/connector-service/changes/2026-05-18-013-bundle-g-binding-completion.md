---
name: bundle-g-binding-completion
description: Connector Service BindForStep is called by Workflow Service (not ARM); multi-connector bind diagram updated to return contexts to WF
type: component-design
---

# Change: bundle-g-binding-completion

Date: 2026-05-18
Type: component-design
Component: connector-service
Sequence: 013
GitHub Issue: #98
Status: open

## Summary

Bundle G (component-side, Connector Service half). The Internal RPCs table labeled `BindForStep`'s caller as "Activity Runtime Manager" and the Multi-Connector Step sequence diagram returned `ConnectorContexts` directly to ARM. Both conflicted with the Workflow Service design (Step Coordinator is the caller) and with the overview sequence diagram once issue #98 was settled. This change aligns the CS design on the canonical pattern: WF calls `BindForStep`, CS returns `ConnectorContexts` + `sidecarBootstrapToken` to WF, WF then hands them to ARM via `ScheduleActivity`. ARM continues to call `RefreshLease` for long-running steps.

## Before

Internal RPCs:

```
| BindForStep | Activity Runtime Manager | Get connector context for a step |
```

Multi-Connector Step diagram:

```
WF->>CS: Bind(step, connectors map, required capabilities)
CS-->>ARM: ConnectorContexts (named)
ARM-->>WF: Ready to run activity
```

## After

- Internal RPCs table: `BindForStep` Caller is now `Workflow Service`; purpose expanded to "Resolve every connector slot a step references and return named `ConnectorContexts` plus a `sidecarBootstrapToken`. Called by the Step Coordinator before `ScheduleActivity`."
- Multi-Connector Step sequence diagram now shows `WF->>CS: BindForStep(stepKey, slots[] — named connectors + required capabilities)`, `CS-->>WF: ConnectorContexts (named) + sidecarBootstrapToken`, `WF->>ARM: ScheduleActivity(..., connectorContexts, sidecarBootstrapToken)`.
- `RefreshLease` row unchanged (still `Activity Runtime Manager` — this is the only remaining direct ARM→CS call in the step lifecycle).
- Header bumped: Version 7 → 8; Change History row added.

The sidecar lifecycle and lease-revocation surfaces are not touched; this change is scoped to the bind-call origin and the multi-connector diagram.

## Impact

- CS, WF, ARM, and the architecture overview now agree on the bind path.
- No REST surface changes. No audit-event taxonomy changes. The sidecar bootstrap token contract (locked in the 2026-05-17 sidecar-secret-token change) is unchanged — only the receiver of the token at bind time changes from ARM to WF (WF forwards it via `ScheduleActivity`).

## Files changed

- `design/components/connector-service/design.md`
- `design/components/connector-service/changes/2026-05-18-013-bundle-g-binding-completion.md` (this file)

## Related Change Records

- `design/architecture/changes/2026-05-18-013-bundle-g-binding-completion.md`
- `design/components/workflow-service/changes/2026-05-18-002-bundle-g-binding-completion.md`
- `design/components/activity-runtime-manager/changes/2026-05-18-004-bundle-g-binding-completion.md`
