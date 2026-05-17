# Change: incon-013-taxonomy-todo-link

Date: 2026-05-17
Type: component-design
Component: activity-runtime-manager
Sequence: 002
GitHub Issue: #38
Status: open

## Summary

Cross-link ARM TODO-009 (activity lifecycle event taxonomy with Observability) and Trigger Service TODO-001 (platform event `kind` taxonomy, #18). The two work items target one taxonomy — connector-emitted event kinds (`registry.push`, `workflow.completed`, etc.) and ARM-emitted activity/step lifecycle audit kinds (`activity.failed`, `step.completed`, etc.) — and must be designed together so cross-cutting events like `workflow.completed` and `step.completed` use one canonical name from emission through trigger matching, audit storage, and consumer dashboards.

## Before

- ARM `todos.md` TODO-009: "Finalize platform event taxonomy mapping for activity lifecycle events with Observability (added 2026-05-16)."
- Trigger Service `todos.md` TODO-001: "Define platform event taxonomy (canonical `kind` values) (added 2026-05-16, issue #18)."

No cross-reference; risk that the two were resolved independently with incompatible namespaces (e.g. `custos.activity.completed` vs `activity.completed`).

## After

- ARM TODO-009 rewritten to call out the joint scope with Trigger Service TODO-001 (#18) and Observability/Audit Service, and to record the INCON-013 origin.
- Trigger Service TODO-001 rewritten with expanded scope to include activity/step lifecycle kinds and explicit cross-references to ARM TODO-009 and Observability/Audit; existing GitHub issue #18 retained as the tracking ticket.

## Impact

- Whoever picks up #18 will know they must produce a single namespace that covers connector trigger events AND ARM-emitted activity lifecycle audit events.
- Resume Matcher (Trigger Service) and audit consumers (Observability) will see exactly the same `kind` strings for the same conceptual event.
- Prevents a late-stage rename across components when the Observability/Audit Service detailed design lands.

## Related Requirements

- `design/components/trigger-service/design.md` § NormalizedEvent schema (`kind` taxonomy reference)
- `design/components/activity-runtime-manager/design.md` § Activity Contract v1 (`audit.jsonl`)
- Issues: #38 (this change); tracking ticket: #18 (TS-TODO-001)
