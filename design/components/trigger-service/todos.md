# TODOs: Trigger Service

Last Updated: 2026-05-17

## Open

- [ ] TODO-001: Define **unified platform event taxonomy** — canonical dot-namespaced `kind` values (`registry.*`, `pr.*`, `workflow.*`, `scan.*`, plus activity/step lifecycle `activity.*`, `step.*`). Must be defined jointly with ARM (TODO-009 in ARM todos.md) and Observability/Audit Service so connector-emitted event kinds and ARM-emitted activity lifecycle audit events share one namespace. Required before connector authors and audit consumers can target events deterministically (added 2026-05-16, issue #18, scope expanded 2026-05-17 per INCON-013)
- [ ] TODO-002: Decide selector language (tuples vs. JSONPath vs. CEL parity with ADR-011) (added 2026-05-16, issue #19)
- [ ] TODO-003: Specify scheduler leader-election mechanism (added 2026-05-16, issue #20)
- [ ] TODO-004: Cross-component — Workflow Service registers/cancels resume subscriptions over step lifecycle (added 2026-05-16, issue #21)
- [ ] TODO-005: Dead-letter handling and replay UX for dispatch failures (added 2026-05-16, issue #22)
- [ ] TODO-006: Owner of webhook signing keys (Trigger Service per subscription vs. Connector Service per instance) (added 2026-05-16, issue #23)

## Closed
