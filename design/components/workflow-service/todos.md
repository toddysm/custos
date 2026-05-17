# TODOs: Workflow Service

Last Updated: 2026-05-17

## Open

- [ ] TODO-001: Finalize canonical workflow event taxonomy (`workflow.*`, `run.*`, `step.*`) jointly with Trigger Service TS-TODO-001 (#18) and ARM TODO-009 (INCON-013 cross-link). Tracked under those existing issues; no separate WF issue. (added 2026-05-17)
- [ ] TODO-002: Specify the retry-policy YAML schema for the `retry:` block on activity steps — max attempts, backoff curve (constant/linear/exponential), jitter strategy, per-error-class overrides (retryable vs. permanent). REQ-010. (added 2026-05-17, issue #52)
## Closed

- [x] TODO-003: Specify the relationship between `workflow:` step kind and `WorkflowTemplateVersion` invocation. Resolved by Catalog Service design (2026-05-17): `workflow:` accepts only fully-qualified `WorkflowVersion` references; template-with-inline-values is a two-step authoring flow (materialize → reference). Closed 2026-05-17 via Catalog Service design PR, closes issue #53.
