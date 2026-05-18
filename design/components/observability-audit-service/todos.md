# TODOs: Observability and Audit Service

Last Updated: 2026-05-17

## Open

- [ ] TODO-001: Define the alert-rule DSL grammar (eventName/severity/component matchers, throttling, deduplication keys). (added 2026-05-17)
- [ ] TODO-002: Define the Collector ConfigMap merge algorithm for the External Exporter Loader, including validation and rollback on bad config. (added 2026-05-17)
- [ ] TODO-003: Define the dead-letter table schema for failed alert dispatches (lives in SPL or in-service?). (added 2026-05-17)
- [ ] TODO-004: Audit-event taxonomy registry — declare a canonical union of `eventName` values across components for documentation and the alert-rule editor. (added 2026-05-17)
- [ ] TODO-005: Conformance test suite for `LogQueryProvider` and `MetricsQueryProvider` adapters. (added 2026-05-17)
- [ ] TODO-006: Cryptographic hash chain over audit rows for tamper-evidence (deferred to M2+; v1 relies on append-only DDL and the `audit_retention` role). (added 2026-05-17)
- [ ] TODO-007: Define the SSE reconnection / resume-from-cursor semantics for the per-run log tail (last-event-id header, cursor encoding). (added 2026-05-17)

## Closed

_(none yet)_
