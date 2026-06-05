# TODOs: Observability and Audit Service

Last Updated: 2026-06-05 (implementation plan derived + filed under tracker [#692](https://github.com/toddysm/custos/issues/692); OBS-IMPL-001..017 filed)

## Implementation — service host (`custos_obs`)

Service host packaged inside `src/services/observability-audit-service/` (`custos_obs`). Orchestrates the four telemetry pipelines + the inbound read-back API on top of the already-built SPL providers (`LogQueryProvider` / `MetricsQueryProvider`) and the `custos_pg` audit-outbox drain. Plan: [`implementation-plan.md`](implementation-plan.md). Tracker: [#692](https://github.com/toddysm/custos/issues/692) — `OBS-IMPL-000-OBSERVABILITY-AUDIT-SERVICE`.

### Phase A — Scaffold & foundations

- [F] OBS-IMPL-001 (#675): Scaffold the `custos-observability-audit-service` package + CI gate.
- [F] OBS-IMPL-002 (#676): `Settings` model + conditional-requirement validation.
- [F] OBS-IMPL-003 (#677): Error taxonomy + RFC 7807 envelope + `obs.*` audit events.
- [F] OBS-IMPL-004 (#678): `providers.py` wiring — SPL providers from settings.

### Phase B — Audit pipeline

- [F] OBS-IMPL-005 (#679): Audit Outbox Drainer — LISTEN/NOTIFY + polling fallback.
- [F] OBS-IMPL-006 (#680): Audit Pipeline — idempotent dispatch + per-pipeline cursors + lag signal.
- [F] OBS-IMPL-007 (#681): Retention worker — audit retention + outbox GC.

### Phase C — Alerting

- [F] OBS-IMPL-008 (#682): Alert-rule DSL loader + matcher (closes design TODO-001).
- [F] OBS-IMPL-009 (#683): Alerting Dispatcher — webhook + SMTP sinks + dead-letter (closes design TODO-003).

### Phase D — External Exporter Loader (Concern A)

- [F] OBS-IMPL-010 (#684): Collector-config merge + validation algorithm (closes design TODO-002).
- [F] OBS-IMPL-011 (#685): Exporter ConfigMap watcher + Collector reload + rollback.

### Phase E — Query API (Concern B read-back)

- [F] OBS-IMPL-012 (#686): Query API wire models + dependency factories + call-context middleware + permissions.
- [F] OBS-IMPL-013 (#687): Log read-back routes — SSE tail + paged history (closes design TODO-007).
- [F] OBS-IMPL-014 (#688): Metrics + audit read-back routes.

### Phase F — Observability, verification, docs

- [F] OBS-IMPL-015 (#689): OTel self-instrumentation (dogfooding).
- [F] OBS-IMPL-016 (#690): Unit + integration test suite (≥90 % coverage gate).
- [F] OBS-IMPL-017 (#691): Developer documentation — `docs/developers/observability-api.md`.

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
