# TODOs: Trigger Service

Last Updated: 2026-06-04 (M1 implementation filed under tracker [#652](https://github.com/toddysm/custos/issues/652); TS-IMPL-001..021 filed)

## Implementation — M1 vertical slice

First implementation milestone: the MVP vertical slice that unblocks the Workflow Service end-to-end — REQ-004 (manual trigger + REST CRUD), REQ-081 (resume RPCs + `RaiseExternalEvent`), REQ-080 (internal workflow-to-workflow triggers), the full Normalize → Classify → Match(CEL) → Dedup → Dispatch pipeline, the canonical event taxonomy (TODO-001), and CEL selectors (TODO-002). Scheduler / webhook / vendor-push / pull receivers are contract-locked but deferred to M2. Plan: [`implementation-plan.md`](implementation-plan.md). Tracker: [#652](https://github.com/toddysm/custos/issues/652) — `TS-IMPL-000-TRIGGER-SERVICE`.

### Phase A — Scaffold & foundations

- [x] TS-IMPL-001 (#631): Scaffold `custos-trigger-service` + CI gate.
- [x] TS-IMPL-002 (#632): Wire trigger-service Helm subchart.
- [x] TS-IMPL-003 (#633): FastAPI skeleton + health probes + call-context shim.
- [x] TS-IMPL-004 (#634): Settings + locked `trigger.*` error taxonomy.
- [x] TS-IMPL-005 (#635): `custos-cel` `event` binding root (ADR-011 extension).
- [x] TS-IMPL-006 (#636): Canonical event taxonomy registry (resolves TODO-001).

### Phase B — Domain models & persistence

- [x] TS-IMPL-007 (#637): `NormalizedEvent` envelope + API wire + domain models.
- [x] TS-IMPL-008 (#638): Store adapters over `MetadataStoreProvider` + `providers.py`.
- [x] TS-IMPL-009 (#639): Dedup / idempotency.

### Phase C — Pipeline core

- [x] TS-IMPL-010 (#640): Event Normalizer.
- [x] TS-IMPL-011 (#641): CEL selector evaluator (resolves TODO-002).
- [x] TS-IMPL-012 (#642): Classifier + Start Matcher + Resume Matcher.
- [x] TS-IMPL-013 (#643): `WorkflowServiceClient` — Dapr Service-Invocation adapter.
- [x] TS-IMPL-014 (#644): Dispatcher.

### Phase D — Receivers & RPC surface (WF-unblockers)

- [x] TS-IMPL-015 (#645): Manual Receiver + REST CRUD (REQ-004).
- [x] TS-IMPL-016 (#646): Resume RPCs `RegisterResumeSubscription` / `CancelResumeSubscription` (REQ-081).
- [x] TS-IMPL-017 (#647): Internal Event Receiver (REQ-080 + REQ-081 delivery).
- [x] TS-IMPL-018 (#648): App wiring — mount routers + Dapr subscription + lifespan.

### Phase E — Observability, verification, docs

- [x] TS-IMPL-019 (#649): OTel observability + audit events.
- [x] TS-IMPL-020 (#650): Unit + integration test suite (≥90% coverage gate).
- [x] TS-IMPL-021 (#651): Developer documentation — `docs/developers/trigger-api.md`.

## Open

- [x] TODO-003: Specify scheduler leader-election mechanism — resolved 2026-08-28 (Postgres leader-lease row + `epoch` fence + per-fire dedup key). See `changes/2026-08-28-008-scheduler-leader-election.md`. Closes #20.
- [ ] TODO-005: Dead-letter handling and replay UX for dispatch failures (added 2026-05-16, issue #22)
- [x] TODO-006: Owner of webhook signing keys — resolved 2026-08-29 (Connector Service owns signing material per connector instance; Generic Webhook Receiver delegates verification). See `changes/2026-08-29-009-webhook-signing-key-ownership.md`. Closes #23.

## Closed

- [x] TODO-001: Define **unified platform event taxonomy** — canonical dot-namespaced `kind` values. Resolved 2026-06-04: design.md § Event Taxonomy locks the closed platform-owned domain registry (`manual`/`cron`/`webhook`/`workflow`/`run`/`step`/`activity`/`registry`/`pr`/`scan`) + the vendor-domain shape rule, unifying the namespace with ARM TODO-009 + Observability (INCON-013). See [`changes/2026-06-04-007-event-taxonomy.md`](changes/2026-06-04-007-event-taxonomy.md), closes #18.
- [x] TODO-002: Decide selector language. Resolved 2026-06-04: selectors are CEL boolean expressions over a new `event` binding root (ADR-011 parity with `inputMapping`); legacy field/match-type tuples desugar to CEL. See [`changes/2026-06-04-006-selector-cel-parity.md`](changes/2026-06-04-006-selector-cel-parity.md), closes #19.
