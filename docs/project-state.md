# Project State: Custos

> Durable capability inventory and work ledger for the Custos platform.
> This file is the source of truth for "where the project is" and is
> reconciled against code, design docs, and GitHub issues/PRs.

Last reconciled: 2026-08-28

Trigger M2 implementation checkpoint: 2026-11-05. This scoped update does not claim
a fresh reconciliation of unrelated capabilities.

## Configuration

| Key | Value |
|---|---|
| Project name | Custos |
| Requirements folder | `design/requirements/` |
| Component designs | `design/components/` |
| Architecture registry | `design/architecture/components.md` |
| Project state file | `docs/project-state.md` |
| Tracking mode | GitHub Issues (label scheme `type:` / `phase:` / `status:` / `component:`) |

## Capability inventory

Status vocabulary: **Implemented** · **In progress** · **Planned** · **Deferred**.

| ID | Component | Slug | Status | Open work |
|---|---|---|---|---|
| COMP-001 | API Gateway | api-gateway | Implemented | M2 items — #990 |
| COMP-002 | AuthN/AuthZ Service | auth-service | Implemented | Scoped execution-context issuance landed through [#1040](https://github.com/toddysm/custos/pull/1040). |
| COMP-003 | Workflow Service | workflow-service | In progress | Observability Client integration — #989. Execution identity and Connector contract alignment landed through [#1040](https://github.com/toddysm/custos/pull/1040). |
| COMP-004 | Trigger Service | trigger-service | In progress | M2 receivers — [#988](https://github.com/toddysm/custos/issues/988); [approved design](../design/requirements/trigger-m2-receivers.md) and [19-item delivery plan](../design/components/trigger-service/implementation-plan-m2.md). Durable progress landed via [#1047](https://github.com/toddysm/custos/pull/1047); [#1044](https://github.com/toddysm/custos/pull/1044) delivers its lifecycle/guarded deployment integration and saturation/restart tests. After that merge, 17/19 delivery items are complete; #1018 deployed certification and #1019 final documentation remain. |
| COMP-005 | Connector Service | connector-service | Implemented | Scoped binding and real credential issuance landed through [#1040](https://github.com/toddysm/custos/pull/1040) and [#1041](https://github.com/toddysm/custos/pull/1041). |
| COMP-006 | Activity Runtime Manager | activity-runtime-manager | In progress | Production ownership/readiness, real credentials and private bootstrap landed through [#1041](https://github.com/toddysm/custos/pull/1041), including #1043/#1042 repairs. Profile activation and deployed certification remain in #1017/#1018. Existing M2 deferrals — #991; Kata tier validation — #763 remain separate. |
| COMP-007 | Definition/Template/Catalog Service | catalog-service | Implemented | — |
| COMP-008 | Storage Provider Layer | storage-provider-layer | Implemented | schema-revision policy — #993; durable receiver-progress prerequisite [#1045](https://github.com/toddysm/custos/issues/1045) merged through #1047. |
| COMP-009 | Observability and Audit Service | observability-audit-service | Implemented | M2 deferrals — #992 |
| COMP-010 | Web UI and Template Designer | web-ui | Deferred (M2+) | not yet implemented — #994 |
| COMP-011 | Local Dev & Test CLI | custosctl | Implemented | — |

## Work ledger (in-flight)

| Item | Kind | Component | State | Notes |
|---|---|---|---|---|
| #20 | Design TODO | trigger-service | Done | Scheduler leader-election — Postgres leader-lease row; resolved via #997 (2026-08-28); implementation follows in #988 |
| #22 | Design TODO | trigger-service | Done | Dead-letter handling + replay UX — durable DeadLetterEntry store + TTL + trigger:admin replay routes; resolved via #1002 (2026-08-29); implementation follows in #988 |
| #23 | Design TODO | trigger-service | Done | Owner of webhook signing keys — Connector Service owns per-instance signing material; resolved via #1000 (2026-08-29); implementation follows in #988 |
| #763 | Enhancement | activity-runtime-manager | Open | Validate vm/microvm (Kata) tier specifics |
| [#988](https://github.com/toddysm/custos/issues/988) | Implementation gap | trigger-service | In progress | #1044 completes lifecycle integration, bringing delivery to 17/19 after merge. #1018 certification, #1019 documentation and reviewed inventory close-out remain required. The session stops after #1044; #988 is not complete. |
| [#1017](https://github.com/toddysm/custos/issues/1017) | Lifecycle and deployment | trigger-service | Delivered in #1044 | Independent bounded discovery and fair durable operation claims; revision16 readiness, cancellation-safe checkpoints/releases, actual multi-replica effects, native saturation/restart coverage and guarded profile defaults. Merge state and exact tested SHA are recorded on #988/#1017; no deployed certification claimed. |
| [#1038](https://github.com/toddysm/custos/issues/1038) | Runtime prerequisite | activity-runtime-manager | Done | Merged through [#1041](https://github.com/toddysm/custos/pull/1041) after owner review on 2026-11-04. All 54 reviewed-head checks passed, including 15 native ownership/provider and 10 Kubernetes cases. No full deployed receiver certification claimed. |
| [#1045](https://github.com/toddysm/custos/issues/1045) | Scan-progress prerequisite | trigger-service / storage-provider-layer | Done | Merged through [#1047](https://github.com/toddysm/custos/pull/1047) as `34e8d29`. All current-head checks passed; 133 native PostgreSQL and 183 Trigger integration cases executed. Lifecycle activation and deployed certification remain separate. |
| #989 | Implementation gap | workflow-service | Open | Full Observability Client integration |
| #990 | Implementation gap | api-gateway | Open | M2 items (thin-client, OpenAPI ext, device-code, rate-limiter, multi-region) |
| #991 | Implementation gap | activity-runtime-manager | Open | M2 deferrals (manifest signing, artifact schema, secret slots, short-form refs, policy-eval@1) |
| #992 | Implementation gap | observability-audit-service | Open | M2 deferrals (taxonomy registry, conformance suite, audit hash chain) |
| #993 | Implementation gap | storage-provider-layer | Open | Multi-revision adapter schema-upgrade policy |
| #994 | Implementation gap | web-ui | Open | Component not yet implemented (deferred M2+) |

## Deferred / Won't-do

The #988 design's decisions D01-D05 and delivery scope were explicitly approved
on 2026-10-28. Original design TODOs #20/#22/#23 remain resolved. Review and
implementation status is maintained in the #988 issue ledger; do not mark
COMP-004 Implemented until all required work and its inventory close-out merge.

| Item | Reason |
|---|---|
| COMP-010 Web UI (#994) | Intentionally deferred to M2+ milestone |
| ARM manifest signing, artifact schema validation, secret slots (#991) | M2+ scope |
| Audit hash chain (#992, TODO-006) | M2+; v1 relies on append-only DDL + `audit_retention` role |

## How this file is maintained

- Refreshed during status reconciliation and at session end.
- A component's row moves to **Implemented** only when its implementation
   tracker is closed and its remaining work is either done or filed as a
   tracked gap issue.
- Gap issues are filed under `type:implementation` + `component:<slug>`.
