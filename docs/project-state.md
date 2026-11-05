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
| COMP-002 | AuthN/AuthZ Service | auth-service | Implemented | Scoped execution-context prerequisite [#1037](https://github.com/toddysm/custos/issues/1037) merged through #1040. |
| COMP-003 | Workflow Service | workflow-service | In progress | Observability Client integration — #989; background execution identity/Connector contract prerequisite #1037 merged through #1040. |
| COMP-004 | Trigger Service | trigger-service | In progress | M2 receivers — [#988](https://github.com/toddysm/custos/issues/988); [approved revision-7 design](../design/requirements/trigger-m2-receivers.md) and [19-item delivery plan](../design/components/trigger-service/implementation-plan-m2.md). Fifteen merged; lifecycle [#1044](https://github.com/toddysm/custos/pull/1044) is blocked on durable-progress prerequisite [#1045](https://github.com/toddysm/custos/issues/1045), now in progress after design approval/merge via #1046. Certification and final documentation remain. |
| COMP-005 | Connector Service | connector-service | Implemented | Workflow binding alignment and credential issuance prerequisites #1037/#1038 merged through #1040/#1041; lifecycle/deployed certification remain under #988. |
| COMP-006 | Activity Runtime Manager | activity-runtime-manager | In progress | Production ownership/readiness, real credentials and private bootstrap merged through [#1041](https://github.com/toddysm/custos/pull/1041), including #1043/#1042 repairs. Profile activation #1017 and deployed certification #1018 remain. Existing M2 deferrals — #991; Kata validation — #763 remain separate. |
| COMP-007 | Definition/Template/Catalog Service | catalog-service | Implemented | — |
| COMP-008 | Storage Provider Layer | storage-provider-layer | Implemented | schema-revision policy — #993; proposed durable receiver progress — [#1045](https://github.com/toddysm/custos/issues/1045), not yet implemented. |
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
| [#988](https://github.com/toddysm/custos/issues/988) | Implementation gap | trigger-service | In progress | Fifteen of 18 original delivery items merged. One proposed prerequisite makes 19; its design approval is pending. #1017/#1044 remain open, followed by #1018 certification, #1019 documentation and reviewed inventory close-out. |
| [#1038](https://github.com/toddysm/custos/issues/1038) | Runtime prerequisite | activity-runtime-manager | Done | Merged via [#1041](https://github.com/toddysm/custos/pull/1041) at `a4eaa5e`; no deployed receiver certification claimed. |
| [#1045](https://github.com/toddysm/custos/issues/1045) | Scan-progress prerequisite | trigger-service / storage-provider-layer | In progress | D08 approved/merged via #1046; Build authorized 2026-11-04T17:03:00.903-08:00. Implementing metadata revision16, bounded discovery, fair fenced progress and scheduled continuation. Current native tests/reviews are required before merge. #1044 remains preserved; no deployed certification claimed. |
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
