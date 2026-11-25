# Project State: Custos

> Durable capability inventory and work ledger for the Custos platform.
> This file is the source of truth for "where the project is" and is
> reconciled against code, design docs, and GitHub issues/PRs.

Last reconciled: 2026-08-28

Trigger M2 implementation checkpoint: 2026-11-20. Revision-8/D09 and its
20-item breakdown are approved; the design merged in [#1049](https://github.com/toddysm/custos/pull/1049).
Binding prerequisite [#1048](https://github.com/toddysm/custos/issues/1048) is
complete through [#1050](https://github.com/toddysm/custos/pull/1050) at `d943298`.
Eighteen of twenty approved delivery items are merged. The separately authorized
scheduler-baseline repair [#1051](https://github.com/toddysm/custos/issues/1051)
is delivered by [#1052](https://github.com/toddysm/custos/pull/1052):
[actual-service run44666364885](https://github.com/toddysm/custos/actions/runs/44666364885)
passed managed installation, trusted scheduler readiness and Helm tests at
`46c4548`, with owned cleanup verified. Deployed receiver certification
[#1018](https://github.com/toddysm/custos/issues/1018) remains required and unchecked.
This scoped update does not claim a fresh reconciliation of unrelated capabilities.

Wait/cancellation repair checkpoint: 2026-11-25. The accepted scope of
[#1055](https://github.com/toddysm/custos/issues/1055) is delivered: all five
children are closed, including [#1058's owner-accepted unresolved-cause disposition](https://github.com/toddysm/custos/issues/1058#issuecomment-7176238741).
[Ordinary deployed verification](https://github.com/toddysm/custos/actions/runs/45442520273)
passed public completion/readback, the registered unexpired PT5M wait with exact
300-second TTL, Trigger-signed cancellation, independent runtime termination
and unchanged accepted inputs. [Separate controlled-failure verification](https://github.com/toddysm/custos/actions/runs/45515362894)
proved diagnostic capture for an injected fault, not the historical HTTP 503
cause. That failure remains intermittent/non-reproduced with root cause unknown;
recurrence is not ruled out. This scoped repair does not complete
[#1053](https://github.com/toddysm/custos/issues/1053),
[#1018](https://github.com/toddysm/custos/issues/1018) or
[#988](https://github.com/toddysm/custos/issues/988), certify receivers, or reset
existing execution budgets.

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
| COMP-004 | Trigger Service | trigger-service | In progress | M2 receivers — [#988](https://github.com/toddysm/custos/issues/988); [approved D09 design](../design/requirements/trigger-m2-receivers.md) and [20-item delivery plan](../design/components/trigger-service/implementation-plan-m2.md), merged in [#1049](https://github.com/toddysm/custos/pull/1049). Eighteen delivery items are merged, including binding capacity [#1050](https://github.com/toddysm/custos/pull/1050). Separate actual-service baseline repair [#1051](https://github.com/toddysm/custos/issues/1051) is delivered by [#1052](https://github.com/toddysm/custos/pull/1052); [#1018](https://github.com/toddysm/custos/issues/1018), [#1019](https://github.com/toddysm/custos/issues/1019) and reviewed inventory close-out remain required. The accepted wait/cancellation repair scope [#1055](https://github.com/toddysm/custos/issues/1055) is delivered with the historical HTTP 503 cause unknown. No deployed receiver certification claimed. |
| COMP-005 | Connector Service | connector-service | Implemented | Scoped binding and real credential issuance landed through [#1040](https://github.com/toddysm/custos/pull/1040) and [#1041](https://github.com/toddysm/custos/pull/1041). |
| COMP-006 | Activity Runtime Manager | activity-runtime-manager | In progress | Production ownership/readiness, real credentials and private bootstrap landed through [#1041](https://github.com/toddysm/custos/pull/1041), including #1043/#1042 repairs; managed activation landed through [#1044](https://github.com/toddysm/custos/pull/1044), bounded reference-file loading through [#1050](https://github.com/toddysm/custos/pull/1050). [#1051](https://github.com/toddysm/custos/issues/1051) repairs real-service smoke prerequisites/startup ordering, not the truthful default-off readiness contract. [#1018](https://github.com/toddysm/custos/issues/1018) retains deployed certification. Existing M2 deferrals — #991; Kata tier validation — #763 remain separate. |
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
| [#988](https://github.com/toddysm/custos/issues/988) | Implementation gap | trigger-service | In progress | Eighteen of twenty approved delivery items merged, including #1048 through #1050. Separate baseline repair #1051 is delivered by #1052; #1018, #1019 and reviewed inventory close-out remain required. #988 is not complete. |
| [#1017](https://github.com/toddysm/custos/issues/1017) | Lifecycle and deployment | trigger-service | Delivered in #1044 | Independent bounded discovery and fair durable operation claims; revision16 readiness, cancellation-safe checkpoints/releases, actual multi-replica effects, native saturation/restart coverage and guarded profile defaults. Merge state and exact tested SHA are recorded on #988/#1017; no deployed certification claimed. |
| [#1038](https://github.com/toddysm/custos/issues/1038) | Runtime prerequisite | activity-runtime-manager | Done | Merged through [#1041](https://github.com/toddysm/custos/pull/1041) after owner review on 2026-11-04. All 54 reviewed-head checks passed, including 15 native ownership/provider and 10 Kubernetes cases. No full deployed receiver certification claimed. |
| [#1045](https://github.com/toddysm/custos/issues/1045) | Scan-progress prerequisite | trigger-service / storage-provider-layer | Done | Merged through [#1047](https://github.com/toddysm/custos/pull/1047) as `34e8d29`. All current-head checks passed; 133 native PostgreSQL and 183 Trigger integration cases executed. Lifecycle activation and deployed certification remain separate. |
| [#1048](https://github.com/toddysm/custos/issues/1048) | Binding-capacity prerequisite | shared call-context / deployment / four managed consumers | Done | Merged through [#1050](https://github.com/toddysm/custos/pull/1050) as `d943298`; exact-head review and all 63 checks passed. [Hosted configuration lifecycle](https://github.com/toddysm/custos/actions/runs/44642624597) passed six real storage/recovery tests with owned cleanup. [Operator procedure](operations/execution-bindings.md) distinguishes that evidence from #1018 deployed application certification. Final docs remain #1019. |
| [#1051](https://github.com/toddysm/custos/issues/1051) | Scheduler-baseline repair | activity-runtime-manager / deployment | Delivered in [#1052](https://github.com/toddysm/custos/pull/1052) | Separately authorized prerequisite for #1018, not an added approved delivery-plan item. Real production wiring/schema/platform/executor preseed, opt-in JSONB initialization, Auth verifier, CSI/TLS/named Secrets, scoped Kubernetes access and test-client networking. [Actual baseline44666364885](https://github.com/toddysm/custos/actions/runs/44666364885) at `46c4548` passed installation, trusted real scheduler readiness and Helm checks; owned registry/kind cleanup verified. Receiver admission stays paused. |
| [#1055](https://github.com/toddysm/custos/issues/1055) | Wait/cancellation repair | workflow-service / cross-service | Delivered (owner-accepted) | Landed: 2026-11-25. Required children #1056-#1060 are closed; merged delivery: [#1054](https://github.com/toddysm/custos/pull/1054), [#1061](https://github.com/toddysm/custos/pull/1061), [#1064](https://github.com/toddysm/custos/pull/1064), [#1062](https://github.com/toddysm/custos/pull/1062), [#1065](https://github.com/toddysm/custos/pull/1065). Docs (developer): [wait diagnostics and cancellation](developers/workflow-resume-subscriptions.md); Docs (user/operator): [local cluster evidence and recovery](users/evaluation/local-cluster.md). Historical HTTP 503 cause remains unknown under explicit owner acceptance; successful verification and controlled fault injection are not proof of its elimination. COMP-004 remains In progress; #1053/#1018/#988 and their execution limits remain separate. |
| [#1018](https://github.com/toddysm/custos/issues/1018) | Deployed certification | trigger-service / cross-service | In progress | Binding capacity #1048 is complete; #1052 repairs the separate #1051 scheduler baseline. All receiver business/failure/security/scale/shutdown certification criteria remain unchecked. Passing scheduler readiness, public Helm checks and configuration/regression tests are not receiver-to-Workflow effects certification. |
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
Subsequent D06-D08 approvals remain recorded there. Revision-8/D09 and its
breakdown were explicitly approved on 2026-11-06; [#1049](https://github.com/toddysm/custos/pull/1049)
merged after separate explicit authorization to implement #1048. That approval
does not certify #1018 or complete the epic.

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
