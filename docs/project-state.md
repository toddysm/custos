# Project State: Custos

> Durable capability inventory and work ledger for the Custos platform.
> This file is the source of truth for "where the project is" and is
> reconciled against code, design docs, and GitHub issues/PRs.

Last reconciled: 2026-08-28

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
| COMP-002 | AuthN/AuthZ Service | auth-service | Implemented | — |
| COMP-003 | Workflow Service | workflow-service | In progress | Observability Client integration — #989 |
| COMP-004 | Trigger Service | trigger-service | In progress | M2 receivers — #988; design TODOs #22 / #23 (#20 resolved) |
| COMP-005 | Connector Service | connector-service | Implemented | — |
| COMP-006 | Activity Runtime Manager | activity-runtime-manager | Implemented | M2 deferrals — #991; Kata tier validation — #763 |
| COMP-007 | Definition/Template/Catalog Service | catalog-service | Implemented | — |
| COMP-008 | Storage Provider Layer | storage-provider-layer | Implemented | schema-revision policy — #993 |
| COMP-009 | Observability and Audit Service | observability-audit-service | Implemented | M2 deferrals — #992 |
| COMP-010 | Web UI and Template Designer | web-ui | Deferred (M2+) | not yet implemented — #994 |
| COMP-011 | Local Dev & Test CLI | custosctl | Implemented | — |

## Work ledger (in-flight)

| Item | Kind | Component | State | Notes |
|---|---|---|---|---|
| #20 | Design TODO | trigger-service | Done | Scheduler leader-election — Postgres leader-lease row; resolved via #997 (2026-08-28); implementation follows in #988 |
| #22 | Design TODO | trigger-service | Open | Dead-letter handling + replay UX |
| #23 | Design TODO | trigger-service | Open | Owner of webhook signing keys |
| #763 | Enhancement | activity-runtime-manager | Open | Validate vm/microvm (Kata) tier specifics |
| #988 | Implementation gap | trigger-service | Open | M2 receivers (scheduler, webhook, vendor-push, pull) |
| #989 | Implementation gap | workflow-service | Open | Full Observability Client integration |
| #990 | Implementation gap | api-gateway | Open | M2 items (thin-client, OpenAPI ext, device-code, rate-limiter, multi-region) |
| #991 | Implementation gap | activity-runtime-manager | Open | M2 deferrals (manifest signing, artifact schema, secret slots, short-form refs, policy-eval@1) |
| #992 | Implementation gap | observability-audit-service | Open | M2 deferrals (taxonomy registry, conformance suite, audit hash chain) |
| #993 | Implementation gap | storage-provider-layer | Open | Multi-revision adapter schema-upgrade policy |
| #994 | Implementation gap | web-ui | Open | Component not yet implemented (deferred M2+) |

## Deferred / Won't-do

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
