---
name: bucket-a-alignment
description: Assign REQ-080/REQ-081 to M2, add contract-vs-implementation framing, and clarify M1 scope (OIDC contract-locked but disabled, traces deferred to M2, Web UI not deployed)
type: requirement
---

# Change: bucket-a-alignment

Date: 2026-05-18
Type: requirement
Sequence: 006
GitHub Issues: #91, #96
Status: open

## Summary

Bucket A of the design-inconsistency cleanup touched requirements in three ways:

1. **Milestone assignment for REQ-080 and REQ-081.** Both requirements had been added on 2026-05-16 with status `Open` but no milestone row. Implementation planning could not pull them in until they were placed in the milestone table.
2. **Contract-vs-implementation framing.** Component designs (Auth, API Gateway, Trigger, Observability, Web UI) had grown to define a v1 surface that exceeds what M1 will actually deliver. Without an explicit framing rule, reviewers reconciling component designs against the milestone table kept hitting apparent contradictions (OIDC, device-code routes, trigger receivers, traces, Web UI). Resolving these case-by-case was leading to drift; a single framing rule resolves all of them at once.
3. **M1 row clarifications.** The M1 row needed parenthetical clarifications so that the apparent mismatches against component designs read as intentional contract-vs-implementation splits rather than scope conflicts.

This change keeps the requirements file as the authoritative source for *when code paths light up*, while pointing readers at component designs for *what shape those code paths must have when they do*.

## Before

```
| M1 — Core engine | +3 months (≈ 2026-08-13) | ... minimal auth (REQ-035 API tokens); ... basic logs + Prometheus metrics (REQ-040, REQ-042, REQ-078); ... |
| M2 — Triggers & action breadth | +6 months | Scheduled trigger (REQ-005); registry webhook trigger (REQ-006); extensible connector model beyond registries (REQ-074); SBOM action (REQ-017); ... |
```

REQ-080 and REQ-081 appear in the Functional Requirements table with `Open` status but are absent from the milestone table. No preamble exists explaining how to read component-design features that exceed M1.

## After

A new preamble paragraph is inserted under the existing scope-vs-capacity note:

> **Contract vs implementation (added 2026-05-18):** Component design documents define **v1 contracts** — the locked interface surface, schemas, audit events, and storage migrations a component will own across all milestones. The milestone table below tracks the **first implementation milestone** for each requirement. A requirement may therefore be _contract-locked in v1_ (its interfaces/schemas appear in component designs and the Storage Provider Layer's migration set) while its _implementation_ is deferred to a later milestone (its code paths are stubbed, its routes 404, its dispatcher arms are no-ops, its tables exist but are unused). Reviewers reconciling this document with component designs should read mismatches through that lens: a feature mentioned in a component design but not in M1 below is contract-locked but not yet implemented.

The M1 row gains three parenthetical clarifications:

```
| M1 — Core engine | ... minimal auth (REQ-035 API tokens — OIDC/RBAC contract-locked but disabled, see Auth Service design); ... basic logs + Prometheus metrics (REQ-040, REQ-042, REQ-078) — **traces deferred to M2**; ... Web UI (COMP-010) **not deployed in M1**. |
```

The M2 row absorbs REQ-080, REQ-081, and the initial Web UI deployment:

```
| M2 — Triggers & action breadth | +6 months | Scheduled trigger (REQ-005); registry webhook trigger (REQ-006); extensible connector model beyond registries (REQ-074); internal workflow-to-workflow triggers (REQ-080); dual-purpose event delivery for start and resume (REQ-081); ... OpenTelemetry tracing (REQ-043); ... Web UI initial deployment (COMP-010 baseline). |
```

The Change History gains a row for 2026-05-18 referencing #91 and #96.

## Impact

- **Implementation planning** can now pull REQ-080 and REQ-081 into the M2 backlog, alongside scheduled and webhook triggers, where the receiver work they require also lands.
- **Reviewers** comparing component designs against the milestone table have a single decision rule for the OIDC, device-code, trigger-receiver, trace, and Web UI mismatches: contract first, implementation when the milestone says so.
- **No requirement is reclassified**: existing priorities, IDs, and statuses are unchanged. Only milestone assignments are added (REQ-080, REQ-081) and existing M1 scope is annotated more precisely.
- **Storage Provider Layer** picks up a corollary in the parallel architecture-side change record: all seven interfaces' migrations land in M1 even though Auth and Observability features come online later. That clarification is owned by `design/architecture/changes/2026-05-18-010-bucket-a-alignment.md`.

## Related Requirements

REQ-034, REQ-035, REQ-036, REQ-040, REQ-042, REQ-043, REQ-056, REQ-078, REQ-080, REQ-081
