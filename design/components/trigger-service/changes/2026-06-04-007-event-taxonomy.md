# Change: event-taxonomy

Date: 2026-06-04
Type: component-design
Component: trigger-service
Sequence: 007
GitHub Issue: #18
Status: closed

## Summary

Resolves TODO-001 (and the INCON-013 cross-component scope expansion). Defines
the **canonical platform event taxonomy**: a closed registry of dot-namespaced
`kind` strings (`<domain>.<event>`) that selectors match on and that connector
authors, ARM (TODO-009), and Observability/Audit all share. The registry is the
authoritative source for the unified `kind` namespace; it is implemented as
`custos_trigger/taxonomy.py` and documented as the platform-canonical source so
ARM/WF/Observability consume the same strings rather than re-inventing them.

## Before

`NormalizedEvent.kind` carried free-form examples
(`registry.push|registry.tag|pr.merged|workflow.completed|cron.tick|manual.fire|…`)
with no enumerated registry, no shape rules, and no owner. TODO-001 (scope
expanded by INCON-013 to unify with ARM TODO-009 + Observability) left the
namespace undefined, so connector authors could not target events
deterministically and audit consumers could not rely on stable kind strings.

## After

### Kind shape rule

`^[a-z][a-z0-9]*(\.[a-z][a-z0-9_]*)+$` — lowercase, at least one dot; the first
segment is the **domain**. Examples: `workflow.completed`, `step.retry_scheduled`,
`registry.push`.

### Platform-owned domains (closed registry — exact membership validated)

| Domain | Canonical kinds | Emitter |
|---|---|---|
| `manual` | `manual.fire` | Manual Receiver (REQ-004) |
| `cron` | `cron.tick` | Scheduler Receiver (REQ-005, M2) |
| `webhook` | `webhook.received` | Generic Webhook Receiver (M2) |
| `workflow` | `workflow.started`, `workflow.completed`, `workflow.failed`, `workflow.cancelled` | Workflow Service (`custos.workflow.events`) |
| `run` | `run.started`, `run.completed`, `run.failed`, `run.cancelled` | Workflow Service run lifecycle |
| `step` | `step.started`, `step.succeeded`, `step.failed`, `step.retry_scheduled`, `step.waiting`, `step.resumed`, `step.timed_out` | Workflow Service step lifecycle |
| `activity` | `activity.scheduled`, `activity.started`, `activity.succeeded`, `activity.failed`, `activity.timed_out`, `activity.cancelled` | Activity Runtime Manager (ARM TODO-009) |
| `registry` | `registry.push`, `registry.tag`, `registry.delete` | Connector (OCI registry) |
| `pr` | `pr.opened`, `pr.merged`, `pr.closed`, `pr.review_requested`, `pr.synchronized` | Connector (SCM) |
| `scan` | `scan.started`, `scan.completed`, `scan.failed`, `scan.vulnerable` | Connector (security) |

### Connector-authored (vendor) kinds

Connector authors may emit kinds under a **vendor-reserved domain** declared in
the connector manifest (e.g. `ghcr.*`, `github.*`, `acr.*`). These are validated
for **shape only** (the regex above), not for membership — the platform does not
enumerate every vendor event. A vendor domain MUST NOT collide with a
platform-owned domain.

### Rules

1. Platform domains are a locked set; their kind lists are enumerated and
   validated exactly (`is_canonical_kind`). Adding a platform kind requires an
   explicit registry edit guarded by an enum-grid test.
2. Selectors match on `event.kind` via CEL (`==`, `.startsWith("registry.")`,
   `in [...]`).
3. The Internal Event Receiver maps the `custos.workflow.events` envelope
   `status` field onto `workflow.<status>` / `run.<status>` canonical kinds.
4. INCON-013 resolution: this registry is the single source of truth for the
   unified namespace. ARM emits the `activity.*` strings verbatim; Observability
   indexes audit events by the same `kind`. The module may later be promoted to
   a shared library (`custos-common`) so the three services import rather than
   mirror — that promotion is a non-breaking move and out of scope for M1.

## Rationale

- **Determinism for authors.** A closed platform registry + a shape rule for
  vendor extensions lets connector and workflow authors target events without
  guessing.
- **One namespace across services.** Directly resolves INCON-013 — connector
  events and activity/step lifecycle audit events live in one dot-namespaced
  space.
- **Extensible.** Vendor domains keep connector innovation un-gated while
  protecting the platform-owned namespace.

## Impact

- Trigger Service: `custos_trigger/taxonomy.py` —
  `CANONICAL_EVENT_KINDS` frozenset, `PLATFORM_DOMAINS`, `is_canonical_kind()`,
  `validate_kind()`; consumed by the Normalizer, the selector type-checker's
  `event.kind` enum hints, and subscription validation.
- ARM TODO-009 / Observability: consume the `activity.*` / `step.*` strings;
  cross-referenced, not duplicated.
