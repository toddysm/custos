# Trigger Service Public API

Last Updated: 2026-06-04

The Trigger Service turns inbound events into workflow runs. It owns three
surfaces:

1. A **REST surface** for managing *start* trigger subscriptions and firing them
   manually (`/v1/workspaces/{workspaceId}/triggers...`).
2. An **internal RPC surface** the Workflow Service calls to register and cancel
   *resume* waits (`waitFor:` steps).
3. An **internal event receiver** that consumes the platform's
   `custos.workflow.events` stream and drives both start subscriptions and
   resume waits.

Every event — whatever its origin — is normalized into a single
[`NormalizedEvent`](#normalizedevent-envelope) envelope and run through the
linear `Classify → Match → Dedup → Dispatch` pipeline.

> **Milestone note.** M1 ships the REST surface, the internal RPC contract, the
> `custos.workflow.events` receiver, CEL selectors, dedup, and dispatch. The
> connector-driven receivers (webhook, vendor-push, pull/poller), the cron
> scheduler, and the declarative `spec.triggers:` YAML parser are
> [deferred to M2](#deferred-to-m2). Where this document describes a deferred
> surface it says so explicitly.

> **Doc-as-contract.** Every fenced `json`, `yaml`, and `cel` block in this
> guide is parsed and validated against the real models, the CEL compiler, and
> the canonical taxonomy by
> [`tests/test_docs_examples.py`](../../src/services/trigger-service/tests/test_docs_examples.py),
> so the examples cannot drift from the code.

---

## Contents

- [Authentication and call context](#authentication-and-call-context)
- [REST surface](#rest-surface)
  - [Create a subscription](#create-a-subscription)
  - [Read a subscription](#read-a-subscription)
  - [Update a subscription](#update-a-subscription)
  - [Delete a subscription](#delete-a-subscription)
  - [Fire a subscription manually](#fire-a-subscription-manually)
  - [Error taxonomy (RFC 7807)](#error-taxonomy-rfc-7807)
- [Internal RPC contract](#internal-rpc-contract)
- [NormalizedEvent envelope](#normalizedevent-envelope)
- [Event taxonomy reference](#event-taxonomy-reference)
- [CEL selector guide](#cel-selector-guide)
  - [Legacy selector desugaring](#legacy-selector-desugaring)
- [Dispatch, dedup, and resume semantics](#dispatch-dedup-and-resume-semantics)
- [Declarative trigger YAML (M2)](#declarative-trigger-yaml-m2)
- [Configuration](#configuration)
- [Deferred to M2](#deferred-to-m2)

---

## Authentication and call context

REST routes are tenant-scoped and require a signed Custos call context. The
workspace in the path **must** match the workspace in the call context; a
mismatch is rejected with `403` and the call-context error code
`workspace_mismatch` (defense-in-depth). Permissions are checked per route:
`read` for `GET`, `write` for `POST`/`PATCH`, `delete` for `DELETE`, and `fire`
for the `:fire` action.

Call-context failures (missing or invalid context, permission denied, workspace
mismatch) are **not** RFC 7807 documents — they use the shared call-context
error envelope:

```json
{
  "error": {
    "code": "workspace_mismatch",
    "detail": "call context workspace does not match the request path workspace"
  }
}
```

The call-context error codes are `callctx_missing` (`401`), `permission_denied`
(`403`), and `workspace_mismatch` (`403`). Everything else — validation,
dispatch, dedup — uses the [RFC 7807 taxonomy](#error-taxonomy-rfc-7807) below.

The internal RPC routes (`/RegisterResumeSubscription`,
`/CancelResumeSubscription`) and the event receiver routes (`/dapr/subscribe`,
`/internal/events/workflow`) are invoked service-to-service over Dapr and carry
no end-user call context, so they bypass the call-context middleware.

---

## REST surface

All subscription routes live under the workspace-scoped prefix:

```
/v1/workspaces/{workspaceId}/triggers
```

| Method | Path | Body | Success | Notes |
|---|---|---|---|---|
| `POST` | `/v1/workspaces/{workspaceId}/triggers` | `SubscriptionCreate` | `201` `Subscription` | Create a start subscription |
| `GET` | `/v1/workspaces/{workspaceId}/triggers/{subscriptionId}` | — | `200` `Subscription` | Read one |
| `PATCH` | `/v1/workspaces/{workspaceId}/triggers/{subscriptionId}` | `SubscriptionPatch` | `200` `Subscription` | Partial update |
| `DELETE` | `/v1/workspaces/{workspaceId}/triggers/{subscriptionId}` | — | `204` | Soft-delete (state → `expired`) |
| `POST` | `/v1/workspaces/{workspaceId}/triggers/{subscriptionId}:fire` | `ManualFireRequest` | `200` `ManualFireResult` | Fire now |

All request and response bodies are JSON with **camelCase** field names.

### Create a subscription

Create a *start* subscription that fires a workflow when a matching event
arrives. `selector` is an optional [CEL](#cel-selector-guide) boolean predicate;
omit it to fire unconditionally on the subscription's source. `inputMapping`
carries the `${{ … }}` placeholder map handed to the started run.

Request body — `SubscriptionCreate`:

<!-- doctest: SubscriptionCreate -->
```json
{
  "sourceType": "internal",
  "workflowId": "build-and-sign",
  "targetWorkflowVersionId": "wfv-2026-06-01",
  "selector": "event.kind == \"workflow.completed\" && event.data.status == \"succeeded\"",
  "inputMapping": {
    "image": "${{ event.data.outputs.image }}"
  }
}
```

`sourceType` is one of `manual`, `scheduled`, `webhook`, `vendor-push`, `pull`,
`internal`. A minimal manual subscription needs only the source type and target
workflow:

<!-- doctest: SubscriptionCreate -->
```json
{
  "sourceType": "manual",
  "workflowId": "promote-image"
}
```

The response is the full stored `Subscription`:

<!-- doctest: Subscription -->
```json
{
  "workspaceId": "ws-acme",
  "subscriptionId": "9f1c2b7e4a6d4f0b8c3e1a2d4f6b8c0e",
  "kind": "start",
  "sourceType": "internal",
  "workflowId": "build-and-sign",
  "targetWorkflowVersionId": "wfv-2026-06-01",
  "selector": "event.kind == \"workflow.completed\"",
  "inputMapping": {"image": "${{ event.data.outputs.image }}"},
  "state": "active",
  "createdAt": "2026-06-04T12:00:00Z",
  "updatedAt": "2026-06-04T12:00:00Z"
}
```

A selector that fails to compile is rejected at create time with
`422 trigger.selector_invalid`.

### Read a subscription

`GET /v1/workspaces/{workspaceId}/triggers/{subscriptionId}` returns the
`Subscription` shown above, or `404 trigger.subscription_not_found`.

> Reading a subscription requires a backend that implements the subscription
> read surface. The in-memory backend does; the M1 Postgres adapter implements
> only the durable write surface and returns
> `501 trigger.api.subscription_read_unsupported` for reads.

### Update a subscription

`PATCH` accepts a sparse `SubscriptionPatch` — only the supplied fields change.
`state` drives the `active` → `paused` → `expired` transitions; `selector` and
`inputMapping` re-author the match and mapping. An explicit `null` clears
`selector` or `targetWorkflowVersionId`; omitting a field leaves it untouched.

<!-- doctest: SubscriptionPatch -->
```json
{
  "state": "paused",
  "selector": "event.data.tier == \"gold\""
}
```

### Delete a subscription

`DELETE` is a soft-delete: the subscription transitions to `expired` and stops
matching. The call is idempotent and returns `204` even if the subscription was
already expired.

### Fire a subscription manually

`POST /v1/workspaces/{workspaceId}/triggers/{subscriptionId}:fire` synthesizes a
`manual.fire` event from the request `inputs`, runs it through the subscription's
selector, and — on a match — dispatches a run.

Request body — `ManualFireRequest`:

<!-- doctest: ManualFireRequest -->
```json
{
  "inputs": {
    "tier": "gold",
    "image": "ghcr.io/acme/app:1.4.2"
  }
}
```

Success response — `ManualFireResult`:

<!-- doctest: ManualFireResult -->
```json
{
  "runId": "run-7b3c9a"
}
```

Outcomes:

| Condition | Status | Problem `code` |
|---|---|---|
| Run dispatched | `200` | — |
| Subscription inactive or selector did not match | `409` | `trigger.api.subscription_not_fireable` |
| Duplicate fire (same dedup key, still within TTL) | `409` | `trigger.dedup_duplicate` |
| Workflow Service dispatch failed | `502` | `trigger.dispatch_failed` |

### Error taxonomy (RFC 7807)

Validation, dispatch, dedup, and resource errors are returned as
`application/problem+json` with a stable `code`. The `type` URI is
`https://errors.custos.dev/<code-with-slashes>` (e.g.
`https://errors.custos.dev/trigger/selector_invalid`).

> Call-context / auth failures (`401`/`403`) are the exception: they use the
> [call-context error envelope](#authentication-and-call-context)
> (`{"error": {"code": ..., "detail": ...}}`), not a Problem document.

| `code` | HTTP | Meaning |
|---|---|---|
| `trigger.subscription_not_found` | 404 | No such subscription in the workspace |
| `trigger.selector_invalid` | 422 | Selector failed to compile |
| `trigger.selector_type_error` | 422 | Selector did not evaluate to a boolean |
| `trigger.dispatch_failed` | 502 | Dispatch to the Workflow Service failed |
| `trigger.dedup_duplicate` | 409 | A run for this event was already dispatched |
| `trigger.loop_detected` | 409 | Fan-out depth limit exceeded |
| `trigger.api.bad_request` | 400 | Malformed request or workspace mismatch |
| `trigger.api.subscription_not_fireable` | 409 | Subscription inactive or selector miss |
| `trigger.api.subscription_read_unsupported` | 501 | Backend has no subscription read surface |
| `trigger.api.resume_read_unsupported` | 501 | Backend has no resume read surface |

A representative Problem document:

```json
{
  "type": "https://errors.custos.dev/trigger/selector_invalid",
  "title": "Selector failed to compile",
  "status": 422,
  "code": "trigger.selector_invalid",
  "detail": "selector did not compile: unbound name 'foo'"
}
```

---

## Internal RPC contract

The Workflow Service registers and cancels resume waits over Dapr
service-invocation. These are bare method-name paths (no `/v1` prefix):

| Method path | Body | Success |
|---|---|---|
| `POST /RegisterResumeSubscription` | `RegisterResumeRequest` | `200` `RegisterResumeResponse` |
| `POST /CancelResumeSubscription` | `CancelResumeRequest` | `204` |

A resume wait is keyed by the `(runId, stepId, eventKey)` idempotency triple.
`selector` is an optional CEL predicate that further narrows the match (`null` =
match on the event key alone). `ttl` is an ISO-8601 duration the wait stays live
(e.g. `PT24H`, `P7D`); an absent or unparseable `ttl` falls back to
`TRIGGER_RESUME_DEFAULT_TTL_SECONDS` (default 7 days).

Register request — `RegisterResumeRequest`:

<!-- doctest: RegisterResumeRequest -->
```json
{
  "runId": "run-9",
  "stepId": "step-3",
  "eventKey": "workflow.completed",
  "selector": "event.data.runId == \"run-9\"",
  "ttl": "PT24H"
}
```

Register response — `RegisterResumeResponse`:

<!-- doctest: RegisterResumeResponse -->
```json
{
  "subscriptionId": "res_2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae"
}
```

The `subscriptionId` is deterministic: `res_<sha256(runId\0stepId\0eventKey)>`.

Cancel request — `CancelResumeRequest`:

<!-- doctest: CancelResumeRequest -->
```json
{
  "runId": "run-9",
  "stepId": "step-3",
  "eventKey": "workflow.completed"
}
```

**Replay semantics.** Registration is idempotent on the triple:

- Re-registering a still-live wait returns the existing `subscriptionId` with no
  new write.
- If a replay carries a **divergent** selector, the original registration wins
  and a `resume.subscription.divergent` audit event is emitted.
- If the prior wait has lapsed past its TTL, the stale row is dropped and the
  registration is treated as fresh.
- The selector is compiled on **every** path, so malformed CEL is rejected with
  `422` even on a replay.

Cancellation is idempotent: cancelling an unknown or already-expired triple is a
clean `204` no-op.

Resume registrations are stored under the reserved `_resume` workspace sentinel
(the triple is globally unique because run ids are global); the tenant workspace
is taken from the inbound event at delivery time.

---

## NormalizedEvent envelope

Every receiver normalizes its source payload into one `NormalizedEvent` before
the event enters the pipeline. It is also the shape selectors evaluate against
via the `event` binding root.

<!-- doctest: NormalizedEvent -->
```json
{
  "schemaVersion": "1",
  "eventId": "1f3a9c5e-2b7d-5e4a-9c1b-8d2f6a4b0c3e",
  "source": {
    "type": "internal",
    "occurredAt": "2026-06-04T12:00:00Z"
  },
  "kind": "workflow.completed",
  "subject": "run-9",
  "data": {
    "workflowVersionId": "wfv-9",
    "runId": "run-9",
    "status": "succeeded",
    "outputs": {"image": "ghcr.io/acme/app:1.4.2"}
  },
  "raw": {
    "headers": {},
    "body": ""
  }
}
```

Fields:

| Field | Type | Notes |
|---|---|---|
| `schemaVersion` | `"1"` | Envelope version (literal) |
| `eventId` | string | Stable id; deterministic for replays of the same logical event |
| `source.type` | `SourceType` | `manual` / `scheduled` / `webhook` / `vendor-push` / `pull` / `internal` |
| `source.connectorInstanceId` | string \| null | Set for connector-sourced events |
| `source.subscriptionId` | string \| null | Set for manual fires |
| `source.vendor` | string \| null | Vendor namespace, when applicable |
| `source.occurredAt` | string | ISO-8601 timestamp |
| `kind` | string | Canonical [taxonomy](#event-taxonomy-reference) kind; validated at construction |
| `subject` | string | The event subject (e.g. the run id or subscription id) |
| `data` | object | Normalized, vendor-agnostic payload — the selector's `event.data.*` |
| `raw.headers` | object | Lower-cased inbound transport headers |
| `raw.body` | string | Raw textual body, retained for audit |

The two platform-internal sources are produced by `normalize_manual_fire`
(`kind = manual.fire`, `source.type = manual`) and `normalize_workflow_event`
(`source.type = internal`, `kind` derived from the lifecycle status — see the
taxonomy below). Connector-sourced events arrive already shaped as a
`NormalizedEvent` from the Connector Runtime.

---

## Event taxonomy reference

`kind` strings are dot-namespaced `<domain>.<event>[.<subevent>...]` values. The
first segment is the **domain**. Two tiers exist:

- **Platform-owned domains** are a *closed* registry, validated for exact
  membership. Adding a platform kind is a deliberate registry edit.
- **Connector-authored (vendor) domains** let plugin authors emit kinds under a
  vendor-reserved domain (e.g. `ghcr.*`, `github.*`, `acr.*`), validated for
  **shape only**, never colliding with a platform domain.

The closed platform registry:

| Domain | Canonical kinds |
|---|---|
| `manual` | `manual.fire` |
| `cron` | `cron.tick` |
| `webhook` | `webhook.received` |
| `workflow` | `workflow.started`, `workflow.completed`, `workflow.failed`, `workflow.cancelled` |
| `run` | `run.started`, `run.completed`, `run.failed`, `run.cancelled` |
| `step` | `step.started`, `step.succeeded`, `step.failed`, `step.retry_scheduled`, `step.waiting`, `step.resumed`, `step.timed_out` |
| `activity` | `activity.scheduled`, `activity.started`, `activity.succeeded`, `activity.failed`, `activity.timed_out`, `activity.cancelled` |
| `registry` | `registry.push`, `registry.tag`, `registry.delete` |
| `pr` | `pr.opened`, `pr.merged`, `pr.closed`, `pr.review_requested`, `pr.synchronized` |
| `scan` | `scan.started`, `scan.completed`, `scan.failed`, `scan.vulnerable` |

The internal `custos.workflow.events` receiver maps the Workflow Service's
lifecycle `status` vocabulary onto the `workflow.*` domain: `queued`/`running`/
`started` → `workflow.started`, `succeeded` → `workflow.completed`, `failed` →
`workflow.failed`, `cancelled` → `workflow.cancelled`.

---

## CEL selector guide

Selectors are [CEL](cel-expressions.md) boolean expressions evaluated against the
`NormalizedEvent` as the `event` root. They are compiled and type-checked when a
subscription is created (a malformed or mistyped selector is rejected up front)
and evaluated at match time.

Available bindings:

- `event.kind` — string
- `event.subject` — string
- `event.source.type` / `.connectorInstanceId` / `.subscriptionId` / `.vendor` / `.occurredAt`
- `event.data.*` — the normalized payload (dynamic)
- `event.raw.headers` / `event.raw.body`

Worked selectors (each compiles against the real schema):

<!-- doctest: cel -->
```cel
event.data.tier == "gold"
```

<!-- doctest: cel -->
```cel
event.kind == "workflow.completed" && event.data.status == "succeeded"
```

<!-- doctest: cel -->
```cel
event.data.repository == "ghcr.io/acme/app"
```

**Supported.** Equality (`==`, `!=`), ordering comparisons (`<`, `<=`, `>`,
`>=`), membership (`in`), boolean logic (`&&`, `||`, `!`), string literals, and
member access.

**Not supported** in the v1 `custos-cel` subset: method-call syntax such as
`event.data.repository.startsWith("...")`, regular-expression functions, and
JSONPath. Use the desugared range form below for prefix matching.

A selector that evaluates to a non-boolean is a `422 trigger.selector_type_error`;
a runtime resolution failure (e.g. a missing field) is treated as a no-match.

### Legacy selector desugaring

The legacy `field: matchType:value` tuple form is lowered to CEL before storage
via `desugar_legacy_selector`. The field addresses the vendor payload, so it is
rooted at `event.data.`:

| Legacy form | Desugared CEL |
|---|---|
| `repository` `eq` `ghcr.io/acme/app` | see block below |
| `repository` `prefix` `ghcr.io/acme/` | see block below |

`eq` lowers to an equality:

<!-- doctest: cel -->
<!-- doctest: desugar field=repository match=eq value=ghcr.io/acme/app -->
```cel
event.data.repository == "ghcr.io/acme/app"
```

`prefix` lowers to a half-open lexicographic range (the upper bound increments
the final code point of the prefix):

<!-- doctest: cel -->
<!-- doctest: desugar field=repository match=prefix value=ghcr.io/acme/ -->
```cel
event.data.repository >= "ghcr.io/acme/" && event.data.repository < "ghcr.io/acme0"
```

The `regex` and `jsonpath` match types have no representation in the v1 subset
and are rejected — author an explicit CEL selector instead.

---

## Dispatch, dedup, and resume semantics

Every matched event flows through `Classify → Match → Dedup → Dispatch`.

**Dedup.** Before dispatching, the pipeline reserves a deterministic dedup key
`trigger.dedup.v1:<sha256(subscriptionId, eventId)>` in the metadata store with a
TTL (`TRIGGER_DEDUP_TTL_SECONDS`, default 24h). A reserve that finds the key
already present collapses the event to a duplicate — at-least-once redelivery of
the same logical event therefore dispatches exactly once.

**Dispatch outcomes** (`DispatchStatus`):

| Status | Meaning |
|---|---|
| `dispatched` | The Workflow Service RPC succeeded (run started or step resumed) |
| `duplicate` | The event was a replay; no RPC issued |
| `dead_lettered` | Retries exhausted or a permanent failure |
| `loop_rejected` | Fan-out depth limit (`TRIGGER_FANOUT_MAX_DEPTH`, default 16) exceeded |

Transient Workflow Service failures are retried with exponential backoff up to
`TRIGGER_DISPATCH_MAX_RETRIES` (default 5); non-retryable failures fail fast.

**Fan-out guard.** A trigger may start a workflow whose completion fires another
trigger. Each dispatch carries a depth counter; once it exceeds the configured
maximum the dispatch is rejected (`loop_rejected` / `trigger.loop_detected`) to
break runaway cascades.

**Resume.** A `workflow.*` (or other internal) event whose `(runId, stepId,
eventKey)` triple matches a live resume registration delivers a
`RaiseExternalEvent` to the waiting run instead of starting a new one. The
delivery is deduped on the same key so a redelivered event resumes the step
once.

**Audit events.** Each pipeline stage emits a stable audit event the
Observability/Audit service consumes:

| Event | Emitted when |
|---|---|
| `trigger.matched` | A subscription or resume wait matched the event |
| `trigger.deduped` | A duplicate was collapsed |
| `trigger.dispatched` | A run was started |
| `resume.delivered` | A waiting step was resumed |
| `trigger.dispatch.failed` | A dispatch failed terminally |
| `trigger.loop.detected` | A fan-out loop was rejected |

---

## Declarative trigger YAML (M2)

> **Deferred to M2.** The declarative `spec.triggers:` block below is the locked
> design contract for authoring triggers in workflow YAML. The parser that turns
> it into Trigger Service subscriptions lives in the Workflow Service and is not
> implemented in M1 — author triggers through the [REST surface](#rest-surface)
> for now. The example is checked for well-formed YAML only; its embedded
> selectors use connector-source semantics (e.g. `startsWith`) that the M1 CEL
> subset does not implement.

```yaml
spec:
  triggers:
    - type: manual

    - type: scheduled
      cron: "0 */6 * * *"
      timezone: UTC

    # Push mode: the registry emits webhooks to us.
    - type: registry.push
      connector: ghcr-prod
      mode: push
      selector: event.data.repository == "ghcr.io/acme/app"

    # Pull mode: poll a source with no reliable webhook.
    - type: registry.push
      connector: acr-prod
      mode: pull
      pollInterval: 5m
      selector: event.data.repository == "acme.azurecr.io/app"

    # Internal: another workflow's completion drives this one.
    - type: workflow.completed
      workflow: build-and-sign
      selector: event.kind == "workflow.completed" && event.data.status == "succeeded"
      inputMapping:
        image: ${{ event.data.outputs.image }}
```

Each declarative entry maps to a `SubscriptionCreate` the Workflow Service parser
issues on the author's behalf: `type` → `sourceType` + match domain, `selector`
→ `selector` (CEL or desugared legacy sugar), and `inputMapping` → `inputMapping`.

---

## Configuration

The service is configured by environment variables (design § Configuration):

| Variable | Default | Purpose |
|---|---|---|
| `TRIGGER_WEBHOOK_BASE_URL` | _(required)_ | Public base URL for inbound webhooks (M2 receivers) |
| `TRIGGER_DEDUP_TTL_SECONDS` | `86400` | Dedup key lifetime |
| `TRIGGER_POLLER_DEFAULT_INTERVAL_SECONDS` | `60` | Default pull-poll interval (M2) |
| `TRIGGER_RESUME_DEFAULT_TTL_SECONDS` | `604800` | Default resume-wait lifetime (7 days) |
| `TRIGGER_DISPATCH_MAX_RETRIES` | `5` | Max retries for a transient dispatch failure |
| `TRIGGER_FANOUT_MAX_DEPTH` | `16` | Fan-out depth cap before `loop_rejected` |
| `TRIGGER_SCHEDULER_LEADER_LEASE_SECONDS` | `30` | Scheduler leader lease (M2) |
| `TRIGGER_PUBSUB_COMPONENT` | `custos-pubsub` | Dapr pub/sub component name |
| `TRIGGER_NORMALIZED_TOPIC` | `custos.triggers.normalized` | Normalized-event topic |
| `TRIGGER_WORKFLOW_EVENTS_TOPIC` | `custos.workflow.events` | Internal lifecycle-event topic |
| `TRIGGER_WF_ENDPOINT` | _(required)_ | Dapr app-id of the Workflow Service |
| `TRIGGER_CONNECTOR_ENDPOINT` | _(required)_ | Dapr app-id of the Connector Service |
| `TRIGGER_METADATA_STORE` | _(required)_ | SPL metadata-store DSN (secret) |

Health and readiness probes are exposed at `/healthz` and `/readyz` on the HTTP
port; the Dapr app-id is `trigger-service`.

---

## Deferred to M2

The following are designed and reserved in the SPL v1 schema but not implemented
in M1:

- **Cron scheduler** (`scheduled` triggers) — requires a leader-election
  mechanism (design TODO-003).
- **Generic webhook receiver** — inbound webhook auth + de-multiplexing per
  connector instance (design TODO-006).
- **Vendor-push receivers** — host for connector `listen(mode=push)` streams.
- **Pull receivers / pollers** — interval polling for connectors without
  reliable push (REQ-074).
- **Declarative `spec.triggers:` parser** — the Workflow Service YAML parser that
  creates subscriptions from workflow definitions.
- **Postgres subscription/resume read surface** — the M1 Postgres adapter
  implements only the durable write surface; reads return
  `501 ...read_unsupported`.
- **Selective dedup-key clear admin API** — operators currently wait out the
  dedup TTL window (design TODO-007).
- **Dead-letter handling and replay UX** for terminal dispatch failures (design
  TODO-005).
