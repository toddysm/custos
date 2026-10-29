# Trigger Service Public API

Last Updated: 2026-10-29

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
- [Provider-backed reads](#provider-backed-reads)
- [Dead-letter storage capability](#dead-letter-storage-capability)
- [Deferred to M2](#deferred-to-m2)

---

## Provider-backed reads

Trigger stores use the asynchronous
[`TriggerQueryProvider`](../../src/libs/storage-provider-layer/src/custos_spl/interfaces/trigger_queries.py)
capability implemented by the in-memory and PostgreSQL metadata providers.
Subscription reads return the base row and latest **appended** selector together;
an older caller-supplied selector timestamp does not override append order.
The internal receiver drains bounded candidate pages rather than relying on
test-only synchronous accessors or raw SQL rehydration.

Queries are workspace-scoped, ordered by identifier, and use opaque keyset
cursors bound to the workspace, query kind, filters and time cutoff. The default
page contains up to 50 rows, capped at 200; invalid cursors or non-positive
limits raise explicit errors. Keep the same filters and cutoff while paging.
Pages are not a cross-request snapshot of concurrent writes.

Source-scoped resume queries exclude expired and legacy unassociated waits.
Point reads retain expired registrations so the existing re-registration path
can inspect and replace them. Due-schedule queries include only enabled rows
with `nextFireAt` at or before a timezone-aware cutoff. These provider operations
do not themselves implement external receivers or scheduler execution.

Registration uses the companion `ResumeRegistrationProvider` capability for
an atomic insert-or-refresh. Concurrent requests for a new or expired wait
return the same winning handle without deleting or overwriting another live
registration. Original-selector-wins and divergence audit behavior also apply
to these races; the raw immutable resume-write operation remains unchanged.

Apply the normal metadata migrations to install revision-5 query indexes.
This is an index-only additive revision; existing revision-4 row shapes and
other services' schema minimum remain compatible. Older adapters without the
query capability fail explicitly instead of returning an empty match set.

## Fenced scheduler storage

The optional
[`SchedulerStateProvider`](../../src/libs/storage-provider-layer/src/custos_spl/interfaces/scheduler.py)
capability provides deployment-scoped acquire/renew/release, database-time scans,
fenced due-fire advancement, and bounded workspace/deployment recovery pages.
PostgreSQL metadata revision 6 adds lease and pending-fire tables; revision 9
adds durable coalescing counts and distinct suppression settlement. The general
metadata schema minimum remains revision 4. Scheduler readiness requires
revision 9 and fails rather than falling back to unfenced writes.

[`SchedulerLeadership`](../../src/services/trigger-service/src/custos_trigger/scheduler.py)
is a scan-loop collaborator, not a background worker. Call `poll` at least as
often as the configured renewal interval. Defaults are a 30-second lease and
10-second renewal; invalid or non-positive combinations fail configuration.
Only provider time decides ownership. Renewals retain the epoch, acquisition
after expiry/release advances it, and backend failure clears local authority
and propagates an error. Reacquiring a partial live lease schedules renewal
without assuming a fresh time-to-live.

The advance operation takes a captured subscription revision and due instant.
It checks the current live fence, active managed association and planned-time
window, then commits the pending occurrence and next-fire value atomically.
Stale tokens raise `LeaseExpired`; changed eligibility/revisions raise
`ImmutableViolation`. A pending event is not marked delivered until the
receiver acknowledges publication. Recovery can therefore republish the same
stable event identity rather than lose an event.

Use the same deployment ID for replicas of one logical scheduler and a distinct
holder ID per process. Generic legacy schedule maintenance methods are not
scheduler fencing APIs. The scheduled receiver below consumes this capability; production lifecycle,
workspace discovery, readiness and deployment enablement remain
[#1017](https://github.com/toddysm/custos/issues/1017).

### Scheduled receiver and recovery

[`ScheduledReceiver`](../../src/services/trigger-service/src/custos_trigger/receivers/scheduled.py)
is a workspace-bound scan collaborator. Share deployment leadership across
replicas, supply an acknowledged normalized-event publisher and an audit sink,
then call `tick()`. It does not start a background worker at import or app startup.
One tick handles at most one pending page and one due page (default 50 rows each,
maximum 200); it renews provider authority before external effects.
Unmanaged or corrupt due configuration raises an explicit error rather than
silently advancing or dispatching an unprovable target.

Intervals retain their creation-time UTC anchor. Five-field cron uses the
configured IANA zone, skips nonexistent local times and includes both repeated
hour instants. Recovery coalesces to the most recent due occurrence. Exact skipped
counts are persisted with the occurrence and included in its coalescing audit;
long cron counting yields and refreshes leadership in bounded chunks rather
than publishing a backlog.

`cron.tick` events retain the pending row's stable ID and planned UTC timestamp.
`process_scheduled_event()` uses the existing classifier, selector matcher and
dispatcher but selects only the event's originating subscription. Unrelated
unconditional cron subscriptions cannot fire from another schedule's event.
The producer and consumer recheck eligibility/revision: paused, expired, missing
or reconfigured associations are suppressed rather than rebound to a new target.
Already accepted downstream effects are not undone by a later pause.

Publication failure leaves the occurrence pending and does not roll back its
already committed next-fire advancement. A new leader retries the original
event; durable dispatch receipts prevent a duplicate Workflow effect. Suppressed
pending rows record `discarded_at` and a bounded reason, not `delivered_at`.
Broker adapter wiring and worker/retention activation remain separately gated
by [#1010](https://github.com/toddysm/custos/issues/1010) and
[#1017](https://github.com/toddysm/custos/issues/1017).

## Dead-letter storage capability

The optional
[`DeadLetterStoreProvider`](../../src/libs/storage-provider-layer/src/custos_spl/interfaces/dead_letters.py)
persists failed-dispatch inputs and fenced replay state. PostgreSQL metadata
revision 7 is additive; the shared metadata minimum remains revision 4.
`check_dead_letters_ready` rejects missing migrations or unavailable storage.
The memory provider has the same contracts but is not restart-durable.

Each version-1 capture holds the workspace, entry/subscription identity,
original idempotency key, normalized event **without `raw`**, resolved start or
resume remote procedure call (RPC) target, and safe dispatch-failure history.
Do not pass transport headers, raw bodies, authentication, signatures or signing
material. Unknown envelope/target fields and known credential fields, including
nested mapped copies, are rejected rather than silently changing replay inputs.
Normalizers must still keep application values credential-free: arbitrary
strings cannot reliably be identified as secrets. Failure history stores a
closed classification (`transient`, `permanent`, `configuration`, `unsafe`) and
optional HTTP status, not arbitrary exception text.
The denylist includes the canonical `x-custos-callctx` signed-context header and
Connector Service credential material (`username`/`token`), including nested
copies in resolved inputs.

Limits are enforced before persistence: 256 KiB of encoded JSON per event or
target, nesting depth 32, identifiers 512 UTF-8 bytes, and 1–64 original dispatch
attempts. Retention defaults to 30 days (configurable from 1 second to 365 days);
operator attempts default to 3 (bounded to 1–100). Stored inputs are immutable,
so replay does not rematch a changed subscription or generate a new key.
PostgreSQL stores the bounded snapshot as JSON, not JSONB: it preserves exponent
notation and numeric types so accepted captures remain readable within the same
size bound through replay and retention cleanup.

Queries are workspace-scoped, ordered by entry ID and filterable by subscription,
status and inclusive creation-time bounds. Pages default to 50, cap at 200, and
reject cursors reused with a different workspace or filter.

Claiming atomically consumes an attempt and returns a fresh token and increasing
epoch. Claims default to 60 seconds, are bounded to 300 seconds, and cannot be
renewed. Another active claimant receives `LeaseBusy`; stale completion receives
`LeaseExpired`. Expired claims are recoverable with an `interrupted` history
entry, and the final failed/interrupted attempt becomes `discarded`. Successful
completion records `replayed` and an optional result identity.

Provider time is authoritative, including after database lock waits. No claim
starts at or after retention expiry. A previously acquired live claim can finish
past retention; discard and retention deletion cannot steal it. Terminal entries
remain inspectable until retention deletion. The deletion primitive does **not**
emit audit events: callers must arrange durable expiration audit before removal.

### Durable dispatch and replay (opt-in)

`TRIGGER_DURABLE_DISPATCH_ENABLED=true` selects the durable dispatcher. It
requires the upgraded-writer configuration gate, an authenticated Workflow caller
verifier, and metadata revision 8. `WF_TRIGGER_DELIVERY_ENABLED=true` selects the
matching Workflow endpoint/worker capability. Both flags default to `false`;
production authentication, activation and operations remain
[#1015](https://github.com/toddysm/custos/issues/1015) and
[#1017](https://github.com/toddysm/custos/issues/1017). Do not enable the new
receivers until those prerequisites are deployed.

The optional
[`DeliveryStateProvider`](../../src/libs/storage-provider-layer/src/custos_spl/interfaces/delivery.py)
adds receipt fencing, atomic Run/runtime-input snapshots and durable resume
mirrors. The general metadata minimum remains revision 4. In-flight reservations
return a retryable failure, not duplicate success. Failure capture and completion
of the producer receipt commit in one transaction, so neither storage failure nor
a stale worker can acknowledge uncaptured work or create a phantom failure after
another worker succeeds.

Initial dispatch and replay are bounded to 300 seconds; durable retry counts are
0–63 (at most 64 recorded attempts). Workflow producer leases last 30 seconds;
Trigger/replay claims last at most 300. Only provider time controls ownership.
The dispatch budget is capped by the remaining lease, with 30 seconds reserved
for durable finalization; it never consumes the entire ownership window.
Completed receipts retain the original result, while abandoned pending claims
can be recovered with a new token. Same-request contention can extend the minimum
retention without renewing the current worker's lease.
If the final permitted replay is abandoned, the next replay request persists its
`interrupted` history and `discarded` state, then returns `trigger.replay_unsafe`
without issuing another downstream call. It does not leave a retryable `replaying`
row after the attempt limit is exhausted.

The original normalized event and resolved target are snapshotted atomically
with the initial reservation, before any remote call. Recovery uses that snapshot
even if subscription configuration changes. Terminal dispatch/capture clears the
pending snapshot; bounded retention cleanup can erase expired orphan payloads
without deleting identity/fencing metadata or rematching a new target.

Starts use the original deterministic run ID and the runtime's atomic
ignore-existing policy, including completed instances. Resumes use protocol-2
channels bound to workspace, run, step and event name, plus durable consumer
acknowledgement. Retrying an uncertain send therefore cannot satisfy a later
same-name wait. Run inputs persist the protocol choice and compiled graph before
scheduling. Start lifecycle events reuse the original persisted timestamp so
republishing after an uncertain acknowledgement keeps their normalized identity.
Runtime submission is recorded separately from consumer completion. After an
event has been consumed, its acknowledgement and cleanup remain recoverable with
durable, capped-backoff retries rather than discarding the payload after a short
storage outage. Expired waits explicitly cancel their registrations and remove
their durable mirrors.

Replay uses the stored target, resolved inputs and original key; it does not
rematch changed subscriptions. Legacy or missing targets are inspection/discard
only when safety cannot be established: replay returns
`409 trigger.replay_unsafe`. In durable mode, unversioned resume registrations
are captured as unsafe without issuing a blind legacy delivery. Keep legacy
mode until old waits are drained; never silently upgrade their runtime history.
Existing public request formats and the default legacy path remain valid.

The receipt horizon covers dead-letter retention plus bounded in-flight work.
It is a **minimum**, not an independent purge timer: dead-letter cleanup must not
remove downstream receipts or runtime-input snapshots. Do not purge Dapr history
or unresolved delivery records while recovery remains possible. Authorized
administration and audited payload retention cleanup remain
[#1016](https://github.com/toddysm/custos/issues/1016).

## Authentication and call context

REST routes are tenant-scoped and require a signed Custos call context. The
workspace in the path **must** match the workspace in the call context; a
mismatch is rejected with `403` and the call-context error code
`workspace_mismatch` (defense-in-depth). Permissions are checked per route:
`read` for `GET`, `write` for `POST`/`PATCH`, `delete` for `DELETE`, and `fire`
for the `:fire` action.

Set `TRIGGER_AUTHZ_ENDPOINT` to the Auth Service base URL. Trigger uses
`custos-callctx` to verify Ed25519 signatures against
`/.well-known/jwks.json`, the `custos-auth` issuer, the `custos.internal`
audience, expiry and issue time. An unsigned JSON context is accepted only by
the explicit non-production development shim (empty authz endpoint);
`ENVIRONMENT=production` forbids that shim. Global or blank workspace claims
cannot administer a workspace.

`trigger:admin` is a separate exact permission for dead-letter administration
(the routes remain [#1016](https://github.com/toddysm/custos/issues/1016)).
It is granted to the built-in workspace operator and administrator roles, not
viewer or author roles, and does not imply any subscription permission.

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

The call-context error codes include `callctx_missing` / `callctx_invalid`
(`401`), `callctx_unavailable` (`503`, public-key verification unavailable),
`permission_denied`, `workspace_required`, and `workspace_mismatch` (`403`).
Everything else — validation,
dispatch, dedup — uses the [RFC 7807 taxonomy](#error-taxonomy-rfc-7807) below.

The internal RPC routes (`/RegisterResumeSubscription`,
`/CancelResumeSubscription`) and the event receiver routes (`/dapr/subscribe`,
`/internal/events/workflow`) are invoked service-to-service over Dapr and carry
no end-user call context. Only their exact method/path pairs select independent
mesh authentication: POST for registration, cancellation and event delivery,
and GET for subscription discovery. There is no blanket webhook-prefix bypass.

Configure Dapr's [app API token authentication](https://docs.dapr.io/operations/security/app-api-token/)
and supply the same secret to Trigger through `APP_API_TOKEN`. Requests must
carry the sidecar's `dapr-api-token`. Dapr **>=1.18.6 with mutual TLS** is
required (the deployment pin is stable **1.18.9**); older runtimes do not
authenticate caller headers. Resume calls require `dapr-caller-app-id` and
`dapr-caller-namespace` to match `TRIGGER_WORKFLOW_APP_ID` and
`TRIGGER_WORKFLOW_NAMESPACE`. These are identities, not HTTP addresses.
For outbound invocation, the explicit app ID is qualified with the configured
namespace; namespace qualification is not part of the authenticated app-ID header.
The app authenticates the sidecar before trusting either header. Missing or
blank token configuration returns `503 mesh_unavailable`; nonblank credentials
are compared without normalization. Invalid tokens return
`401 mesh_unauthenticated`, and a different caller returns `403 mesh_caller_denied`.
Discovery and broker-delivery callbacks reject service-invocation caller headers;
another application cannot use service invocation to masquerade as a broker callback.
The shared secret must remain private to the application and its sidecar.
Dapr mutual transport authentication, invocation allowlists, broker publishing
restrictions and direct app-port network policy remain required deployment
controls, wired by [#1017](https://github.com/toddysm/custos/issues/1017).

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

#### Receiver configuration

`scheduled`, `webhook`, `vendor-push`, and `pull` requests require
`sourceConfig`. Manual/internal requests remain unchanged and do not accept it.
Configuration is persisted now; the scheduler, webhook and polling receiver
workers are separate M2 delivery items, not activated by this API change.

**Rollout gate:** `TRIGGER_RECEIVER_CONFIGURATION_ENABLED` defaults to `false`.
Deploy the new code with this gate closed until **all** subscription writers
have upgraded to the atomic mutation path, then explicitly enable it. Do not
roll back to legacy writers while managed receiver configurations exist.
Creating/changing receiver configuration or making it active returns
`503 trigger.source_unavailable` while the gate is closed. Reads and pure
pause/delete remain available; manual/internal requests are unchanged.

| `sourceType` | `sourceConfig` |
|---|---|
| `scheduled` | Exactly one of five-field `cron` or positive integer `intervalSeconds`. Cron accepts an IANA `timezone` (default `UTC`); intervals do not. |
| `webhook`, `vendor-push` | Required `connectorInstanceId`; no schedule or polling fields. |
| `pull` | Required `connectorInstanceId`; optional integer `pollIntervalSeconds` of at least 10. |

Unknown fields, boolean/string intervals, randomized/hash cron extensions and
invalid combinations are rejected with `422 trigger.source_config_invalid`.
Polling intervals express requested cadence; Connector Service still owns
runtime polling policy, higher configured floors, activation and capabilities.

<!-- doctest: SubscriptionCreate -->
```json
{
  "sourceType": "scheduled",
  "workflowId": "nightly-scan",
  "sourceConfig": {"cron": "0 1 * * *", "timezone": "America/New_York"}
}
```

<!-- doctest: SubscriptionCreate -->
```json
{
  "sourceType": "pull",
  "workflowId": "scan-new-image",
  "sourceConfig": {"connectorInstanceId": "registry", "pollIntervalSeconds": 60}
}
```

Successful creation returns `201` with the stored `sourceConfig` in the
subscription response. Connector references are checked only in the trusted
workspace: missing and foreign references both return
`404 trigger.source_not_found`. The PostgreSQL reader selects only existence,
not credentials or target configuration. It requires Connector Service's
instance schema in the shared metadata database. Custom/in-memory deployments
must inject a scoped reference reader; missing or unavailable validation returns
`503 trigger.source_unavailable`, never implicit acceptance.

Managed receiver state, selector revision and schedule eligibility commit
atomically. Schedules use the subscription ID; interval rows use an empty
storage-level `cron`, with `sourceConfig.intervalSeconds` and subscription
`createdAt` providing the UTC interval and anchor. Cron calculation skips
nonexistent local times and includes both repeated-hour instants. State-only
updates preserve schedule progress. Internal revision fields are not exposed in
responses; concurrent stale writes return `409 trigger.subscription_conflict`.
Metadata-only patches preserve the locked lifecycle state even if a legacy
writer changed it without advancing the selector revision. `expired` is
terminal: a deleted receiver cannot be patched back to active or paused.
Providers without the optional atomic mutation capability retain legacy
manual/internal creates and safe unmanaged updates. That legacy path keeps its
original nontransactional guarantees and cannot create or modify managed
receiver state. Legacy state-only operations do not append selector metadata,
so a concurrent selector/input-mapping edit is not overwritten by pause/delete.

### Read a subscription

`GET /v1/workspaces/{workspaceId}/triggers/{subscriptionId}` returns the
`Subscription` shown above, or `404 trigger.subscription_not_found`.

> Reading a subscription requires a backend that implements the subscription
> read surface. Both in-memory and PostgreSQL providers implement it. A custom
> backend without this capability returns
> `501 trigger.api.subscription_read_unsupported`.

### Update a subscription

`PATCH` accepts a sparse `SubscriptionPatch` — only the supplied fields change.
`state` drives the `active` → `paused` → `expired` transitions; `selector` and
`inputMapping` re-author the match and mapping. An explicit `null` clears
`selector` or `targetWorkflowVersionId`; omitting a field leaves it untouched.
`sourceType` and `sourceConfig` may be replaced together, subject to the same
source-specific validation. A null `sourceType`, like a null `state`, means no
change. Clearing `sourceConfig` is allowed only for manual/internal or inactive
subscriptions. Activating a connector-backed subscription revalidates its
reference; pure pause/delete remains available during a reference-store outage.

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
| `trigger.source_config_invalid` | 422 | Invalid receiver configuration or external resume association |
| `trigger.source_not_found` | 404 | No connector reference in the trusted workspace |
| `trigger.source_unavailable` | 503 | Rollout gate closed, scoped authentication unconfigured, or reference validation unavailable |
| `trigger.subscription_conflict` | 409 | Concurrent update or forbidden transition out of terminal `expired` state |
| `trigger.replay_unsafe` | 409 | The original target lacks a provably safe versioned replay contract |
| `trigger.dispatch_unavailable` | 503 | Durable storage or current delivery ownership is unavailable; delivery was not acknowledged |
| `trigger.resume_divergent` | 409 | Attempt to rebind an existing resume key to a different source |
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
Fractional seconds are preserved using the same microsecond rounding as
Workflow mirrors; a subsecond value never becomes the multi-day default.
Values rounding to zero microseconds and unrepresentable durations or
deadlines are rejected explicitly.

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
External waits add the owning run's `workspaceId` and a `source` association.
Their selector must be nonblank; the reserved `_resume` workspace is forbidden.

<!-- doctest: RegisterResumeRequest -->
```json
{
  "runId": "run-9",
  "stepId": "step-3",
  "eventKey": "image-ready",
  "workspaceId": "ws-acme",
  "source": {
    "connectorInstanceId": "registry",
    "deliveryModes": ["pull", "push"],
    "pollIntervalSeconds": 60
  },
  "selector": "event.data.image == \"example/app:v1\"",
  "ttl": "PT24H"
}
```

**Scoped authentication gate:** registration and cancellation with a tenant
workspace require a Workflow-service authenticator configured by
[`create_app`](../../src/services/trigger-service/src/custos_trigger/app.py).
It must verify the Workflow principal on every transport, including direct
app-port calls. Production constructs it from `APP_API_TOKEN` and the explicit
Workflow app ID and namespace; missing configuration fails before tenant access.
The development shim still has no implicit verifier for scoped resume operations
and returns `503 trigger.source_unavailable` unless one is explicitly supplied.
Enabling the rollout flag alone does not open these RPCs; forged call-context or
`dapr-caller-app-id` headers are not trusted. Authenticated scoped cancellation
remains possible while the rollout gate is closed.

`deliveryModes` is a nonempty, duplicate-free subset of `push`/`pull`, normalized
to a stable order. `pollIntervalSeconds` requires `pull` and is at least 10.
Connector existence is checked for new registrations and expiry refreshes.
External cancellation sends the same `workspaceId` alongside the original
triple; omitting it targets only legacy internal waits.

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
- If the prior wait has lapsed past its TTL, it is refreshed atomically only
  when its source association is unchanged.
- A divergent source is audited and rejected with
  `409 trigger.resume_divergent`, including expired keys and concurrent
  registrations. The original source is never overwritten.
- The selector is compiled on **every** path, so malformed CEL is rejected with
  `422` even on a replay.

Cancellation is idempotent: cancelling an unknown or already-expired triple is a
clean `204` no-op.

Legacy registrations remain under `_resume`. External registrations are stored
in the supplied trusted run workspace, so source queries and cancellation cannot
cross tenants even when triples collide. These are internal calls: Workflow
must derive workspace from the owning run, never from authored source input.

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
| `TRIGGER_SCHEDULER_LEADER_RENEW_SECONDS` | `10` | Positive renewal cadence strictly shorter than the lease; epochs remain stable during renewal |
| `TRIGGER_RECEIVER_CONFIGURATION_ENABLED` | `false` | Acknowledge that all writers are upgraded before enabling managed receiver writes; does not bypass scoped RPC authentication |
| `TRIGGER_DURABLE_DISPATCH_ENABLED` | `false` | Enable revision-8 receipts, atomic failed-dispatch capture and versioned replay after all prerequisites are ready |
| `TRIGGER_DEADLETTER_RETENTION_SECONDS` | `2592000` | Captured-payload retention; 1 second to 365 days |
| `TRIGGER_DEADLETTER_MAX_REPLAY_ATTEMPTS` | `3` | Bounded operator attempts, including interrupted attempts; 1–100 |
| `TRIGGER_PUBSUB_COMPONENT` | `custos-pubsub` | Dapr pub/sub component name |
| `TRIGGER_NORMALIZED_TOPIC` | `custos.triggers.normalized` | Normalized-event topic |
| `TRIGGER_WORKFLOW_EVENTS_TOPIC` | `custos.workflow.events` | Internal lifecycle-event topic |
| `TRIGGER_WF_ENDPOINT` | legacy address | Retained for compatibility; an explicit Dapr app ID takes precedence for dispatch |
| `TRIGGER_WORKFLOW_APP_ID` | `workflow-service` | Expected authenticated Workflow app ID; also the explicit Dapr dispatch target |
| `TRIGGER_WORKFLOW_NAMESPACE` | unset (chart: release namespace) | Required authenticated caller namespace; never inferred from an HTTP URL |
| `TRIGGER_CONNECTOR_ENDPOINT` | _(required)_ | Dapr app-id of the Connector Service |
| `TRIGGER_METADATA_STORE` | _(required)_ | SPL metadata-store DSN (secret) |
| `TRIGGER_AUTHZ_ENDPOINT` | empty (development only) | Auth Service base URL for signed-context public keys |
| `APP_API_TOKEN` | unset | Secret shared with the local sidecar; required for production mesh routes |
| `ENVIRONMENT` | `development` | `production` forbids unsigned development contexts |

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
