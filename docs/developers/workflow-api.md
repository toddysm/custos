# Workflow Service Public API

Last Updated: 2026-06-04

## Audience

API consumers who drive Custos workflows from outside the cluster
through the public REST surface (operator tooling, the Custos UI,
end-user SDKs) and in-cluster siblings that drive workflows
through the Internal RPC surface (today the Trigger Service;
tomorrow any sibling that needs to issue `StartRun` /
`CancelRun` / `RaiseExternalEvent` without minting a public
HTTP request). Workflow-service contributors maintaining the API
Adapter + Validator sub-module should also start here — the
locked error taxonomy and idempotency contract documented below
are the source of truth the wire models and exception handlers
ship against. Workflow *authors* should start with the
[CEL Expressions](cel-expressions.md) and
[Workflow Compilation](workflow-compilation.md) docs — the
public API is concerned with *executing* workflows, not
*writing* them.

## Cross-references

- Design:
  [`design/components/workflow-service/design.md` § Public
  Interface](../../design/components/workflow-service/design.md#public-interface),
  [§ Idempotency Model](../../design/components/workflow-service/design.md#idempotency-model),
  [§ Operation: Start Run](../../design/components/workflow-service/design.md#operation-start-run),
  [§ Failure Modes](../../design/components/workflow-service/design.md#failure-modes),
  and [§ Configuration](../../design/components/workflow-service/design.md#configuration)
  — the canonical, locked contract for the API Adapter + Validator
  sub-module.
- Implementation plan:
  [`design/components/workflow-service/implementation-plan.md`](../../design/components/workflow-service/implementation-plan.md)
  — the WF-IMPL-061..072 task ledger that shipped this sub-module.
- Companion docs:
  [Workflow Run Controller](workflow-run-controller.md) — the
  in-process lifecycle owner the API Adapter dispatches into;
  [Workflow Step Coordinator](workflow-step-coordinator.md) —
  the per-step execution engine sitting under the Run Controller;
  [Auth API](auth-api.md) — call-context headers + workspace
  authorization model the API Adapter delegates to.
- Public Python surface:
  [`custos_workflow.api`](../../src/services/workflow-service/src/custos_workflow/api/__init__.py)
  (wire models, problem envelope, dependency factories, routers)
  and [`custos_workflow.validator`](../../src/services/workflow-service/src/custos_workflow/validator/__init__.py)
  (`StartRunValidator`, `IdempotencyLedger`).

## Overview

The API Adapter + Validator is the **fifth sub-module** in the
workflow service host. Its job is one sentence: *be the only way
into the Workflow Service from outside the process boundary*.
That covers the public REST surface (mounted under
`/v1/workspaces/{ws}/...`), the Internal RPC surface (mounted
under `/internal/...` for in-cluster siblings), pre-execution
validation (workspace authorization, workflow-version lookup,
inputs schema match, `(workspaceId, idempotencyKey)` dedup), and
the RFC 7807 `application/problem+json` envelope every error
travels in.

The Validator gates fresh `StartRun` requests before durable acceptance.
The caller-bound starter can derive an opaque run ID earlier for replay lookup;
matching accepted requests recover their original immutable inputs without
repeating mutable Catalog validation. Fresh work passes four gates:

1. **Workspace authorization** — the verified call-context workspace (set
   by `CallContextMiddleware` from Auth-signed Gateway claims in production)
   must match the path / body workspace; mismatch → 403
   `workflow.validator.workspace_unauthorized`.
2. **Catalog lookup** — the `workflowVersionId` must resolve via
   the bound `CatalogClient`; absent → 404
   `workflow.validator.workflow_version_not_found`.
3. **Inputs schema match** — the caller's `inputs` payload must
   validate against the workflow's published JSON Schema (derived
   from `spec.inputs`); violation → 422
   `workflow.validator.inputs_schema_error` with a structured
   `validation` extension carrying every rejected field.
4. **Idempotency ledger** — when the caller supplied a
   non-empty `idempotencyKey`, the
   `(workspaceId, idempotencyKey)` pair must either be absent
   from the ledger (a fresh request) or carry the *same*
   request fingerprint (a replay); a divergent fingerprint → 409
   `workflow.validator.idempotency_conflict`.

After all four gates pass, the validator returns a `ValidatedStartRun` to the
selected orchestration path. The caller-bound durable starter consumes
the validated version without a second unscoped Catalog lookup. Matching accepted
replays recover the original immutable snapshot before consulting Catalog.
The legacy development path still uses `RunController.start_run`.

### Public authentication boundary

External clients send their bearer credential to the API Gateway, not identity
headers to Workflow. Gateway authorizes the route and forwards a signed
`x-custos-callctx` JWT over Dapr. Workflow authenticates its local sidecar token
before trusting the exact `api-gateway` caller and configured namespace, then
uses the shared JWKS verifier (Ed25519, issuer `custos-auth`, audience
`custos.internal`). It checks the signed caller, URL workspace and exact grant:
`workflow:execute` for start, `run:read` for reads, `run:cancel` for cancellation.
Plain workspace/principal headers cannot override verified identity or act as a
production fallback. Duplicate authentication headers are rejected.

Public verification does not require background execution bindings. Trigger's
private start/resume paths keep their separate caller and signed-operation checks;
a public JWT is not private execution authority. Missing trust configuration fails
at construction. Invalid credentials return 401, wrong caller/scope/grant returns
403, and unavailable JWKS returns 503; errors never echo tokens. Exact GET health
probes remain unauthenticated. No blanket internal or Dapr-prefix bypass is added.

### Caller-bound public execution

Set `WF_PUBLIC_EXECUTION_ENABLED=true` to activate durable public starts. This is
separate from inbound verification and requires the existing execution registry,
durable provider and scoped Catalog/runtime wiring. Invalid enabled configuration
fails activation. Managed deployments expose the opt-in as
`global.receiverRuntime.publicExecutionEnabled` (default `false`).
With enforced authentication and the gate disabled, new public starts return 503;
reads and cancellation do not require execution bindings. There is no production
fallback to the unscoped development controller.

Provision exactly one binding for the verified caller's principal **and target
workspace**, plus a separate server-side credential for that same service account
through the existing execution secret boundary. Workspace defaults are not public
selection policy. Missing or ambiguous bindings return 503. Auth checks each
downstream operation against that account's actual grants: `workflow:execute`
alone does not imply Catalog read or Connector bind authority. No roles are added
automatically, and incoming bearer tokens are never retained for execution.

The public request schema is unchanged; clients cannot supply an execution
identity, credential or permission list. Current support is same-account,
workspace-scoped service-account execution, not human/OIDC delegation or
execution as a different account. Cross-workspace administrators must use a
provisioned caller account in the target workspace.

New runs atomically persist their non-secret binding reference with protocol 3
runtime input. Retries, children and waits retain that pin; credential rotation
cannot retarget accepted work. A conflicting caller or payload under an accepted
key returns 409, while missing original bindings fail explicitly rather than
selecting a new default. Legacy entries are not silently migrated. Keyed UUIDv5
and keyless UUIDv4 run IDs and body-over-header key precedence are preserved.

Fresh caller-bound input validation retains the same `422
workflow.validator.inputs_schema_error` response and `validation` extension as
the legacy validator path, before scheduling or durable acceptance. For example,
an integer input named `count` supplied as a string has a validation entry with
`loc: "/count"` and `code: "type"`. This is not an accepted-work conflict (`409`)
or an unavailable upstream dependency (`503`); denied downstream grants remain
`403`. Matching accepted replays still recover their original immutable snapshot.

The Gateway masks downstream5xx and dependency failures as
`503 downstream-unavailable`; that public code alone does not distinguish Auth,
signed-context creation, request-key persistence, Dapr transport or Workflow
execution failures. A lost/failed response also does not establish whether durable
acceptance occurred. Keep the original key for supported recovery rather than
inventing a new identity. Private, request-correlated diagnostic records retain
only bounded phase/status/source fields, never bearer/context tokens, request
bodies or raw error messages; see the
[failure-evidence procedure](workflow-resume-subscriptions.md#deterministic-registration-time-and-failure-evidence).

At the Gateway's `dapr-route` boundary, `errorType` retains a closed transport
exception type. For upstream 5xx responses, `responseEnvelope` distinguishes a
`dapr-error-envelope`, `workflow-error-envelope`, or `unrecognized` body before
the unchanged public 503 masking. `upstreamErrorCode` retains only
`ERR_DIRECT_INVOKE`, `ERR_APP_CHANNEL_NIL`, or
`workflow.workflow_runtime_unavailable` with the matching envelope.
Only error JSON of at most 16 KiB is inspected; oversized, malformed and
ambiguous bodies are unrecognized and no body or message is retained.
Duplicate members in any object, including ignored nested fields, and non-JSON
`NaN`/`Infinity` constants are rejected before envelope classification.
Null envelope/code fields indicate unavailable evidence, including transport
failures and legacy records. The collector validates these pairings against
the request correlation. An observed envelope shape is **not proof of its origin
or the underlying cause**; missing Workflow records do not prove handler entry
never occurred, and a healthy later run cannot explain an earlier 503.

#### Authentication failures before handler entry

`WORKFLOW_PUBLIC_START_FAILURE` also covers public middleware rejections, before
validation, durable acceptance or run-ID allocation. These records use
`phase: authentication` for mesh/JWT/scope rejection or `phase: jwks` with
`reason: jwks_unavailable` for unavailable verification keys. Correlation must be
one canonical UUID header; absent, malformed or duplicate correlation is recorded
as unavailable, never copied as arbitrary text.

The fixed `reason`, allowlisted causal `errorType` and bounded source frames
distinguish key-fetch HTTP, transport and decoding failures. `responseStatus`
is the Workflow response (for example 503); `httpStatus`, when present, is the
upstream JWKS HTTP status (for example 500). A pre-handler rejection has no run ID.
No token, key ID, JWT claim, URL, payload or exception message is retained.
The smoke failure collector matches these records to the public request even
when the Workflow pod is Ready and no run was accepted; malformed records produce
an explicit collection error. Public responses and retry behavior are unchanged.

Readiness is **not** evidence that public authentication keys are available:
the existing startup gate checks metadata and the workflow worker, while the
public verifier fetches JWKS lazily. A first key-fetch failure can therefore
return 503 while `/readyz` remains 200. This is now observable, not silently fixed.
Do not add blind public POST retries or infer the cause of an earlier failure
from a later successful request.

Readiness-gating evaluation: prefer a separately designed, bounded initial
JWKS warm-up for enforced public authentication over performing an Auth request
on every readiness probe. It would need a public verifier/cache warm-up API,
usable-key validation, a startup deadline and failure/recovery tests. It would
also make Auth/sidecar reachability a startup dependency and would not prevent
later cache-refresh failures. No gating or key-refresh policy change is enabled
here, and health probes remain unauthenticated.

The boundary with the Run Controller is sharp: the API Adapter
**never** touches Dapr Workflow, the persistent run store, the
metadata-store provider, or the step / activity wiring. It owns
the wire surface and the pre-execution gates; the Run Controller
owns everything below the FastAPI handler.

```mermaid
sequenceDiagram
    autonumber
    participant Caller as Caller (REST / RPC)
    participant Mid as CallContextMiddleware
    participant API as API Adapter (route)
    participant Val as StartRunValidator
    participant Cat as Catalog Service
    participant Ldg as IdempotencyLedger
    participant Ctl as RunController
    participant Dpr as Dapr Workflow
    Caller->>Mid: POST /v1/workspaces/{ws}/runs
    Mid->>API: request.state.call_context
    API->>Val: validate_start_run(ws, wvId, inputs, idemKey, ctx)
    Val->>Val: 1. workspace authz
    Val->>Cat: 2. lookup(workflowVersionId)
    Cat-->>Val: WorkflowVersion
    Val->>Val: 3. inputs schema match
    Val->>Ldg: 4. record_or_replay(ws, idemKey, fingerprint)
    Ldg-->>Val: LedgerEntry(replayed)
    Val-->>API: ValidatedStartRun
    API->>Ctl: start_run(ws, wvId, inputs, idemKey)
    Ctl->>Dpr: schedule_new_workflow
    Dpr-->>Ctl: instance_id
    Ctl-->>API: RunRef
    API-->>Caller: 202 + RunRefResponse
```

## REST API

All public routes live under `/v1/workspaces/{ws}/...`. The `{ws}`
path segment must match the canonical DNS-1123-like workspace
grammar (`^[a-z][a-z0-9-]{0,62}$`); a violation surfaces as a 400
`workflow.api.bad_request` envelope before any handler runs.

| Method | Path | Body | Success | Description |
|---|---|---|---|---|
| `POST` | `/v1/workspaces/{ws}/runs` | `StartRunRequest` | 202 `RunRefResponse` | Start a workflow run. Honors `Idempotency-Key` header per RFC; body field `idempotencyKey` wins when both are present. |
| `GET` | `/v1/workspaces/{ws}/runs` | — | 200 `RunListResponse` | List runs in workspace; supports `status`, `workflowVersionId`, `cursor`, `limit ≤ 200` query parameters. |
| `GET` | `/v1/workspaces/{ws}/runs/{run_id}` | — | 200 `RunResponse` | Fetch a single run plus its step timeline. |
| `POST` | `/v1/workspaces/{ws}/runs/{run_id}:cancel` | `CancelRunRequest` | 202 `RunRefResponse` | Request cancellation; idempotent against terminal-cancel states. |
| `GET` | `/v1/workspaces/{ws}/runs/{run_id}/steps/{step_id}` | — | 200 `StepResponse` | Fetch a single step's compiled-graph projection. |
| `GET` | `/v1/workspaces/{ws}/runs/{run_id}/steps/{step_id}/logs` | — | 501 `application/problem+json` | Documented stub — delegates to Observability Service (COMP-009) when the *Full Observability Client integration* sub-module lands. |

### Request envelope: `StartRunRequest`

`POST /v1/workspaces/{ws}/runs` and `POST /internal/runs:start`
share the same field set. The internal variant additionally
requires a top-level `workspaceId` body field because the
`/internal/` URL surface carries no `{ws}` path segment.

```json
{
  "workflowVersionId": "wfv-2026-05-31-abc",
  "inputs": {
    "image": "registry.example/app:1.2.3"
  },
  "idempotencyKey": "client-supplied-opaque-string"
}
```

- `workflowVersionId` — required; the Catalog `WorkflowVersion.id`
  the run instantiates.
- `inputs` — optional; defaults to `{}`. Validated at the
  Validator gate against the workflow's published JSON Schema.
- `idempotencyKey` — optional; falls back to the
  `Idempotency-Key` HTTP header. An empty string in either field
  is normalised to "no key supplied".

### Response envelope: `RunRefResponse`

The lightweight handle returned by `StartRun` (and reused as the
per-item shape inside `RunListResponse`).

```json
{
  "runId": "run-2026-05-31-xyz",
  "status": "running",
  "workspaceId": "ws-prod",
  "workflowVersionId": "wfv-2026-05-31-abc",
  "startedAt": "2026-05-31T12:34:56Z"
}
```

`status` is one of the locked `RunStatus` lifecycle values:
`queued`, `running`, `cancelling`, `cancelled`, `succeeded`,
`failed`. `startedAt` is `null` for adapters that return a
freshly-queued row before the runtime stamps a start time.

### Response envelope: `RunResponse`

`GET /v1/workspaces/{ws}/runs/{run_id}` returns the full run
record plus the step timeline so a single read serves the typical
UI / SDK workflow-detail screen without an N+1 follow-up.

```json
{
  "runId": "run-2026-05-31-xyz",
  "status": "succeeded",
  "workspaceId": "ws-prod",
  "workflowVersionId": "wfv-2026-05-31-abc",
  "reason": null,
  "startedAt": "2026-05-31T12:34:56Z",
  "updatedAt": "2026-05-31T12:35:42Z",
  "inputs": {},
  "outputs": null,
  "steps": []
}
```

Per-step lifecycle state + per-attempt history (`steps[]`,
`inputs`, `outputs`) ride on follow-on tasks; the wire shape is
contract-stable today so SDK clients can branch on field presence
without a future-breaking change.

### Cancel envelope: `CancelRunRequest`

```json
{
  "reason": "operator initiated; safety review escalation"
}
```

The `reason` field is optional and capped at 1024 characters; it
surfaces on every subsequent read of the run via
`RunResponse.reason`. Cancelling an already-cancelled or already-
cancelling run is an idempotent no-op that returns 202 with the
current `RunRef`. Cancelling a run that is already in a terminal
non-cancel state (`succeeded` / `failed`) surfaces the 409
`workflow.run_state_conflict` envelope.

### List envelope: `RunListResponse`

`GET /v1/workspaces/{ws}/runs` returns a paginated slice; the
opaque `nextCursor` is forwarded unchanged on the next request.

```json
{
  "items": [
    {
      "runId": "run-1",
      "status": "succeeded",
      "workspaceId": "ws-prod",
      "workflowVersionId": "wfv-2026-05-31-abc",
      "startedAt": "2026-05-31T12:34:56Z"
    }
  ],
  "nextCursor": "opaque-page-token"
}
```

An empty `items` list with a non-`null` `nextCursor` is a legal
"keep paging, nothing in this window" response. `nextCursor` is
`null` on the final page.

### Step envelope: `StepResponse`

`GET /v1/workspaces/{ws}/runs/{run_id}/steps/{step_id}` projects
the persisted run's compiled `ExecutionGraph` into a per-step
wire shape.

```json
{
  "stepId": "scan",
  "kind": "activity",
  "status": "pending",
  "attempts": [],
  "startedAt": null,
  "finishedAt": null,
  "outputs": null
}
```

`kind` is a free-form string (`activity`, `let`, `waitFor`,
`for`, `approval`, `workflow`) rather than an enum so additions
to the step-kind taxonomy do not force a breaking change to
this wire model. Per-step state plumbing lands with a follow-on
task; the fields are present today with stable defaults so SDK
clients can branch on field presence without a breaking change.

When the persisted run has no compiled graph yet, OR the
requested `stepId` is not a node in the compiled graph, the
route returns the 404 `workflow.step_not_found` envelope (the
two cases are intentionally collapsed so SDK branch logic stays
uniform).

### Step log stream stub

`GET /v1/workspaces/{ws}/runs/{run_id}/steps/{step_id}/logs` ships
as a documented 501 stub until the *Full Observability Client
integration* sub-module lands the real handler. The envelope is
locked so SDK clients can branch deterministically on `code`:

```json
{
  "type": "https://errors.custos.dev/workflow/api/not_implemented",
  "title": "Not implemented",
  "status": 501,
  "detail": "Step log streaming is delegated to the Observability Service (COMP-009); deferred until the Full Observability Client integration sub-module lands.",
  "instance": "/v1/workspaces/ws-prod/runs/run-1/steps/scan/logs",
  "code": "workflow.api.not_implemented",
  "workspaceId": "ws-prod",
  "runId": "run-1",
  "stepId": "scan"
}
```

## Internal RPC API

The configured Trigger service reaches the workflow service through a flat
`/internal/...` prefix rather than the public
`/v1/workspaces/{ws}/...` shape. The prefix is not an authentication
exemption: enforced deployments require the local sidecar app token,
the exact Trigger app/namespace and an Auth-signed operation context.
Because the workspace
is no longer carried in the URL, every Internal RPC body promotes
`workspaceId` to a top-level field.

| Method | Path | Body | Success | Description |
|---|---|---|---|---|
| `POST` | `/internal/runs:start` | `InternalStartRunRequest` | 202 `RunRefResponse` | Internal RPC: start a workflow run. Same idempotency contract as the public REST surface. |
| `POST` | `/internal/runs:raiseEvent` | `RoutedResumeRequest` | 202 submitted / 204 consumed (empty body) | Version-2 resume; opaque `runId` and `stepId` travel in the body alongside the existing versioned fields. |
| `POST` | `/internal/runs/{run_id}:cancel` | `InternalCancelRunRequest` | 202 `RunRefResponse` | Internal RPC: request cancellation. |
| `POST` | `/internal/runs/{run_id}/steps/{step_id}:raiseEvent` | `RaiseExternalEventRequest` | 202 (legacy/submitted), 204 (version-2 consumed) | Internal RPC: deliver an external event into a running workflow's `wait_for_external_event` step. |

Version-2 Trigger delivery uses the fixed `runs:raiseEvent` method with target
IDs in its body. This preserves opaque child addresses without encoded
separators, backslashes or dot segments in Dapr's security-sensitive method path.
Existing path-addressed requests remain accepted; legacy child slashes remain
literal path separators. The secure Dapr runtime rejects encoded separators.

For version 2, 202 proves only runtime submission; retry the identical request
until 204 confirms the matching durable consumer receipt. A run becoming terminal
before consumption remains unsafe (409), not successful delivery. Trigger does
not finalize replay from an unacknowledged 202. Legacy version-1 asynchronous
responses remain 202.

### `InternalStartRunRequest`

```json
{
  "workspaceId": "ws-prod",
  "workflowVersionId": "wfv-2026-05-31-abc",
  "inputs": {
    "image": "registry.example/app:1.2.3"
  },
  "idempotencyKey": "trigger-service-issued-key"
}
```

### `InternalCancelRunRequest`

Cancellation requires `executionOperation=workflow-cancel` with the existing
`run:cancel` permission, obtained through the credential-backed
`rpc/callctx.exchangeExecution` operation from the actual Trigger sidecar.
The operator must provision an account and binding with that workspace grant;
the default workspace-executor role does not acquire it. Ordinary Gateway
contexts, legacy grants and start/resume contexts cannot authorize this RPC,
and a cancellation context cannot authorize start or resume.

The body workspace must equal the signed workspace **before any run lookup or
runtime call**. This is workspace administration: an authorized account may
cancel another account's run in the same workspace without replacing its
accepted execution pin or immutable input. Public-start activation controls
starts only; cancellation uses the independently configured private boundary.
Mixed/incomplete production configurations fail closed.

```json
{
  "workspaceId": "ws-prod",
  "reason": "trigger condition retracted"
}
```

The existing controller returns 202 with a RunRef, 404 for a missing run, 409 for
an invalid terminal transition and 503 for dependency/runtime failure.
`cancelling` is in progress, not terminal evidence; only `cancelled` confirms
the terminal controller state. This does not introduce an autonomous Trigger
cancellation policy or a new cancellation state machine.

### `RaiseExternalEventRequest`

```json
{
  "workspaceId": "ws-prod",
  "eventName": "approval.granted",
  "payload": {
    "approver": "user-123",
    "decision": "approve"
  },
  "idempotencyKey": "ts-event-0001"
}
```

Idempotency on this surface is keyed by
`(workspaceId, runId, stepId, eventName, idempotencyKey)` inside
the controller's in-process event-dispatch ledger. A duplicate
body within the `WF_IDEMPOTENCY_KEY_TTL` window is a no-op (still
returns 202) — no second event lands on the workflow. Omitting
`idempotencyKey` opts out of dedup entirely; every such call
dispatches.

## Locked error taxonomy

Every error a Run Controller method or a Validator method may
raise is translated into a single
`application/problem+json` envelope by the
[`register_exception_handlers`](../../src/services/workflow-service/src/custos_workflow/api/errors.py)
chain. The `code` extension field is the canonical
machine-readable selector for client branch logic; the `type`
URI MAY change in future without bumping the `code`.

| `code` | HTTP | Python class | Trigger |
|---|---|---|---|
| `workflow.run_not_found` | 404 | `RunNotFoundError` | `GetRun` / `CancelRun` / `RaiseExternalEvent` against an unknown run id. |
| `workflow.run_state_conflict` | 409 | `RunStateConflictError` | `CancelRun` against a run in terminal non-cancel state, OR `RaiseExternalEvent` against a run in any terminal state. |
| `workflow.workflow_runtime_unavailable` | 503 | `WorkflowRuntimeUnavailableError` | Dapr Workflow component is unreachable; new starts fail, in-flight runs pause at the next step boundary. |
| `workflow.validator.workflow_version_not_found` | 404 | `WorkflowVersionNotFoundError` | The `workflowVersionId` does not resolve in Catalog. |
| `workflow.validator.inputs_schema_error` | 422 | `InputsSchemaError` | Caller `inputs` failed the workflow's published JSON-Schema; carries a structured `validation` extension. |
| `workflow.validator.idempotency_conflict` | 409 | `IdempotencyConflictError` | `(workspaceId, idempotencyKey)` already maps to a *different* request fingerprint within the TTL window. |
| `workflow.validator.workspace_unauthorized` | 403 | `WorkspaceUnauthorizedError` | The call-context workspace disagrees with the path / body workspace. |
| `workflow.step_not_found` | 404 | — (route-local) | The persisted run carries no compiled step with the requested id (or the run has not yet been compiled). |
| `workflow.api.not_implemented` | 501 | — (route-local) | Documented stub route — today: step log streaming. |
| `workflow.api.bad_request` | 400 | — (catch-all) | Request body / query / path validation rejected by FastAPI / Pydantic. |

### Envelope shape (RFC 7807 + extensions)

```json
{
  "type": "https://errors.custos.dev/workflow/validator/inputs_schema_error",
  "title": "Inputs failed schema validation",
  "status": 422,
  "detail": "Caller-supplied inputs failed the workflow's published JSON-Schema.",
  "instance": "/v1/workspaces/ws-prod/runs",
  "code": "workflow.validator.inputs_schema_error",
  "workspaceId": "ws-prod",
  "validation": [
    {
      "loc": "/image",
      "code": "type_error.string",
      "message": "value is not a valid string"
    }
  ]
}
```

The base RFC 7807 fields (`type`, `title`, `status`, `detail`,
`instance`) are always present. The `code` field is always
present and is the canonical selector. Per-kind extension fields
(`runId`, `workflowId`, `workflowVersion`, `workspaceId`,
`idempotencyKey`, `validation`, `principal`,
`currentStatus`, `attemptedStatus`) appear only when known —
`null`-valued extras are stripped from the wire envelope.

The `validation[].loc` field is a JSON-pointer string per
[RFC 6901](https://www.rfc-editor.org/rfc/rfc6901) (`/image`,
not `["image"]`) so SDK clients can directly resolve the
offending field against the original request body without
re-parsing.

## Idempotency Model

Two layers operate end to end:

1. **`StartRun` idempotency** — caller-supplied `idempotencyKey`,
   dedup'd against `(workspaceId, idempotencyKey)` for a
   configurable TTL window (default `PT24H`, set via
   `WF_IDEMPOTENCY_KEY_TTL` per design.md § Configuration).
2. **Step-attempt idempotency** — engine-derived
   `(runId, stepId, attempt)` triple. Owned by the Step
   Coordinator and out of scope for this doc; see the
   [Step Coordinator developer doc](workflow-step-coordinator.md)
   for the full surface.

### Precedence: header vs body

The wire surface accepts the idempotency key in two places:

- The `Idempotency-Key` HTTP header (RFC draft "The
  Idempotency-Key HTTP Header Field").
- The `idempotencyKey` body field on
  `StartRunRequest` / `InternalStartRunRequest`.

When both contain a nonempty key, **the body field wins**. The empty string
and whitespace-only values are normalised to "no key in that field":
a blank body falls back to a nonempty header. Genuinely keyless execution
requires both fields to be absent or blank. Gateway applies the same
normalization and forwards keyed retries to durable Workflow acceptance
rather than caching a transient HTTP response:

```bash
# Header alone — falls through to the validator.
curl -X POST https://wf.custos.example/v1/workspaces/ws-prod/runs \
  -H 'Idempotency-Key: client-supplied-key' \
  -H 'Content-Type: application/json' \
  -d '{"workflowVersionId": "wfv-1"}'

# Body field overrides header.
curl -X POST https://wf.custos.example/v1/workspaces/ws-prod/runs \
  -H 'Idempotency-Key: header-key-ignored-because-body-present' \
  -H 'Content-Type: application/json' \
  -d '{"workflowVersionId": "wfv-1", "idempotencyKey": "body-key-wins"}'

# Empty body field falls back to the supplied header.
curl -X POST https://wf.custos.example/v1/workspaces/ws-prod/runs \
  -H 'Idempotency-Key: header-fallback' \
  -H 'Content-Type: application/json' \
  -d '{"workflowVersionId": "wfv-1", "idempotencyKey": ""}'
```

### Replay vs conflict

The `(workspaceId, idempotencyKey)` pair maps to a *request
fingerprint* — a canonical-JSON SHA-256 of the
`(workflowVersionId, inputs)` pair computed by
[`compute_request_fingerprint`](../../src/services/workflow-service/src/custos_workflow/validator/idempotency_ledger.py).
Within the TTL window:

| Lookup outcome | Validator action | Response |
|---|---|---|
| Key absent from ledger | Mint fresh entry, mark `replayed=False`. | `202` with a fresh `RunRef`; OTel counter `custos_workflow_idempotency_outcomes_total{outcome="fresh"}` bumps. |
| Key present, fingerprint **matches** | Reuse stored entry, mark `replayed=True`. | `202` with the *original* `RunRef` (no second Catalog round-trip, no second Dapr instance); counter `{outcome="replay"}` bumps. |
| Key present, fingerprint **differs** | Raise `IdempotencyConflictError`. | `409 application/problem+json` with `code="workflow.validator.idempotency_conflict"` and the `idempotencyKey` extension; counter `{outcome="conflict"}` bumps. |

Once the TTL window expires, the entry is garbage-collected
lazily and a subsequent request with the same key is treated as
fresh. Requests that omit the key entirely produce no counter
sample (the `fresh/replay/conflict` split is meaningless when
the caller opted out).

## Configuration

The API Adapter and authentication configuration are listed below; the rest of the host's config
([`README.md` § Configuration](../../src/services/workflow-service/README.md#configuration))
remains the source of truth for the variables the validator
collaborators (Catalog client, Dapr Workflow component, etc.)
bind against.

| Variable | Required | Default | Description |
|---|---|---|---|
| `WF_REQUIRE_CALL_CONTEXT` | No | unset (dev mode) | Exact `"1"` requires signed verification. `ENVIRONMENT=production` also requires it, regardless of this flag. Only non-production without a verifier retains the legacy plain-header shim (absent identity yields `None`, not fabricated values). |
| `WF_AUTHZ_ENDPOINT` | Production | — | Auth base URL supplying `/.well-known/jwks.json`; configuring it enables public signed verification even without execution bindings or the legacy flag. |
| `APP_API_TOKEN` | With signed verification | — | Secret shared with the local Dapr sidecar, never the public client. |
| `WF_GATEWAY_NAMESPACE` | With signed verification | — | Exact authenticated Dapr namespace of `api-gateway`. The managed receiver overlay projects the release namespace. |
| `WF_PUBLIC_EXECUTION_ENABLED` | No | `false` | Strict boolean opt-in for caller-bound durable public starts. Requires matching caller bindings and execution-aware dependencies; disabled enforced starts return 503, not legacy fallback. |
| `WF_IDEMPOTENCY_KEY_TTL` | No | `PT24H` | ISO-8601 duration window for `(workspaceId, idempotencyKey)` dedup. Months/years are rejected (the resulting calendar-dependent window is incompatible with the ledger contract). |
| `WF_CATALOG_ENDPOINT` | Yes (production) | — | Catalog Service endpoint the validator's `CatalogClient` resolves `workflowVersionId` against. |
| `WF_ARM_ENDPOINT` | Yes (production) | — | Activity Runtime Manager Dapr app-id the outbound ARM adapter resolves `schedule_activity` / `cancel_activity` calls against. When unset, the workflow-service falls back to the in-process `NoopActivityRuntimeClient` so the dev / test path stays sidecar-free. |
| `WF_CONNECTOR_ENDPOINT` | Yes (production) | — | Connector Service Dapr app-id the outbound Connector adapter resolves `bind_for_step` against. When unset, the workflow-service falls back to the in-process `NoopConnectorClient` so the dev / test path stays sidecar-free. |
| `WF_OUTBOUND_RPC_TIMEOUT_MS` | No | `10000` | Per-request timeout (in milliseconds) shared by both the ARM and Connector outbound Dapr adapters. Both adapters reuse the single lifespan-owned `httpx.AsyncClient` (one socket pool per worker), and this value is converted to seconds and threaded into each adapter's per-request `httpx` timeout. Must be a positive integer; bad values fail at startup so the worker never flips `/readyz` to 200 with a misconfigured timeout. |

## Observability

Each inbound request walks the `OTelHttpServerMiddleware` so the
API surface ships a uniform metric + span shape, regardless of
which route handles the request.

### Metrics

| Instrument | Kind | Labels | Source |
|---|---|---|---|
| `custos_workflow_http_server_duration_ms` | Histogram (ms) | `http.method`, `http.route` (template path, NOT live URL), `http.status_code` | One sample per request, recorded by `OTelHttpServerMiddleware`. |
| `custos_workflow_api_errors_total` | Counter | `wf.error.kind` (a member of the locked taxonomy above) | Bumped exactly once per Problem+JSON-emitting request, by the matching exception handler. |
| `custos_workflow_idempotency_outcomes_total` | Counter | `wf.idempotency.outcome ∈ {fresh, replay, conflict}` | Bumped exactly once per `StartRun` that supplied an idempotency key; requests without a key produce no sample. |

Label cardinality is explicitly bounded: `http.route` is the
FastAPI template path (`/v1/workspaces/{ws}/runs/{run_id}`), so a
pathological client cannot mint a fresh metric series per unique
URL.

### Spans

A single `custos_workflow.http.request` span wraps every inbound
request. Attributes:

| Attribute | Semantic | Source |
|---|---|---|
| `http.method`, `http.route`, `http.status_code` | Standard OTel HTTP-server semconv. | Always populated. |
| `wf.workspace.id` | Owning workspace. | Path `ws` parameter when present. |
| `wf.run.id` | Run identifier. | Path `run_id` parameter, OR `request.state.wf_run_id` (set by `StartRun` after the controller mints a fresh run id). |
| `wf.workflow_version.id` | Catalog `WorkflowVersion.id`. | `request.state.wf_workflow_version_id` (set by `StartRun` after the validator confirms the workflow version). |
| `wf.idempotency.outcome` | `fresh` / `replay` / `conflict`. | `request.state.wf_idempotency_outcome` (set by `StartRun` on validation, by the conflict handler on conflict). |
| `wf.error.kind` | One of the locked taxonomy codes. | `request.state.wf_error_kind` (set by Problem+JSON-emitting exception handlers). |

The span status remains `UNSET` for Problem+JSON responses —
those are HTTP-level errors, not span-level errors. The span
status flips to `ERROR` only on unhandled exceptions that escape
the route boundary.

## Extension points

The API Adapter was built around two Protocol boundaries and one
documented stub:

- **Durable `IdempotencyLedger`** —
  [`IdempotencyLedger`](../../src/services/workflow-service/src/custos_workflow/validator/idempotency_ledger.py)
  is a Protocol with two async methods. The in-memory adapter
  ships today (`InMemoryIdempotencyLedger`); the durable adapter
  (`MetadataStoreProvider`-backed) is filed as a separate
  follow-up issue per
  [`design/components/workflow-service/todos.md`](../../design/components/workflow-service/todos.md).
  Production wiring will bind the durable adapter; no change to
  `StartRunValidator` is required.
- **Real `CatalogClient`** — the validator already consumes the
  same `CatalogClient` Protocol the Run Controller is built with;
  swapping the in-memory test fake for the production Dapr-backed
  client requires no validator change.
- **Step log streaming** — `GET .../steps/{step_id}/logs` ships
  as a 501 stub. The *Full Observability Client integration*
  sub-module will replace the stub with a real streaming
  handler; the route signature is contract-stable.

## Worked examples

The three examples below are exercised end-to-end by
[`tests/integration/test_api_end_to_end.py`](../../src/services/workflow-service/tests/integration/test_api_end_to_end.py)
(the WF-IMPL-071 integration suite), which drives
`custos_workflow.create_app` via `httpx.AsyncClient` over
`httpx.ASGITransport` through the full FastAPI lifespan. The
[doc-examples test](../../src/services/workflow-service/tests/test_docs_examples_api.py)
pins every documented endpoint, every documented `code`, and
every fenced ` ```json ` example in this doc against the live
`api.models` / `api.errors` / `api.routes` surface so the docs
cannot drift away from the running code.

### Example 1 — happy-path REST `StartRun` + `GetRun`

A fresh `StartRun` against a known workflow version, no
idempotency key. The validator's Catalog gate confirms the
version exists; the inputs gate accepts the payload; the
controller mints a fresh `RunRef`.

```bash
curl -X POST https://wf.custos.example/v1/workspaces/ws-prod/runs \
  -H 'Content-Type: application/json' \
  -H 'X-Custos-Workspace: ws-prod' \
  -H 'X-Custos-Principal: user-alice' \
  -d '{
    "workflowVersionId": "wfv-2026-05-31-abc",
    "inputs": {"image": "registry.example/app:1.2.3"}
  }'
```

Response: `202 Accepted` with a `RunRefResponse`.

```json
{
  "runId": "run-2026-05-31-xyz",
  "status": "running",
  "workspaceId": "ws-prod",
  "workflowVersionId": "wfv-2026-05-31-abc",
  "startedAt": null
}
```

Follow up:

```bash
curl https://wf.custos.example/v1/workspaces/ws-prod/runs/run-2026-05-31-xyz \
  -H 'X-Custos-Workspace: ws-prod' \
  -H 'X-Custos-Principal: user-alice'
```

```python
import httpx

async with httpx.AsyncClient(base_url="https://wf.custos.example") as client:
    response = await client.post(
        "/v1/workspaces/ws-prod/runs",
        headers={
            "X-Custos-Workspace": "ws-prod",
            "X-Custos-Principal": "user-alice",
        },
        json={
            "workflowVersionId": "wfv-2026-05-31-abc",
            "inputs": {"image": "registry.example/app:1.2.3"},
        },
    )
    response.raise_for_status()
    run_ref = response.json()
```

OTel: `custos_workflow_http_server_duration_ms` records one
sample with `http.route="/v1/workspaces/{ws}/runs"`,
`http.method="POST"`, `http.status_code=202`. The
`custos_workflow.http.request` span carries
`wf.workspace.id="ws-prod"`,
`wf.workflow_version.id="wfv-2026-05-31-abc"`, and
`wf.run.id="run-2026-05-31-xyz"`.

### Example 2 — idempotent replay with body-field precedence

Two `StartRun` calls against the same workspace with the same
`idempotencyKey` in the body. The second call returns the
*original* `runId` without scheduling a second workflow
instance, regardless of what the HTTP header carries.

First call:

```python
import httpx

body = {
    "workflowVersionId": "wfv-2026-05-31-abc",
    "inputs": {"image": "registry.example/app:1.2.3"},
    "idempotencyKey": "client-issued-key-001",
}

async with httpx.AsyncClient(base_url="https://wf.custos.example") as client:
    headers = {
        "X-Custos-Workspace": "ws-prod",
        "X-Custos-Principal": "user-alice",
        "Idempotency-Key": "different-header-value",
    }
    first = await client.post("/v1/workspaces/ws-prod/runs", headers=headers, json=body)
    second = await client.post("/v1/workspaces/ws-prod/runs", headers=headers, json=body)

assert first.status_code == 202
assert second.status_code == 202
assert first.json()["runId"] == second.json()["runId"]  # same run; replay
```

Both responses carry the same `runId`. OTel counter
`custos_workflow_idempotency_outcomes_total` bumps once with
`{outcome="fresh"}` (first call) and once with
`{outcome="replay"}` (second call). The body's
`idempotencyKey` wins over the `Idempotency-Key` header.

### Example 3 — validator inputs-schema rejection

A `StartRun` with a malformed input. The validator's inputs gate
returns `422` with a structured `validation` extension.

```bash
curl -X POST https://wf.custos.example/v1/workspaces/ws-prod/runs \
  -H 'Content-Type: application/json' \
  -H 'X-Custos-Workspace: ws-prod' \
  -H 'X-Custos-Principal: user-alice' \
  -d '{
    "workflowVersionId": "wfv-2026-05-31-abc",
    "inputs": {"image": 12345}
  }'
```

Response: `422 application/problem+json`.

```json
{
  "type": "https://errors.custos.dev/workflow/validator/inputs_schema_error",
  "title": "Inputs failed schema validation",
  "status": 422,
  "detail": "Caller-supplied inputs failed the workflow's published JSON-Schema.",
  "instance": "/v1/workspaces/ws-prod/runs",
  "code": "workflow.validator.inputs_schema_error",
  "workspaceId": "ws-prod",
  "validation": [
    {
      "loc": "/image",
      "code": "type_error.string",
      "message": "value is not a valid string"
    }
  ]
}
```

OTel counter `custos_workflow_api_errors_total` bumps with
`{wf.error.kind="workflow.validator.inputs_schema_error"}`; the
span carries the same kind on `wf.error.kind` but keeps its
status `UNSET` (Problem+JSON is an HTTP-level error, not a
span-level error).
