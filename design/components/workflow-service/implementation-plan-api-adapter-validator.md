# `workflow-service` — API Adapter + Validator Implementation Plan

> Derived from [`design/components/workflow-service/design.md`](design.md) on 2026-05-31.
> Source of truth: that design doc (§ Internal Structure rows for **API Adapter** + **Validator**, § Operation: Start Run, § Public Interface, § Idempotency Model, § Configuration, § Failure Modes) and the existing [`RunController`](../../../src/services/workflow-service/src/custos_workflow/runs/controller.py) public surface already shipped via WF-IMPL-037..040.
> This plan is owned by the `implement-component` skill; regenerate fresh whenever the design changes.

## Summary

The **API Adapter + Validator** is the fifth sub-module to land inside the workflow-service host (`src/services/workflow-service/`, Python package `custos_workflow`), after the Expression Evaluator (`src/libs/custos-cel/`), the Definition Compiler (`custos_workflow.compiler` + adjacent packages, tracker [#363](https://github.com/toddysm/custos/issues/363)), the Run Controller (`custos_workflow.runs`, tracker [#399](https://github.com/toddysm/custos/issues/399)), and the Step Coordinator (`custos_workflow.steps` + `custos_workflow.clients`, tracker [#432](https://github.com/toddysm/custos/issues/432)). Per design.md § Internal Structure it owns *the inbound REST and Internal RPC surface and the pre-execution checks that gate every `StartRun`* — workflow-version existence, inputs schema match, workspace authorization, and `(workspaceId, idempotencyKey)` dedup. After this sub-module merges, the workflow-service stops being reachable only in-process: the public REST API surface (`POST /v1/workspaces/{ws}/runs`, GET / list / `:cancel`, step fetch + log-stream stub) and the internal RPC surface (`StartRun`, `CancelRun`, `RaiseExternalEvent`) consumed by the Trigger Service and API Gateway both come online. This is the prerequisite for any end-to-end test against COMP-004 (Trigger Service) and COMP-001 (API Gateway), and for any subsequent sub-module to be exercised in a real cluster.

## Boundary with the deferred sub-modules

| Deferred sub-module | What it owns | What this plan ships as a stub |
|---|---|---|
| **Resume Subscription Manager** | `waitFor:` step kind + replay re-registration. | `RaiseExternalEvent` RPC handler + thin `RunController.raise_external_event` adapter that calls Dapr Workflow's `raise_event` primitive. Useful even without `waitFor:` (Dapr buffers events on the instance); becomes load-bearing once `waitFor:` lands. |
| **Sub-Orchestration Manager** | `for:` / `approval:` / `workflow:` step kinds. | No surface change — `StartRun` already accepts any compiled definition; `for:` / `approval:` / `workflow:` continue to fail at the Step Coordinator with `step.kind_not_implemented`. |
| **Real ARM / Connector Client adapters** | Production Dapr Service Invocation bridges. | None. Wiring stays behind the WF-IMPL-049 / WF-IMPL-050 Protocols. |
| **Full Observability Client integration** | Audit-event sink wiring + log-stream delegation. | `GET …/steps/{stepId}/logs` route ships returning **501 Not Implemented** with an explanatory envelope until Observability integration lands; OTel HTTP-server spans / latency histograms / error counters DO land here. |
| **Durable `IdempotencyLedger` (Postgres-backed adapter)** | `MetadataStoreProvider`-resident persistence of the `(workspaceId, idempotencyKey)` ledger. | `InMemoryIdempotencyLedger` ships behind the Protocol so every test path is exercised; the Postgres adapter is a separate follow-up issue filed post-tracker. |

## Conventions

- Task prefix: `WF-IMPL-`.
- Numbering starts at `WF-IMPL-061` (next free id after the WF-IMPL-001..060 range used by `custos-cel`, the Definition Compiler, the Run Controller, and the Step Coordinator; verified via `gh issue list --label component:workflow-service --search "WF-IMPL- in:title"`).
- One task = one PR = one GitHub issue.
- Labels per existing repo convention: `component:workflow-service`, `phase:implementation`, `type:implementation`. (No `phase:A`/`phase:B` labels in this repo — the phase grouping is reflected in this plan only.)
- Phases run sequentially; tasks within a phase may run in parallel if dependencies allow.
- Quality gate: `ruff format . && ruff check . && mypy src tests && pytest -q` from `src/services/workflow-service/`, honoring the existing `--cov-fail-under=90` floor.
- New code lives under `src/services/workflow-service/src/custos_workflow/api/` (mirroring the existing `custos_catalog.api` layout: `dependencies.py`, `models.py`, `routes/`, `rpc.py`, `errors.py`) plus `src/custos_workflow/validator/` (separate package — pre-execution checks are a distinct concern from HTTP wire).

## Dependency graph

```mermaid
flowchart TD
    A061[WF-IMPL-061: API error taxonomy + RFC 7807 envelope]
    A062[WF-IMPL-062: wire Pydantic models]
    A063[WF-IMPL-063: Validator package + IdempotencyKey ledger]

    B064[WF-IMPL-064: FastAPI dependency factories]
    B065[WF-IMPL-065: REST routes — runs]
    B066[WF-IMPL-066: REST routes — steps + log-stream stub]

    C067[WF-IMPL-067: Internal RPC routes — StartRun/CancelRun]
    C068[WF-IMPL-068: RaiseExternalEvent bridge]

    D069[WF-IMPL-069: App wiring — mount routers + exception handlers]
    D070[WF-IMPL-070: OTel HTTP-server observability]

    E071[WF-IMPL-071: Unit + integration test suite (>=90%)]
    E072[WF-IMPL-072: Developer documentation]

    A061 --> A062
    A062 --> A063
    A063 --> B064
    B064 --> B065
    B064 --> B066
    B065 --> C067
    C067 --> C068
    B065 --> D069
    B066 --> D069
    C067 --> D069
    C068 --> D069
    D069 --> D070
    D070 --> E071
    E071 --> E072
```

## Phase A — Foundations (errors, models, validator)

### `WF-IMPL-061`: Public API error taxonomy + RFC 7807 problem envelope

- **Scope**:
  - New package `src/custos_workflow/api/__init__.py` (re-exports + `register_exception_handlers`).
  - `src/custos_workflow/api/errors.py` — `ProblemDetail` Pydantic model (`type`, `title`, `status`, `detail`, `instance`, plus extension fields `runId`, `workspaceId`, `idempotencyKey`, `validation` for field-level rejections). Exception-handler functions that translate every `RunController` and `Validator` error class to a single `application/problem+json` envelope (RFC 7807). Locked taxonomy: `workflow.run_not_found` → 404, `workflow.run_state_conflict` → 409, `workflow.workflow_runtime_unavailable` → 503, `workflow.validator.workflow_version_not_found` → 404 (mapped from `CatalogClient` 404), `workflow.validator.inputs_schema_error` → 422, `workflow.validator.idempotency_conflict` → 409, `workflow.validator.workspace_unauthorized` → 403, `workflow.api.bad_request` → 400 catch-all.
  - `tests/api/test_errors.py` — every documented kind round-trips through the handler, the wire body matches `application/problem+json` content-type, extension fields are preserved.
- **Acceptance criteria**:
  - 8 documented kinds each have a dedicated handler test; status code and `type` URI verified.
  - `register_exception_handlers(app)` is idempotent (safe to call twice in tests).
  - Coverage on `api/errors.py` = 100 %.
- **Depends on**: _(none)_.
- **Complexity**: S.

### `WF-IMPL-062`: API wire Pydantic models

- **Scope**:
  - `src/custos_workflow/api/models.py` — `StartRunRequest` (`workflow_version_id`, `inputs`, `idempotency_key?` — body field; the wire camelCase / snake_case mapping uses `ConfigDict(populate_by_name=True, alias_generator=to_camel)`), `StartRunResponse` / `RunRefResponse` (`run_id`, `status`, `workspace_id`, `workflow_version_id`, `started_at?`), `RunResponse` (full run + `Step[]` timeline + `inputs` + `outputs?`), `StepResponse` (per-step state + `attempts[]` summary), `CancelRunRequest` (`reason?`), `RaiseExternalEventRequest` (`event_name`, `payload`, `idempotency_key?`), `RunListResponse` + `RunListQuery` (status filter, `workflow_version_id` filter, page cursor + limit).
  - `tests/api/test_models.py` — JSON round-trip for every model; alias generator produces wire camelCase; `extra="forbid"` rejects unknown fields; serialization tag on every request model.
- **Acceptance criteria**:
  - All request / response shapes documented in design.md § Public Interface have a corresponding Pydantic model exported from `api.models`.
  - 100 % round-trip coverage; unknown fields rejected.
- **Depends on**: `WF-IMPL-061`.
- **Complexity**: S.

### `WF-IMPL-063`: Validator package + Idempotency-Key ledger

- **Scope**:
  - New package `src/custos_workflow/validator/__init__.py`.
  - `src/custos_workflow/validator/errors.py` — `ValidatorError` base + 4 locked subclasses (`WorkflowVersionNotFoundError`, `InputsSchemaError`, `IdempotencyConflictError`, `WorkspaceUnauthorizedError`); each with a locked `kind` string consumed by `api/errors.py`.
  - `src/custos_workflow/validator/idempotency_ledger.py` — `IdempotencyLedger` Protocol (`record_or_replay(workspace_id, idempotency_key, request_fingerprint) -> LedgerEntry`); `InMemoryIdempotencyLedger` with `WF_IDEMPOTENCY_KEY_TTL` (default `PT24H`, design.md § Configuration); deterministic `request_fingerprint = sha256(workflow_version_id || canonical_json(inputs))`. Returns hit (replay) on `(workspace_id, idempotency_key)` if fingerprint matches; raises `IdempotencyConflictError` on fingerprint mismatch within TTL window.
  - `src/custos_workflow/validator/inputs.py` — `validate_inputs_against_schema(inputs, schema)` using `jsonschema` (already a workflow-service dep through Pydantic / Catalog client); raises `InputsSchemaError` with the failing JSON Pointer.
  - `src/custos_workflow/validator/service.py` — `StartRunValidator` orchestrator: looks up `WorkflowVersion` via `CatalogClient` (already wired through `RunController`), checks workspace match against `CallContext`, runs inputs schema match, consults ledger; returns a `ValidatedStartRun` value object the API hands to `RunController.start_run`.
  - `tests/validator/test_idempotency_ledger.py`, `tests/validator/test_inputs.py`, `tests/validator/test_service.py` — Hypothesis tests for ledger TTL semantics + fingerprint determinism; positive + negative inputs schema cases; service-level error mapping.
- **Acceptance criteria**:
  - Same `(workspace_id, idempotency_key, fingerprint)` → ledger hit; different fingerprint within TTL → `IdempotencyConflictError`.
  - `validate_inputs_against_schema` produces a stable JSON-pointer string for nested failures (Hypothesis test, 200 examples).
  - `StartRunValidator` raises `WorkspaceUnauthorizedError` when `CallContext.workspace_id != path workspace`.
  - Coverage on `validator/` = 95 %+.
- **Depends on**: `WF-IMPL-062`.
- **Complexity**: M.

## Phase B — Public REST surface

### `WF-IMPL-064`: FastAPI dependency factories

- **Scope**:
  - `src/custos_workflow/api/dependencies.py` — `get_run_controller(request) -> RunController` (off `app.state.run_components`), `get_validator(request) -> StartRunValidator`, `get_call_context(request) -> CallContext` (off `request.state.call_context`), `WorkspacePath` param dependency (`Path(...)` with regex from design.md § Public Interface).
  - `tests/api/test_dependencies.py` — every factory returns the bound component; missing `app.state.run_components` raises a clear 503.
- **Acceptance criteria**:
  - Dependencies are FastAPI-Depends-compatible and reusable across `routes/` and `rpc.py`.
  - Missing state raises 503 with `workflow.api.bad_request` body, not a 500.
- **Depends on**: `WF-IMPL-063`.
- **Complexity**: S.

### `WF-IMPL-065`: REST routes — runs

- **Scope**:
  - `src/custos_workflow/api/routes/__init__.py` — `all_routers` tuple (mirrors catalog-service convention).
  - `src/custos_workflow/api/routes/runs.py` — `POST /v1/workspaces/{ws}/runs` (start; reads `Idempotency-Key` HTTP header per RFC, body-field `idempotencyKey` overrides header; returns 202 with `RunRefResponse`); `GET /v1/workspaces/{ws}/runs` (list with filters + cursor); `GET /v1/workspaces/{ws}/runs/{runId}` (full `RunResponse` with timeline); `POST /v1/workspaces/{ws}/runs/{runId}:cancel` (202 on success).
  - Wires every route through `StartRunValidator` (where applicable) → `RunController` → response model.
  - `tests/api/routes/test_runs.py` — happy path for each verb against `httpx.AsyncClient(app=app)` with fake runtime + fake catalog client; header-vs-body idempotency precedence; replay returns original `runId`; conflict on divergent inputs returns 409 with `idempotency_conflict` body.
- **Acceptance criteria**:
  - Every endpoint matches the design.md § Public Interface — REST API table (path, method, status code, body shape).
  - `Idempotency-Key` header fallback works; body field takes precedence per design note.
  - Replay returns the original `run_id` with status from the persisted record.
- **Depends on**: `WF-IMPL-064`.
- **Complexity**: M.

### `WF-IMPL-066`: REST routes — steps + log-stream stub

- **Scope**:
  - `src/custos_workflow/api/routes/steps.py` — `GET /v1/workspaces/{ws}/runs/{runId}/steps/{stepId}` (full `StepResponse`); `GET /v1/workspaces/{ws}/runs/{runId}/steps/{stepId}/logs` returns **501 Not Implemented** with body `{type: "...not_implemented", detail: "Step log streaming is delegated to the Observability Service (COMP-009); deferred until the Full Observability Client integration sub-module lands."}`.
  - `tests/api/routes/test_steps.py` — step fetch returns the persisted attempts; log endpoint returns 501 with the locked body.
- **Acceptance criteria**:
  - Step fetch round-trips state for completed + in-flight steps.
  - Log endpoint returns exactly 501 with the documented body; no streaming attempted.
- **Depends on**: `WF-IMPL-064`.
- **Complexity**: S.

## Phase C — Internal RPC inbound surface

### `WF-IMPL-067`: Internal RPC routes — `StartRun` / `CancelRun`

- **Scope**:
  - `src/custos_workflow/api/rpc.py` — `POST /internal/runs:start` (`StartRun` RPC, body = `StartRunRequest` + `workspaceId`), `POST /internal/runs/{runId}:cancel` (`CancelRun` RPC). Routes share the same `StartRunValidator` + `RunController` plumbing as the public surface; the `/internal/` prefix is distinct so the Helm chart / mesh can pin mTLS-only access (actual mTLS gate is out of scope — comes with the API Gateway integration).
  - `tests/api/test_rpc.py` — Trigger-Service-shaped `StartRun` call succeeds; idempotent replay returns the same `runId`; cancel-by-id works for an in-flight run.
- **Acceptance criteria**:
  - Internal `StartRun` accepts the same body shape as the public POST, plus an explicit `workspaceId` (no path param at the `/internal/` prefix).
  - `CancelRun` dispatches to `RunController.cancel_run` and returns 202 on success, 404 on unknown run id.
- **Depends on**: `WF-IMPL-065`.
- **Complexity**: M.

### `WF-IMPL-068`: `RaiseExternalEvent` bridge (Trigger Service inbound RPC)

- **Scope**:
  - Extend `custos_workflow.runs.controller.RunController` with `async def raise_external_event(*, run_id, step_id, event_name, payload, idempotency_key) -> None` that calls the Dapr Workflow wrapper's `raise_event` primitive idempotent on `(runId, stepId, eventName, idempotencyKey)` — dedup key persisted via the existing `RunStore` (new column / table TBD; minimal addition since no historical state is required, only a "seen recently" set with the same TTL window as `WF_IDEMPOTENCY_KEY_TTL`).
  - `src/custos_workflow/api/rpc.py` — `POST /internal/runs/{runId}/steps/{stepId}:raiseEvent` route mapping the RPC body to the controller method.
  - `tests/runs/test_raise_external_event.py` + `tests/api/test_rpc_raise_event.py` — replay of the same `(runId, stepId, eventName, idempotencyKey)` produces a single Dapr call; calling on an unknown `runId` returns 404; calling on a terminal-state run returns 409.
- **Acceptance criteria**:
  - Duplicate `(runId, stepId, eventName, idempotencyKey)` within TTL → one Dapr `raise_event` call (verified by the fake runtime).
  - Method is safe to call even when no `waitFor:` step is currently buffering the event (Dapr stores it on the instance — confirmed against the Dapr Workflow Python SDK semantics in the test harness).
  - This task is the public bridge that the Resume Subscription Manager sub-module will load-bear on; until then the route is reachable but does not produce any visible step progress.
- **Depends on**: `WF-IMPL-067`.
- **Complexity**: M.

## Phase D — App wiring + observability

### `WF-IMPL-069`: Mount routers + exception handlers in `create_app`

- **Scope**:
  - `src/custos_workflow/app.py` — call `register_exception_handlers(app)` and `app.include_router(...)` for every router from `api.routes.all_routers` + `api.rpc.router`; tag the public surface with `tags=["runs", "steps"]` and the internal surface with `tags=["internal"]` (drives OpenAPI grouping).
  - Update `src/custos_workflow/_telemetry.py` if needed to extend the OTel `RequestsInstrumentor` scope.
  - `tests/app/test_app.py` (extension): asserts every documented route is in `app.routes`; asserts `application/problem+json` content-type on a forced 404; OpenAPI `paths` dict contains the documented endpoints.
- **Acceptance criteria**:
  - `create_app()` returns an app with the full public + internal surface mounted.
  - OpenAPI `/openapi.json` lists every documented endpoint with the right tag.
- **Depends on**: `WF-IMPL-065`, `WF-IMPL-066`, `WF-IMPL-067`, `WF-IMPL-068`.
- **Complexity**: S.

### `WF-IMPL-070`: OTel HTTP-server observability

- **Scope**:
  - Extend the existing OTel scaffolding (WF-IMPL-044 / WF-IMPL-058) with HTTP-server spans (`http.server.duration` histogram), per-endpoint latency histogram tagged by `http.route`, error counter tagged by `wf.error.kind` (the locked taxonomy from WF-IMPL-061), idempotency-cache hit/miss counter (from the Validator ledger).
  - Span attributes: `wf.run.id` (start / cancel / get), `wf.workspace.id`, `wf.workflow_version.id`, `wf.idempotency.outcome ∈ {fresh, replay, conflict}`.
  - `tests/api/test_observability.py` — span attributes + counter increments asserted via the in-memory OTel meter / tracer.
- **Acceptance criteria**:
  - Every public + internal route emits exactly one server span tagged with the documented attributes.
  - Idempotency-cache outcome counter ticks for fresh / replay / conflict — one increment per request.
- **Depends on**: `WF-IMPL-069`.
- **Complexity**: M.

## Phase E — Verification + documentation

### `WF-IMPL-071`: Unit + integration test suite (≥90 % coverage gate)

- **Scope**:
  - End-to-end suite under `tests/integration/test_api_end_to_end.py` driving the full app via `httpx.AsyncClient(app=create_app(run_components=fake_components))` — start run → poll status → cancel → verify lifecycle events + Validator ledger state.
  - Negative path suites — validator errors, run-state conflicts, runtime-unavailable, unknown workspace, header-vs-body idempotency precedence.
  - Coverage budget pinned at `--cov-fail-under=90` (matches existing workflow-service floor).
- **Acceptance criteria**:
  - All happy + failure paths from design.md § Failure Modes have at least one test.
  - Total workflow-service coverage stays ≥ 90 % after this task lands.
- **Depends on**: `WF-IMPL-070`.
- **Complexity**: M.

### `WF-IMPL-072`: Developer documentation — `docs/developers/workflow-api.md`

- **Scope**:
  - New doc with audience + cross-references + REST surface (mirrors design.md § Public Interface tables) + RPC surface + Validator semantics + locked error taxonomy table + idempotency model (header / body precedence + TTL + fingerprint + replay vs conflict) + log-stream stub explanation + extension points + worked `curl` + `httpx` examples.
  - Mermaid sequence: caller → API Adapter → Validator → RunController → Dapr Workflow.
  - `tests/test_docs_examples_api.py` — extracts every ```yaml``` / ```json``` fence + every documented kind string + every documented endpoint from the markdown and verifies they exhaustively match the live `api.models` / `api.errors` / `api.routes` surface (mirrors the WF-IMPL-060 pattern).
  - Update `docs/developers/README.md` index row.
  - Update `src/services/workflow-service/README.md` status block.
- **Acceptance criteria**:
  - Doc-examples test ticks for every documented endpoint, every documented kind, every documented model.
  - All ```json``` request / response examples are valid against the live Pydantic models.
- **Depends on**: `WF-IMPL-071`.
- **Complexity**: M.

## Out of scope (deferred)

- **`waitFor:` step kind + Resume Subscription Manager** — `RaiseExternalEvent` ships as a thin bridge here; the buffering side (`waitFor:` step kind + `ResumeSubscriptionMirror` replay) is its own sub-module.
- **API Gateway mTLS gating of the `/internal/` prefix** — the prefix and tag ship here; the mesh / mTLS policy ships with the API Gateway integration.
- **Real Catalog Service HTTP client** — the validator continues to use the `CatalogClient` Protocol the Run Controller already exposes; the real Dapr Service Invocation adapter is part of the deferred "Real ARM / Connector / Catalog client adapters" sub-module.
- **`GET …/steps/{stepId}/logs` actual streaming** — returns 501 here; ships with "Full Observability Client integration".
- **`(workspaceId, idempotencyKey)` ledger durability** — `InMemoryIdempotencyLedger` is what ships; the Postgres-backed adapter (`MetadataStoreProvider`-resident) is a separate follow-up issue filed post-tracker.

## Open questions

_(All resolved during plan approval on 2026-05-31: tracker slug = `WF-IMPL-000-API-ADAPTER`; durable ledger split to a follow-up issue; tracker auto-closes via `Closes #NNN` linkage on the final task PR — same convention as WF-IMPL-000-STEP-COORDINATOR.)_
