# workflow-service

Custos Workflow Service (COMP-003). Owns the orchestration state machine:
Run / Step / StepAttempt lifecycle over Dapr Workflow, the Definition
Compiler that turns a published `WorkflowVersion` into a runtime-ready
`ExecutionGraph`, the Expression Evaluator integration (`custos-cel`),
sub-orchestration management for dynamic loops and approval gates, resume
subscription lifecycle against the Trigger Service, and publication of
workflow lifecycle events to `custos.workflow.events`.

Design: [`design/components/workflow-service/design.md`](../../../design/components/workflow-service/design.md).

## Status

**Phase A scaffold + FastAPI surface + WorkflowDocument models** —
WF-IMPL-013 ([#347](https://github.com/toddysm/custos/issues/347)),
WF-IMPL-014 ([#348](https://github.com/toddysm/custos/issues/348)),
WF-IMPL-015 ([#349](https://github.com/toddysm/custos/issues/349)), and
WF-IMPL-016 ([#350](https://github.com/toddysm/custos/issues/350)). The
package skeleton, the runnable `create_app()` factory, the
`python -m custos_workflow` entry point, the `/healthz` and `/readyz`
probes, the call-context middleware shim, the Helm subchart, the
CI gate (`.github/workflows/python-services.yml`), and the typed
`WorkflowDocument` model + YAML loader are real. The remaining
Definition Compiler internals (DAG construction, retry / on-error
binding) land in WF-IMPL-017 onwards, tracked under
[#363](https://github.com/toddysm/custos/issues/363) (WF-IMPL-000-COMPILER).

The Expression Evaluator (the first sub-module) is already in
[`src/libs/custos-cel/`](../../libs/custos-cel) and shipped via
WF-IMPL-001 through WF-IMPL-012 (#176–#187).

## Configuration

Per [`design/components/workflow-service/design.md`](../../../design/components/workflow-service/design.md) § Configuration:

| Variable | Required | Default | Description |
|---|---|---|---|
| `WF_DAPR_WORKFLOW_COMPONENT` | Yes | — | Name of the Dapr Workflow component to bind. |
| `WF_PUBLISH_TOPIC` | No | `custos.workflow.events` | Dapr Pub/Sub topic for lifecycle event publication. |
| `WF_ARM_ENDPOINT` | Yes | — | Activity Runtime Manager service endpoint. |
| `WF_TS_ENDPOINT` | Yes | — | Trigger Service service endpoint. |
| `WF_CONNECTOR_ENDPOINT` | Yes | — | Connector Service endpoint. |
| `WF_CATALOG_ENDPOINT` | Yes | — | Catalog Service endpoint (read-only `WorkflowVersion`). |
| `WF_RUN_HISTORY_RETENTION` | No | `90d` | How long to keep terminal-run metadata before archival. |
| `WF_RESUME_SUB_DEFAULT_TTL` | No | `PT24H` | Default TTL for `RegisterResumeSubscription` when caller does not specify. |
| `WF_REGISTER_SUB_MAX_RETRIES` | No | `5` | Max retries when registering a resume subscription with TS before failing the wait step. |
| `WF_EXPR_TIMEOUT_MS` | No | `100` | Per-expression evaluation timeout. |
| `WF_IDEMPOTENCY_KEY_TTL` | No | `PT24H` | Window for `(workspaceId, StartRun idempotencyKey)` dedup. |

Process bind:

| Variable | Default | Purpose |
|---|---|---|
| `HOST` | `0.0.0.0` | Address the uvicorn process binds to. |
| `PORT` | `8080` | Port the uvicorn process listens on. |
| `WF_REQUIRE_CALL_CONTEXT` | `""` (dev shim) | Set to the exact literal `"1"` to enforce that every non-probe request carries `X-Custos-Workspace` and `X-Custos-Principal` headers (returns `401` `callctx_missing` otherwise). Any other value — including `"true"`, `"yes"`, `"TRUE"` — leaves the dev shim active. |

## Local development

```bash
cd src/services/workflow-service
pip install -e "../../libs/custos-cel[dev]"
pip install -e ".[dev]"
ruff check .
ruff format --check .
mypy src tests
pytest -q
```

`python -m custos_workflow` starts uvicorn against
`custos_workflow:create_app` (`factory=True`), honouring `HOST` /
`PORT`. The lifespan flips `app.state.ready` immediately because Phase A
has no startup dependencies; WF-IMPL-016+ gates the readiness flip on
the Definition Compiler bootstrap and the Catalog client warm-up.

Probe quick-check:

```bash
curl -fsS http://localhost:8080/healthz
curl -fsS http://localhost:8080/readyz
```

## Layout

```
src/services/workflow-service/
├── pyproject.toml
├── README.md
├── src/
│   └── custos_workflow/
│       ├── __init__.py        # re-exports __version__ + create_app
│       ├── __main__.py        # python -m custos_workflow entry point
│       ├── _version.py        # package version
│       ├── app.py             # FastAPI factory + lifespan
│       ├── call_context.py    # CallContext + middleware shim
│       ├── healthz.py         # /healthz + /readyz routes
│       ├── document/          # WorkflowDocument Pydantic models + YAML loader
│       │   ├── __init__.py    # public re-exports
│       │   ├── models.py      # WorkflowDocument + Step union + RetryPolicy …
│       │   └── loader.py      # parse_document() + DocumentParseError
│       └── py.typed
└── tests/
    ├── test_smoke.py             # package import + factory smoke
    ├── test_app.py               # factory shape + lifespan + env flag
    ├── test_call_context.py      # header presence/absence + dev/prod mode
    ├── test_healthz.py           # liveness/readiness status codes
    ├── test_document_models.py   # WorkflowDocument + Step union + retry
    └── test_document_loader.py   # YAML → WorkflowDocument + error wrapping
```

Subsequent WF-IMPL-* tasks add the remaining Definition Compiler
modules under `src/custos_workflow/` (`bindings/`, `graph/`,
`callsites/`, `retry/`, `on_error/`, `errors.py`, `compiler.py`,
`_telemetry.py`).
