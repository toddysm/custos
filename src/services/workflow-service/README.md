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

**Scaffold only** — WF-IMPL-013 ([#347](https://github.com/toddysm/custos/issues/347)).
The package skeleton, the `create_app()` factory placeholder, the
`python -m custos_workflow` entry point, and the CI gate
(`.github/workflows/python-services.yml`) are real. Everything else is
incremental work tracked under [#363](https://github.com/toddysm/custos/issues/363)
(WF-IMPL-000-COMPILER), starting with the Definition Compiler sub-module.

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

## Local development

```bash
cd src/services/workflow-service
pip install -e ../../libs/custos-cel[dev]
pip install -e ".[dev]"
ruff check .
ruff format --check .
mypy src tests
pytest -q
```

`python -m custos_workflow` will currently raise `NotImplementedError` from
`create_app()` — that is the documented scaffold behaviour. The factory is
wired in WF-IMPL-015 ([#349](https://github.com/toddysm/custos/issues/349)).

## Layout

```
src/services/workflow-service/
├── pyproject.toml
├── README.md
├── src/
│   └── custos_workflow/
│       ├── __init__.py        # scaffold: __version__ + create_app stub
│       ├── __main__.py        # python -m custos_workflow entry point
│       └── py.typed
└── tests/
    └── test_smoke.py          # scaffold import + stub-contract assertions
```

Subsequent WF-IMPL-* tasks add modules under `src/custos_workflow/`
(`document/`, `bindings/`, `graph/`, `callsites/`, `retry/`, `on_error/`,
`errors.py`, `compiler.py`, `_telemetry.py`) plus the FastAPI surface
under `app.py`, `healthz.py`, `call_context.py`.
