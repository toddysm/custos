# trigger-service

Custos Trigger Service (COMP-004). The platform's event ingestion and dispatch
broker: it receives signals (manual, scheduled, webhook, vendor-push, pull,
internal), normalizes them into a `NormalizedEvent` envelope, classifies them
as workflow-start or step-resume, matches them to `Subscription` rows via CEL
selectors, deduplicates them, and dispatches to the Workflow Service
(`StartRun` / `RaiseExternalEvent`).

Design: [`design/components/trigger-service/design.md`](../../../design/components/trigger-service/design.md).
Implementation plan: [`design/components/trigger-service/implementation-plan.md`](../../../design/components/trigger-service/implementation-plan.md).

## Status

**Scaffolding (M1 in progress)** — the `TS-IMPL-000-TRIGGER-SERVICE` milestone
([#652](https://github.com/toddysm/custos/issues/652)) is underway. This task
(`TS-IMPL-001`) lands the package skeleton: the `custos-trigger-service`
distribution, the `custos_trigger` package + entry point, the ruff / mypy
(strict) / pytest (≥90 % coverage) quality-gate toolchain, and the CI job. The
runtime surfaces — settings + error taxonomy, the canonical event taxonomy, the
CEL selector evaluator, the normalize → classify → match → dedup → dispatch
pipeline, the manual / resume-RPC / internal-event receivers, and observability
— land across the subsequent `TS-IMPL-*` phases.

Tracking issue: [#652](https://github.com/toddysm/custos/issues/652)
(`TS-IMPL-000`).

## Layout

```
src/custos_trigger/
  __init__.py    # package marker + version (create_app lands in TS-IMPL-003)
  __main__.py    # `python -m custos_trigger` / console-script entry point
  py.typed       # PEP 561 typing marker
tests/
  test_scaffold.py  # import + version smoke test
```

## Development

From this directory (`src/services/trigger-service`):

```bash
pip install -e ../../libs/storage-provider-layer[dev]
pip install -e ../../libs/custos-postgres[dev]
pip install -e ../../libs/custos-cel[dev]
pip install -e ".[dev]"

ruff format . && ruff check . && mypy src tests
pytest -q --cov=custos_trigger --cov-report=term-missing --cov-fail-under=90
```
