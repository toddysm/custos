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

**Implemented** — the `TS-IMPL-000-TRIGGER-SERVICE` milestone
([#652](https://github.com/toddysm/custos/issues/652)) is complete; all 21 child
tasks (TS-IMPL-001 … TS-IMPL-021) are merged and the tracking issue is closed.
The service implements the full design: typed settings + the locked `trigger.*`
error taxonomy, the `custos-cel` event-binding root + canonical event-taxonomy
registry, the `NormalizedEvent` envelope + subscription/domain models, SPL store
adapters over `MetadataStoreProvider` + `providers.py` wiring, reserve-before-
dispatch dedup / idempotency, the Event Normalizer, the CEL selector evaluator,
the Classifier + Start Matcher + Resume Matcher, the `WorkflowServiceClient`
Dapr Service-Invocation adapter (`StartRun` / `RaiseExternalEvent`), the
Dispatcher, the Manual Receiver + REST CRUD (REQ-004), the Register / Cancel
ResumeSubscription RPCs (REQ-081), the Internal Event Receiver (REQ-080 /
REQ-081) + Dapr subscription + app wiring, and OTel observability + audit
events. Backed by a unit + integration suite at the ≥90 % coverage gate
(TS-IMPL-020) and developer docs at
[`docs/developers/trigger-api.md`](../../../docs/developers/trigger-api.md)
(TS-IMPL-021).

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
