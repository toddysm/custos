# observability-audit-service

Custos Observability and Audit Service (COMP-009). The platform's single
observer-side surface: it drains the SPL audit outbox into the durable audit
store, enforces audit retention (90-day default, configurable upward), manages
the OTel Collector exporter bundle (the External Exporter Loader), dispatches
alerts (webhook + SMTP in M1), and serves the inbound read-back APIs — per-run
log tail (SSE), audit query, and run-scoped metrics — consumed by the Custos
UI/CLI via the API Gateway.

The SPL providers and the Loki/Prometheus/Postgres adapters this service
consumes already exist; this service orchestrates them, it does not create
them.

Design: [`design/components/observability-audit-service/design.md`](../../../design/components/observability-audit-service/design.md).
Implementation plan: [`design/components/observability-audit-service/implementation-plan.md`](../../../design/components/observability-audit-service/implementation-plan.md).

## Status

**Implemented** — the `OBS-IMPL-000-OBSERVABILITY-AUDIT-SERVICE` milestone
([#692](https://github.com/toddysm/custos/issues/692)) is complete; all 17 child
tasks (OBS-IMPL-001 … OBS-IMPL-017) are merged and the tracking issue is closed.
The service implements the full design: typed settings + conditional-requirement
validation, the `obs.*` error taxonomy + RFC 7807 envelope + audit events,
`providers.py` SPL provider wiring, the Audit Outbox Drainer (LISTEN/NOTIFY +
polling fallback), the Audit Pipeline (idempotent dispatch + per-pipeline
cursors + lag signal), the retention worker (audit retention + outbox GC), the
alert-rule DSL loader + matcher, the Alerting Dispatcher (webhook + SMTP sinks +
dead-letter), the collector-config merge / validation + Exporter ConfigMap
watcher + Collector reload / rollback (External Exporter Loader), the read-back
Query API (log SSE tail + paged history, metrics + audit routes) with
call-context middleware + permission enforcement, and OTel self-instrumentation.
Backed by a unit + integration suite at the ≥90 % coverage gate (OBS-IMPL-016)
and developer docs at
[`docs/developers/observability-api.md`](../../../docs/developers/observability-api.md)
(OBS-IMPL-017).

Tracking issue: [#692](https://github.com/toddysm/custos/issues/692)
(`OBS-IMPL-000`).

## Layout

```
src/custos_obs/
  __init__.py    # package marker + version + create_app re-export
  _version.py    # standalone version string
  __main__.py    # `python -m custos_obs` / console-script entry point
  app.py         # FastAPI create_app() factory + lifespan readiness hook
  health.py      # /healthz + /readyz probes
  py.typed       # PEP 561 typing marker
tests/
  test_scaffold.py  # import + version + probe smoke tests
```

## Development

From this directory (`src/services/observability-audit-service`):

```bash
pip install -e ../../libs/storage-provider-layer[dev]
pip install -e ../../libs/custos-postgres[dev]
pip install -e ../../libs/custos-loki[dev]
pip install -e ../../libs/custos-prometheus[dev]
pip install -e ../../libs/custos-callctx[dev]
pip install -e ".[dev]"

ruff format . && ruff check . && mypy src tests
pytest -q --cov=custos_obs --cov-report=term-missing --cov-fail-under=90
```

Run the service locally:

```bash
python -m custos_obs   # serves /healthz + /readyz on 0.0.0.0:8080
```
