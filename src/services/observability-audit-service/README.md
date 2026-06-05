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

**Scaffolding (M1 in progress)** — the `OBS-IMPL-000-OBSERVABILITY-AUDIT-SERVICE`
milestone ([#692](https://github.com/toddysm/custos/issues/692)) is underway.
This task (`OBS-IMPL-001`) lands the package skeleton: the
`custos-observability-audit-service` distribution, the `custos_obs` package +
entry point, the `/healthz` + `/readyz` probes, the ruff / mypy (strict) /
pytest (≥90 % coverage) quality-gate toolchain, and the CI job. The runtime
surfaces — typed settings + error taxonomy, the SPL provider wiring, the audit
outbox drainer + pipeline + retention worker, the alert-rule DSL + dispatcher,
the External Exporter Loader, the read-back API surface, and observability —
land across the subsequent `OBS-IMPL-*` phases.

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
