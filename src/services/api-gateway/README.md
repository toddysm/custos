# api-gateway

Custos API Gateway (COMP-001). The single uniform HTTPS entrypoint for every
external Custos caller — UI, CLI, SDK, third-party automation, and inbound
webhooks. The gateway contains **no domain logic**: it terminates TLS, validates
request shape, delegates every authentication and authorization decision to the
Auth Service, mints the signed call context that internal RPCs travel on,
deduplicates idempotent writes, applies coarse rate limits, normalizes errors
into a single RFC 7807 envelope, and routes each request to the owning
downstream component via Dapr service invocation.

Design: [`design/components/api-gateway/design.md`](../../../design/components/api-gateway/design.md).

## Status

**Phase A scaffold (AGW-IMPL-001, AGW-IMPL-002)** — the `custos_gateway`
package, its `pyproject.toml` (ruff + mypy strict + pytest with a
`--cov-fail-under=90` floor), the `python -m custos_gateway` entry point, the
typed `Settings` + `load_settings()` loader over the design Configuration table,
the `create_app(*, settings=...)` factory with a lifespan readiness gate, the
`/healthz` + `/readyz` probes, and the CI gate
(`.github/workflows/python-services.yml`) are in place. Subsequent tasks layer
in the error envelope + correlation-id middleware (AGW-IMPL-003), Auth Service
delegation + call-context minting (Phase B), the cross-cutting write-path
middleware (Phase C), the downstream router + route registry + webhook +
device-code surfaces (Phase D), full `create_app` wiring + OpenAPI +
observability (Phase E), and Helm wiring + verification + docs (Phase F).

Tracker: [#732](https://github.com/toddysm/custos/issues/732) —
`AGW-IMPL-000-API-GATEWAY`.

## Layout

```
src/custos_gateway/
  __init__.py      # package metadata + version, re-exports create_app
  __main__.py      # `python -m custos_gateway` CLI entry point
  _version.py      # standalone version string
  app.py           # create_app() FastAPI factory + lifespan readiness gate
  settings.py      # Settings dataclass + load_settings() over CUSTOS_GATEWAY_*
  health.py        # /healthz (liveness) + /readyz (readiness) probes
tests/
  conftest.py      # gateway_env + settings fixtures
  test_scaffold.py # package-import + factory + CLI smoke tests
  test_settings.py # configuration parsing + validation
  test_app.py      # app factory + health probe behavior
```

## Development

The gateway depends on two path-local libraries (`custos-spl`,
`custos-callctx`). Install them first, then the service:

```bash
pip install -e src/libs/storage-provider-layer[dev]
pip install -e src/libs/custos-callctx[dev]
pip install -e src/services/api-gateway[dev]
```

Quality gates run from `src/services/api-gateway`:

```bash
ruff format . && ruff check . && mypy src tests && pytest -q
```
