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

**Phase A scaffold (AGW-IMPL-001)** — the `custos_gateway` package, its
`pyproject.toml` (ruff + mypy strict + pytest with a `--cov-fail-under=90`
floor), the `python -m custos_gateway` entry point, the placeholder
`create_app()` factory, and the CI gate
(`.github/workflows/python-services.yml`) are in place. Subsequent tasks layer
in settings + health probes (AGW-IMPL-002), the error envelope + correlation-id
middleware (AGW-IMPL-003), Auth Service delegation + call-context minting
(Phase B), the cross-cutting write-path middleware (Phase C), the downstream
router + route registry + webhook + device-code surfaces (Phase D), full
`create_app` wiring + OpenAPI + observability (Phase E), and Helm wiring +
verification + docs (Phase F).

Tracker: [#732](https://github.com/toddysm/custos/issues/732) —
`AGW-IMPL-000-API-GATEWAY`.

## Layout

```
src/custos_gateway/
  __init__.py      # package metadata + version
  __main__.py      # `python -m custos_gateway` CLI entry point
  app.py           # create_app() FastAPI factory (placeholder; grows per phase)
tests/
  test_scaffold.py # package-import + factory + CLI smoke tests
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
