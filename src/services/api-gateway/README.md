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

**Phase A scaffold (AGW-IMPL-001, AGW-IMPL-002, AGW-IMPL-003) + Auth
delegation & enforcement (AGW-IMPL-004, AGW-IMPL-005) + workspace resolution
(AGW-IMPL-006) + call-context minting (AGW-IMPL-007) + startup permission
validation (AGW-IMPL-008) + write-path idempotency (AGW-IMPL-009) + write-path
rate limiting (AGW-IMPL-010)** — the `custos_gateway`
package, its `pyproject.toml` (ruff + mypy strict + pytest with a
`--cov-fail-under=90` floor), the `python -m custos_gateway` entry point, the
typed `Settings` + `load_settings()` loader over the design Configuration table,
the `create_app(*, settings=...)` factory with a lifespan readiness gate, the
`/healthz` + `/readyz` probes, the locked RFC 7807 error taxonomy +
`application/problem+json` envelope, the correlation-id ingress middleware
(`x-correlation-id` on every response), the `AuthServiceClient` over Dapr
service invocation (`verify_and_authorize` / `callctx_sign` /
`get_permissions` + Noop/Fake doubles), the AuthN/AuthZ `require_permission`
dependency (verify-and-authorize per route, `principal` + `auditEventId` on
`request.state`, webhook + auth-bootstrap bypass classifier), the
URL-authoritative `resolve_workspace` dependency (path `{workspaceId}` binds
`request.state.workspace_id`; a body naming a different workspace is rejected
`400 workspace-mismatch`; unscoped routes resolve cleanly), the
`mint_call_context` dependency (one signed `x-custos-callctx` per authenticated
request via `callctx.sign`, staged with `x-correlation-id` on the outbound Dapr
metadata), the startup permission validator (`startup.py` cross-checks every
route's declared `requiredPermission` against the Auth Service registry inside
the lifespan and refuses to become ready on any undeclared permission), the
write-path Idempotency Coordinator (`idempotency.py` derives the
`(workspaceId, principalId, route, idempotencyKey)` key + SHA-256 request hash,
reserves/completes records on the SPL `MetadataStoreProvider`, and maps the four
reserve outcomes onto proceed / replay / `409 idempotency-in-flight` +
`Retry-After` / `409 idempotency-key-reuse`), the write-path Rate Limiter
(`ratelimit.py` runs per-principal and per-workspace in-memory token buckets;
a write is admitted only when both buckets can afford it, otherwise it is
rejected `429 rate-limited` with `Retry-After` + the `RateLimit-*` headers; the
`tryConsume(bucketKey, cost) -> Allow | Deny` interface keeps a Dapr/Redis-backed
coordinated limiter a drop-in M2 replacement; the per-replica bucket map is
bounded with LRU eviction to bound memory), and the
CI gate
(`.github/workflows/python-services.yml`) are in place. Subsequent tasks layer
in the remaining cross-cutting
write-path middleware (Phase C), the downstream router + route registry +
webhook + device-code surfaces (Phase D), full `create_app` wiring + OpenAPI +
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
  errors.py        # locked error taxonomy + RFC 7807 problem+json envelope
  middleware/
    __init__.py    # cross-cutting middleware package
    auth.py        # require_permission dependency + bypass classifier
    callctx_mint.py # signed x-custos-callctx minting + outbound metadata
    correlation.py # x-correlation-id ingress + UUIDv7 generation
    idempotency.py # write-path dedup coordinator (reserve/complete + hash)
    ratelimit.py   # per-principal + per-workspace token-bucket rate limiter
    workspace.py   # URL-authoritative workspace resolver + mismatch guard
  startup.py       # startup route-permission validation against Auth registry
  clients/
    __init__.py    # outbound delegation clients package
    auth.py        # AuthServiceClient over Dapr (verify/sign/permissions)
tests/
  conftest.py        # gateway_env + settings fixtures
  test_scaffold.py   # package-import + factory + CLI smoke tests
  test_settings.py   # configuration parsing + validation
  test_app.py        # app factory + health probe behavior
  test_errors.py     # error taxonomy grid + envelope rendering
  test_correlation.py # correlation-id ingress + propagation
  test_auth_client.py # Auth Service Dapr client + doubles
  test_auth_middleware.py # require_permission enforcement + bypass
  test_workspace_middleware.py # URL-authoritative workspace resolution
  test_callctx_mint_middleware.py # signed call-context minting + metadata
  test_startup.py # startup route-permission validation + lifespan wiring
  test_idempotency_middleware.py # write-path dedup reserve/complete + hashing
  test_ratelimit_middleware.py # token-bucket rate limiting + RateLimit headers
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
