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

**Implemented** — the `AGW-IMPL-000-API-GATEWAY` milestone
([#732](https://github.com/toddysm/custos/issues/732)) is complete; all 21 child
tasks (AGW-IMPL-001 … AGW-IMPL-021) are merged and the tracking issue is closed.
The
`custos_gateway`
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
the lifespan; a transient Auth Service / Dapr-sidecar outage at boot is
non-fatal — the gateway stays up not-ready and converges in the background with
exponential backoff (issue #815) — while a permission drift or non-retryable
contract error keeps it permanently not-ready instead of crash-looping), the
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
bounded with LRU eviction to bound memory), the ingress Request Validator
(`validate.py` enforces per-route body-size caps — 1 MB default, 5 MB for
workflow/template publish — rejecting oversized bodies `413 body-too-large`,
requires a JSON content type on standard write routes `415
unsupported-media-type`, and classifies the webhook + auth-bootstrap raw-body
families that bypass content-type enforcement), the Downstream Router
(`router.py` forwards each request to its owning component over Dapr service
invocation using the lifespan-owned `httpx.AsyncClient` — carrying the signed
`x-custos-callctx` + `x-correlation-id` headers — passes any non-server-error
downstream response through raw, body + status + end-to-end headers, and masks a
downstream `5xx` or sidecar transport error as `503 downstream-unavailable`),
the declarative M1 Route Registry (`routes/registry.py` enumerates every
external route in the design's M1 contract set as a frozen `RouteSpec` table —
each carrying its owning Dapr `app_id`, `requiredPermission`,
`requiresIdempotencyKey`, `maxBodyBytes`, and `rateLimitClass` — and
`build_registry_router()` mounts each one with its `require_permission`
dependency, so every declared permission participates in the startup registry
cross-check, plus a thin pass-through endpoint that forwards to the owning
component via the downstream router and shapes the reply), the anonymous Webhook
Pass-through (`routes/webhook.py` mounts `POST /v1/webhooks/{connectorInstanceId}`
with no `require_permission` dependency — the gateway's single anonymous ingress —
enforcing a 1 MB body cap `413 body-too-large`, stripping `Authorization` (and the
hop-by-hop / framing headers), appending the caller's source IP to
`X-Forwarded-For`, generating/propagating an `x-correlation-id`, and forwarding
the untouched body to Trigger Service via the downstream router without minting a
call context — an unknown `{connectorInstanceId}` surfaces the downstream
`404 webhook-route-not-found` raw), the Device-Code Session Manager M1 stub
(`routes/devicecode.py` mounts the three auth-bootstrap routes `POST
/v1/auth/login/device`, `POST /v1/auth/login/device/{deviceCode}/poll`, and `GET
/v1/auth/login/device/{userCode}` — anonymous, no `require_permission`, no call
context, all under the auth-bootstrap bypass prefix — gated on a configured OIDC
issuer (`Settings.device_code_enabled`); M1 ships OIDC disabled so every handler
returns `503`, while the `DeviceCodeStore` persistence seam + `app.state` binding
+ `CUSTOS_GATEWAY_DEVICE_CODE_TTL` config are declared for M3 activation),
the fully wired application factory (`app.py` AGW-IMPL-016 — `create_app(*,
settings=..., auth_client=..., downstream_router=..., metadata_store=...,
device_code_store=..., rate_limiter=...)` mounts the registry, webhook, and
device-code routers behind the CORS + correlation middleware stack; the lifespan
builds an owned `httpx.AsyncClient` + `DownstreamRouter` (closed on shutdown)
unless one is injected, builds a Dapr-backed `DaprAuthServiceClient` over that
client unless an Auth client is injected, binds the `RateLimiter`, idempotency
metadata store, and device-code store onto `app.state`, runs the startup
permission cross-check against the bound Auth client, and flips readiness; each
registry route runs the `resolve_workspace` →
`require_permission` → `mint_call_context` dependency chain and a forwarder that
validates body size + content type, charges the rate limiter, reserves/replays/
completes idempotency, forwards over Dapr, and shapes the reply — every stage
skipped when it does not apply or its backing resource is unbound),
the OpenAPI 3.1 document (`openapi.py` AGW-IMPL-017 — `/openapi.json` post-
processed from FastAPI introspection: the `BearerAuth` + `WebhookNoAuth` security
schemes referenced by the right routes, the `x-custos-required-permission` /
`x-custos-idempotent` operation extensions sourced from the route registry on
every operation, and the shared RFC 7807 `ProblemDetails` error schema as each
operation's default response),
the OpenTelemetry instrumentation (`_telemetry.py` AGW-IMPL-018 — every proxied
request opens a `custos_gateway.request` span carrying `{http.method, http.route,
workspaceId, principalId, correlationId, decisionAuditEventId}`, increments the
`gateway_requests_total` / `gateway_rate_limit_denials_total` /
`gateway_idempotency_replays_total` counters, and observes the
`gateway_request_duration_seconds` / `gateway_downstream_duration_seconds`
histograms; `opentelemetry-api`-only, inert until an SDK + exporter is wired),
the Helm subchart wiring (AGW-IMPL-019 — `deploy/helm/charts/api-gateway`
projects the `CUSTOS_GATEWAY_*` config + TLS secret refs through the ConfigMap),
the in-process conformance suite (AGW-IMPL-020 —
`tests/integration/test_conformance.py` exercises authn/authz, workspace,
idempotency (all four reserve outcomes), rate-limit, validation, routing,
webhook, and the device-code 503 against a stub downstream + fake Auth +
in-memory SPL store), the developer documentation (AGW-IMPL-021 —
[`docs/developers/api-gateway.md`](../../../docs/developers/api-gateway.md),
pinned to the running code by `tests/test_docs_examples.py`), and the
CI gate
(`.github/workflows/python-services.yml`) are in place. The component
implementation is complete.

Tracker: [#732](https://github.com/toddysm/custos/issues/732) —
`AGW-IMPL-000-API-GATEWAY`.

## Layout

```
src/custos_gateway/
  __init__.py      # package metadata + version, re-exports create_app
  __main__.py      # `python -m custos_gateway` CLI entry point
  _version.py      # standalone version string
  app.py           # create_app() factory: lifespan wiring + CORS + all routers
  openapi.py       # OpenAPI 3.1 doc at /openapi.json: schemes + x-custos-* + errors
  _telemetry.py    # OTel per-request span + request/ratelimit/idempotency metrics
  settings.py      # Settings dataclass + load_settings() over CUSTOS_GATEWAY_*
  health.py        # /healthz (liveness) + /readyz (readiness) probes
  errors.py        # locked error taxonomy + RFC 7807 problem+json envelope
  router.py        # downstream Dapr router + response shaper (raw 2xx / 503)
  routes/
    __init__.py    # route registry package re-exports
    _forwarding.py # shared downstream-router lookup + response shaper
    registry.py    # declarative M1 RouteSpec table + registry router factory
    webhook.py     # anonymous POST /v1/webhooks/{connectorInstanceId} forward
    devicecode.py  # auth-bootstrap /v1/auth/login/device* routes (M1 503 stub)
  middleware/
    __init__.py    # cross-cutting middleware package
    auth.py        # require_permission dependency + bypass classifier
    callctx_mint.py # signed x-custos-callctx minting + outbound metadata
    correlation.py # x-correlation-id ingress + UUIDv7 generation
    idempotency.py # write-path dedup coordinator (reserve/complete + hash)
    ratelimit.py   # per-principal + per-workspace token-bucket rate limiter
    validate.py    # body-size caps + content-type enforcement + route classes
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
  test_validate_middleware.py # body-size + content-type + route classification
  test_router.py # downstream Dapr router pass-through + 503 mapping
  test_route_registry.py # M1 route registry contract + forwarding seam
  test_forwarding.py # shared downstream-router lookup + response shaper
  test_webhook.py # anonymous webhook pass-through forwarding + body cap
  test_devicecode.py # device-code auth-bootstrap routes + M1 503 stub + seam
  test_pipeline.py # end-to-end ingress pipeline through the wired create_app
  test_openapi.py # OpenAPI 3.1 doc: schemes + x-custos-* extensions + error schema
  test_telemetry.py # OTel spans + request/ratelimit/idempotency counters + histograms
  test_docs_examples.py # pins docs/developers/api-gateway.md to the running code
  integration/
    harness.py # in-process conformance harness: stub downstream + fake Auth + SPL store
    test_conformance.py # cross-cutting pipeline conformance across every stage
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
