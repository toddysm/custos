# API Gateway

Last Updated: 2026-06-04

The **API Gateway** (`COMP-001`) is the single, uniform HTTPS entrypoint for
Custos. Every external caller — the web UI, the CLI, CI integrations, and inbound
connector webhooks — reaches the platform through it. The gateway owns **no
domain logic**: its public REST surface is the *union* of every downstream
component's externally-facing contract, mounted under `/v1/`. Its job is the set
of cross-cutting concerns that every request shares — TLS termination,
authentication and authorization delegation, call-context minting, request
validation, idempotency, rate limiting, and a uniform error envelope — after
which the request is forwarded verbatim to the owning component over Dapr
service-invocation.

> **Doc-as-contract.** The error-taxonomy table, the route-registry permission
> set, the configuration table, and the Problem+JSON envelope example in this
> guide are all validated against the running code by
> [`tests/test_docs_examples.py`](../../src/services/api-gateway/tests/test_docs_examples.py),
> so the document cannot drift from the implementation.

> **Milestone note.** M1 ships the full ingress pipeline and the M1 route
> registry. The OIDC **device-code login flow** is wired but disabled — every
> device-code endpoint returns `503` until an issuer is configured (see
> [Device-code login flow](#device-code-login-flow)). A coordinated
> (Redis/Dapr-state) rate limiter, multi-region routing, and schema-translation
> versioning are deferred to M2+.

---

## Contents

- [Entrypoint contract](#entrypoint-contract)
- [Request pipeline](#request-pipeline)
- [Authentication and authorization](#authentication-and-authorization)
- [Call-context minting](#call-context-minting)
- [Idempotency](#idempotency)
- [Rate limiting](#rate-limiting)
- [Request validation](#request-validation)
- [Error envelope (RFC 7807)](#error-envelope-rfc-7807)
- [Route registry](#route-registry)
- [Webhook ingress](#webhook-ingress)
- [Device-code login flow](#device-code-login-flow)
- [Configuration](#configuration)

---

## Entrypoint contract

The gateway listens for TLS traffic and exposes:

- The **public REST surface** under `/v1/` — the M1 [route registry](#route-registry).
- The **anonymous webhook ingress** at `/v1/webhooks/{connectorInstanceId}`.
- The **auth-bootstrap** device-code routes under `/v1/auth/login/...`.
- The Kubernetes **probes** `/healthz` (liveness) and `/readyz` (readiness),
  which return a fixed probe shape and are excluded from the OpenAPI document.
- The generated **OpenAPI 3.1 document** at `/openapi.json`.

The Dapr app-id is `api-gateway`. Readiness only flips `true` after the startup
**permission cross-check** succeeds: every permission declared by the route
registry is validated against the Auth Service permission registry, so a
permission name that drifts from the platform is a loud boot failure rather than
a per-request surprise.

CORS is enforced for the configured origin allow-list only — there is **no
wildcard origin**. The CORS layer sits outermost, so every response (success or
Problem+JSON error) carries both the CORS headers and the `x-correlation-id`
stamped by the inner correlation middleware.

---

## Request pipeline

A request flows through the cross-cutting stages in a fixed order. The first
stages are ASGI middleware; the per-route stages run as FastAPI dependencies and
the forwarder body, in this order:

1. **CORS** — preflight `OPTIONS` is short-circuited here; allowed origins only.
2. **Correlation id** — `x-correlation-id` is read or generated and bound to the
   request; it is echoed on every response and propagated downstream.
3. **Workspace resolution** — the `{workspaceId}` path segment is extracted.
4. **Authentication + authorization** — the bearer is verified and authorized
   against the route's declared permission in a single Auth Service round trip.
5. **Call-context minting** — a fresh signed `x-custos-callctx` is minted for the
   authorized principal.
6. **Body-size + content-type validation** — oversized or wrong-media-type
   requests are rejected before any forward.
7. **Rate limiting** — write requests are charged against the per-principal and
   per-workspace token buckets.
8. **Idempotency** — write requests reserve/replay/complete an idempotency
   record keyed by `(workspace, principal, route, idempotency-key)`.
9. **Forward** — the request is proxied verbatim to the owning component over
   Dapr, carrying the minted call context and the correlation id.
10. **Response shaping** — a downstream `2xx`/`4xx` passes through unchanged; a
    downstream `5xx` or transport error is masked as `503 downstream-unavailable`.

The anonymous [webhook](#webhook-ingress) and [device-code](#device-code-login-flow)
routes deliberately skip the auth, call-context, idempotency, and rate-limit
stages.

---

## Authentication and authorization

Every non-bypass request **must** present an `Authorization: Bearer <token>`
header. The gateway never makes an authentication or authorization decision
itself: it extracts the bearer, resolves the workspace, and asks the Auth Service
to **verify-and-authorize** the token against the route's declared permission in
a single round trip.

- A missing or malformed bearer, or a token the Auth Service rejects, yields
  `401 invalid-token`.
- A valid token that lacks the route's permission yields `403 permission-denied`,
  carrying the `auditEventId` of the Auth Service decision.

On success the gateway carries the decision forward (`principalId`,
`auditEventId`, the permission, and the resolved workspace) so the later pipeline
stages and the downstream forward never need to re-query the Auth Service.

The permission a route requires is **declared** on the route and cross-checked
against the Auth Service registry at startup, so a route can never reference a
permission the platform does not know about.

---

## Call-context minting

For every **authenticated** request the gateway mints a fresh signed call-context
(`x-custos-callctx`) by asking the Auth Service to sign one for the authorized
principal, and propagates it — together with the request correlation id
(`x-correlation-id`) — through the Dapr service-invocation metadata of the
downstream call. The minted context records `callerComponent: "api-gateway"` so
downstream audit trails attribute the hop correctly.

Downstream components verify the signature **locally** against the Auth Service
JWKS; they never call back to the Auth Service per request. Exactly one context
is minted per request. The anonymous webhook and auth-bootstrap routes mint no
context.

---

## Idempotency

Write requests (`POST`/`PUT`/`PATCH`/`DELETE`) on routes that require an
idempotency key SHOULD supply an `Idempotency-Key` header so a retry can replay
the original response. The header is optional: an absent or blank header yields a
freshly generated key, so a retry the **gateway itself** performs is still
deduplicated, but client-driven replay requires the client to resend the same
key. The gateway reserves an idempotency record keyed by the
`(workspace, principal, route, idempotency-key)` tuple and fingerprints the
request (method, route, workspace, body, selected headers) so a key reused with a
**different** payload is detected.

The reservation has four outcomes:

| Outcome | Result |
|---|---|
| **Reserved** (first use) | The request is forwarded; the response snapshot is stored under the key on completion. |
| **Replay** (same key, same payload, completed) | The stored response snapshot is returned **without** re-forwarding. |
| **In flight** (same key, still processing) | `409 idempotency-in-flight` with a `Retry-After` header. |
| **Key reuse** (same key, different payload) | `409 idempotency-key-reuse`. |

Read requests never engage idempotency. The record TTL is governed by
`CUSTOS_GATEWAY_IDEMPOTENCY_TTL` (default 24h).

---

## Rate limiting

Write requests are charged against two token buckets — one **per principal** and
one **per workspace**. Every rate-limited write surfaces the full `RateLimit-*`
header set — `RateLimit-Limit`, `RateLimit-Remaining`, and `RateLimit-Reset` —
reflecting the most restrictive bucket, so clients can self-throttle. When either
bucket is exhausted the request is rejected with `429 rate-limited`, carrying the
same `RateLimit-*` headers plus a `Retry-After` header.

Reads are not rate-limited. The bucket rates and bursts are configured by the
`CUSTOS_GATEWAY_RATE_LIMIT_*` variables (defaults: per-principal 20 rps / 40
burst, per-workspace 200 rps / 400 burst). The M1 limiter is in-process per
replica; a coordinated cross-replica limiter is deferred to M2.

---

## Request validation

Two request-shape checks run before any forward:

- **Body size** — requests larger than the route's cap are rejected with
  `413 body-too-large`. The default cap is 1 MiB; workflow/template *publish*
  routes are raised to 5 MiB (`CUSTOS_GATEWAY_BODY_MAX_BYTES_DEFAULT` /
  `CUSTOS_GATEWAY_BODY_MAX_BYTES_PUBLISH`).
- **Content type** — a write with a body that is not `application/json` is
  rejected with `415 unsupported-media-type`.

---

## Error envelope (RFC 7807)

Every **gateway-originated** failure is rendered as a uniform
`application/problem+json` body, so SDK clients never see FastAPI's default error
shape. The envelope mirrors RFC 7807 §3.1 with three extensions:

- `code` — the structured taxonomy string clients branch on. The `type` URI MAY
  change without bumping `code`, so clients SHOULD treat the URI as opaque.
- `correlationId` — the request correlation id (also returned in the
  `x-correlation-id` response header).
- `auditEventId` — populated for outcomes the Auth Service audited
  (`permission-denied`), absent otherwise.

The `type` URI is the prefix `https://custos.dev/errors/` with the `code`
appended verbatim. A representative Problem document:

<!-- doctest: ProblemDetail -->
```json
{
  "type": "https://custos.dev/errors/permission-denied",
  "title": "Permission denied",
  "status": 403,
  "detail": "The caller is not permitted to perform 'workflow:execute' in workspace 'ws-42'.",
  "instance": "/v1/workspaces/ws-42/runs",
  "correlationId": "01HZX9N6Y0K3J2QF4ABCDEF123",
  "code": "permission-denied",
  "auditEventId": "evt-1a2b3c4d"
}
```

The taxonomy is **locked**: a code absent from the table below can never reach
the wire. The closed code → HTTP-status set:

| `code` | HTTP | Meaning |
|---|---|---|
| `invalid-token` | 401 | Missing, malformed, or rejected bearer token |
| `permission-denied` | 403 | Token lacks the route's required permission |
| `workspace-mismatch` | 400 | Path workspace disagrees with the call context or body |
| `idempotency-in-flight` | 409 | A request with this idempotency key is still processing |
| `idempotency-key-reuse` | 409 | The idempotency key was reused with a different payload |
| `rate-limited` | 429 | The per-principal or per-workspace write bucket is exhausted |
| `body-too-large` | 413 | Request body exceeds the route's size cap |
| `unsupported-media-type` | 415 | A write body is not `application/json` |
| `downstream-unavailable` | 503 | The owning component returned `5xx` or was unreachable |
| `webhook-route-not-found` | 404 | The webhook target instance is unknown downstream |
| `device-code-expired` | 400 | The device-code session has expired |
| `gateway-startup-permission-missing` | 500 | A route declares a permission the Auth Service does not know (boot-time panic) |

`gateway-startup-permission-missing` is a startup-time refusal-to-boot, not a
served response — it is listed so the taxonomy is complete, but the gateway never
reaches the wire with it.

---

## Route registry

The gateway's public surface is a **declarative** table: one row per external
route, each carrying four cross-cutting attributes — the `required_permission`
the route enforces, whether it `requires an idempotency key`, its
`max_body_bytes` cap, and its `rate_limit_class`. The request is forwarded
verbatim to the owning component, so a gateway route template only needs to
*match* the inbound path; the gateway never rewrites it.

Routes are grouped by the owning downstream component (its Dapr app-id):

| Component | Dapr app-id | Surface |
|---|---|---|
| Auth Service | `custos-auth` | Service accounts, tokens, role bindings |
| Catalog Service | `catalog-service` | Workflows, templates, activity-types, connector-types |
| Workflow Service | `workflow-service` | Run lifecycle (start, read, cancel, steps) |
| Trigger Service | `trigger-service` | Trigger subscription CRUD + manual fire |
| Connector Service | `connector-service` | Connector instance lifecycle + lease admin |
| Observability & Audit | `observability-audit-service` | Logs, metrics, audit reads |

Each route declares exactly one Auth Service permission. The distinct permissions
the registry enforces across all routes:

- `admin:service-account`, `admin:role-binding`, `admin:connector`
- `catalog:workflows:read`, `catalog:workflows:write`
- `catalog:templates:read`, `catalog:templates:write`
- `catalog:activity-types:read`, `catalog:activity-types:write`
- `catalog:connector-types:read`, `catalog:connector-types:write`
- `workflow:execute`
- `run:read`, `run:cancel`
- `trigger:subscriptions:read`, `trigger:subscriptions:write`,
  `trigger:subscriptions:delete`, `trigger:subscriptions:fire`
- `connector:read`
- `logs:read`, `metrics:read`, `audit:read`

A representative slice of the run-lifecycle routes (Workflow Service):

| Method | Path | Permission | Idempotency key | Rate class |
|---|---|---|---|---|
| `POST` | `/v1/workspaces/{workspaceId}/runs` | `workflow:execute` | required | write |
| `GET` | `/v1/workspaces/{workspaceId}/runs` | `run:read` | — | read |
| `GET` | `/v1/workspaces/{workspaceId}/runs/{runId}` | `run:read` | — | read |
| `POST` | `/v1/workspaces/{workspaceId}/runs/{runId}:cancel` | `run:cancel` | required | write |

Write methods default to requiring an idempotency key and the `write` rate
class; reads bill the `read` class and bypass idempotency. The
workflow/template *publish* routes raise the body cap to 5 MiB; every other route
uses the 1 MiB default.

Single-permission **management** routes (service accounts, tokens, role bindings)
are in the M1 registry. Auth-management routes that authorize on an OR of two
permissions, or on authentication alone, are deferred. Internal Dapr RPC routes
(`/rpc/*`, `/internal/v1/*`), the anonymous webhook pass-through, and the
device-code flow are intentionally **outside** the registry.

---

## Webhook ingress

`POST /v1/webhooks/{connectorInstanceId}` is the gateway's single **anonymous**
ingress. Unlike every other route it carries no bearer, resolves no workspace,
and mints no call context — signature verification belongs to the Trigger Service
and the per-connector plugin, not the gateway. The gateway's job is narrow:

1. Enforce the 1 MiB body-size cap (the body is never inspected or altered).
2. Forward the request to the Trigger Service over Dapr — body verbatim, headers
   minus `Authorization`, `Host`, and any caller-supplied `x-custos-callctx`
   (dropping those prevents a caller from smuggling an authenticated context onto
   an intentionally anonymous route), plus the caller's source IP appended to
   `X-Forwarded-For`.
3. Generate or propagate a correlation id so the anonymous hop stays traceable.

An unknown `{connectorInstanceId}` is forwarded as-is; the Trigger Service
surfaces `404 webhook-route-not-found`, which passes back through the response
shaper unchanged.

---

## Device-code login flow

The CLI authenticates via the OIDC device-code flow (RFC 8628). The gateway owns
the session-state endpoints and relays the actual OIDC token verification to the
Auth Service / configured issuer:

| Method | Path | Purpose |
|---|---|---|
| `POST` | `/v1/auth/login/device` | Start a device-code session |
| `POST` | `/v1/auth/login/device/{deviceCode}/poll` | CLI polls for completion |
| `GET` | `/v1/auth/login/device/{userCode}` | Browser landing page |

All three are **auth-bootstrap** routes: they are hit *before* the caller holds a
bearer token, so they bypass authentication and mint no call context.

> **M1 behavior.** The flow is gated on a configured OIDC issuer
> (`CUSTOS_GATEWAY_OIDC_DEFAULT_ISSUER`). M1 ships with OIDC **disabled**, so
> every device-code endpoint returns `503 downstream-unavailable` — the issuer
> the gateway relays to is not available. The persistence seam and the TTL config
> (`CUSTOS_GATEWAY_DEVICE_CODE_TTL`) are in place so M3 can activate the flow by
> configuring an issuer, without changing routing or middleware order.

---

## Configuration

The gateway is configured exclusively through environment variables, plus the
Dapr sidecar coordinates used to reach downstream components and the Auth Service:

| Variable | Default | Purpose |
|---|---|---|
| `CUSTOS_GATEWAY_LISTEN_ADDR` | `:8443` | TLS listen address |
| `CUSTOS_GATEWAY_TLS_CERT_REF` | _(required)_ | Dapr secret reference for the TLS certificate |
| `CUSTOS_GATEWAY_TLS_KEY_REF` | _(required)_ | Dapr secret reference for the TLS private key |
| `CUSTOS_GATEWAY_CORS_ALLOWED_ORIGINS` | _(required)_ | JSON array of allowed UI origins (no wildcard) |
| `CUSTOS_GATEWAY_BODY_MAX_BYTES_DEFAULT` | `1048576` | Default request-body size cap (bytes) |
| `CUSTOS_GATEWAY_BODY_MAX_BYTES_PUBLISH` | `5242880` | Body cap for workflow/template publish routes |
| `CUSTOS_GATEWAY_RATE_LIMIT_PRINCIPAL_WRITES_RPS` | `20` | Per-principal write rate (rps) |
| `CUSTOS_GATEWAY_RATE_LIMIT_PRINCIPAL_WRITES_BURST` | `40` | Per-principal write burst |
| `CUSTOS_GATEWAY_RATE_LIMIT_WORKSPACE_WRITES_RPS` | `200` | Per-workspace write rate (rps) |
| `CUSTOS_GATEWAY_RATE_LIMIT_WORKSPACE_WRITES_BURST` | `400` | Per-workspace write burst |
| `CUSTOS_GATEWAY_IDEMPOTENCY_TTL` | `24h` | Idempotency-record lifetime |
| `CUSTOS_GATEWAY_DEVICE_CODE_TTL` | `15m` | Device-code session lifetime |
| `CUSTOS_GATEWAY_DEVICE_CODE_POLL_INTERVAL` | `5s` | Device-code poll-interval hint returned to the CLI |
| `CUSTOS_GATEWAY_OIDC_DEFAULT_ISSUER` | _(empty)_ | Default OIDC issuer; empty disables the device-code flow |
| `DAPR_HTTP_HOST` | `127.0.0.1` | Dapr sidecar HTTP host |
| `DAPR_HTTP_PORT` | `3500` | Dapr sidecar HTTP port |
| `ENVIRONMENT` | `development` | Operational environment tag |

Durations accept a unit suffix (`24h`, `15m`, `5s`) or a bare second count. A
missing required variable or any malformed value fails fast at startup.
