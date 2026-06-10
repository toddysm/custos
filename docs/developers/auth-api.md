# Developer Guide: Auth API

Last Updated: 2026-05-28

The **Auth Service** is Custos's authoritative provider of identity,
authorization, and call-context signing. This page documents the
REST and RPC surfaces you will integrate against when building
gateway clients, internal services, or platform tooling. For the
broader design rationale and component boundaries see
[`design/components/auth-service/design.md`](../../design/components/auth-service/design.md).

## Contents

- [Base URL and authentication](#base-url-and-authentication)
- [Call-context header](#call-context-header)
- [Error envelope and error taxonomy](#error-envelope-and-error-taxonomy)
- [Permission registry](#permission-registry)
- [Built-in roles](#built-in-roles)
- [Tenants](#tenants)
- [Workspaces](#workspaces)
- [Service accounts](#service-accounts)
- [Service tokens](#service-tokens)
- [Role bindings](#role-bindings)
- [Roles and permissions catalog](#roles-and-permissions-catalog)
- [Principals](#principals)
- [Auth: token verify and OIDC callback](#auth-token-verify-and-oidc-callback)
- [Authz: verify-and-authorize](#authz-verify-and-authorize)
- [JWKS and `.well-known`](#jwks-and-well-known)
- [Internal RPC surface](#internal-rpc-surface)
- [Worked examples](#worked-examples)

---

## Base URL and authentication

All endpoints are served by the auth-service deployment and routed
through the API gateway:

```sh
https://<gateway>/auth/...
```

Within the cluster, the service exposes the paths below at port 8080.
This guide uses the in-cluster paths.

Two complementary mechanisms front every call:

1. **Bearer tokens (`Authorization: Bearer <service-token>`)** —
   used only by bootstrap endpoints (`POST /v1/auth/verify`,
   `POST /v1/authz/verify-and-authorize`, and their RPC peers).
   These endpoints accept a raw service-token and return a
   `PrincipalResponse` envelope. Everywhere else the auth service
   refuses bearer tokens.
2. **Call-context header (`x-custos-callctx`)** — used by every
   admin endpoint. The gateway mints a signed call-context after a
   successful verify and forwards it to upstream services. The auth
   service is itself an upstream of the gateway: it enforces
   permissions out of the call-context rather than re-verifying a
   bearer on every request.

## Call-context header

The header name is the constant `x-custos-callctx`. The payload is a
short-lived EdDSA-signed JWT in production; in development the
auth-service runs a __dev-shim__ mode that accepts a plain JSON
object for ergonomics (this mode is disabled when
`CUSTOS_AUTH_CALLCTX_VERIFIER_URL` is set).

The dev-shim JSON shape, equivalent to the production JWT claim set,
is:

```yaml
principal_id: "user-1"
tenant_id: "t-acme"      # optional; may be null
workspace_id: "ws-1"     # optional; may be null
permissions:             # optional; coerced to set on the server
  - catalog:workflows:read
  - audit:read
iat: 1717000000          # optional unix seconds
exp: 1717003600          # optional unix seconds
```

In production every field except the optional `iat` / `exp` is set
inside the JWT and the `aud` claim is `custos.internal`, the `iss`
claim is `custos-auth`. Components verify the JWT against the
[`JWKS`](#jwks-and-well-known) endpoint at startup and on a refresh
interval. They do not call the auth-service per request.

**Bypass list.** The auth service skips the call-context check for a
small, deliberately enumerated set of paths. These either bootstrap
identity (no call-context exists yet) or expose public infrastructure:

| Path | Reason |
|---|---|
| `/healthz`, `/readyz` | Kubernetes probes |
| `/v1/auth/verify` | Bootstrap: verify a bearer to obtain a Principal |
| `/v1/authz/verify-and-authorize` | Gateway hot-path: verify + authorize in one shot |
| `/v1/auth/login/oidc/callback` | External OIDC redirect |
| `/.well-known/jwks.json` | Public-key endpoint |
| `/v1/permissions` | Public permission registry — the gateway cross-checks its route grants at startup, before it holds a call-context |
| `/openapi.json`, `/docs`, `/docs/oauth2-redirect`, `/redoc` | OpenAPI / docs |
| `/rpc/authn.verifyToken` | Bootstrap RPC: verify a bearer |
| `/rpc/authz.verifyAndAuthorize` | Bootstrap RPC: hot-path |
| `/rpc/callctx.sign` | Bootstrap RPC: mint a call-context |

`POST /rpc/authz.authorize` and `POST /rpc/callctx.verify` are NOT
bypassed: their callers already hold a valid call-context.

---

## Error envelope and error taxonomy

Every non-`2xx` response uses a uniform envelope:

```json
{
  "error": {
    "code": "permission_denied",
    "detail": "principal lacks 'admin:role-binding' on workspace ws-1",
    "issues": []
  }
}
```

`code` is always present and machine-readable. `detail` is a
human-readable summary. `issues` is an __optional__ array that
appears only on `request_validation_failed` (Pydantic body
validation) and carries one record per offending field:
`{"loc": [...], "msg": "...", "type": "..."}`.

The full set of codes emitted by the auth service is:

| Code | HTTP | Emitted by |
|---|---|---|
| `not_found` | 404 | Most endpoints; cross-tenant reads collapse to 404 (existence-hiding) |
| `conflict` | 409 | `POST /v1/tenants`, `POST /v1/tenants/{t}/workspaces`, `POST /v1/service-accounts` on duplicate identifier |
| `invalid_request` | 400 | Business-rule violations not caught by Pydantic (e.g. service-account creation outside a workspace, workspace under a disabled tenant) |
| `invalid_role_scope` | 400 | `POST /v1/workspaces/{ws}/role-bindings` with a role that cannot bind at workspace scope |
| `request_validation_failed` | 422 | Pydantic body validation failure; `issues[]` lists every offending field |
| `unauthenticated` | 401 | `POST /v1/auth/verify`, `POST /v1/authz/verify-and-authorize`, `POST /rpc/authz.verifyAndAuthorize` when the bearer cannot be authenticated |
| `permission_denied` | 403 | Any admin endpoint whose `require_permission(...)` dependency rejects the call-context |
| `callctx_missing` | 401 | Middleware: `x-custos-callctx` absent on a non-bypassed path |
| `callctx_invalid` | 400 | Middleware: dev-shim JSON failed validation |
| `callctx_malformed` | 400 | Middleware: header value not parseable as JSON |
| `oidc_verification_failed` | 401 | `POST /v1/auth/login/oidc/callback` — ID-token verification failed |
| `oidc_exchange_failed` | 502 | `POST /v1/auth/login/oidc/callback` — provider rejected code exchange |
| `oidc_not_enabled` | 503 | `POST /v1/auth/login/oidc/callback` — `CUSTOS_AUTH_OIDC_ENABLED=false` |
| `oidc_not_configured` | 503 | `POST /v1/auth/login/oidc/callback` — no issuers configured |
| `oidc_not_implemented` | 503 | `POST /v1/auth/login/oidc/callback` — verifier/provisioner not wired |
| `not_implemented` | 501 | `POST /v1/roles` — custom roles are M2+ |
| `http_error` | varies | Defensive wrapper for raw `HTTPException` |

`POST /rpc/callctx.verify` does __not__ return error envelopes; it
returns HTTP 200 with a structured `{valid: bool, reason: "..."}`
payload. The closed-set reason codes are: `malformed`, `unknown_kid`,
`bad_signature`, `expired`, `wrong_audience`, `wrong_issuer`.

---

## Permission registry

Permissions are flat strings of the form `<resource>:<verb>` (with
`admin:<resource>` for management verbs). The complete platform
registry is:

| Permission | Owning service | Granted by |
|---|---|---|
| `admin:service-account` | auth-service | `workspace.admin`, `platform.admin` |
| `admin:role-binding` | auth-service | `workspace.admin`, `tenant.admin`, `platform.admin` |
| `admin:workspace` | auth-service | `tenant.admin`, `platform.admin` |
| `catalog:workflows:read` | catalog-service | `workspace.viewer/author/operator/admin`, `platform.admin` |
| `catalog:workflows:write` | catalog-service | `workspace.author/operator/admin`, `platform.admin` |
| `catalog:templates:read` | catalog-service | `workspace.viewer/author/operator/admin`, `platform.admin` |
| `catalog:templates:write` | catalog-service | `workspace.author/operator/admin`, `platform.admin` |
| `catalog:activity-types:read` | catalog-service | `workspace.viewer/author/operator/admin`, `platform.admin` |
| `catalog:activity-types:write` | catalog-service | `workspace.author/operator/admin`, `platform.admin` |
| `catalog:connector-types:read` | catalog-service | `workspace.viewer/author/operator/admin`, `platform.admin` |
| `catalog:connector-types:write` | catalog-service | `workspace.author/operator/admin`, `platform.admin` |
| `workflow:execute` | workflow-service | `workspace.author/operator/admin`, `platform.admin` |
| `run:read` | workflow-service | `workspace.viewer/author/operator/admin`, `platform.admin` |
| `run:cancel` | workflow-service | `workspace.author/operator/admin`, `platform.admin` |
| `connector:read` | connector-service | `workspace.viewer/author/operator/admin`, `platform.admin` |
| `admin:connector` | connector-service | `workspace.operator/admin`, `platform.admin` |
| `trigger:subscriptions:read` | trigger-service | `workspace.operator/admin`, `platform.admin` |
| `trigger:subscriptions:write` | trigger-service | `workspace.operator/admin`, `platform.admin` |
| `trigger:subscriptions:delete` | trigger-service | `workspace.operator/admin`, `platform.admin` |
| `trigger:subscriptions:fire` | trigger-service | `workspace.operator/admin`, `platform.admin` |
| `audit:read` | auth + observability-audit | `workspace.viewer/author/operator/admin`, `tenant.admin`, `platform.admin` |
| `logs:read` | observability-audit-service | `workspace.viewer/author/operator/admin`, `platform.admin` |
| `metrics:read` | observability-audit-service | `workspace.viewer/author/operator/admin`, `platform.admin` |

The live registry is exposed read-only via `GET /v1/permissions`.

---

## Built-in roles

| Role id | Scope | Grants (cumulative) |
|---|---|---|
| `role:workspace.viewer` | workspace | `catalog:workflows:read`, `catalog:templates:read`, `catalog:activity-types:read`, `catalog:connector-types:read`, `connector:read`, `audit:read`, `run:read`, `logs:read`, `metrics:read` |
| `role:workspace.author` | workspace | viewer + `catalog:workflows:write`, `catalog:templates:write`, `catalog:activity-types:write`, `catalog:connector-types:write`, `workflow:execute`, `run:cancel` |
| `role:workspace.operator` | workspace | author + `admin:connector`, `trigger:subscriptions:read/write/delete/fire` |
| `role:workspace.admin` | workspace | operator + `admin:role-binding`, `admin:service-account` |
| `role:tenant.admin` | tenant | `admin:workspace`, `admin:role-binding` |
| `role:platform.admin` | platform | **all permissions at every scope** (authorize engine short-circuits) |

`role:platform.admin` is a blanket-allow role: the permission tuple
is intentionally empty and the authorization engine treats any
binding of it as "allow every permission at every scope". Custom
roles are not supported in M1 (`POST /v1/roles` returns `501
not_implemented`); the catalog is exposed read-only via
`GET /v1/roles`.

---

## Tenants

### Create a tenant

```sh
POST /v1/tenants
```

Permissions: `platform.admin`

Request body:

```yaml
tenant_id: t-acme
display_name: Acme Inc.
```

- `tenant_id` is a stable, client-supplied identifier, 1-120 chars.
- `display_name` is 1-200 chars, freeform.

Response `201 Created`:

```yaml
tenant_id: t-acme
display_name: Acme Inc.
disabled_at: null
created_at: "2026-05-27T18:00:00Z"
```

Errors: `409 conflict` if the `tenant_id` already exists.

### List tenants

```sh
GET /v1/tenants
```

Permissions: `platform.admin` or `tenant.admin`.

Returns every tenant for `platform.admin`; for `tenant.admin`
returns only the caller's own tenant (an empty list if the
call-context lacks `tenant_id`).

Response `200 OK`:

```yaml
tenants:
  - tenant_id: t-acme
    display_name: Acme Inc.
    disabled_at: null
    created_at: "2026-05-27T18:00:00Z"
```

---

## Workspaces

### Create a workspace

```ini
POST /v1/tenants/{tenant_id}/workspaces
```

Permissions: `platform.admin` or `tenant.admin` (`tenant.admin` may
only create inside their own tenant — cross-tenant calls collapse to
`404 not_found`).

Request body:

```yaml
workspace_id: ws-payments
display_name: Payments
```

Response `201 Created`:

```yaml
workspace_id: ws-payments
tenant_id: t-acme
display_name: Payments
disabled_at: null
created_at: "2026-05-27T18:00:00Z"
```

Errors: `404 not_found` if the tenant does not exist or is
cross-tenant; `400 invalid_request` if the tenant is disabled;
`409 conflict` if `workspace_id` is already taken inside the tenant.

### List workspaces

```sh
GET /v1/workspaces
```

Permissions: any authenticated principal.

Returns workspaces visible to the caller:

- `platform.admin` — all workspaces, every tenant.
- `tenant.admin` — every workspace inside the caller's
   `ctx.tenant_id`.
- otherwise — only the caller's `ctx.workspace_id` (if set).

### Get a workspace

```sh
GET /v1/workspaces/{workspace_id}
```

Returns `404 not_found` on cross-tenant reads (existence-hiding).

---

## Service accounts

### Create a service account

```sh
POST /v1/service-accounts
```

Permissions: `admin:service-account` (typically held by
`role:workspace.admin`). The service account is created in the
caller's current workspace, taken from `ctx.workspace_id`.

Request body:

```yaml
principal_id: sa-ci-publisher
display_name: CI publisher
```

Response `201 Created`:

```yaml
kind: serviceAccount
principal_id: sa-ci-publisher
workspace_id: ws-payments
display_name: CI publisher
disabled_at: null
disabled_reason: null
created_at: "2026-05-27T18:00:00Z"
```

Errors: `400 invalid_request` if the call-context has no
`workspace_id`; `409 conflict` if the `principal_id` already exists
in the workspace.

Audit: emits `principal.created`.

---

## Service tokens

Service tokens are opaque bearer credentials minted for a service
account. The plaintext value is **returned exactly once**; only the
salted hash is persisted.

### Mint a token

```sh
POST /v1/service-accounts/{principal_id}/tokens
```

Permissions: `admin:service-account`.

Request body:

```yaml
ttl_seconds: 86400      # optional; null → use default; 1..10y
```

Response `201 Created`:

```yaml
token_id: tk-01HZX...
service_account_id: sa-ci-publisher
token: cst_eyJ...        # plaintext, shown once
issued_at: "2026-05-27T18:00:00Z"
expires_at: "2026-05-28T18:00:00Z"
```

Errors: `404 not_found` if the service account does not exist or is
cross-workspace; `400 request_validation_failed` if `ttl_seconds` is
outside the supported range.

Audit: emits `token.issued`.

### List tokens for a service account

```sh
GET /v1/service-accounts/{principal_id}/tokens
```

Permissions: `admin:service-account`.

Response `200 OK`:

```yaml
tokens:
  - token_id: tk-01HZX...
    service_account_id: sa-ci-publisher
    issued_at: "2026-05-27T18:00:00Z"
    expires_at: "2026-05-28T18:00:00Z"
    revoked_at: null
```

### Revoke a single token

```sh
DELETE /v1/tokens/{token_id}
```

Permissions: `admin:service-account`.

Request body:

```yaml
reason: rotated by CI
```

Response: `204 No Content`. Idempotent — already-revoked tokens
return `204` without re-emitting an audit row.

Audit: emits `token.revoked`.

### Revoke all tokens for a service account

```ini
DELETE /v1/service-accounts/{principal_id}/tokens
```

Permissions: `admin:service-account`.

Request body:

```yaml
reason: service account disabled
```

Response `200 OK`:

```yaml
revoked_count: 4
```

Already-revoked tokens are skipped (not counted, no audit row).

---

## Role bindings

A **role binding** grants one role to one principal at one scope.
Phase D supports workspace-scoped bindings only; tenant and platform
scopes are M2.

### Create a binding

```sh
POST /v1/workspaces/{workspace_id}/role-bindings
```

Permissions: `admin:role-binding`. The caller's call-context must
include a `tenant_id` matching the workspace; the workspace-scope
resolver collapses cross-tenant requests to `404`.

Request body:

```yaml
principal_id: sa-ci-publisher
role_id: role:workspace.author
```

Response `201 Created`:

```yaml
binding_id: rb-01HZX...
principal_id: sa-ci-publisher
role_id: role:workspace.author
scope_kind: workspace
scope_id: ws-payments
bound_at: "2026-05-27T18:00:00Z"
bound_by: user-1
```

Errors:

- `400 invalid_role_scope` if the role is not allowed at workspace
   scope (e.g. `role:platform.admin`).
- `404 not_found` if the workspace does not exist or is
   cross-tenant.

Audit: emits `role-binding.granted`. The service also publishes a
binding-changed event so other services can invalidate their authz
cache.

### Delete a binding

```sh
DELETE /v1/workspaces/{workspace_id}/role-bindings/{binding_id}
```

Permissions: `admin:role-binding`.

Response: `204 No Content`. `404 not_found` for unknown or
cross-tenant bindings.

Audit: emits `role-binding.revoked`.

---

## Roles and permissions catalog

```sh
GET /v1/roles
GET /v1/permissions
```

Both are read-only and visible to any authenticated principal. They
expose the [built-in role](#built-in-roles) and
[permission registry](#permission-registry) tables above. `POST
/v1/roles` is reserved for M2+ custom roles and returns `501
not_implemented`.

---

## Principals

### Get the calling principal

```sh
GET /v1/principals/me
```

Returns the caller's own principal record, derived from the
call-context. Useful for UIs that need to render the active identity
without an extra lookup.

Response `200 OK`:

```yaml
kind: serviceAccount
principal_id: sa-ci-publisher
workspace_id: ws-payments
display_name: CI publisher
disabled_at: null
disabled_reason: null
created_at: "2026-05-27T18:00:00Z"
```

### Disable a principal

```sh
POST /v1/principals/{principal_id}/disable
```

Permissions: `platform.admin` or `tenant.admin`.

Request body:

```yaml
reason: offboarding
```

Soft-disables a user or service account. Cross-tenant attempts
collapse to `404 not_found`. Audit: emits `principal.disabled`.

---

## Auth: token verify and OIDC callback

### Verify a service token

```sh
POST /v1/auth/verify
```

Permissions: none (bypassed).

Request body:

```yaml
token: cst_eyJ...
```

Returns `200 OK` with a `PrincipalResponse` envelope on success and
`401 unauthenticated` on any failure (the audit row carries the
disambiguating reason — `unknown_token`, `revoked`, `expired`,
`principal_disabled`, ...).

This is the bootstrap endpoint every component calls **before**
having a call-context. Once a principal is returned, the gateway
mints a call-context via [`callctx.sign`](#callctxsign) and
propagates it.

### OIDC callback

```sh
POST /v1/auth/login/oidc/callback
```

Permissions: none (bypassed).

Request body:

```yaml
issuer: corp-okta
code: auth-code-from-idp
state: csrf-state-cookie
redirect_uri: https://gateway.example.com/oidc/callback   # optional
```

Server-side flow:

1. Resolve the issuer by id (`CUSTOS_AUTH_OIDC_ISSUERS`).
2. Exchange `code` at the issuer's `token_endpoint`.
3. Verify the ID-token via the issuer's JWKS, enforcing
   `iss`/`aud`/`exp`/`nonce`.
4. Provision: link or create a User principal with zero bindings.
5. Return the `PrincipalResponse` plus `newly_provisioned: bool`.

Errors are surfaced with the `oidc_*` codes listed in the
[error taxonomy](#error-envelope-and-error-taxonomy).

Audit: emits `authn.success` / `authn.failure` with
`authentication_type=oidc`.

---

## Authz: verify-and-authorize

```sh
POST /v1/authz/verify-and-authorize
```

Permissions: none (bypassed).

Request body:

```yaml
token: cst_eyJ...
permission: catalog:workflows:read
workspace_id: ws-payments
```

Response `200 OK`:

```yaml
principal_id: sa-ci-publisher
allowed: true
reason: ok
audit_event_id: ae-01HZX...
```

This is the API gateway's hot-path: one round-trip composes the
`verify` and `authorize` primitives. The decision is encoded in the
body (`allowed: bool`) — the HTTP status stays `200` on a deny so
clients can distinguish failure modes (`401 unauthenticated` for a
bad token vs `200 allowed=false` for a permission denial). The
audit row carries the underlying verify and authorize primitives
separately.

---

## JWKS and `.well-known`

```sh
GET /.well-known/jwks.json
```

Permissions: none (public).

Returns the active Ed25519 verification key set as an RFC 7517 JWK
Set:

```json
{
  "keys": [
    {
      "kty": "OKP",
      "crv": "Ed25519",
      "alg": "EdDSA",
      "use": "sig",
      "kid": "a1b2c3d4e5f60718",
      "x": "VGhpcyBpcyBub3QgYSByZWFsIGtleQ"
    }
  ]
}
```

- The active key is listed first.
- Keys retired within the rotation overlap window are also included.
- `kid` is a 16-char hex prefix of `SHA-256(raw_public_key)`.
- `Cache-Control: public, max-age=<half rotation period>`.

Every Custos component fetches this endpoint and verifies signed
call-contexts locally; the auth-service is **not** on the hot path
of call-context verification.

---

## Internal RPC surface

All RPC methods are mounted under `/rpc/` and follow the convention
`POST /rpc/<namespace>.<method>`. Dapr service-invocation forwards
them as `POST /v1.0/invoke/custos-auth/method/<namespace>.<method>`.

### `authn.verifyToken`

```sh
POST /rpc/authn.verifyToken
```

Bypassed (bootstrap). Verifies a service token and returns the
principal envelope, or `principal: null` on any failure (the RPC
flavour does **not** raise `401` — failure is signalled by `null`).

```yaml
# Request
token: cst_eyJ...

# Response
principal:
  kind: serviceAccount
  principal_id: sa-ci-publisher
  workspace_id: ws-payments
  display_name: CI publisher
  disabled_at: null
  disabled_reason: null
  created_at: "2026-05-27T18:00:00Z"
```

### `authz.authorize`

```sh
POST /rpc/authz.authorize
```

Requires a valid call-context (post-bootstrap). The caller has
already authenticated the subject elsewhere and supplies the
`principal_id` explicitly. `caller_component` is recorded in the
audit row.

```yaml
# Request
principal_id: sa-ci-publisher
permission: catalog:workflows:read
workspace_id: ws-payments
caller_component: workflow-service

# Response
allowed: true
reason: ok
audit_event_id: ae-01HZX...
```

### `authz.verifyAndAuthorize`

```sh
POST /rpc/authz.verifyAndAuthorize
```

Bypassed (bootstrap). RPC peer of
`POST /v1/authz/verify-and-authorize`; identical request and
response shape. Provided so internal callers wired through Dapr do
not need to traverse the public REST surface.

### `callctx.sign`

```sh
POST /rpc/callctx.sign
```

Bypassed (bootstrap). Mints an EdDSA-signed call-context JWT.

```yaml
# Request
principal_id: sa-ci-publisher
workspace_id: ws-payments   # optional; null → platform-global ctx
caller_component: api-gateway
ttl_seconds: 300            # optional; default 300, max 86400
permissions: [artifacts.publish, artifacts.read]  # optional; embedded grants (max 256 entries, each 1..128 chars)
audience: null              # optional; overrides aud; default "custos.internal"

# Response
token: eyJhbGciOiJFZERTQSIsImtpZCI6ImExYjJjMyJ9.eyJqdGkiOiJjLTAxIn0.sig
kid: a1b2c3d4e5f60718
jti: c-01HZX...
iat: 1717003600
exp: 1717003900
```

JWT claims (decoded):

```yaml
actingPrincipalId: sa-ci-publisher
workspaceId: ws-payments
callerComponent: api-gateway
iat: 1717003600
exp: 1717003900
jti: c-01HZX...
aud: custos.internal
iss: custos-auth
# permissions is OMITTED entirely when the mint request supplied
# none. When supplied, it is preserved verbatim (order + duplicates)
# on the wire; the verifier collapses to a set on receipt.
permissions:
  - catalog:workflows:read
  - catalog:workflows:write
```

`permissions` is the AS-IMPL-030 "fat call-context" enabler: the API
gateway pre-computes the effective grants for the principal +
workspace at mint time and embeds them in the JWT so downstream
components can authorize a request locally without re-calling
`authz.authorize`. Empty lists are treated identically to omitting
the field — the claim is dropped from the JWT.

`audience` is the per-mint audience override. When the API gateway
fans a request out to a specific component (e.g. catalog-service),
it sets `audience: custos.catalog` so the resulting JWT is rejected
by every other component. Omitting `audience` (or passing `null`)
falls back to the signer's configured default
(`custos.internal`). Empty strings are rejected with `422`.

M1 trust model: only the API gateway is permitted to invoke
`callctx.sign`. M2 will pin the dependency to a `callctx:sign`
permission.

### `callctx.verify`

```sh
POST /rpc/callctx.verify
```

Requires a valid call-context. Verifies a call-context JWT
in-process against the local KeyRing. Returns `200` regardless of
outcome; the verdict is in the body. External components do not call
this on the hot path — they fetch [`JWKS`](#jwks-and-well-known) and
verify locally.

```yaml
# Request
token: eyJhbGciOiJFZERTQSIsImtpZCI6ImExYjJjMyJ9.eyJqdGkiOiJjLTAxIn0.sig
audience: null              # optional; defaults to "custos.internal"

# Response (success)
valid: true
reason: ""
acting_principal_id: sa-ci-publisher
workspace_id: ws-payments
caller_component: api-gateway
iat: 1717003600
exp: 1717003900
kid: a1b2c3d4e5f60718
jti: c-01HZX...
permissions:
  - catalog:workflows:read
  - catalog:workflows:write
```

`audience` lets the caller verify a token that was minted for a
specific component (e.g. `custos.catalog`). When omitted, the
default `custos.internal` audience is enforced. Empty strings are
rejected.

`permissions` echoes the embedded grants from the JWT. The field is
always present in success responses: it is an empty list when the
mint request did not embed any. A malformed `permissions` claim on
the wire (non-list, or any entry that is not a non-empty string)
fails verification with `reason: "malformed"`.

Closed-set reason codes on failure: `malformed`, `unknown_kid`,
`bad_signature`, `expired`, `wrong_audience`, `wrong_issuer`.

Audit: emits `call-context.invalid` on a negative outcome.

---

## Worked examples

### Example 1: Service account mint + use

This example creates a service account in `ws-payments`, mints a
token, and exercises it against `POST /v1/auth/verify`. The call
sequence assumes the operator's call-context already grants
`admin:service-account` in `ws-payments`.

**Step 1 — create the service account.**

```yaml
# POST /v1/service-accounts
# x-custos-callctx: {"principal_id":"user-admin","tenant_id":"t-acme","workspace_id":"ws-payments","permissions":["admin:service-account"]}
principal_id: sa-ci-publisher
display_name: CI publisher
```

The auth-service responds `201 Created` with the canonical
service-account envelope (`kind: serviceAccount`,
`workspace_id: ws-payments`, ...).

**Step 2 — mint a token.**

```yaml
# POST /v1/service-accounts/sa-ci-publisher/tokens
# x-custos-callctx: <same as above>
ttl_seconds: 3600
```

Response (truncated, plaintext returned exactly once):

```yaml
token_id: tk-01HZX...
service_account_id: sa-ci-publisher
token: cst_eyJhbGciOi...
issued_at: "2026-05-27T18:00:00Z"
expires_at: "2026-05-27T19:00:00Z"
```

**Step 3 — verify the token.** No call-context is required; the
endpoint is on the bypass list.

```yaml
# POST /v1/auth/verify
token: cst_eyJhbGciOi...
```

Response: a `PrincipalResponse` envelope identical to the one
returned by `GET /v1/principals/me` once a call-context exists.

### Example 2: Role-binding grant + authorize

This example grants `role:workspace.viewer` to the service account
from Example 1 and then exercises an authorization decision.

__Step 1 — grant the binding.__ The caller's call-context must
include `tenant_id` matching the workspace's tenant; the
workspace-scope resolver collapses cross-tenant requests to `404`.

```yaml
# POST /v1/workspaces/ws-payments/role-bindings
# x-custos-callctx: {"principal_id":"user-admin","tenant_id":"t-acme","workspace_id":"ws-payments","permissions":["admin:role-binding"]}
principal_id: sa-ci-publisher
role_id: role:workspace.viewer
```

Response `201 Created`:

```yaml
binding_id: rb-01HZX...
principal_id: sa-ci-publisher
role_id: role:workspace.viewer
scope_kind: workspace
scope_id: ws-payments
bound_at: "2026-05-27T18:00:00Z"
bound_by: user-admin
```

**Step 2 — authorize via the gateway hot-path.** Bypassed; no
call-context required.

```yaml
# POST /v1/authz/verify-and-authorize
token: cst_eyJhbGciOi...
permission: catalog:workflows:read
workspace_id: ws-payments
```

Response `200 OK` (the binding grants `catalog:workflows:read` via the
`workspace.viewer` role):

```yaml
principal_id: sa-ci-publisher
allowed: true
reason: ok
audit_event_id: ae-01HZX...
```

**Step 3 — authorize a non-granted permission.** Same shape, with
`permission: catalog:workflows:write`. The viewer role does **not** grant
`catalog:workflows:write`, so the response is:

```yaml
principal_id: sa-ci-publisher
allowed: false
reason: permission_not_granted
audit_event_id: ae-01HZY...
```

HTTP status remains `200` — the deny is in the body. `401
unauthenticated` is only returned when the bearer itself fails to
authenticate.

### Example 3: Gateway call-context propagation

This example illustrates the full gateway → service flow: verify a
bearer, mint a signed call-context, propagate it to an upstream
service, and re-verify it there.

**Step 1 — verify the bearer.** The gateway calls the bootstrap RPC
on behalf of the inbound request.

```yaml
# POST /rpc/authn.verifyToken
token: cst_eyJhbGciOi...
```

Response: a `principal` envelope (or `null` on failure).

__Step 2 — mint the call-context.__ The gateway records its own
identity in `caller_component`.

```yaml
# POST /rpc/callctx.sign
principal_id: sa-ci-publisher
workspace_id: ws-payments
caller_component: api-gateway
ttl_seconds: 300
```

Response:

```yaml
token: eyJhbGciOiJFZERTQSIsImtpZCI6ImExYjJjMyJ9.eyJqdGkiOiJjLTAxIn0.sig
kid: a1b2c3d4e5f60718
jti: c-01HZX...
iat: 1717003600
exp: 1717003900
```

**Step 3 — forward to the upstream service.** The gateway sets the
`x-custos-callctx` header to the JWT and proxies the request:

```yaml
# Hypothetical upstream call
method: GET
path: /v1/workspaces/ws-payments/workflows
headers:
  x-custos-callctx: eyJhbGciOiJFZERTQSIsImtpZCI6ImExYjJjMyJ9.eyJqdGkiOiJjLTAxIn0.sig
```

**Step 4 — the upstream verifies locally.** It already fetched
[`/.well-known/jwks.json`](#jwks-and-well-known) at startup and
periodically refreshes. No round-trip to the auth-service is
required on the hot path. If the upstream needs an authoritative
recheck (rare — diagnostics only) it can call `POST
/rpc/callctx.verify`, which returns:

```yaml
valid: true
reason: ""
acting_principal_id: sa-ci-publisher
workspace_id: ws-payments
caller_component: api-gateway
iat: 1717003600
exp: 1717003900
kid: a1b2c3d4e5f60718
jti: c-01HZX...
```

If verification fails, `valid` is `false` and `reason` is one of
the closed-set codes listed under [`callctx.verify`](#callctxverify).
Components must reject the request and return `401
callctx_invalid` to their caller.
