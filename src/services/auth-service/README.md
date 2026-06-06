# custos-auth-service

Custos Auth Service (COMP-002).

Auth Service owns identity issuance, identity verification, and authorization
decisions for every human and programmatic caller of Custos. It is the single
source of truth for "may principal P perform action A on resource R in
workspace W?" and owns the internal signed call-context contract that every
other component verifies on inbound RPCs.

## Status

**Implemented** — the `AS-IMPL-000` milestone
([#267](https://github.com/toddysm/custos/issues/267)) is complete; all 31 child
tasks (AS-IMPL-001 … AS-IMPL-031) are merged and the tracking issue is closed.
The service implements the full design: the SPL `AuthStoreProvider` + Postgres
migrations + schema-revision startup gate, the tenant / workspace + principal
(User / ServiceAccount) + OidcIdentity models with management endpoints, the
`permissions.yaml` loader + built-in v1 roles + RoleBinding store with
scope-rule enforcement, the `authorize(principal, permission, workspace)`
decision engine + 60 s decision cache with `custos.auth.binding-changed` pub/sub
invalidation, service-account token mint / verify / revoke / expiry (REQ-035)
with authn caching + immediate revocation eviction, the internal signed
call-context contract (EdDSA JWT signer + Dapr Secrets key resolution + JWKS
endpoint + 7-day rotation + the `callctx.verify` verifier helper shipped to
every component), the OIDC verifier + GitHub + Entra ID presets + zero-binding
provisioning policy, the FastAPI REST surface + Internal RPCs
(`authn.verifyToken` / `authz.authorize` / `authz.verifyAndAuthorize` /
`callctx.sign` / `callctx.verify`), and observability + audit emission. Backed
by a unit + integration suite at the ≥90 % coverage gate (AS-IMPL-027/028) and
developer docs at
[`docs/developers/auth-api.md`](../../../docs/developers/auth-api.md)
(AS-IMPL-029).

Tracking issue: [#267](https://github.com/toddysm/custos/issues/267) (AS-IMPL-000).

Design reference:
[`design/components/auth-service/design.md`](../../../design/components/auth-service/design.md).

## Local development

```bash
cd src/services/auth-service
python3 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
pytest -q
```

Run the FastAPI app directly (uvicorn, dev mode):

```bash
custos-auth-service
# or
uvicorn custos_auth:create_app --factory --reload
```

The app is configured via environment variables. For the current, authoritative
set of supported settings, see `custos_auth/settings.py` and the configuration
table in
[`design/components/auth-service/design.md`](../../../design/components/auth-service/design.md).
This includes settings such as `CUSTOS_AUTH_STORE_DSN`,
`CUSTOS_AUTH_METADATA_STORE_DSN`, and `CUSTOS_AUTH_OIDC_ENABLED`.

## Internal RPC surface (AS-IMPL-025)

Every other Custos component invokes auth-service through Dapr
service-invocation using the **app-id `custos-auth`**. Dapr forwards
`POST /v1.0/invoke/custos-auth/method/{name}` to the app as `POST /{name}`;
we project the dotted RPC method names from the design's "Internal RPC"
table onto an `/rpc/` prefix so the internal surface stays visibly
separate from the public REST surface (`/v1/...`).

| Dapr method name              | App path                          | Purpose                                                                |
| ----------------------------- | --------------------------------- | ---------------------------------------------------------------------- |
| `rpc/authn.verifyToken`       | `POST /rpc/authn.verifyToken`     | Verify a bearer token; returns `{principal: PrincipalResponse | null}` |
| `rpc/authz.authorize`         | `POST /rpc/authz.authorize`       | Decide a permission against a workspace; returns `Decision`            |
| `rpc/authz.verifyAndAuthorize`| `POST /rpc/authz.verifyAndAuthorize` | Composed verify + authorize (also exposed at `/v1/authz/...`)       |
| `rpc/callctx.sign`            | `POST /rpc/callctx.sign`          | Mint a signed call-context JWT (EdDSA / Ed25519)                       |
| `rpc/callctx.verify`          | `POST /rpc/callctx.verify`        | Verify a call-context locally; returns `CallContext | InvalidContext`  |

Bootstrap RPCs (`authn.verifyToken`, `authz.verifyAndAuthorize`,
`callctx.sign`) are bypassed by the call-context middleware — by
definition the caller has no call-context to send yet. The other two RPCs
require a valid call-context header.

External components do not need to call `callctx.verify` on the hot path —
they fetch the JWKS at `/.well-known/jwks.json` once per rotation period
and verify locally via the `custos_callctx` helper library (AS-IMPL-019).
The RPC exists so audit / admin tooling can ask auth-service to render a
verification verdict for an arbitrary token.
