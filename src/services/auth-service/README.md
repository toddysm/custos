# custos-auth-service

Custos Auth Service (COMP-002).

Auth Service owns identity issuance, identity verification, and authorization
decisions for every human and programmatic caller of Custos. It is the single
source of truth for "may principal P perform action A on resource R in
workspace W?" and owns the internal signed call-context contract that every
other component verifies on inbound RPCs.

## Status

AS-IMPL-001 (#236, Phase A) — scaffold only. The FastAPI app currently exposes
`/healthz` and `/readyz` returning 200 unconditionally; no managers, no
authorization, no persistence. Subsequent issues will land:

- AS-IMPL-002 (#237) — Helm subchart.
- AS-IMPL-003/004 (#238/#239) — SPL `AuthStoreProvider` + Postgres migrations
  + schema-revision startup gate.
- AS-IMPL-005..010 (#240–#245) — Tenancy + principal model + permission/role
  registry + RoleBinding store.
- AS-IMPL-011/012 (#246/#247) — `authorize()` decision engine + cache.
- AS-IMPL-013..016 (#248–#251) — Service tokens (REQ-035).
- AS-IMPL-017..019 (#252–#254) — Internal signed call-context (EdDSA JWT)
  + JWKS endpoint + rotation + verifier helper library.
- AS-IMPL-020..023 (#255–#258, **M3**) — OIDC verifier + GitHub preset +
  Entra preset + provisioning policy.
- AS-IMPL-024..026 (#259–#261) — Public REST/RPC surface + observability.
- AS-IMPL-027..029 (#262–#264) — Verification + docs.
- AS-IMPL-030/031 (#265/#266) — Cross-component follow-ups.

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

The app currently has no environment variables (the configuration table in
the design lands incrementally across AS-IMPL-002, AS-IMPL-004, AS-IMPL-017,
AS-IMPL-018, AS-IMPL-020).

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
