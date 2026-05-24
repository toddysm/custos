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
