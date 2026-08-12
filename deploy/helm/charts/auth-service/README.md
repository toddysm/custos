# auth-service

Custos Auth Service (COMP-002) component subchart. See
[`design/components/auth-service/design.md`](../../../../design/components/auth-service/design.md)
for the design.

## What this chart ships (AS-IMPL-002, #237)

| Resource | Purpose |
|---|---|
| `Deployment` | Runs the `custos-auth-service` container. Liveness/readiness on `/healthz` + `/readyz`. Dapr sidecar annotations from `dapr.*` values. |
| `Service` | ClusterIP, port `8080`. |
| `ConfigMap` | Non-secret `CUSTOS_AUTH_*` env vars from `design.md § Configuration`. Note that `CUSTOS_AUTH_CALL_CONTEXT_KEY_REF` is a Dapr secret-store _reference_ and is intentionally projected through the ConfigMap. |
| `ServiceAccount` | Pod identity. Workspace-scoped RBAC (if any is needed) is added by AS-IMPL-024 once the public surface is wired. |
| `ExternalSecret` | Skeleton (disabled by default). AS-IMPL-004 (Phase B) wires the AS-side Postgres DSN env var(s) once `AuthStoreProvider` lands. |
| `ServiceMonitor` | Opt-in Prometheus scrape config on `/metrics`. Activated in AS-IMPL-026. |

The signing key for the internal call-context JWT is __not__ materialized
into a Kubernetes Secret by this chart — the service resolves it at
runtime via the Dapr Secrets API using the reference in
`CUSTOS_AUTH_CALL_CONTEXT_KEY_REF`. AS-IMPL-017 / AS-IMPL-018 cover the
signer and JWKS rotation.

## Configuration

All non-secret `CUSTOS_AUTH_*` env vars are surfaced under the `config:`
block in [`values.yaml`](values.yaml). `oidcIssuers` and
`callContextKeyRef` are empty by default; operators set them per profile when
those authentication features are enabled. First-admin credentials are owned
by the umbrella chart's Secret-backed bootstrap Job, not this subchart.

- Implementation tracker: [#267](https://github.com/toddysm/custos/issues/267) (AS-IMPL-000).
- This chart: [#237](https://github.com/toddysm/custos/issues/237) (AS-IMPL-002).
