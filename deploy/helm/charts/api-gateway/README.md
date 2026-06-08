# api-gateway

Helm subchart for the Custos API Gateway (COMP-001). Rendered by the `custos`
umbrella chart (`deploy/helm/custos`) as a conditional dependency
(`api-gateway.enabled`).

Design: [`design/components/api-gateway/design.md`](../../../../design/components/api-gateway/design.md).

## What it renders

- **Deployment** — single container, Dapr sidecar annotations
  (`dapr.io/app-id: api-gateway`), `/healthz` + `/readyz` probes, env via
  `envFrom` (ConfigMap always; Secret when `externalSecret.enabled`).
- **Service** — `ClusterIP` on `name=http` port `8080`.
- **ConfigMap** — the non-secret `CUSTOS_GATEWAY_*` env vars from design.md
  § Configuration, including the TLS cert/key Dapr secret references and the
  CORS allow-list (rendered as a JSON array).
- **HTTPRoute** — routes `/v1`, `/openapi.json`, and `/webhooks` to the gateway
  Service through the shared `custos` Gateway.
- **ServiceAccount**, **ServiceMonitor** (opt-in), **ExternalSecret** (opt-in).

## Configuration contract

Non-secret env (ConfigMap, from the `config:` block in `values.yaml`), all from
design.md § Configuration. The TLS cert/key refs are Dapr secret *references*
(lookup keys the gateway resolves through the Dapr Secrets API), not the cert
material, so they are non-secret config and ship in the ConfigMap:

| Env var | Default | Source |
|---|---|---|
| `CUSTOS_GATEWAY_LISTEN_ADDR` | `:8443` | `config.listenAddr` |
| `CUSTOS_GATEWAY_TLS_CERT_REF` | `secretref://custos-gateway-tls/tls.crt` | `config.tlsCertRef` |
| `CUSTOS_GATEWAY_TLS_KEY_REF` | `secretref://custos-gateway-tls/tls.key` | `config.tlsKeyRef` |
| `CUSTOS_GATEWAY_CORS_ALLOWED_ORIGINS` | `["https://app.custos.example"]` (placeholder, see note) | `config.corsAllowedOrigins` |
| `CUSTOS_GATEWAY_BODY_MAX_BYTES_DEFAULT` | `1048576` | `config.bodyMaxBytesDefault` |
| `CUSTOS_GATEWAY_BODY_MAX_BYTES_PUBLISH` | `5242880` | `config.bodyMaxBytesPublish` |
| `CUSTOS_GATEWAY_RATE_LIMIT_PRINCIPAL_WRITES_RPS` | `20` | `config.rateLimitPrincipalWritesRps` |
| `CUSTOS_GATEWAY_RATE_LIMIT_PRINCIPAL_WRITES_BURST` | `40` | `config.rateLimitPrincipalWritesBurst` |
| `CUSTOS_GATEWAY_RATE_LIMIT_WORKSPACE_WRITES_RPS` | `200` | `config.rateLimitWorkspaceWritesRps` |
| `CUSTOS_GATEWAY_RATE_LIMIT_WORKSPACE_WRITES_BURST` | `400` | `config.rateLimitWorkspaceWritesBurst` |
| `CUSTOS_GATEWAY_IDEMPOTENCY_TTL` | `24h` | `config.idempotencyTtl` |
| `CUSTOS_GATEWAY_DEVICE_CODE_TTL` | `15m` | `config.deviceCodeTtl` |
| `CUSTOS_GATEWAY_DEVICE_CODE_POLL_INTERVAL` | `5s` | `config.deviceCodePollInterval` |
| `CUSTOS_GATEWAY_OIDC_DEFAULT_ISSUER` | `""` (device-code flow disabled) | `config.oidcDefaultIssuer` |

No `CUSTOS_GATEWAY_*` variable carries a secret value — the TLS refs above are
lookup keys, not cert material — so the **ExternalSecret** block stays a
disabled stub in every shipped profile. Operators who need to project an
additional secret env var enable it and add `externalSecret.data` entries; the
Deployment then appends a `secretRef` `envFrom` after the ConfigMap.

> **`CUSTOS_GATEWAY_CORS_ALLOWED_ORIGINS`** is marked Required with no default
> in design.md § Configuration (no wildcard allowed). The single placeholder
> origin shipped here lets the profiles render without an operator-supplied
> value; it is not a design-sanctioned default — operators must supply their
> UI origins.

## TLS

The gateway terminates TLS on `CUSTOS_GATEWAY_LISTEN_ADDR` (`:8443`) using the
cert/key it resolves from the Dapr Secrets API via `CUSTOS_GATEWAY_TLS_CERT_REF`
/ `CUSTOS_GATEWAY_TLS_KEY_REF`. The in-cluster HTTP listener the Dapr sidecar
and the `/healthz` / `/readyz` probes use is `service.port` (`8080`).

## Probes

`livenessProbe` hits `/healthz` and `readinessProbe` hits `/readyz`. A
`startupProbe` (also `/healthz`) gates both until the process is serving HTTP.
The gateway's startup permission cross-check converges readiness in the
background when the Auth Service / Dapr sidecar are not yet reachable on a cold
cluster, so `/readyz` returns `503` until that first validation succeeds (issue
#815); the startupProbe gives that convergence a generous budget before liveness
can restart the pod. Tune the cold-start budget via
`startupProbe.periodSeconds` × `startupProbe.failureThreshold` (default
`5 × 30 = 150s`); set `startupProbe.enabled: false` to drop it.
