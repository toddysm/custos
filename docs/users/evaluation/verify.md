# Verify the Deployment

Last Updated: 2026-06-08

After [installing](install-connected.md) the platform, work through this
checklist to confirm it is healthy and to find the externally reachable API
gateway endpoint. The commands assume the reference release name `custos` in the
`custos-system` namespace.

```bash
export RELEASE=custos
export NS=custos-system
```

## 1. Check pod and rollout status

All control-plane Deployments should be fully rolled out:

```bash
kubectl get pods -n "$NS"

for d in api-gateway auth-service workflow-service trigger-service \
         connector-service activity-runtime-manager catalog-service \
         observability-audit-service; do
  kubectl rollout status deployment/custos-$d -n "$NS" --timeout=5m
done
```

The `custos-migrate` and `custos-bootstrap` job pods should show `Completed`.
Each service pod runs **two containers** (the service plus its injected Dapr
sidecar); both must be `Ready` (`2/2`).

## 2. Confirm the database is ready

The CloudNativePG `Cluster` backs every service:

```bash
kubectl get cluster custos -n "$NS"
kubectl wait --for=condition=Ready cluster/custos -n "$NS" --timeout=5m
```

## 3. Check service health probes

Each service exposes two HTTP probes that the kubelet uses:

| Probe | Meaning | Failure effect |
|---|---|---|
| `/healthz` | **Liveness** — the HTTP server is accepting connections | A failing liveness probe restarts the pod |
| `/readyz` | **Readiness** — the service has converged and can serve traffic | A failing readiness probe removes the pod from its Service endpoints (no restart) |

### API gateway background readiness convergence

The api-gateway's readiness has a deliberate behavior worth understanding:

- It returns `/healthz` `200` as soon as its HTTP server is up.
- It only flips `/readyz` to `200` **after** a startup permission cross-check
  succeeds — every permission its route registry declares is validated against
  the Auth Service permission registry.
- If the Auth Service or the Dapr sidecar is not yet reachable at boot (a
  transient transport error or a retryable `408` / `429` / `5xx`), the gateway
  **stays up but not-ready** and keeps retrying in the background with
  exponential backoff. It does **not** crash-loop. `/readyz` converges to `200`
  once the dependency becomes reachable.
- A permission **drift** (or any non-retryable Auth Service contract error) is a
  permanent failure: the gateway stays up but never becomes ready, and `/readyz`
  returns `503` with an operator-actionable `detail`.

So a gateway pod that is `Running` but briefly `0/1 Ready` shortly after install
is expected — give it a minute to converge. A gateway stuck not-ready for
longer signals a real dependency or permission problem; inspect `/readyz`:

```bash
kubectl exec -n "$NS" deploy/custos-api-gateway -c api-gateway -- \
  curl -fsS localhost:8080/readyz || echo "not ready — inspect the response detail"
```

See [Troubleshooting](troubleshooting.md) for the not-ready failure modes.

## 4. Run the chart's smoke test

The chart ships a Helm test that exercises the platform end to end:

```bash
helm test "$RELEASE" -n "$NS" --logs
```

A passing run reports the test pod as `Succeeded` and prints its logs. A failure
prints the failing assertion — cross-reference [Troubleshooting](troubleshooting.md).

## 5. Find the gateway endpoint

North-south traffic enters through the Envoy Gateway listener
(`HTTPS:443`, hostname `custos.local` in the eval profile). Find its address and
TLS material:

```bash
# Gateway resource and its programmed address.
kubectl get gateway custos -n "$NS" -o wide

# The Envoy Gateway data-plane Service (type LoadBalancer or NodePort).
kubectl get svc -n "$NS" -l gateway.envoyproxy.io/owning-gateway-name=custos

# The cert-manager-issued TLS certificate for the listener.
kubectl get certificate -n "$NS"
```

The eval profile uses the hostname `custos.local` with a self-signed certificate.
For local access, map that hostname to the gateway's external address (or use
`kubectl port-forward`) and reach the API under
`https://custos.local/v1/...`. Because the certificate is self-signed, clients
must trust the issuing CA (or use `curl -k` for evaluation).

```bash
# Example: port-forward the gateway Service for local testing.
kubectl port-forward -n "$NS" \
  svc/$(kubectl get svc -n "$NS" -l gateway.envoyproxy.io/owning-gateway-name=custos -o jsonpath='{.items[0].metadata.name}') \
  8443:443
# then, in another shell:
curl -k https://custos.local:8443/v1/  --resolve custos.local:8443:127.0.0.1
```

## Verification checklist

- [ ] All control-plane pods `Running` and `2/2` Ready.
- [ ] `custos-migrate` and `custos-bootstrap` jobs `Completed`.
- [ ] CNPG `Cluster custos` reports `Ready`.
- [ ] api-gateway `/readyz` returns `200` (after background convergence).
- [ ] `helm test` succeeds.
- [ ] Gateway address and certificate resolved; `https://custos.local/v1/` reachable.

## Next step

Continue to [First workflow](first-workflow.md) to authenticate and run a
workflow end to end.

## Related documentation

| Document | Description |
|---|---|
| [API Gateway (developer)](../../developers/api-gateway.md) | Probe contract and readiness semantics in detail |
| [Troubleshooting](troubleshooting.md) | Failure modes, debug commands, and known issues |
| [First workflow](first-workflow.md) | Authenticate and run a sample workflow |
