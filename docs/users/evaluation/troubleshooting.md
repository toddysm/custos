# Troubleshooting & Known Issues

Last Updated: 2026-06-08

This page covers the failure modes you are most likely to hit when standing up an
evaluation deployment, with concrete debug commands and a symptom → cause →
resolution table. It assumes you have followed [Prerequisites](prerequisites.md),
one of the install guides, and [Verify](verify.md).

Throughout, set your release name and namespace once:

```bash
export NS=custos-system
export RELEASE=custos
```

## First-look debug commands

Start every investigation with the cluster's own view of what is wrong:

```bash
# Overall pod state — look for Pending, CrashLoopBackOff, ImagePullBackOff, Init.
kubectl get pods -n "$NS" -o wide

# Why a specific pod is stuck — scheduling, probes, pulls, and the event tail.
kubectl describe pod <pod> -n "$NS"

# Recent namespace events, newest last.
kubectl get events -n "$NS" --sort-by=.lastTimestamp | tail -40

# Logs for a service container and its injected Dapr sidecar.
kubectl logs <pod> -n "$NS" -c <service-container>
kubectl logs <pod> -n "$NS" -c daprd

# Embedded Postgres (CloudNativePG) cluster status.
kubectl get cluster -n "$NS"
kubectl describe cluster custos -n "$NS"
```

## Symptom → cause → resolution

| Symptom | Likely cause | Resolution |
|---|---|---|
| Pod stuck `Pending` | No PersistentVolume can be bound (no default StorageClass), or the node has insufficient CPU/memory for the eval footprint (~6 vCPU / 12 GiB). | `kubectl describe pod` and read the events. Provision a default StorageClass, or schedule onto a larger node. See [Prerequisites](prerequisites.md#cluster-requirements). |
| Pod `CrashLoopBackOff` | A dependency is unreachable at boot (Postgres, Auth Service, Dapr sidecar), or a config/secret value is wrong. | `kubectl logs <pod> -c <service>` for the panic/error; `kubectl logs <pod> -c daprd` to confirm the sidecar is healthy. Fix the offending Secret/value and let the pod restart. |
| `ImagePullBackOff` / `ErrImagePull` (connected) | Wrong `global.imageRegistry`/`global.imageTag`, or the registry is unauthenticated/unreachable. | Confirm the image override matches a published tag (default `dev`). See [Connected install](install-connected.md). |
| `ImagePullBackOff` / `ErrImagePull` (airgapped) | Images were not mirrored into the internal registry, or `global.imageRegistry` does not point at it. | Re-run the image push step from the bundle and verify with `crane`/`docker pull`. See [Air-gapped install](install-airgapped.md). |
| `custos-migrate` pre-install hook fails / times out | The CNPG `custos-app` Secret does not exist yet — the embedded Postgres Cluster is created *after* pre-install hooks. | Pre-provision Postgres before install. See [Connected install](install-connected.md#4-pre-provision-postgres-install-ordering-caveat). |
| api-gateway pod is `Running` but `0/1` Ready; gateway returns `503` | The gateway is still converging its startup permission cross-check (a transient dependency hiccup), so `/readyz` is not yet `200`. | This self-heals — the gateway retries with backoff and never crash-loops. Wait, then inspect `/readyz`. See [Background readiness](#api-gateway-stays-not-ready) below. |
| api-gateway never becomes Ready; `/readyz` detail shows a permission error | Permission drift — the gateway's required permissions are not granted, which is a permanent (not transient) failure. | The gateway intentionally stays up-but-not-ready. Fix the permission/role binding, then restart the deployment. |
| Service pod shows `1/2` containers ready | The Dapr sidecar (`daprd`) was injected but is not passing its readiness check (often because the Dapr control plane is unhealthy). | Inspect the sidecar: `kubectl logs <pod> -c daprd` and confirm the Dapr control plane is healthy. See [Dapr sidecar problems](#dapr-sidecar-problems). |
| Service pod shows a single container (`1/1`, no `daprd`) | The Dapr sidecar was **not injected** — the pod/namespace is missing Dapr injection, or the injector was not running when the pod started. | Confirm Dapr injection is enabled and the Dapr control plane was healthy, then restart the deployment. See [Dapr sidecar problems](#dapr-sidecar-problems). |
| `helm install` fails on missing CRDs (Gateway API, CNPG, ESO) | The out-of-band operators were not installed before the chart. | Install the required operators first. See [Prerequisites](prerequisites.md#pre-install-operators). |
| `helm install` fails with a CRD apply *conflict* (`conflict occurred while applying object ... Kind=CustomResourceDefinition`) | A previous install left the chart-shipped Dapr / Gateway API CRDs behind — `helm uninstall` never removes `crds/`, so Helm 4 server-side apply conflicts with the stale copies. | Delete the stale CRDs, then re-run install. See [Uninstall §5 (Remove chart-shipped CRDs)](uninstall.md). |
| `helm test` fails | A control-plane service is not ready, or the seeded test token is missing/invalid. | Re-run with logs: `helm test $RELEASE -n $NS --logs`; then debug the failing service from its pod logs. |
| Gateway endpoint unreachable | The `Gateway` has no address, DNS for `custos.local` is not pointed at it, or TLS trust is missing. | Re-check the gateway endpoint and port-forward fallback in [Verify](verify.md). Add `-k` to `curl` for the eval self-signed certificate. |

## api-gateway stays not-ready

The api-gateway separates **liveness** from **readiness** deliberately:

- `/healthz` returns `200` as soon as the HTTP server is accepting connections.
- `/readyz` only flips to `200` **after** a startup permission cross-check
   against the Auth Service succeeds.

If the Auth Service or the Dapr sidecar is not reachable at boot, the gateway
retries with exponential backoff. It **does not** crash-loop, and `/readyz`
converges to `200` once the dependency comes up. A persistent `503` after the
rest of the platform is healthy signals a real dependency or permission problem
— inspect the readiness detail:

```bash
kubectl port-forward -n "$NS" deploy/custos-api-gateway 8080:8080 &
curl -fsS localhost:8080/readyz || echo "not ready — read the response detail"
```

A response detail naming a permission failure indicates **permission drift**
(permanent): the gateway stays up but never becomes ready. Correct the
role/permission binding and restart the deployment:

```bash
kubectl rollout restart deployment/custos-api-gateway -n "$NS"
```

## Dapr sidecar problems

Every control-plane service pod should report **2/2** containers (the service
plus the injected `daprd` sidecar). There are two distinct failure shapes:

- **`1/2` containers** — the `daprd` sidecar **was** injected but is not passing
   its readiness check (commonly because the Dapr control plane is unhealthy).
- **A single container (`1/1`, no `daprd`)** — the sidecar was **not injected**
   at all; the pod or namespace is missing Dapr injection, or the injector was
   not running when the pod was created.

```bash
# Confirm the Dapr control plane is healthy.
kubectl get pods -n dapr-system

# Inspect the sidecar's own logs (only present when it was injected).
kubectl logs <pod> -n "$NS" -c daprd

# Inspect the pod's containers and injection annotations.
kubectl describe pod <pod> -n "$NS"
```

If the Dapr control plane is unhealthy, the chart-managed Dapr install may not
have completed — re-check the install output and the `dapr-system` namespace.
If the sidecar was never injected (single-container pod), confirm injection is
enabled and restart the deployment once the Dapr control plane is healthy.

On a cold-start install the umbrella runs a `pre-install`/`pre-upgrade` hook Job
(`<release>-dapr-injector-wait`) that blocks until the `dapr-sidecar-injector`
Deployment in `dapr-system` is Ready, so the service pods are injected on first
creation and you should not normally see un-injected (`1/1`) pods
([#847](https://github.com/toddysm/custos/issues/847)). If you do, inspect that
hook Job's logs and confirm the injector is healthy:

```bash
kubectl logs job/"$RELEASE"-dapr-injector-wait -n "$NS"
kubectl rollout status deployment/dapr-sidecar-injector -n dapr-system
```

The gate is on by default; set `dapr.injectorReadyGate.enabled=false` only when
Dapr sidecar injection is not used.

## Known M1 limitations

These are expected behaviors in the M1 "Core engine" milestone, not bugs:

- **No Web UI.** All interaction is through the HTTP API
   (see [First use](first-workflow.md)).
- __Pre-provisioned tokens only.__ The interactive OIDC device-code flow is
   disabled; authenticate with a `cst_...` service token.
- **No HA.** The eval profile is single-replica with no PodDisruptionBudgets,
   HorizontalPodAutoscalers, or anti-affinity.
- **No off-cluster backups.** Postgres/Loki/Prometheus data lives on
   in-cluster PersistentVolumes only.

## Filing issues

If you hit a problem not covered here, gather the failing pod's description and
logs (commands above) and file an issue on the
[project issue tracker](https://github.com/toddysm/custos/issues). Known issues
and the in-progress evaluation-deployment documentation are tracked under the
[`USERDOC-000-EVAL-DEPLOY` tracker (#824)](https://github.com/toddysm/custos/issues/824).

## Next step

| Step | Description |
|---|---|
| [Uninstall](uninstall.md) | Remove the evaluation deployment and clean up data |

## Related documentation

| Document | Description |
|---|---|
| [Prerequisites](prerequisites.md) | Tooling, cluster requirements, and out-of-band operators |
| [Connected install](install-connected.md) | Connected install, including the Postgres ordering caveat |
| [Air-gapped install](install-airgapped.md) | Offline-bundle install and image mirroring |
| [Verify](verify.md) | Health checks and gateway endpoint discovery |
| [First use](first-workflow.md) | Authenticate and run a workflow |
