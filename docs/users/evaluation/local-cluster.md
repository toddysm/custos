# Getting Started — Deploy on a Local Cluster (kind / Docker Desktop)

Last Updated: 2026-06-08

This quickstart stands up the Custos M1 control plane on a **local Kubernetes
cluster** — either [`kind`](https://kind.sigs.k8s.io/) or **Docker Desktop
Kubernetes** — using images you build locally. It is the fastest way to try the
platform on a laptop.

It mirrors the `connected-eval` profile exercised by the project's deployment
regression gate ([`.github/workflows/deploy-smoke.yml`](../../../.github/workflows/deploy-smoke.yml)),
so the commands here are known-good. For a managed/remote cluster, follow
[Install — connected](install-connected.md) instead; this page is the local
variant of the same flow.

> **M1 evaluation build.** No Web UI, pre-provisioned tokens only, single-replica
> infrastructure. See the [evaluation overview](overview.md).

## 1. Prerequisites

Install on your workstation:

| Tool | Version | Notes |
|---|---|---|
| Docker | recent | Builds the Custos images; backs both kind and Docker Desktop K8s |
| **kind** *or* Docker Desktop | kind ≥ 0.23 / Docker Desktop with Kubernetes | Pick one local-cluster option |
| `kubectl` | matches the cluster | |
| Helm | **≥ 3.16** | |
| `git`, `make` | any recent | Clone + build helpers |

**Resourcing.** The eval footprint needs roughly **6 vCPU / 12 GiB** available to
the cluster. In Docker Desktop, raise the VM's CPU/memory in
**Settings → Resources** before starting.

Clone the repository:

```bash
git clone https://github.com/toddysm/custos.git
cd custos
```

Set the shared variables used throughout:

```bash {"promptEnv":"false"}
export RELEASE=custos
export NS=custos-system
export CHART=deploy/helm/custos
export VALUES=deploy/helm/custos/values-connected-eval.yaml
export IMAGE_PREFIX=ghcr.io/toddysm/custos   # local image tags; not pulled remotely
export CLUSTER=custos-local
```

## 2. Create the local cluster

Choose **one** option.

### Option A — kind

```bash
kind create cluster --name "$CLUSTER" --image kindest/node:v1.31.2
kubectl cluster-info --context "kind-$CLUSTER"
```

### Option B — Docker Desktop Kubernetes

Enable Kubernetes in **Docker Desktop → Settings → Kubernetes → Enable
Kubernetes**, wait for it to report *running*, then select its context:

```bash
kubectl config use-context docker-desktop
```

Both options ship a default `standard`/`hostpath` StorageClass, which the
PersistentVolume-backed eval dependencies (Postgres, Loki, Prometheus) use.

## 3. Install the pre-install operators

The umbrella chart references CRDs from operators that must exist **before**
`helm install`. Install them at the versions pinned by the regression gate:

> The Gateway API CRDs are shipped by the chart itself (the Envoy Gateway
> subchart's `crds/`), so they are **not** pre-installed here. Installing them
> out-of-band with `kubectl apply` makes Helm 4's server-side CRD apply conflict
> with the cluster-installed copy (owned by the `kubectl-client-side-apply`
> field manager) and fails `helm install` with CRD ownership conflicts. Helm 3
> tolerated this; Helm 4 does not.

```bash
# CloudNativePG operator (provisions Postgres).
helm repo add cnpg https://cloudnative-pg.github.io/charts
helm repo add jetstack https://charts.jetstack.io
helm repo add external-secrets https://charts.external-secrets.io
helm repo update

helm install cnpg cnpg/cloudnative-pg \
  --version 0.22.1 \
  --namespace cnpg-system --create-namespace \
  --wait --timeout 5m

# cert-manager (the chart's gateway TLS Certificate + selfSigned Issuer).
helm install cert-manager jetstack/cert-manager \
  --version v1.20.2 \
  --namespace cert-manager --create-namespace \
  --set crds.enabled=true \
  --wait --timeout 5m

# External Secrets Operator (the connected-eval ClusterSecretStore).
helm install external-secrets external-secrets/external-secrets \
  --version 0.10.0 \
  --namespace external-secrets --create-namespace \
  --set installCRDs=true \
  --wait --timeout 5m
```

Because cert-manager is pre-installed here, you install the umbrella chart with
`certManager.install=false` in step 6 so its bundled subchart is skipped. The
other bundled subcharts (Dapr, Envoy Gateway, Redis) stay at their eval defaults.

## 4. Build the Custos images and make them available

Build all service and job images locally (context is the repo root). Pass
`IMAGE_REGISTRY="$IMAGE_PREFIX"` so the build tags match the `kind load` commands
below (the Makefile default is also `ghcr.io/toddysm/custos`):

```bash {"cwd":"../../.."}
make docker-build IMAGE_REGISTRY="$IMAGE_PREFIX"   # tags $IMAGE_PREFIX/<svc>:dev and $IMAGE_PREFIX/custos-<job>:dev
```

**Make the images reachable by the cluster:**

- **kind** — kind nodes do not share your Docker daemon, so load each image into
  the cluster:

  ```bash
  for s in api-gateway auth-service workflow-service trigger-service \
           connector-service activity-runtime-manager catalog-service \
           observability-audit-service; do
    kind load docker-image "$IMAGE_PREFIX/$s:dev" --name "$CLUSTER"
  done
  for j in migrate bootstrap; do
    kind load docker-image "$IMAGE_PREFIX/custos-$j:dev" --name "$CLUSTER"
  done
  ```

- **Docker Desktop Kubernetes** — the cluster shares the Docker daemon, so the
  images you just built are already visible. **No load step is needed.**

> The eval values set `imagePullPolicy` so locally present `:dev` images are used
> without a registry pull. If you see `ErrImagePull`, confirm the image tags
> above exist (`docker images | grep custos`) and, for kind, that the load step
> completed.

## 5. Vendor chart dependencies and pre-provision Postgres

```bash {"cwd":"../../.."}
# Pull the vendored subcharts into the umbrella chart.
make deps      # == cd deploy/helm/custos && helm dependency update
```

Pre-provision Postgres before installing. The migration runs as a **pre-install
hook** that needs the CNPG-generated `custos-app` Secret, but the embedded CNPG
`Cluster` would only be created *after* pre-install hooks — so render and apply
the chart's own `Cluster` first and wait for it (see the
[install-ordering caveat](install-connected.md#4-pre-provision-postgres-install-ordering-caveat)):

```bash {"cwd":"../../.."}
kubectl create namespace "$NS"

helm template "$RELEASE" "$CHART" -f "$VALUES" \
  --namespace "$NS" \
  --set cnpg.storageClass=standard \
  --show-only charts/cnpg/templates/cluster.yaml > /tmp/pg-cluster.yaml

kubectl apply -n "$NS" -f /tmp/pg-cluster.yaml
kubectl wait --for=condition=Ready cluster/custos -n "$NS" --timeout=5m
```

> On Docker Desktop the default StorageClass is `hostpath`, not `standard`. If
> `--set cnpg.storageClass=standard` does not bind, drop the flag to use the
> cluster default, or pass your actual class (`kubectl get storageclass`).

## 6. Install Custos

```bash {"cwd":"../../.."}
helm install "$RELEASE" "$CHART" -f "$VALUES" \
  --namespace "$NS" \
  --set postgres.embedded=false \
  --set certManager.install=false \
  --wait --timeout 20m
```

- `postgres.embedded=false` — reuse the Postgres `Cluster` you pre-provisioned.
- `certManager.install=false` — cert-manager was installed out-of-band in step 3.

## 7. Verify and run a workflow

Run the in-cluster synthetic scenario:

```bash
helm test "$RELEASE" -n "$NS" --logs
```

Check the control-plane pods are healthy (each service pod is **2/2** — the
service plus its injected Dapr sidecar):

```bash
kubectl get pods -n "$NS"
```

Reach the API gateway via a port-forward (simplest on a local cluster):

```bash
kubectl port-forward -n "$NS" deploy/custos-api-gateway 8080:8080
# in another shell:
curl -fsS localhost:8080/healthz
```

Then follow [First use](first-workflow.md) to authenticate and run a sample
workflow against `http://localhost:8080`. For deeper health checks and gateway
endpoint discovery, see [Verify](verify.md); if anything is stuck, see
[Troubleshooting](troubleshooting.md).

## 8. Tear down

```bash
# kind: delete the whole cluster (removes everything at once).
kind delete cluster --name "$CLUSTER"
```

On **Docker Desktop**, uninstall the release and namespace instead of deleting
the cluster (see [Uninstall](uninstall.md) for full cleanup, including PVCs and
the pre-provisioned Postgres `Cluster`):

```bash
helm uninstall "$RELEASE" -n "$NS"
kubectl delete cluster custos -n "$NS"
kubectl delete namespace "$NS"
```

## Related documentation

| Document | Description |
|---|---|
| [Prerequisites](prerequisites.md) | Tooling and operator details |
| [Install — connected](install-connected.md) | The managed/remote-cluster variant of this flow |
| [Verify](verify.md) | Health checks and gateway endpoint discovery |
| [First use](first-workflow.md) | Authenticate and run a workflow |
| [Troubleshooting](troubleshooting.md) | Common failure modes and debug commands |
| [Uninstall](uninstall.md) | Full teardown and cleanup |
