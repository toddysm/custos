# Install — Connected (eval)

Last Updated: 2026-06-08

This guide installs the **`connected-eval`** profile end to end. Custos and its
dependency images are pulled from the public registry
(`ghcr.io/toddysm/custos`), so your cluster needs outbound access to it. For an
isolated cluster, use [Install — air-gapped](install-airgapped.md) instead.

Before you start, complete the [Prerequisites](prerequisites.md): a Kubernetes
≥ 1.28 cluster with a default `StorageClass`, Helm ≥ 3.16, and the out-of-band
operators (Gateway API CRDs, CloudNativePG, External Secrets Operator).

## 1. Get the chart and vendor dependencies

Clone the repository and pull the chart's dependency subcharts:

```bash
git clone https://github.com/toddysm/custos.git
cd custos
make deps          # runs: helm dependency update deploy/helm/custos
```

`make deps` downloads the vendored subcharts (Dapr, Envoy Gateway, cert-manager,
Redis, Loki, Prometheus, OTel Collector, CNPG) into the chart so a single
`helm install` brings everything up.

## 2. Set release variables

The reference release name is `custos` and the namespace is `custos-system`.
These are used throughout this guide and by the in-cluster Service DNS names.

```bash
export RELEASE=custos
export NS=custos-system
export CHART=deploy/helm/custos
export VALUES=deploy/helm/custos/values-connected-eval.yaml
```

## 3. Choose an image tag

By default the chart references the `dev` image tag
(`global.imageTag: dev`). For an evaluation pin it to a published release tag:

```bash
export IMAGE_TAG=v0.1.0     # use a tag published at ghcr.io/toddysm/custos
```

You can override the registry and tag at install time:

| Override | Default | Purpose |
|---|---|---|
| `global.imageTag` | `dev` | Tag for all Custos service/job images |
| `global.imageRegistry` | `ghcr.io/toddysm/custos` | Registry for Custos images (leave as-is for connected) |
| `global.imagePullSecrets` | `[]` | Pull secrets, if your mirror needs auth |

## 4. Pre-provision Postgres (install-ordering caveat)

> **Why this step exists.** The `custos-migrate` job runs as a Helm
> **`pre-install` hook** and reads the CloudNativePG-generated `custos-app`
> Secret. But the embedded CNPG `Cluster` is an ordinary chart resource, created
> **after** pre-install hooks — a chicken-and-egg problem on a fresh install.
> The fix is to create the CNPG `Cluster` first, wait for it to be Ready, then
> install the chart with `postgres.embedded=false` so it reuses the same
> database (and the migrate hook finds its Secret).

Create the namespace and render just the chart's own `Cluster` resource, then
apply and wait for it:

```bash
kubectl create namespace "$NS"

helm template "$RELEASE" "$CHART" -f "$VALUES" \
  --namespace "$NS" \
  --set postgres.storageClass=standard \
  --show-only charts/cnpg/templates/cluster.yaml > /tmp/pg-cluster.yaml

kubectl apply -n "$NS" -f /tmp/pg-cluster.yaml
kubectl wait --for=condition=Ready cluster/custos -n "$NS" --timeout=5m
```

Replace `standard` with your cluster's `StorageClass` if it differs (run
`kubectl get storageclass`).

## 5. Install the chart

Install with `postgres.embedded=false` (reuse the pre-provisioned database) and
`certManager.install=false` if you installed cert-manager out-of-band; otherwise
leave cert-manager to the chart.

```bash
helm install "$RELEASE" "$CHART" -f "$VALUES" \
  --namespace "$NS" \
  --set global.imageTag="$IMAGE_TAG" \
  --set postgres.embedded=false \
  --wait --timeout 20m
```

`--wait` blocks until the workloads and install hooks are ready. The
`custos-migrate` pre-install hook applies the database schemas; the
`custos-bootstrap` post-install hook seeds the default tenant, workspace, roles,
and admin binding.

> If your cluster already runs Dapr, Envoy Gateway, or cert-manager, disable the
> corresponding subchart so the chart doesn't install a second copy:
> `--set dapr.install=false`, `--set envoyGateway.install=false`,
> `--set certManager.install=false`.

## 6. Confirm the release installed

```bash
helm status "$RELEASE" -n "$NS"
kubectl get pods -n "$NS"
```

`helm status` should report `STATUS: deployed`. All control-plane pods should
reach `Running`/`Ready`, and the `custos-migrate` and `custos-bootstrap` job
pods should show `Completed`.

## Next step

Continue to [Verify](verify.md) to confirm the platform is healthy and find the
API gateway endpoint.

## Related documentation

| Document | Description |
|---|---|
| [Prerequisites](prerequisites.md) | Tooling, cluster requirements, and pre-install operators |
| [Verify](verify.md) | Health checks and finding the gateway endpoint |
| [Troubleshooting](troubleshooting.md) | Install failure modes and known issues |
| [Reference deployment](../../../design/architecture/reference-deployment.md) | Full topology, profile matrix, and supply chain |
