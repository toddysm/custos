# Getting Started — Deploy on a Local Cluster (kind / Docker Desktop)

Last Updated: 2026-06-09

This quickstart stands up the Custos M1 control plane on a **local Kubernetes
cluster** — either [`kind`](https://kind.sigs.k8s.io/) or **Docker Desktop
Kubernetes** — using images you build locally. It is the fastest way to try the
platform on a laptop.

It follows the same `connected-eval` profile as the project's deployment
regression gate ([`.github/workflows/deploy-smoke.yml`](../../../.github/workflows/deploy-smoke.yml)).
For a managed/remote cluster, follow
[Install — connected](install-connected.md) instead; this page is the local
variant of the same flow.

> **M1 evaluation build.** No Web UI, pre-provisioned tokens only, single-replica
> infrastructure. See the [evaluation overview](overview.md).

> **Prefer to script it?** [`custosctl`](../../developers/custosctl.md) is the developer CLI that automates this exact flow — `custosctl up` performs steps 2–6 (create the `kind` cluster, install prereqs, `helm install`), and `custosctl e2e` also seeds the OOTB catalog and runs a sample workflow. This page is the readable walkthrough of what the CLI does under the hood.

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

Set the shared variables used throughout. **In Runme, run this cell first and keep the rest of the guide in the same session** — the exports persist across cells, but a notebook reload resets them, so re-run this cell before any later step if you reload. A later step running with these unset (e.g. an empty `$VALUES`/`$RELEASE`) makes `helm install` fail while rendering the chart:

```bash {"promptEnv":"false"}
export REPO_ROOT="$(git rev-parse --show-toplevel)"   # repo root; later cells use absolute paths so they work from any cwd
export RELEASE=custos
export NS=custos-system
export CHART="$REPO_ROOT/deploy/helm/custos"
export VALUES="$REPO_ROOT/deploy/helm/custos/values-connected-eval.yaml"
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

## 3. Install the out-of-band prerequisites

The umbrella chart does **not** bundle the heavy upstream operators (Dapr, Envoy
Gateway, cert-manager, Prometheus, Loki, the OpenTelemetry Collector, and the
Redis pub/sub broker). Bundling them pushed the packaged chart past Helm's 1 MB
release-Secret limit, so `helm install` failed before anything was applied
([#851](https://github.com/toddysm/custos/issues/851)). They are installed
out-of-band instead — the chart still ships the CRs that target their CRDs (Dapr
Components/Subscriptions, the Gateway/GatewayClass/Certificate, the Grafana
dashboards), so those operators must exist **before** `helm install`.

The repo ships an idempotent installer that brings them all up at the versions
the regression gate pins. It is safe to re-run:

```bash
# Dapr, Envoy Gateway (+ Gateway API CRDs), cert-manager, Prometheus, Loki,
# the OTel Collector, and the Redis pub/sub broker the umbrella's CRs target.
"$REPO_ROOT/scripts/install-prereqs.sh"

# CloudNativePG operator (provisions Postgres) is required and not covered by
# the script above. The External Secrets Operator below is OPTIONAL for
# connected-eval: that profile reads its DSNs directly from the CNPG
# `custos-app` Secret and no longer renders an ESO ClusterSecretStore
# (`secrets.eso.enabled=false`). It is installed here only so the ESO CRDs exist
# if you re-enable the store; skip the `external-secrets` repo/install otherwise.
helm repo add cnpg https://cloudnative-pg.github.io/charts
helm repo add external-secrets https://charts.external-secrets.io
helm repo update

helm upgrade --install cnpg cnpg/cloudnative-pg \
  --version 0.22.1 \
  --namespace cnpg-system --create-namespace \
  --wait --timeout 5m

helm upgrade --install external-secrets external-secrets/external-secrets \
  --version 0.10.0 \
  --namespace external-secrets --create-namespace \
  --set installCRDs=true \
  --wait --timeout 5m
```

`scripts/install-prereqs.sh` installs the Redis broker as the `custos-redis`
release so the `custos-redis-master` Service and `custos-redis` Secret the
chart's Dapr pub/sub Component references resolve. Run
`"$REPO_ROOT/scripts/install-prereqs.sh" --help` for the available flags (e.g. `--registry`
for a partial air-gapped mirror, `--keycloak` / `--sealed-secrets` for the air-gapped
auth/secrets backends). The operators installed here (Dapr, Envoy Gateway,
cert-manager, and the observability backends) are deliberately left to their
defaults in step 6: the umbrella's `dapr.install` / `envoyGateway.install` keys
no longer install any operator — they gate whether the chart renders its own glue
CRs (the Dapr Components/Subscriptions and the GatewayClass), so they must stay at
their default `true` for those resources to land against the pre-installed CRDs.

## 4. Build the Custos images and make them available

Build all service and job images locally (context is the repo root). Pass
`IMAGE_REGISTRY="$IMAGE_PREFIX"` so the build tags match the `kind load` commands
below (the Makefile default is also `ghcr.io/toddysm/custos`):

```bash {"cwd":"../../.."}
make -C "$REPO_ROOT" docker-build IMAGE_REGISTRY="$IMAGE_PREFIX"   # tags $IMAGE_PREFIX/<svc>:dev and $IMAGE_PREFIX/custos-<job>:dev
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
# Resolve the chart's local subchart dependencies (the Custos services + the
# embedded CNPG/MinIO/ESO charts). The heavy upstream operators are installed
# out-of-band in step 3, so this no longer pulls anything from the network.
# `make deps` first purges deploy/helm/custos/charts/ so a directory vendored
# before the chart was slimmed can't re-install the externalized operators
# (which would fail the install with a numeric-label decode error).
make -C "$REPO_ROOT" deps      # == cd deploy/helm/custos && rm -rf charts && helm dependency update
```

Pre-provision Postgres before installing. The migration runs as a **pre-install
hook** that needs the CNPG-generated `custos-app` Secret, but the embedded CNPG
`Cluster` would only be created *after* pre-install hooks — so render and apply
the chart's own `Cluster` first and wait for it (see the
[install-ordering caveat](install-connected.md#4-pre-provision-postgres-install-ordering-caveat)):

```bash {"cwd":"../../.."}
# Idempotent: step 3's prereqs already created custos-system, so tolerate it.
kubectl create namespace "$NS" --dry-run=client -o yaml | kubectl apply -f -

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
# --- Show exactly what we're about to install (catches unset/stale Runme env) ---
echo "RELEASE = ${RELEASE:?run the step-1 variables cell first}"
echo "NS      = ${NS:?run the step-1 variables cell first}"
echo "CHART   = ${CHART:?run the step-1 variables cell first}"
echo "VALUES  = ${VALUES:?run the step-1 variables cell first}"

# --- Validate RELEASE is a usable Kubernetes name. A numeric/dotted value
#     (e.g. "0.1" carried in from a Runme env prompt) is NOT a valid release
#     name and historically rendered a bare numeric app.kubernetes.io/instance
#     label, failing the install with
#     'cannot unmarshal number into ObjectMeta.metadata.labels'. Re-run the
#     step-1 variables cell if this fires (RELEASE=custos). ---
if ! printf '%s' "$RELEASE" | grep -Eq '^[a-z]([a-z0-9-]*[a-z0-9])?$'; then
  echo "!! RELEASE='$RELEASE' is not a valid release name. Re-run the step-1 variables cell (RELEASE=custos)." >&2
  exit 1
fi

# --- Preflight: render + server-side decode WITHOUT mutating the cluster.
#     This exercises the same Build()/decode path as the real install, so any
#     manifest error surfaces here with a clear message instead of a half-done
#     install. ---
echo "--- preflight (helm install --dry-run=server) ---"
helm install "$RELEASE" "$CHART" -f "$VALUES" \
  --namespace "$NS" \
  --set postgres.embedded=false \
  --dry-run=server >/dev/null
echo "preflight OK"

# --- Real install. --wait blocks until every workload is Ready (up to 20m). ---
echo "--- installing (this blocks until all workloads are Ready) ---"
helm install "$RELEASE" "$CHART" -f "$VALUES" \
  --namespace "$NS" \
  --set postgres.embedded=false \
  --wait --timeout 20m
```

What the cell does, in order:

1. **Echoes the resolved variables** so you can confirm Runme actually has them
   (an empty `$VALUES`/`$RELEASE` from a reloaded notebook is a common cause of
   a render failure). It hard-stops with a clear message if any are unset.
2. **Validates `$RELEASE` is a usable Kubernetes name** and stops if not. A
   numeric/dotted value (e.g. `0.1`) is invalid and historically caused the
   `cannot unmarshal number into ObjectMeta.metadata.labels` install error.
3. **Preflights with `--dry-run=server`** — renders and decodes the manifests
   against the live API server without changing anything, so a bad manifest
   fails here cleanly instead of leaving a half-applied release.
4. **Installs for real** with `--wait` (blocks until every workload is Ready).

A `pre-install` hook Job (`$RELEASE-dapr-injector-wait`) runs first and blocks
until the out-of-band `dapr-sidecar-injector` Deployment in `dapr-system` is
Ready, so the service pods are sidecar-injected on first creation rather than
racing the injector on a cold start
([#847](https://github.com/toddysm/custos/issues/847)).

The flags:

- `postgres.embedded=false` — reuse the Postgres `Cluster` you pre-provisioned.

> **Why no `dapr.install=false` / `envoyGateway.install=false`?** In this slimmed
> chart those keys no longer gate any operator install — the operators were

> vendored out-of-band in step 3. What they gate now is whether the umbrella> pub/sub broker and gateway would never come up.

> renders its OWN glue CRs: the Dapr Components/Subscriptions> pre-installed CRDs. Passing `=false` would silently drop all of them and the

> (`templates/dapr-*.yaml`) and the GatewayClass (`templates/gatewayclass.yaml`).> Leave them at their default `true` so those resources render against the

> **Reinstalling after a teardown?** A `helm uninstall` leaves the out-of-band
> operators (and their CRDs) in place — that's expected, since the chart no
> longer owns them. Just re-run this step. If you also removed the operators
> (e.g. you ran the optional CRD cleanup in [section 8](#8-tear-down)), re-run
> step 3 first so the chart's CRs find their CRDs.

> **Docker Desktop + Helm 4: the install may block on the `Gateway`.** Helm 4's
> `--wait` (kstatus) waits for *every* release resource — including the `Gateway`
> custom resource — to report ready. The chart's `Gateway` only becomes
> `Programmed` once Envoy Gateway's `LoadBalancer` Service receives an external
> address, but Docker Desktop has **no LoadBalancer provider**, so that Service
> stays `<pending>` and `helm install --wait` blocks until it times out — even
> though every pod is already Ready. (Helm 3, which CI pins, ignores CR readiness,
> so this only bites locally with Helm 4.)
>
> Workaround: while the step-6 install is still running, open a **second
> terminal** and give the Envoy Service a placeholder ingress address. Nothing
> reverts it (there is no LoadBalancer controller), so the `Gateway` flips to
> `Programmed` and the install completes:

```bash
# Run in a SECOND terminal WHILE the step-6 `helm install --wait` is blocking.
# Only needed on Docker Desktop with Helm 4; harmless to skip on Helm 3.
NS=custos-system
SVC=$(kubectl get svc -n envoy-gateway-system \
  -l gateway.envoyproxy.io/owning-gateway-namespace="$NS" -o name | head -1)
kubectl patch "$SVC" -n envoy-gateway-system --subresource=status --type=merge \
  -p '{"status":{"loadBalancer":{"ingress":[{"ip":"127.0.0.1"}]}}}'
kubectl get gateway custos -n "$NS"   # PROGRAMMED should flip to True
```

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

Reach the API gateway via a port-forward (simplest on a local cluster).
`kubectl port-forward` is a **foreground** process that runs until you stop it
(Ctrl-C), so it can't share a cell with the `curl` that uses it. The cell below
is self-contained: it starts the port-forward in the background, probes
`/healthz`, then tears it down again.

```bash
# Self-contained health probe: background the port-forward, curl, then stop it.
kubectl port-forward -n "$NS" deploy/custos-api-gateway 8080:8080 >/tmp/custos-pf.log 2>&1 &
PF_PID=$!
# Wait for the local listener to come up before curling.
for i in $(seq 1 10); do curl -fsS -m 2 localhost:8080/healthz >/dev/null 2>&1 && break; sleep 1; done
echo "healthz: $(curl -fsS -m 5 localhost:8080/healthz)"   # {"status":"ok"}
echo "readyz:  $(curl -fsS -m 5 localhost:8080/readyz)"    # {"status":"ready"}
kill "$PF_PID" 2>/dev/null
```

To actually drive the gateway (next step), keep a port-forward running in its
**own dedicated terminal** — not in this notebook, where it would block the cell
indefinitely:

```bash
kubectl port-forward -n custos-system deploy/custos-api-gateway 8080:8080
```

Leave it running, then follow [First use](first-workflow.md) to authenticate and
run a sample workflow against `http://localhost:8080`. For deeper health checks
and gateway endpoint discovery, see [Verify](verify.md); if anything is stuck,
see [Troubleshooting](troubleshooting.md).

## 8. Tear down

```bash
# kind: delete the whole cluster (removes everything at once).
kind delete cluster --name "$CLUSTER"
```

On **Docker Desktop**, uninstall the release and namespace instead of deleting
the cluster (see [Uninstall](uninstall.md) for full cleanup, including PVCs and
the pre-provisioned Postgres `Cluster`).

Every command below is **idempotent** — each tolerates an already-removed or
never-created resource — so this same block also cleans up a **partially failed
install**. If `helm install` (step 6) errored partway, no release was recorded,
so `helm uninstall --ignore-not-found` is a harmless no-op and the remaining
commands still remove the pre-provisioned Postgres `Cluster` and the namespace.
You can re-run this block safely as many times as needed.

```bash
# Remove the Custos release if one was recorded (no-op if the install never
# completed far enough to create a release).
helm uninstall "$RELEASE" -n "$NS" --ignore-not-found

# Delete the pre-provisioned Postgres Cluster and the namespace (which also
# removes any half-created workloads, Secrets, and ConfigMaps inside it).
kubectl delete cluster custos -n "$NS" --ignore-not-found
kubectl delete namespace "$NS" --ignore-not-found
```

The out-of-band prerequisites from step 3 (Dapr, Envoy Gateway, cert-manager,
Prometheus, Loki, the OTel Collector, Redis, CNPG, ESO) are **not** removed by
the block above — the chart never owned them. They are harmless to leave
installed for a re-run. To fully clean them up, uninstall each Helm release and
then delete their CRDs. Skip this if other workloads on the cluster use Dapr or
the Gateway API:

```bash
for rel in custos-redis custos-loki custos-prometheus custos-otel-collector; do
  helm uninstall "$rel" -n "$NS" --ignore-not-found
done
helm uninstall dapr -n dapr-system --ignore-not-found
helm uninstall envoy-gateway -n envoy-gateway-system --ignore-not-found
helm uninstall cert-manager -n cert-manager --ignore-not-found
helm uninstall cnpg -n cnpg-system --ignore-not-found
helm uninstall external-secrets -n external-secrets --ignore-not-found

# Operators leave their CRDs behind on uninstall. Remove the Dapr + Gateway API
# CRDs so a later reinstall starts clean (Helm 4 server-side apply conflicts
# with stale copies otherwise).
kubectl get crd -o name \
  | grep -E '\.(dapr\.io|gateway\.networking\.k8s\.io|gateway\.networking\.x-k8s\.io|gateway\.envoyproxy\.io)$' \
  | xargs -r kubectl delete
```

> **A delete hangs (namespace stuck `Terminating`)?** A finalizer is usually
> waiting on a controller that is already gone. Find the holdout with
> `kubectl get all,gateway,gatewayclass -n "$NS"` and clear its finalizer, e.g.
> `kubectl patch <resource> -n "$NS" -p '{"metadata":{"finalizers":null}}' --type=merge`.

## Related documentation

| Document | Description |
|---|---|
| [Prerequisites](prerequisites.md) | Tooling and operator details |
| [Install — connected](install-connected.md) | The managed/remote-cluster variant of this flow |
| [Verify](verify.md) | Health checks and gateway endpoint discovery |
| [First use](first-workflow.md) | Authenticate and run a workflow |
| [Troubleshooting](troubleshooting.md) | Common failure modes and debug commands |
| [Uninstall](uninstall.md) | Full teardown and cleanup |
| [`custosctl` CLI](../../developers/custosctl.md) | Script this whole flow (`up`/`e2e`) instead of running the cells by hand |
