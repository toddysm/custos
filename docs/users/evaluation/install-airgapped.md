# Install — Air-Gapped (eval)

Last Updated: 2026-06-08

This guide installs the **`airgapped-eval`** profile on a cluster with **no
internet access**. You build a self-contained offline bundle on a connected
host, transfer it across the air gap, mirror its images into a private registry,
and install from the vendored chart. The bundled **Keycloak** is the OIDC issuer
and **Sealed Secrets** provides platform secrets — both ship inside the chart.

If your cluster can reach the public registry, use
[Install — connected](install-connected.md) instead.

Before you start, complete the [Prerequisites](prerequisites.md): a Kubernetes
≥ 1.28 cluster with a default `StorageClass`, Helm ≥ 3.16, a reachable private
OCI registry mirror, and the out-of-band CloudNativePG operator.

## 1. Build the offline bundle (connected host)

On a host **with** internet access and `docker` + `helm`, build the bundle from
the repository root:

```bash
make bundle VERSION=0.1.0
```

This produces `custos-0.1.0-airgapped.tar.gz` containing the vendored Helm
chart, a `docker save` archive of every required image, the `connectors-core`
OCI artifact, SHA-256 checksums, and an `INSTALL-AIRGAPPED.md` runbook. The
bundled images are the same ones a connected install would pull. To retag the
Custos images to a release tag, pass `CUSTOS_IMAGE_TAG`:

```bash
make -C deploy/offline bundle VERSION=0.1.0 CUSTOS_IMAGE_TAG=v0.1.0
```

See [`deploy/offline/README.md`](../../../deploy/offline/README.md) for the full
bundle contents and CI details.

## 2. Transfer and verify (air-gapped side)

Copy the tarball across the air gap, then expand and verify it:

```bash
tar -xzf custos-0.1.0-airgapped.tar.gz
cd custos-0.1.0-airgapped
sha256sum -c checksums.sha256    # or: shasum -a 256 -c checksums.sha256
```

## 3. Mirror images into your private registry

The `airgapped-eval` values file is pre-wired to a mirror at
`registry.internal` (Custos and Bitnami images under `registry.internal/custos`,
Dapr / cert-manager / Prometheus directly under `registry.internal`). Edit
`global.imageRegistry` and the per-subchart registry overrides in
`chart/custos/values-airgapped-eval.yaml` to match **your** registry first.

Because the chart rewrites image paths per subchart, render the profile to get
the authoritative source-to-target mapping, then load and push each archive to
the reference the install actually expects. The bundled
`INSTALL-AIRGAPPED.md` runbook ships the exact loop; do not hand-map images.

```bash
# Load every archived image locally.
for tar in images/*.tar; do docker load -q -i "$tar"; done
```

Then follow the render-and-push loop in the bundle's `INSTALL-AIRGAPPED.md`
(substitute `values-airgapped-eval.yaml` for the profile). `skopeo copy
docker-archive:images/<file>.tar docker://<target>` works equivalently without a
local Docker daemon.

Also push the **`connectors-core`** OCI artifact (under `connectors/`) into the
same registry.

## 4. Install the chart

The `airgapped-eval` profile enables bundled **Keycloak** (`oidc.keycloak.enabled`)
and **Sealed Secrets** (`secrets.sealed.enabled`) by default — no external OIDC
issuer or secret backend is required. Install from the vendored chart, pointing
the global registry at your mirror:

```bash
helm install custos chart/custos \
  -f chart/custos/values-airgapped-eval.yaml \
  --namespace custos-system --create-namespace \
  --set global.imageRegistry=<your-registry>/custos \
  --wait --timeout 20m
```

> **Postgres ordering caveat.** The same pre-install hook ordering described in
> the [connected install guide](install-connected.md#4-pre-provision-postgres-install-ordering-caveat)
> applies here. If the `custos-migrate` pre-install hook fails because the
> `custos-app` Secret does not yet exist, pre-provision the CNPG `Cluster`
> first, wait for it to be Ready, and install with `--set postgres.embedded=false`.

> **Dapr injector readiness gate.** A `<release>-dapr-injector-wait` pre-install
> hook blocks until the out-of-band `dapr-sidecar-injector` Deployment in
> `dapr-system` is Ready, so the service pods are sidecar-injected on first
> creation rather than racing the injector on a cold start
> ([#847](https://github.com/toddysm/custos/issues/847)). It uses a standalone
> `kubectl` image — mirror `registry.k8s.io/kubectl` (see the offline image
> manifest); the air-gapped overlays point it at `registry.internal/kubectl`.

## 5. Confirm the release installed

```bash
helm status custos -n custos-system
kubectl get pods -n custos-system
```

`helm status` should report `STATUS: deployed`, all control-plane pods should be
`Running`/`Ready`, and the migrate/bootstrap job pods should show `Completed`.

## Next step

Continue to [Verify](verify.md) to confirm the platform is healthy and find the
API gateway endpoint, then follow the air-gapped/direct Helm path in the
[first-admin bootstrap runbook](../../operations/bootstrap-admin.md). Generate
the token and create its temporary Secret only on the isolated side.

## Related documentation

| Document | Description |
|---|---|
| [`deploy/offline/README.md`](../../../deploy/offline/README.md) | How the offline bundle is built and what it contains |
| [Install — connected](install-connected.md) | The connected install path and the Postgres ordering caveat in full |
| [Prerequisites](prerequisites.md) | Tooling, cluster requirements, and pre-install operators |
| [Bootstrap administrator](../../operations/bootstrap-admin.md) | Offline token generation, Secret lifecycle, verification, and recovery |
| [Reference deployment](../../../design/architecture/reference-deployment.md) | Full air-gapped reference recipe and supply chain |
