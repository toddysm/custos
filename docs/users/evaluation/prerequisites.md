# Prerequisites & Cluster Preparation

Last Updated: 2026-06-08

This page lists the tooling, cluster requirements, and pre-install operators you
need before deploying the evaluation profile. Complete the checklist at the end,
then continue to the install guide for your topology.

## Tooling

Install these on the workstation from which you will run the deployment:

| Tool | Version | Purpose |
|---|---|---|
| Kubernetes cluster | **≥ 1.28** | Target cluster (the chart sets `kubeVersion: ">=1.28.0-0"`) |
| `kubectl` | Matching your cluster | Apply CRDs, inspect workloads |
| Helm | **≥ 3.16** | Install the umbrella chart (CI pins `v3.16.2`) |
| `git` | any recent | Clone the repository |

For an **air-gapped** install you additionally need image-handling tooling on a
connected staging host to build and push the offline bundle:

| Tool | Purpose |
|---|---|
| `docker` (or a registry client) | Load bundled image archives |
| Access to a **private OCI registry** | Mirror Custos and dependency images |
| `make` | Build the offline bundle (`make bundle`) |

## Cluster requirements

| Requirement | Applies to | Notes |
|---|---|---|
| A default `StorageClass` (dynamic PV provisioning) | both | Eval dependencies are PersistentVolume-backed (Postgres, Loki, Prometheus). Override with `--set postgres.storageClass=<class>` if you have no default. |
| Outbound egress to `ghcr.io` | connected only | Required to pull `ghcr.io/toddysm/custos/*` and upstream dependency charts/images. |
| A reachable private registry mirror | air-gapped only | All Custos and dependency images are mirrored here; the cluster needs no internet egress. |
| Cluster-admin privileges | both | Installing CRDs and cluster-scoped operators requires admin rights. |

## Pre-install operators

The umbrella chart installs **Dapr**, **Envoy Gateway**, **cert-manager**, and
the **Redis** broker itself (each is a vendored subchart, default `install: true`).
You do **not** install those out-of-band unless your cluster already provides
them — in which case disable the corresponding subchart at install time
(`--set dapr.install=false`, `--set envoyGateway.install=false`,
`--set certManager.install=false`).

The following must be present **before** you run `helm install`, because the
chart references their CRDs (or, for Postgres, because of install-hook ordering).

### Gateway API CRDs — both profiles

The chart templates `Gateway` and `HTTPRoute` resources, so the Gateway API CRDs
must be registered first.

```bash
kubectl apply -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.1.0/standard-install.yaml
```

### CloudNativePG operator — both profiles

The eval profile provisions Postgres through a CloudNativePG `Cluster`. Install
the operator out-of-band; the [install guide](install-connected.md) explains the
ordering caveat (the pre-install migration hook needs the database secret before
the embedded `Cluster` would otherwise be created).

```bash
helm repo add cnpg https://cloudnative-pg.github.io/charts
helm repo update
helm install cnpg cnpg/cloudnative-pg \
  --version 0.22.1 \
  --namespace cnpg-system --create-namespace \
  --wait --timeout 5m
```

### External Secrets Operator — connected profile only

The `connected-eval` profile templates an External Secrets Operator
`ClusterSecretStore`, so its CRDs must exist first. (The `airgapped-eval` profile
uses Sealed Secrets instead, which the chart installs as a subchart — no
out-of-band operator needed.)

```bash
helm repo add external-secrets https://charts.external-secrets.io
helm repo update
helm install external-secrets external-secrets/external-secrets \
  --version 0.10.0 \
  --namespace external-secrets --create-namespace \
  --set installCRDs=true \
  --wait --timeout 5m
```

> **Air-gapped identity.** The `airgapped-eval` profile bundles **Keycloak** as
> the OIDC issuer and **Sealed Secrets** for platform secrets — both are
> installed by the chart, so they are not separate prerequisites. See
> [Install — air-gapped](install-airgapped.md).

## Prerequisites checklist

Before continuing to install, confirm:

- [ ] Kubernetes cluster **≥ 1.28** reachable with cluster-admin `kubectl` access.
- [ ] Helm **≥ 3.16** installed locally.
- [ ] A default `StorageClass` exists (`kubectl get storageclass`).
- [ ] **Gateway API CRDs** applied.
- [ ] **CloudNativePG operator** installed and Ready.
- [ ] *(connected only)* Cluster has egress to `ghcr.io`; **External Secrets Operator** installed.
- [ ] *(air-gapped only)* Private registry mirror reachable and the offline bundle staged.

## Next step

- Connected cluster → [Install — connected](install-connected.md)
- Air-gapped cluster → [Install — air-gapped](install-airgapped.md)

## Related documentation

| Document | Description |
|---|---|
| [Evaluation overview](overview.md) | What the eval profile deploys and the connected vs air-gapped choice |
| [Reference deployment](../../../design/architecture/reference-deployment.md) | Full prerequisite, topology, and supply-chain detail |
| [User Guide index](../README.md) | All evaluation deployment guides |
