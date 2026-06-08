# Uninstall & Cleanup

Last Updated: 2026-06-08

This page removes an evaluation deployment and cleans up its data and cluster
resources. It distinguishes **release teardown** (removing the Custos release)
from **removing shared cluster operators** that you installed as prerequisites.

> **⚠️ Data loss.** Uninstalling deletes the platform. Removing PersistentVolume
> Claims (Postgres, Loki, Prometheus) is **irreversible** — all workflow history,
> audit records, logs, and metrics are destroyed. Back up anything you need
> before proceeding.

Set your release name and namespace once:

```bash
export NS=custos-system
export RELEASE=custos
```

## 1. Uninstall the Helm release

Remove the release. This deletes the control-plane Deployments, Services,
ConfigMaps, the `Gateway`/`GatewayClass`, and the chart-managed cert-manager,
Envoy Gateway, and Dapr resources installed by the chart:

```bash
helm uninstall "$RELEASE" -n "$NS"
```

Confirm the workloads are gone (only the pre-provisioned Postgres `Cluster` and
PVCs should remain):

```bash
kubectl get all -n "$NS"
```

## 2. Remove the pre-provisioned Postgres cluster

If you pre-provisioned Postgres before install (the
[install-ordering caveat](install-connected.md#4-pre-provision-postgres-install-ordering-caveat)),
the CloudNativePG `Cluster` is **not** owned by the Helm release and must be
deleted explicitly. Deleting it destroys the database:

```bash
kubectl delete cluster custos -n "$NS"
```

## 3. Delete PersistentVolumeClaims

PVCs are intentionally retained by `helm uninstall` so data survives a reinstall.
To reclaim storage and **permanently delete all data**, remove them:

```bash
# Review what will be deleted first.
kubectl get pvc -n "$NS"

# Irreversible — destroys Postgres, Loki, and Prometheus data.
kubectl delete pvc --all -n "$NS"
```

Depending on the StorageClass reclaim policy, the underlying PersistentVolumes
may need separate cleanup:

```bash
kubectl get pv | grep "$NS" || true
```

## 4. Delete the namespace

Once the release, Postgres cluster, and PVCs are removed, delete the namespace to
clear any leftover Secrets, ConfigMaps, or cert-manager `Certificate`/`Issuer`
objects scoped to it:

```bash
kubectl delete namespace "$NS"
```

## 5. (Optional) Remove shared cluster operators

The prerequisites you installed out-of-band are **cluster-scoped** and may be
shared with other workloads. Only remove them if no other application depends on
them. See [Prerequisites](prerequisites.md#out-of-band-operators) for how each
was installed.

> **⚠️ Cluster-wide impact.** These operators and their CRDs are not specific to
> Custos. Deleting them affects every workload on the cluster that uses them.

```bash
# External Secrets Operator (connected installs only).
helm uninstall external-secrets -n external-secrets

# CloudNativePG operator.
helm uninstall cnpg -n cnpg-system

# Gateway API CRDs.
kubectl delete -f https://github.com/kubernetes-sigs/gateway-api/releases/download/v1.1.0/standard-install.yaml
```

Remove the operator namespaces if they are no longer needed:

```bash
kubectl delete namespace external-secrets cnpg-system
```

## Teardown checklist

- [ ] `helm uninstall` completed and control-plane workloads are gone.
- [ ] Pre-provisioned Postgres `Cluster custos` deleted (if it was created).
- [ ] PVCs deleted (only if you intend to destroy all data).
- [ ] Namespace `custos-system` deleted.
- [ ] (Optional) Shared operators removed, only if unused elsewhere.

## Related documentation

| Document | Description |
|---|---|
| [Prerequisites](prerequisites.md) | How the out-of-band operators were installed |
| [Connected install](install-connected.md) | Includes the Postgres pre-provision step |
| [Air-gapped install](install-airgapped.md) | Offline-bundle install |
| [Troubleshooting](troubleshooting.md) | Debugging a stuck deployment |
