# Air-Gapped Offline Install Bundle

This directory builds `custos-offline-<version>.tar.gz` — a self-contained tarball with vendored Helm charts and pre-pulled container images suitable for installation on clusters with no internet access.

## Usage

```bash
make bundle VERSION=0.1.0
```

Outputs `custos-offline-0.1.0.tar.gz` containing:
- `charts/` — vendored Helm dependencies
- `images/` — Docker image archives (`docker save` tarballs)
- `manifests/` — pre-rendered manifests for `helm template` consumers
- `install.sh` — operator runbook script

## Operator runbook (target air-gapped cluster)

1. Transfer tarball to a host with `kubectl`, `helm`, and registry access.
2. `tar -xzf custos-offline-<version>.tar.gz`
3. Push images: `./install.sh push-images registry.internal/custos`
4. Apply CRDs: `./install.sh apply-crds`
5. Install: `helm install custos charts/custos -f values-airgapped-<profile>.yaml`
6. Verify: `kubectl -n custos-system get pods`

See `design/architecture/reference-deployment.md` for the full air-gapped recipe.
