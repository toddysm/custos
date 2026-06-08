# Air-Gapped Offline Install Bundle

This directory builds `custos-<version>-airgapped.tar.gz` — a self-contained
tarball with the vendored Helm chart and `docker save` archives of every
required container image, suitable for installation on clusters with no internet
access.

## Build

```bash
# From the repo root (delegates here):
make bundle VERSION=0.1.0

# Or directly, retagging the in-repo Custos images to a release tag:
make -C deploy/offline bundle VERSION=1.2.3 CUSTOS_IMAGE_TAG=v1.2.3
```

Building requires `docker` (to pull + `docker save` the images) and `helm` (to
vendor the chart dependencies). The images saved are listed in
[`images.txt`](images.txt); keep that list in sync when subchart versions change
in `deploy/helm/custos/Chart.yaml` (regeneration command is documented at the
top of `images.txt`).

## Output layout

`custos-<version>-airgapped.tar.gz` expands to:

```
chart/custos/         Helm chart with vendored dependency subcharts
images/*.tar          docker save archive per image
connectors/           connectors-core OCI artifact (or a mirror note)
checksums.sha256      SHA-256 of every bundled file
INSTALL-AIRGAPPED.md  operator runbook
```

## CI

`.github/workflows/offline-bundle.yml` builds the bundle on every `v*.*.*` tag
(reusing `build-images.yml` so the bundled images are the same signed images a
connected install would pull) and attaches it as a release asset. A manual
`workflow_dispatch` run publishes the bundle as a workflow artifact instead.

## Operator runbook

The expanded `INSTALL-AIRGAPPED.md` contains the full step-by-step runbook
(verify checksums → load/push images → push connectors → secrets/OIDC → `helm
install`). See `design/architecture/reference-deployment.md` for the complete
air-gapped reference recipe.
