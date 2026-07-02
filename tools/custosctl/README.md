# custosctl

`custosctl` is the local & remote **dev/test CLI** for the Custos platform
(COMP-011, milestone 0.2). It deploys the platform to a local `kind` cluster or
an existing remote kube-context and drives the extension lifecycle (register
connectors/activities, apply/run workflows, `e2e`) against the platform's public
APIs.

Design: [`design/components/custosctl/design.md`](../../design/components/custosctl/design.md).

> **Status:** scaffold + local & remote lifecycle. The root command group, the
> `CUSTOS_*` configuration model, the `--target {local,remote}` abstraction, the
> `doctor` preflight, and the `up`/`down`/`status` commands for **both** targets
> are implemented (#952, #953, #954), along with the typed gateway API client
> that backs the API commands (#955), plus `connector`/`activity` register/list
> (#956, #957), `workflow apply`/`run`/`status` (#958), and `seed-ootb` (#959).
> The remaining commands (`e2e`, #960) and CI/docs (#961, #962) land in the
> sibling DEVCLI tasks.

## Install (editable)

```sh
pip install -e 'tools/custosctl[dev]'
```

## Usage

```sh
custosctl --help
custosctl doctor                 # preflight the local (kind) toolchain
custosctl --target remote doctor # preflight a remote kube-context

# Local lifecycle (target=local): run from inside the checkout, or set CUSTOS_REPO_ROOT
custosctl up                     # create kind cluster, prereqs, helm install --wait
custosctl status                 # kind cluster / release / pod summary (+ gateway if CUSTOS_GATEWAY set)
custosctl down --yes             # uninstall the release and delete the kind cluster

# Remote lifecycle (target=remote): operate against an existing kube-context;
# never creates/deletes the cluster. Prereqs default to skip (CUSTOS_PREREQS=install to run them).
export CUSTOS_TARGET=remote CUSTOS_KUBE_CONTEXT=my-cluster
custosctl up                     # install the platform against the current/selected context
custosctl status
custosctl down --yes             # uninstall the release only
custosctl down --yes --force     # ...also delete the namespace and its PVCs (destructive)

# Catalog (needs CUSTOS_GATEWAY + CUSTOS_TOKEN; GHCR digest-pinned images):
custosctl connector register extensions/connectors/dockerhub  # resolve digest + POST manifest
custosctl connector register extensions/connectors/ghcr --image-ref ghcr.io/acme/custos/ghcr:v0.1.0@sha256:...
custosctl connector list dockerhub                            # list registered versions of a type
custosctl activity register extensions/activities/copy-image  # inject digest + POST manifest
custosctl activity list custos.builtin copy-image             # list versions of an activity-type

# Workflows (needs CUSTOS_WORKSPACE or --workspace):
custosctl workflow apply my-workflow.yaml                     # publish a workflow definition
custosctl workflow run wfv-... --input image=ghcr.io/x:1      # start a run
custosctl workflow status run-... --watch                     # poll to a terminal status (exit 1 unless succeeded)

# Onboard the OOTB catalog (wraps scripts/seed-ootb.sh; run from the checkout):
custosctl seed-ootb --allow-existing
```

## Configuration

Configuration is read from the environment or an optional `.env` file using the
`CUSTOS_` prefix; CLI flags override env. See the design doc's Configuration
table for the full list. Common keys:

| Variable | Default | Description |
|---|---|---|
| `CUSTOS_TARGET` | `local` | `local` (kind) or `remote` (existing kube-context) |
| `CUSTOS_KUBE_CONTEXT` | `kind-<cluster>` (local) | kube-context to operate against |
| `CUSTOS_CLUSTER` | `custos-local` | kind cluster name (local) |
| `CUSTOS_NAMESPACE` | `custos-system` | release namespace |
| `CUSTOS_REPO_ROOT` | autodetected | Custos checkout the lifecycle commands run against |
| `CUSTOS_HELM_TIMEOUT` | `15m` | `helm install --wait` timeout |
| `CUSTOS_GATEWAY` | — | API Gateway base URL (API commands) |
| `CUSTOS_TOKEN` | — | platform-admin service token (API commands) |
| `CUSTOS_WORKSPACE` | — | default workspace for the `workflow` commands |

## Develop

```sh
cd tools/custosctl
pip install -e '.[dev]'
pytest -q
ruff check . && mypy src tests
```
