# custosctl

`custosctl` is the local & remote **dev/test CLI** for the Custos platform
(COMP-011, milestone 0.2). It deploys the platform to a local `kind` cluster or
an existing remote kube-context and drives the extension lifecycle (register
connectors/activities, apply/run workflows, `e2e`) against the platform's public
APIs.

Design: [`design/components/custosctl/design.md`](../../design/components/custosctl/design.md).

> **Status:** scaffold (DEVCLI-IMPL-001, #952). The root command group, the
> `CUSTOS_*` configuration model, the `--target {local,remote}` abstraction, and
> the `doctor` preflight are implemented. Lifecycle (`up`/`down`/`status`) and
> API commands (`connector`/`activity`/`workflow`/`seed-ootb`/`e2e`) land in the
> sibling DEVCLI tasks (#953–#962).

## Install (editable)

```sh
pip install -e 'tools/custosctl[dev]'
```

## Usage

```sh
custosctl --help
custosctl doctor                 # preflight the local (kind) toolchain
custosctl --target remote doctor # preflight a remote kube-context
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
| `CUSTOS_GATEWAY` | — | API Gateway base URL (API commands) |
| `CUSTOS_TOKEN` | — | platform-admin service token (API commands) |

## Develop

```sh
cd tools/custosctl
pip install -e '.[dev]'
pytest -q
ruff check . && mypy src tests
```
