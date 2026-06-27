# Custos copy-image activity (`copy-image`)

Out-of-the-box (OOTB) Custos **activity** that copies an OCI image from a
bound **source** registry connector to a bound **dest** registry connector.
The canonical binding is **Docker Hub -> GHCR**, but the activity is
registry-agnostic — both slots are `oci-registry` connectors — so it doubles
as a general registry-to-registry copy.

> Status: scaffolding (COPY-IMPL-001). Input/output handling, the skopeo copy
> engine, referrers/multi-arch, error mapping, CI, and the worked example land
> in COPY-IMPL-002...008 (tracker #930).

| | |
|---|---|
| Activity type | `copy-image` |
| Version | `0.1.0` |
| Namespace | `custos.builtin` |
| Contract version | `1` |
| Runtime | `oci-container` (skopeo-based) |
| Connector slots | `source` (`oci.pull`, `oci.list-referrers`), `dest` (`oci.push`) |

## Contract

The activity implements the file-based ARM contract (see
[activity author guide](../../../docs/developers/activity-author.md)): ARM
mounts inputs at `/custos/in` and collects results from `/custos/out`. The
activity never calls platform APIs directly and imports no platform packages.

## Credential model

ARM injects the per-slot resolved registry credentials at
`/custos/in/secrets/<slot>/...`. The activity hands those to `skopeo`, which
performs the registry token exchange and any mid-copy refresh itself (Docker
Hub and GHCR are spec-compliant). Minting, leasing, and refreshing credentials
is the **connector + sidecar's** responsibility, not the activity's. (A
consumer-side proactive Authenticator is only needed for a programmable copy
engine against spec-non-compliant registries — see
[`registry-credential-refresh.md`](../../../design/architecture/registry-credential-refresh.md)
— and is out of scope for v0.1.0.)

## Development

```bash
pip install -e ".[dev]"
ruff check . && ruff format --check .
mypy -p copy_image && mypy tests
pytest -q
docker build -f Containerfile -t custos-copy-image:dev .
```
