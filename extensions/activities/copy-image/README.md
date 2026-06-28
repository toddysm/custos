# Custos copy-image activity (`copy-image`)

Out-of-the-box (OOTB) Custos **activity** that copies an OCI image from a
bound **source** registry connector to a bound **dest** registry connector.
The canonical binding is **Docker Hub -> GHCR**, but the activity is
registry-agnostic — both slots are `oci-registry` connectors — so it doubles
as a general registry-to-registry copy.

> Status: shipped (v0.1.0). Manifest + file-based I/O contract, skopeo copy
> engine, credential materialization, multi-arch + OCI referrers, error
> mapping, CI, and the worked Docker Hub -> GHCR example all landed via
> COPY-IMPL-001...008 (tracker #930).

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

### Inputs (`/custos/in/inputs.json` -> `inputs`)

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `source` | `ImageRef` | yes | — | Source image: `{ ref, digest? }`. When `digest` is set the copy is pinned to it. |
| `destination.repository` | string | yes | — | Destination repository path (without registry host; the host comes from the `dest` connector). |
| `destination.tag` | string | no | `latest` | Destination tag. |
| `copyReferrers` | boolean | no | `false` | Also copy the image's OCI referrers (signatures / SBOM / attestations) via `oras cp --recursive`. |
| `allPlatforms` | boolean | no | `false` | Copy every platform manifest in a multi-arch index (`skopeo copy --all`). |
| `platform` | string | no | — | `os/arch[/variant]` selector (e.g. `linux/arm64`). Selects a single platform from an index. Ignored when `allPlatforms` is true. |

### Outputs (`/custos/out/outputs.json` -> `outputs`)

| Field | Type | Description |
|---|---|---|
| `destinationRef` | string | The `<host>/<repo>:<tag>` written. |
| `digest` | string | The destination manifest digest (`sha256:...`). |
| `bytesCopied` | integer | Reserved by the output schema; **not emitted** by v0.1.0 (skopeo/oras do not surface a reliable byte count). |
| `manifestsCopied` | integer | Number of manifests copied (image + referrers). |
| `reportRef` | `ArtifactRef` | Reference to the `copy-report` artifact (JSON). |

The optional `copy-report` artifact (`/custos/out/artifacts/copy-report`,
`application/json`) records the source ref, destination ref, digest, and
manifest count.

### Connector slots

| Slot | Type | Required | Capabilities |
|---|---|---|---|
| `source` | `oci-registry` | yes | `oci.pull`, `oci.list-referrers` |
| `dest` | `oci-registry` | yes | `oci.push` |

The canonical binding is `source: dockerhub`, `dest: ghcr`, but any
`oci-registry` connector works in either slot.

### Resources & limits

| | |
|---|---|
| CPU | request `250m`, limit `2` |
| Memory | request `256Mi`, limit `1Gi` |
| Ephemeral storage | limit `10Gi` |
| Timeout | `PT30M` (30 minutes) |
| Determinism | `side-effecting` |
| Idempotency | `by-input-hash` (re-copying the same digest is a fast no-op) |

### Error codes

| Code | Class | Raised when |
|---|---|---|
| `source.unauthorized` | permanent | Source registry rejected the credentials / pull. |
| `dest.unauthorized` | permanent | Destination registry rejected the credentials / push auth. |
| `source.not_found` | permanent | Source image / tag does not exist. |
| `dest.push_failed` | retryable | Destination push failed (transient network / registry error). |
| `copy.manifest_mismatch` | permanent | Copied manifest did not match the expected digest. |

The process exit code follows the contract: `0` success, `2` for a
`permanent` failure, `1` for a `retryable` failure.

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
