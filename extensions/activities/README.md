# Custos Out-of-the-Box Activities

This directory holds the out-of-the-box (OOTB) **activities** shipped with
Custos. An activity is a sandboxed unit of work that the Activity Runtime
Manager (ARM) schedules as an OCI container: ARM mounts the file-based
contract under `/custos/in` (inputs, context, per-slot secrets) and collects
results from `/custos/out`. Activities never call platform APIs directly and
import no platform packages — see the
[activity author guide](../../docs/developers/activity-author.md).

Each activity lives under `extensions/activities/<activity>/` and is
self-contained: its own `activity-manifest.yaml`, `pyproject.toml`,
`Containerfile`, application package under `app/`, and `tests/`. The
[`activities` CI job](../../.github/workflows/python-services.yml) lints,
type-checks, tests, and builds each activity's image.

| Activity | Type | Connector slots | Summary |
|---|---|---|---|
| [`copy-image`](copy-image/) | `copy-image` | `source` (`oci.pull`, `oci.list-referrers`), `dest` (`oci.push`) | Copy an OCI image between two registry connectors. Canonical binding **Docker Hub -> GHCR**; registry-agnostic. Supports multi-arch (`allPlatforms` / `platform`) and OCI referrers (`copyReferrers`). |

## Credential model

ARM injects the per-slot resolved registry credentials at
`/custos/in/secrets/<slot>/...`. Minting, leasing, and refreshing those
credentials is the **connector + sidecar's** responsibility — not the
activity's. An activity consumes the injected material for the duration of a
single run.

## Manifest contract

Every activity publishes a versioned `ActivityManifest` (`custos.dev/v1`) that
declares its inputs/outputs JSON Schemas, the connector slots and capabilities
it requires, resource limits, declared error codes, and determinism /
idempotency semantics. ARM resolves the manifest by reference
(`namespace/type@version`) before scheduling the container.
