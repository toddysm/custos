# Activity Author Guide

Last Updated: 2026-06-04

> **Audience:** developers writing **activities** — the units of work a Custos
> workflow step executes. This guide covers the file-based activity contract,
> the Activity Manifest, the sandbox and isolation model, and the `ARM_*`
> operator configuration that governs how the Activity Runtime Manager (ARM)
> runs your code.
>
> **Source of truth:** the component design,
> [`design/components/activity-runtime-manager/design.md`](../../design/components/activity-runtime-manager/design.md).
> This guide is the author-facing distillation; when the two disagree, the
> design document wins.

## What an activity is

An activity is a single OCI container image that performs one unit of work
(scan an image, generate an SBOM, promote a tag, …) and is published to the
Catalog with a versioned **Activity Manifest**. The Workflow Service schedules
an activity by reference (`namespace/type@version`); ARM resolves the manifest,
stands the image up in a hardened sandbox, hands it typed inputs, and maps its
outcome back to the workflow.

The contract is **file-based and language-agnostic**: ARM never calls into your
process. It writes inputs to a known path, starts the container, and reads
outputs back when the container exits. Your activity can be written in any
language that can read and write JSON files.

## The activity contract

### Sandbox filesystem layout

ARM mounts two tmpfs trees into every activity Pod: `/custos/in` (read-only,
written by ARM before start) and `/custos/out` (written by your activity).

| Path | Direction | Description |
|---|---|---|
| `/custos/in/inputs.json` | ARM → activity | The typed inputs envelope (below). Read-only. |
| `/custos/in/ctx.json` | ARM → activity | Execution context: `runId`, `stepId`, `attempt`, `workspaceId`, activity type/version, connector handles (no credentials), deadline. Read-only. |
| `/custos/in/secrets/<connector>/<key>` | ARM → activity | One file per injected secret, namespaced under the manifest connector slot name. Plaintext credentials live **only** here. Read-only, `0400`. |
| `/custos/in/sidecar-token` | ARM → activity | Bootstrap token for the sidecar Connector API. Send it in the `Custos-Sidecar-Token` header on every sidecar request. `0400`. |
| `/custos/in/artifacts/<name>` | ARM → activity | Upstream artifacts you consume: when an input `ArtifactRef` references an artifact a previous step produced, ARM fetches it and stages it here as a plain file before start. Read-only. |
| `/custos/out/outputs.json` | activity → ARM | The outputs envelope (below). Required on success. |
| `/custos/out/artifacts/<name>` | activity → ARM | Files you produce, keyed by `spec.outputs.artifacts[].name`. ARM uploads them and rewrites `outputs.json` with store IDs. |
| `/custos/out/audit.jsonl` | activity → ARM | Optional structured audit lines, forwarded to Observability/Audit. |

### `inputs.json`

ARM writes this before starting your container. The `inputs` field is shaped by
your manifest's input JSON Schema (Draft 2020-12). Secrets never appear here.

```json
{
  "schemaVersion": "1",
  "contractVersion": "1",
  "activity": { "type": "scan-image", "version": "1.2.0" },
  "step": { "runId": "run-7f3a", "stepId": "scan", "attempt": 1 },
  "inputs": {
    "image": { "ref": "ghcr.io/acme/app:v1", "digest": "sha256:abc" },
    "severity": "high"
  }
}
```

When an input value is an `ArtifactRef` (`{ "kind": "ArtifactRef", "name": ...,
"id": ... }`) produced by an earlier step, ARM fetches that artifact by `id`
before your container starts and stages it at `/custos/in/artifacts/<name>`. Read
the local file — you never call the artifact store yourself. Both directions
cross the pod boundary through the **ARM↔pod I/O bridge** (ARM streams
`/custos/in` in through an init container and streams `/custos/out` back through
a native-sidecar collector); the bridge is transparent to your activity, which
only ever reads and writes plain files under `/custos`.

### `outputs.json` — success

Your activity writes this on success. You **cannot** know artifact-store IDs at
write time, so you reference each produced artifact by its manifest-declared
`name`; ARM rewrites the envelope after the sandbox exits.

```json
{
  "schemaVersion": "1",
  "contractVersion": "1",
  "status": "success",
  "outputs": {
    "findings": 12,
    "reportRef": { "kind": "ArtifactRef", "name": "report" }
  }
}
```

After ARM's two-phase finalization, the Workflow Service sees every
`ArtifactRef` fully populated with `id`, `mediaType`, `digest`, and `size`, plus
an ARM-synthesized `produced[]` list. You are responsible only for (1) writing
the file at `/custos/out/artifacts/<name>` and (2) referencing it by `name`.

### `outputs.json` — failure

```json
{
  "schemaVersion": "1",
  "contractVersion": "1",
  "status": "failure",
  "error": {
    "code": "registry.unauthorized",
    "class": "permanent",
    "message": "no credentials for ghcr.io/acme/app"
  },
  "outputs": {}
}
```

`error.class` is the authoritative retry signal. It is one of `permanent`
(do not retry), `retryable` (the orchestrator may retry), or `cancelled`.

### Exit codes

The exit code is the **fallback** signal; a valid `outputs.json` always wins.
Per ADR-008 (four states):

| Exit code | Meaning |
|---|---|
| `0` | Success — requires a valid `outputs.json` with `status: "success"`. |
| `1` | Retryable failure. |
| `2` | Permanent failure. |
| _other_ | Any other code (including SIGKILL/137, OOM) maps to **retryable** by default — an uncategorized crash is more likely transient. |

A clean exit (`0`) **without** a parseable `outputs.json` is treated as a
permanent `activity.contract_violation`: a clean exit cannot be trusted as
success without the envelope that proves it.

## The Activity Manifest

The manifest is published to the Catalog as JSON; YAML is shown here for
readability. The image `digest` is pinned at publish time so tag drift can never
silently change behavior.

```yaml
apiVersion: custos.dev/v1
kind: ActivityManifest
metadata:
  type: scan-image
  version: 1.2.0
  namespace: custos.builtin
  description: "Scan an OCI image for vulnerabilities."
  labels:
    category: security
  owner: custos-maintainers
spec:
  contractVersion: "1"
  runtime:
    kind: oci-container
    image: ghcr.io/custos/scan-image:1.2.0
    digest: sha256:abc
    isolation:
      minTier: microvm
  inputs:
    schema:
      $schema: "https://json-schema.org/draft/2020-12/schema"
      type: object
      required: [image]
      properties:
        image: { $ref: "custos://types/ImageRef" }
        severity: { type: string, enum: [low, medium, high, critical] }
  outputs:
    schema:
      $schema: "https://json-schema.org/draft/2020-12/schema"
      type: object
      required: [findings, reportRef]
      properties:
        findings: { type: integer }
        reportRef: { $ref: "custos://types/ArtifactRef" }
    artifacts:
      - name: report
        mediaType: application/vnd.cyclonedx+json
        required: true
  connectors:
    - name: registry
      type: oci-registry
      required: true
      capabilities: [oci.pull]
  resources:
    cpu: { request: "500m", limit: "2" }
    memory: { request: "512Mi", limit: "2Gi" }
    ephemeralStorage: { limit: "5Gi" }
    timeout: PT15M
  errors:
    - code: registry.unauthorized
      class: permanent
    - code: scan.engine_failed
      class: retryable
  determinism: side-effecting
  idempotency: by-input-hash
```

### Field highlights

- **`spec.runtime`** — `kind` is `oci-container` in v1. `image` + `digest` are
  required; ARM runs the image's `ENTRYPOINT`/`CMD` as built (no `command`/
  `args` override in v1).
- **`spec.inputs.schema` / `spec.outputs.schema`** — JSON Schema Draft 2020-12.
  May `$ref` platform types via `custos://types/<Name>` (`ImageRef`,
  `OciDescriptor`, `ConnectorRef`, `ArtifactRef`, `Duration`).
- **`spec.outputs.artifacts[]`** — declare each file you emit by `name`,
  `mediaType`, and `required`. ARM uploads them and expands the matching
  `ArtifactRef`s in `outputs`.
- **`spec.connectors[]`** — connector slots the workflow must bind. ARM injects
  the resolved secrets under `/custos/in/secrets/<name>/`. `capabilities` are
  dot-namespaced advisory tokens the connector enforces.
- **`spec.resources.timeout`** — the only required resource field (ISO-8601).
  CPU/memory/ephemeral-storage are optional and fall back to the operator
  defaults below.

## Sandbox and isolation model

Every activity Pod runs under a hardened security context: non-root,
read-only root filesystem, all Linux capabilities dropped, no privilege
escalation, the `RuntimeDefault` seccomp profile, no host network/PID/IPC, no
`hostPath` mounts, and the service-account token automount disabled.

The manifest's `runtime.isolation.minTier` sets the **lower bound** on the
sandbox tier:

| Tier | Realization |
|---|---|
| `process` | runc + seccomp/AppArmor. |
| `vm` | Kata with a shared-kernel hypervisor (CLH/MSHV). |
| `microvm` | Kata + Firecracker. |

Each tier maps to a Kubernetes `RuntimeClass` through operator configuration
(`ARM_RUNTIME_CLASS_*`), not a hard-coded name. A workflow step may **upgrade**
the tier but never downgrade below the manifest floor. If a requested tier maps
to no configured `RuntimeClass`, ARM fails the attempt with
`system.runtime_unavailable` rather than silently downgrading.

## Operator configuration (`ARM_*`)

ARM is configured from environment variables, validated at startup; a missing
required variable fails fast. The full table — including defaults — lives in the
service [`README.md`](../../src/services/activity-runtime-manager/README.md#configuration).
The author-relevant knobs:

- `ARM_DEFAULT_TIER` — cluster-default isolation tier when a manifest omits
  `isolation.minTier`.
- `ARM_MAX_TIMEOUT` — absolute ceiling that clamps your manifest `timeout` and
  the step deadline.
- `ARM_OUTPUT_MAX_BYTES` — maximum `outputs.json` size.
- `ARM_ARTIFACT_MAX_BYTES` — per-artifact upload ceiling.

## Publish & onboard checklist

Every OOTB activity ships three additional deliverables (see
[`design/architecture/ootb-publishing-onboarding.md`](../../design/architecture/ootb-publishing-onboarding.md)):

1. **A dedicated publish workflow** — `.github/workflows/publish-activity-<name>.yml`
   that builds + pushes `ghcr.io/<owner>/custos/<name>` on an
   `activity-<name>-vX.Y.Z` tag, stamps the OCI annotations, and signs the image
   (SBOM + cosign + SLSA via the shared composite actions).
2. **An onboarding entry** — a registration block in
   [`scripts/seed-ootb.sh`](../../scripts/seed-ootb.sh) so the activity-type is
   registered into a running catalog.
3. **An OOTB index row** — a row in
   [`extensions/activities/README.md`](../../extensions/activities/README.md)
   and the top-level [`extensions/README.md`](../../extensions/README.md).

## See also

- Design: [`design/components/activity-runtime-manager/design.md`](../../design/components/activity-runtime-manager/design.md)
- Implementation plan: [`design/components/activity-runtime-manager/implementation-plan.md`](../../design/components/activity-runtime-manager/implementation-plan.md)
- Service README: [`src/services/activity-runtime-manager/README.md`](../../src/services/activity-runtime-manager/README.md)
- Catalog API (publishing manifests): [catalog-api.md](catalog-api.md)
- Connector Plugin Author Guide (binding connectors): [connector-plugin-author.md](connector-plugin-author.md)
