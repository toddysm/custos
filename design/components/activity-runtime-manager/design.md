# Component Design: Activity Runtime Manager

Slug: `activity-runtime-manager`
Last Updated: 2026-05-16
Version: 0 (in progress)
Status: Draft

> This document captures the design decisions locked in so far. Sections marked **(pending)** will be filled out in subsequent design iterations.

## Responsibility

The Activity Runtime Manager (ARM) is the component that **executes activities** on behalf of the Workflow Service. Activities are the pluggable units of real work in Custos (vulnerability scanning, SBOM generation, signature verification, image promotion, custom user code, etc.). ARM resolves the activity definition, materializes its inputs, runs it in a sandboxed runtime, captures outputs and logs, and returns a structured result to the orchestrator.

It owns activity execution. It does **not** own orchestration control flow, connector credential resolution, or workflow data transformation.

## Boundaries

- **Owns**:
  - Activity resolution (type + version → executable image/handler).
  - Inputs materialization on the activity sandbox filesystem.
  - Sandbox lifecycle (start, monitor, cancel, timeout).
  - Outputs parsing and validation against the activity's declared output schema.
  - Artifact upload to the artifact store.
  - Log streaming to Observability.
  - Result mapping (exit code + outputs envelope → orchestrator-facing result).
  - Resource limiting and secret injection at the sandbox boundary.
- **Does NOT own**:
  - Orchestration state machine, retries, fan-out, approval gates — Workflow Service (ADR-007).
  - Inter-step data transformation or expression evaluation — Workflow Service Expression Evaluator (ADR-011).
  - Connector plugin loading, credential resolution, or context issuance — Connector Service (ADR-005, ADR-013).
  - Trigger ingestion — Trigger Service.

## Activity Contract v1

The contract between the orchestrator and an activity is **file-based**. The orchestrator never speaks to activity code in-process: it writes inputs to a known filesystem location, starts the activity sandbox, and reads outputs back when the activity exits. This keeps activities language-agnostic and runtime-agnostic (OCI container today; HTTP, WASM later).

### Sandbox filesystem layout

| Path | Direction | Owner | Description |
|---|---|---|---|
| `/custos/in/inputs.json` | orchestrator → activity | ARM writes | Inputs envelope (see below). Read-only to activity. |
| `/custos/in/ctx.json` | orchestrator → activity | ARM writes | Execution context: `runId`, `stepId`, `attempt`, `workspaceId`, activity type/version, connector handles (no credentials), deadline. Read-only. |
| `/custos/in/secrets/<name>` | orchestrator → activity | ARM writes | One file per injected secret. Plaintext credentials live ONLY here, never in `inputs.json`. Read-only, tmpfs-mounted. |
| `/custos/out/outputs.json` | activity → orchestrator | activity writes | Outputs envelope (see below). Required at success. |
| `/custos/out/artifacts/` | activity → orchestrator | activity writes | Files produced by the activity (SBOMs, scan reports, signed manifests, etc.). ARM uploads to artifact store and references by `ArtifactRef` in `outputs.json`. |
| `/custos/out/audit.jsonl` | activity → orchestrator | activity appends | Optional structured audit lines. Forwarded to Observability/Audit. |

### `inputs.json` envelope

```json
{
  "schemaVersion": "1",
  "contractVersion": "1",
  "activity": { "type": "scan-image", "version": "1.2.0" },
  "step": { "runId": "...", "stepId": "...", "attempt": 1 },
  "inputs": {
    "image": { "ref": "ghcr.io/acme/app:v1", "digest": "sha256:..." },
    "severity": "high"
  }
}
```

The `inputs` field is shaped by the activity's declared input JSON Schema (Draft 2020-12). Secrets MUST NOT appear in `inputs` — they are injected via `/custos/in/secrets/`. The activity references a secret by logical name (declared in its manifest), and ARM mounts the resolved value file.

### `outputs.json` envelope — success

```json
{
  "schemaVersion": "1",
  "contractVersion": "1",
  "status": "success",
  "outputs": {
    "reportDigest": "sha256:...",
    "findings": 12
  },
  "produced": [
    { "kind": "ArtifactRef", "id": "art-...", "mediaType": "application/vnd.cyclonedx+json" }
  ]
}
```

### `outputs.json` envelope — failure

```json
{
  "schemaVersion": "1",
  "contractVersion": "1",
  "status": "failure",
  "error": {
    "code": "registry.unauthorized",
    "class": "permanent",
    "message": "no credentials for ghcr.io/acme/app",
    "details": { "registry": "ghcr.io" }
  },
  "outputs": {}
}
```

`error.class` aligns with ADR-008 exit-code semantics (`retryable`, `permanent`). Detailed error envelope and exit-code mapping is the next topic and lives in **§Error Envelope & Exit Codes (pending)**.

### Schema validation

Activity input and output schemas are validated **twice**:

1. **At publish time** (when an activity is registered into the Catalog) — schema is parsed and structurally validated. This is the compile-time gate.
2. **At runtime** — ARM validates the materialized `inputs.json` against the activity's input schema before starting the sandbox, and validates `outputs.json` against the output schema before returning to Workflow Service. Defense-in-depth: a broken activity that emits malformed outputs is caught at the ARM boundary, not propagated into the orchestrator.

## Platform Types

Activity authors compose inputs/outputs from platform-defined types so that activities interoperate without each one reinventing the wheel. The initial v1 platform types:

| Type | Purpose |
|---|---|
| `ImageRef` | An OCI image reference. Fields: `ref` (string, fully-qualified), `digest` (optional, `sha256:...`). Always normalized to `registry/repo[:tag][@digest]` form at the ARM boundary. |
| `OciDescriptor` | Mirrors the OCI distribution descriptor. Fields: `ref`, `mediaType`, `digest`, `size`, `artifactType`, `annotations`. The canonical "an artifact in a registry" shape that list/discover activities should emit and downstream activities should consume. |
| `ConnectorRef` | Opaque handle to a connector instance. ARM resolves it to a `ConnectorContext` for activities that need to talk to external systems. Exposes `host`, `endpoint`, `type`, `labels` to expressions — never credentials. |
| `ArtifactRef` | Opaque handle to a file produced by an upstream activity, materialized on the consumer's filesystem when the input is bound. |
| `Duration` | ISO-8601 duration string. |

**Design convention:** list/discover activities MUST return `OciDescriptor` (or a list thereof), not bare strings. This guarantees downstream activities have the digest, mediaType, and annotations they need without reparsing or re-fetching.

## Workflow-Level Primitives Supporting Activities

Activity inputs/outputs alone are not enough; the workflow author needs ways to transform, filter, and fan out between activities. The platform provides three layers of capability, used in this order of preference.

### Layer 1 — Inline CEL in `with:` bindings (cheapest)

Most adapter cases are a one-line expression in the consumer's `with:` block. Example:

```yaml
- id: scan
  activity: scan-image@1
  with:
    image: ${{ imageRef(item.ref, connector("ghcr-prod").host) }}
```

This handles the ~90% case of "the shape is almost right, just normalize one field."

### Layer 2 — `let` step (first-class)

A `let` step is a no-container, pure-data step. It exists to give a name to a reusable or complex transformation so it doesn't have to be duplicated across multiple `with:` blocks, and so it shows up in run inspection as its own step.

```yaml
- id: normalize
  let:
    fullyQualified: ${{ list.outputs.items |> map(d => imageRef(d.ref, connector("ghcr-prod").host)) }}
```

`let` is a workflow primitive. It does not invoke ARM. It runs in the Workflow Service Expression Evaluator. Outputs are durable like any other step output.

### Layer 3 — Dedicated activity

When the transformation/filter requires external I/O (calling a registry, hitting a policy service), crosses a policy boundary, needs cross-workflow reuse, or needs an audit trail of decisions, it becomes a real activity. The canonical filter activity is the built-in `policy-eval@1` (REQ-020), used in two modes:

- `mode: filter` — input `items[]` + `rules`; output `kept[]`, `excluded[]`, `decisions[]`.
- `mode: gate` — input single subject + `rules`; output `allow`/`deny` + reason.

This unifies filtering and gating: same activity, same rule language, two surfaces.

### `forEach` and `where:` clause

`forEach` is the fan-out primitive: it expands a step over a list, scheduling N parallel attempts.

`where:` is syntactic sugar on `forEach` for the common case of "filter then fan out." It compiles to an inline CEL filter on the input list — Layer 1 — so it has no new runtime machinery.

```yaml
- id: scan
  forEach: ${{ list.outputs.items }}
  where: ${{ item.mediaType == "application/vnd.oci.image.manifest.v1+json" }}
  activity: scan-image@1
  with:
    image: ${{ item }}
```

### Push-down `selector` on list activities

List/discover activities SHOULD accept an optional `selector` input that the activity itself can push down to the source (registry tag filter, label selector, etc.) for efficiency. The selector is **advisory**: the authoritative filter is whatever the workflow expresses in CEL (Layer 1) or via `policy-eval` (Layer 3). The orchestrator does not assume the source-side filter is exact.

### Decision tree

```
Need to adapt data between activities?
├── One field, no I/O, no audit needed?            → Layer 1: inline CEL
├── Same transform used in 2+ places, no I/O?      → Layer 2: let step
├── External call, policy decision, or audit?      → Layer 3: dedicated activity
└── Filter before fan-out?                         → forEach + where: (Layer 1 sugar)
```

## Custos CEL Function Set

Expressions in `with:`, `let`, `where:`, and trigger selectors use CEL (Common Expression Language) extended with a Custos-specific function library. The full set is defined in ADR-011; the v1 surface is grouped into six categories:

| Category | Functions |
|---|---|
| String | `concat`, `split`, `join`, `replace`, `lower`, `upper`, `trim`, `startsWith`, `endsWith`, `contains`, `matches`, `extract`, `format` |
| Collection | `map`, `filter`, `flatten`, `distinct`, `length`, `first`, `last`, `contains`, `groupBy` |
| Object | `keys`, `values`, `merge`, `pick`, `omit`, `has` |
| OCI / supply-chain | `imageRef`, `parseRef`, `digestOf`, `tagOf`, `repoOf`, `registryOf`, `mediaTypeMatches`, `isImage`, `isIndex`, `isSbom`, `isSignature`, `isAttestation`, `annotation`, `hasAnnotation` |
| Time | `now()` (frozen at run start — see Determinism), `parseTime`, `formatTime`, `addDuration`, `before`, `after` |
| Encoding | `base64`, `base64Decode`, `hex`, `hexDecode`, `jsonEncode`, `jsonDecode`, `sha256` |

**Explicitly excluded** from the expression sandbox: `eval()`, any file or network I/O, `secret()` accessors, `random()`, and live-clock arithmetic. Anything that would couple expressions to live external state belongs in an activity, not in CEL.

### Determinism rules

- `now()` is frozen at workflow run start. Every evaluation of `now()` within a single run returns the same value. Activities that need a live clock get one inside the sandbox; the workflow-level expression layer is deterministic.
- All Custos-defined functions are pure: same inputs → same outputs.
- This makes replay safe and `let`-step outputs reproducible.

## Connector Metadata Exposure

When `connector("name")` is used in an expression, the resolved object exposes a deliberately narrow surface:

| Field | Purpose |
|---|---|
| `host` | Hostname of the target system (e.g. `ghcr.io`). Used by transforms like `imageRef`. |
| `endpoint` | Full base URL/endpoint where applicable. |
| `type` | Connector type (e.g. `oci-registry`, `github`). Useful for selectors. |
| `labels` | Map of arbitrary key/value pairs set on the connector instance at registration time (Kubernetes-style labels, e.g. `env=prod`, `tier=public`). Used for selecting between connector instances in policy or routing logic. |

Credentials are never reachable from expressions. Activities that need to authenticate receive secrets via `/custos/in/secrets/` only.

## Internal Structure (pending)

The sub-module breakdown (Scheduler, Runtime Driver dispatcher, OCI Container Driver, I/O Broker, Artifact Store Client, Log Streamer, Result Mapper, Resource Limiter, Secret Injector — per components.md COMP-006) will be filled out after the error envelope and activity manifest are locked in.

## Key Operations (pending)

To be filled out:
- Execute activity (happy path).
- Cancel running activity (run cancelled, timeout exceeded).
- Activity failure (retryable vs permanent).
- Artifact upload and `ArtifactRef` materialization on downstream activity.

## Data Models (pending)

To be filled out: activity execution record, attempt record, artifact record relationships.

## Public Interface (pending)

Internal RPC surface (Workflow Service ⇄ ARM):
- `ScheduleActivity(runId, stepId, attempt, activityRef, inputs, connectorRefs, deadline)`
- `CancelActivity(runId, stepId)`
- Activity completion callback delivery to Workflow Service.

## Configuration (pending)

## Dependencies

| Dependency | Type | Purpose |
|---|---|---|
| Workflow Service | Runtime | Scheduling source and completion sink. |
| Connector Service | Runtime | Resolves `ConnectorRef` → `ConnectorContext` (handles, not credentials). |
| Storage Provider Layer | Runtime | Artifact upload via `ArtifactStoreProvider`; step output persistence via `MetadataStoreProvider`. |
| Dapr Secrets API | Runtime | Resolves secret handles to plaintext for sandbox injection. |
| Observability/Audit | Runtime | Log streaming and audit event emission. |
| Catalog Service | Runtime | Activity type/version resolution and schema retrieval. |
| Kubernetes API | Runtime | Sandbox lifecycle (Jobs/Pods) for OCI Container Driver. |

## Failure Modes (pending)

## Open TODOs

- [ ] TODO-001: Define error envelope details and exit-code mapping per ADR-008 (added 2026-05-16).
- [ ] TODO-002: Define activity manifest v1 (schema, versioning, OCI Referrers-based discovery) (added 2026-05-16).
- [ ] TODO-003: Decide sandbox technology per REQ-039 / TODO-002 in requirements (gVisor, Kata, runc+seccomp, or Kubernetes Jobs only) (added 2026-05-16).
- [ ] TODO-004: Specify Runtime Driver dispatcher contract; OCI Container Driver for v1, HTTP/WASM later (added 2026-05-16).
- [ ] TODO-005: Sub-module deep dive (Scheduler, I/O Broker, Artifact Store Client, Log Streamer, Result Mapper, Resource Limiter, Secret Injector) (added 2026-05-16).
- [ ] TODO-006: Finalize platform event taxonomy mapping for activity lifecycle events with Observability (added 2026-05-16).
- [ ] TODO-007: Lock the canonical built-in `policy-eval@1` activity manifest (filter/gate modes) as the reference for the Layer-3 filter pattern (added 2026-05-16).

## Change History

| Date | Change | GitHub Issue |
|---|---|---|
| 2026-05-16 | Initial draft: Activity Contract v1 (file-based), platform types (`ImageRef`, `OciDescriptor`, `ConnectorRef`, `ArtifactRef`, `Duration`), three-layer pattern for transforms and filters, `let` as first-class step, `where:` sugar on `forEach`, push-down selector convention, filter/policy-eval unification, Custos CEL function set and determinism rules, connector metadata exposure surface | pending |
