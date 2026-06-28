# Out-of-the-Box Publishing & Onboarding

Last Updated: 2026-06-27
Status: Proposed (OOTB-005)

This document is the design source of truth for **publishing** out-of-the-box
(OOTB) connector and activity images and **onboarding** their types into a
running Custos catalog, plus the **end-to-end runbook** that ties deployment,
onboarding, and a first real run together. It builds on
[`ootb-catalog.md`](ootb-catalog.md) (OOTB-001, structure) and the reference
implementations under `extensions/` (OOTB-003), and closes the two gaps that
remain after the `copy-image` activity (#889) and the `dockerhub`/`ghcr`
connectors shipped.

## 1. Context & Problem

Two gaps prevent an evaluator from deploying the platform and *using* the OOTB
extensions:

1. **No image publishing.** The canonical
   [`build-images.yml`](../../.github/workflows/build-images.yml) publishes the
   8 services + 2 jobs + `connector-sidecar` to `ghcr.io/<owner>/custos/*` with
   OCI annotations, SBOM, cosign signatures, and SLSA provenance. It does **not**
   build/push the OOTB connector plugins or activities — those are only built
   with `push: false` in
   [`python-services.yml`](../../.github/workflows/python-services.yml). So
   `ghcr.io/toddysm/custos/copy-image`, `.../dockerhub`, and `.../ghcr` never
   reach a registry, and the `copy-image` manifest still carries a placeholder
   digest.
2. **No catalog onboarding.** The
   [`bootstrap` job](../../src/jobs/bootstrap/README.md) seeds
   permissions/roles/tenant/workspace/admin only. Nothing registers the OOTB
   connector-types (`dockerhub`, `ghcr`) or the `custos.builtin/copy-image`
   activity-type into the catalog, so a freshly deployed platform cannot bind or
   run them.

There is also no single guide that chains *deploy -> onboard -> run a real
copy*; the pieces are scattered across the evaluation and connector guides.

## 2. Goals & Non-Goals

### Goals

- Every OOTB extension is **independently publishable** to GHCR via its **own
  dedicated workflow**, with the same supply-chain treatment (OCI annotations,
  SBOM, cosign, SLSA) the core images get.
- A single **idempotent onboarding script** registers the OOTB connector-types
  and activity-types into a running catalog, resolving real published digests.
- A single **end-to-end runbook** takes an evaluator from a deployed platform to
  a working Docker Hub -> GHCR copy and back out to the audit log.
- The authoring **guidelines and skills** require a dedicated publish workflow +
  onboarding entry + OOTB index row for **every** future connector/activity.

### Non-Goals

- Auto-registering OOTB types from a Helm hook (turnkey seeding). Deferred --
  onboarding is an explicit, re-runnable script the runbook invokes and
  operators can run. (Decision D1.)
- A marketplace/registry UI, or a generic "publish any extension" reusable
  workflow that hides per-extension config. Each extension owns its workflow.
  (Decision D3.)
- Changing the Connector Service / ARM / Catalog contracts. Onboarding uses the
  existing public Catalog API.

## 3. Decisions

| ID | Decision | Rationale |
|---|---|---|
| D1 | Onboarding is a **standalone script** (`scripts/seed-ootb.sh`), not a Helm hook | Operator-runnable and re-runnable; no platform coupling; matches the "script the runbook invokes" choice. |
| D2 | Process is **design-tracked** (implement-component: issues + PR-per-task gates) | Consistency with the rest of the OOTB epic. |
| D3 | **One dedicated publish workflow per extension** (`publish-<kind>-<name>.yml`) | Explicit, independently versioned releases; no hidden shared matrix. Also added to the authoring guidelines for all future extensions. |
| D4 | Images publish to `ghcr.io/<owner>/custos/<name>` on a per-extension tag + manual dispatch | Matches the existing image-naming convention and the `copy-image` manifest image ref. |

## 4. Design

### 4.1 Per-extension publish workflows (D3, D4)

Each OOTB extension gets a dedicated workflow at
`.github/workflows/publish-<kind>-<name>.yml` where `<kind>` is `connector` or
`activity`:

- `publish-activity-copy-image.yml`
- `publish-connector-dockerhub.yml`
- `publish-connector-ghcr.yml`

Each workflow:

1. **Triggers** on a per-extension version tag and on `workflow_dispatch`:
   - activity: `activity-<name>-v*.*.*` (e.g. `activity-copy-image-v0.1.0`)
   - connector: `connector-<name>-v*.*.*` (e.g. `connector-dockerhub-v0.1.0`)
   The tag carries the SemVer that MUST match the extension manifest `version`;
   a guard step fails the run on mismatch.
2. **Builds + pushes** the image to
   `ghcr.io/${{ github.repository_owner }}/custos/<name>` tagged `:vX.Y.Z` and
   `:sha-<sha>`, building from the extension directory's `Containerfile`
   (context = the extension dir; no monorepo copy -- extensions are
   self-contained per OOTB-001 section 2).
3. **Stamps OCI annotations** (`org.opencontainers.image.*` +
   `vnd.custos.build.*`) on the manifest, reusing the same annotation contract
   as `build-images.yml`.
4. **Supply chain**: reuses the existing `./.github/actions/sbom-sign` and
   `./.github/actions/slsa-provenance` composite actions to attach a Syft SBOM,
   keyless-sign with cosign (GitHub OIDC), and emit SLSA provenance -- parity
   with core images.
5. **Connectors only**: publishes the `connector-manifest.json` as an OCI
   **referrer** of the pushed image (`artifactType =
   application/vnd.custos.connector.manifest.v1+json`) with the digest-derived
   fallback tag, per `ootb-catalog.md` section 4 and `connections-api.md`.
6. **Outputs** the pushed digest (`<image>@sha256:...`) in the job summary so the
   release operator can pin it (and so the activity manifest's placeholder
   digest can be updated on a real release).

`permissions: { contents: read, packages: write, id-token: write }`.

### 4.2 Catalog onboarding script (D1)

`scripts/seed-ootb.sh` -- idempotent, re-runnable bash, `shellcheck` clean.
Contract:

- **Inputs** (env): `GATEWAY` (base URL), `TOKEN` (platform-admin service token
  -- required because `custos.builtin` activity registration and platform-scoped
  connector-type registration need admin), optional `WS` (defaults
  `ws-default`), optional `INSECURE=1` to pass `curl -k` for the eval
  self-signed cert, optional `IMAGE_PREFIX` (defaults `ghcr.io/toddysm/custos`).
- **What it registers**:
  - Connector-types `dockerhub` and `ghcr` via
    `POST /v1/catalog/connector-types` with
    `{ "manifest": <connector-manifest.json>, "referrerRef": "<image>@<digest>" }`.
  - Activity-type `copy-image` via
    `POST /v1/workspaces/custos.builtin/activity-types` (the manifest
    `metadata.namespace` is `custos.builtin`) with
    `{ "manifest": <activity-manifest as JSON>, "referrerRef": "<image>@<digest>" }`.
- **Digest resolution**: for each extension, resolve the published image digest
  with `docker buildx imagetools inspect <ref> --format '{{json .Manifest}}'`
  (fallback `skopeo inspect docker://<ref>`), inject it into
  `spec.runtime.digest` (activity) / the connector manifest image reference, and
  set `referrerRef`. The script refuses to register against the placeholder
  digest.
- **Idempotency**: a `200` (same digest) is success; a `409` `*_digest_conflict`
  is reported as "bump the version" and is non-fatal only when
  `--allow-existing` is passed; any other non-2xx fails the script with the
  response body. Each registration is logged with its resulting ref.
- **No secrets in the script.** Connector credentials are supplied later when
  the operator creates connector *instances* (the existing connector guides),
  not at type-registration time.

### 4.3 End-to-end runbook

`docs/users/evaluation/copy-image-walkthrough.md` -- a Runme notebook
(`{"cwd":"../../.."}` on shell cells that run from repo root, `{"promptEnv":
"false"}` on the shared-variables cell, per the Runme conventions). Flow:

1. **Prereqs**: link `prerequisites.md` + `install-connected.md` + `verify.md`
   (assumes a deployed eval platform and a platform-admin `TOKEN`).
2. **Publish (or reuse) the images**: note the dedicated publish workflows; for
   a local/dev cluster, how to load locally built `:dev` images.
3. **Onboard**: run `scripts/seed-ootb.sh` to register the two connector-types
   and the `copy-image` activity-type; verify with
   `GET /v1/catalog/connector-types` and `GET .../activity-types`.
4. **Create connector instances**: create a `dockerhub` (source) and a `ghcr`
   (dest) connector instance (cross-link the existing
   `docs/users/connectors/{dockerhub,ghcr}.md` step 3).
5. **Publish + run the copy workflow**: publish a workflow with the
   `custos.builtin/copy-image@0` step (the worked example already in the
   connector guides), `StartRun`, poll the run to completion.
6. **Inspect**: read the run outputs (`destinationRef`, `digest`,
   `manifestsCopied`), the `copy-report` artifact, and the audit log entry.
7. **Troubleshooting** cross-link.

Linked from `docs/users/evaluation/overview.md` / `README.md`.

### 4.4 Guideline & skill updates (D3)

To make "a dedicated publish workflow + onboarding entry + OOTB index row" a
standing requirement for **every** future extension:

- `ootb-catalog.md` section 8 (Build, Test, Publish): replace the high-level
  "Publish" bullet with the per-extension dedicated-workflow + referrer-publish
  contract, and add an onboarding bullet pointing at `seed-ootb.sh`.
- `docs/developers/connector-plugin-author.md` and
  `docs/developers/activity-author.md`: add a "Publish & onboard" section
  requiring (a) a `publish-<kind>-<name>.yml` workflow, (b) a `seed-ootb.sh`
  entry, (c) an `extensions/<kind>s/README.md` index row.
- `.github/skills/implement-component/SKILL.md` and
  `.github/skills/app-design-manager/SKILL.md`: add the same three deliverables
  to the connector/activity definition-of-done so future design + implementation
  plans include them by default.

## 5. Phased Implementation (tracker OOTB-005)

| Task | Deliverable |
|---|---|
| OOTB-PUB-001 | `publish-activity-copy-image.yml` (build/push + SBOM/cosign/SLSA + digest output) |
| OOTB-PUB-002 | `publish-connector-dockerhub.yml` + `publish-connector-ghcr.yml` (build/push + connector-manifest referrer) |
| OOTB-PUB-003 | `scripts/seed-ootb.sh` onboarding script (+ shellcheck) |
| OOTB-PUB-004 | `docs/users/evaluation/copy-image-walkthrough.md` end-to-end runbook |
| OOTB-PUB-005 | Guideline + skill updates (ootb-catalog section 8, author guides, both skills) + design/README links |

## 6. Acceptance Criteria

- Each in-scope extension (`copy-image`, `dockerhub`, `ghcr`) has a dedicated
  publish workflow that pushes a signed, SBOM-annotated image to GHCR and (for
  connectors) publishes the manifest referrer.
- `scripts/seed-ootb.sh` registers all three types into a running catalog,
  idempotently, against real resolved digests, and is shellcheck-clean.
- The runbook renders as a Runme notebook and walks deploy -> onboard -> create
  instances -> run copy -> inspect, with accurate API calls.
- The author guides and both skills require the dedicated workflow + onboarding
  entry + OOTB index row for new extensions.

## 7. Open Items

- Turnkey Helm seeding (auto-registration on install) remains deferred (D1);
  revisit if evaluators find the explicit `seed-ootb.sh` step a friction point.
- Publish workflows for the **pre-existing** reference connectors
  (`oci-registry`, `slack-notifier`) are out of scope here; add them when those
  are next versioned, following the same convention.
