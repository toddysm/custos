# Custos Out-of-the-Box Catalog — Connectors & Activities

Last Updated: 2026-06-30

This directory is the **out-of-the-box (OOTB) catalog**: the connectors and
activities that ship with Custos but are kept **physically decoupled** from the
platform service source (`src/services`, `src/libs`). Nothing here imports a
platform Python package at build time — every extension is independently
buildable, testable, and publishable, and is the canonical worked example the
authoring guides reference.

If you are **authoring** a new extension, start with the getting-started section
below and the two authoring guides:

- [Connector Plugin Author Guide](../docs/developers/connector-plugin-author.md)
- [Activity Author Guide](../docs/developers/activity-author.md)

The structure, file contract, and build/publish conventions are defined
normatively in
[`design/architecture/ootb-catalog.md`](../design/architecture/ootb-catalog.md).

---

## Bundled extensions

### Connectors — [`connectors/`](connectors/)

| Connector | Target kind | Events | Credentials | Summary |
|---|---|---|---|---|
| [`oci-registry`](connectors/oci-registry/) | `oci-registry` | push + pull | Azure Key Vault (KMS) | Full reference connector: every `oci.*` capability, both event-delivery modes, the `oci-list-tags-v1` cursor encoding. |
| [`slack-notifier`](connectors/slack-notifier/) | `slack-webhook` | — | Workload identity | Minimal sink connector; exercises the optional-`events` code path. |
| [`dockerhub`](connectors/dockerhub/) | `oci-registry` | — | `x-dapr-secret` (K8s Secret PAT) | OOTB Docker Hub connector; two-layer token model, live `GET /v2/` health probe. |
| [`ghcr`](connectors/ghcr/) | `oci-registry` | — | `x-dapr-secret` (K8s Secret PAT) | OOTB GitHub Container Registry connector; same two-layer token model targeting `ghcr.io`. |

See [`connectors/README.md`](connectors/README.md) for the connector contract
summary.

### Activities — [`activities/`](activities/)

| Activity | Type | Connector slots | Summary |
|---|---|---|---|
| [`copy-image`](activities/copy-image/) | `copy-image` | `source` (`oci.pull`, `oci.list-referrers`), `dest` (`oci.push`) | Copy an OCI image between two registry connectors. Canonical binding **Docker Hub → GHCR**; registry-agnostic, multi-arch and referrers aware. |

See [`activities/README.md`](activities/README.md) for the activity contract
summary.

---

## What makes an extension

Both kinds share four decoupling principles (full text in
[`ootb-catalog.md` § 2](../design/architecture/ootb-catalog.md)):

1. **Contract-only coupling** — an extension depends on the runtime *contract*
   (the connector JSON-on-stdio hook envelope, or the activity `/custos/in` +
   `/custos/out` file contract), never on a platform package.
2. **Self-contained folders** — one directory that can be copied elsewhere and
   still build and test; its tests and docs live inside it.
3. **No reverse imports** — platform services may reference an extension's
   *artifacts* (e.g. a manifest used as a test fixture) but never import its code.
4. **Image is the unit of distribution** — every extension publishes an OCI
   image plus its manifest; the on-disk source is the buildable origin.

Extensions are **language-agnostic**: the OCI image build recipe is the only
required build interface. Python is the reference language, not a requirement.

---

## Getting started: scaffold a new extension

### 1. Pick a kind and a slug

Choose `connectors/` or `activities/` and a lowercase kebab-case slug that is
stable for the life of the extension (e.g. `my-registry`, `list-tags`). The
folder name, manifest type, and image repository basename all agree.

### 2. Create the required layout

A **connector** (see [layout § 4](../design/architecture/ootb-catalog.md)):

```
extensions/connectors/<name>/
  connector-manifest.json    # REQUIRED — v1 ConnectorManifest
  Containerfile              # REQUIRED — OCI image build recipe (Dockerfile accepted)
  README.md                  # REQUIRED — primary extension doc
  app/                       # REQUIRED — implementation (any language)
  tests/                     # REQUIRED — unit + contract tests, self-contained
  docs/                      # OPTIONAL — only if README grows too large
```

An **activity** (see [layout § 5](../design/architecture/ootb-catalog.md)):

```
extensions/activities/<name>/
  activity-manifest.yaml     # REQUIRED — custos.dev/v1 ActivityManifest
  Containerfile              # REQUIRED — OCI image build recipe (Dockerfile accepted)
  README.md                  # REQUIRED — primary extension doc
  app/                       # REQUIRED — implementation (any language)
  tests/                     # REQUIRED — unit + contract tests, self-contained
  docs/                      # OPTIONAL — only if README grows too large
```

The fastest start is to **copy the closest reference extension** and edit it:

- Bidirectional connector with events → copy [`connectors/oci-registry`](connectors/oci-registry/).
- Registry connector with `x-dapr-secret` credentials → copy [`connectors/dockerhub`](connectors/dockerhub/).
- Minimal sink connector → copy [`connectors/slack-notifier`](connectors/slack-notifier/).
- Activity binding connector slots → copy [`activities/copy-image`](activities/copy-image/).

### 3. Author the manifest

- **Connector:** `connector-manifest.json` MUST validate against
  [`connector-manifest.v1.schema.json`](../design/components/connector-service/schemas/connector-manifest.v1.schema.json).
  Declare capabilities from
  [`capabilities.md`](../design/architecture/capabilities.md) (or `x-<vendor>.*`
  tokens), the target kind, the credential model, and any `events` block.
- **Activity:** `activity-manifest.yaml` is a `custos.dev/v1` `ActivityManifest`
  declaring the inputs/outputs JSON Schemas, connector slots + capabilities,
  resource limits, isolation tier, and error codes.

### 4. Implement against the contract

- **Connector:** implement the `bind` / `listen` / `health` hooks; the image
  entrypoint reads the hook from argv, reads one JSON request from stdin, writes
  exactly one JSON response to stdout, and **exits 0 even on handled errors**
  (errors are `{ "ok": false, "error": { code, detail, data? } }`). Full wire
  contract in the [connector author guide § 4](../docs/developers/connector-plugin-author.md).
- **Activity:** read `/custos/in/inputs.json` + `/custos/in/ctx.json`, resolve
  per-slot secrets from `/custos/in/secrets/<slot>/…`, and write
  `/custos/out/outputs.json` plus declared `/custos/out/artifacts/<name>`. Full
  contract in the [activity author guide](../docs/developers/activity-author.md).

### 5. Test in place

Tests live under the extension's own `tests/` and run with no platform packages
on the path:

```sh
pip install -e extensions/connectors/<name>[dev]
pytest extensions/connectors/<name>/tests -q
```

---

## Build, test, publish lifecycle

| Stage | Interface | Notes |
|---|---|---|
| **Build** | the extension's `Containerfile` | `docker build` / `buildah bud` from the extension directory; no platform build tooling. |
| **Test** | the extension's `tests/` | Runs standalone; connector tests drive `handle()` + the transport shim, activity tests drive the file-contract entrypoint. |
| **Validate manifest** | v1 schema (connector) / ARM models (activity) | Enforced per extension in CI. |
| **CI** | `extensions/**` matrix job | Lints, type-checks, tests, and image-builds each extension on changes under `extensions/**` — see [`.github/workflows/python-services.yml`](../.github/workflows/python-services.yml). |
| **Publish** | `.github/workflows/publish-<kind>-<name>.yml` | Builds + pushes `ghcr.io/<owner>/custos/<name>` on a per-extension version tag with OCI annotations + SBOM/cosign/SLSA; connectors also publish the manifest as an OCI referrer plus the deterministic fallback tag. |
| **Onboard** | [`scripts/seed-ootb.sh`](../scripts/seed-ootb.sh) | Idempotently registers the published connector-/activity-types into a running catalog; every new extension adds an entry. |

The publishing + onboarding contract is specified in
[`design/architecture/ootb-publishing-onboarding.md`](../design/architecture/ootb-publishing-onboarding.md).

---

## See also

- [Developer guide index](../docs/developers/README.md)
- [Connector Plugin Author Guide](../docs/developers/connector-plugin-author.md)
- [Activity Author Guide](../docs/developers/activity-author.md)
- [OOTB catalog structure (design)](../design/architecture/ootb-catalog.md)
- [OOTB publishing & onboarding (design)](../design/architecture/ootb-publishing-onboarding.md)
