# Out-of-the-Box Catalog — Connectors & Activities Structure

Last Updated: 2026-06-25
Status: Accepted (OOTB-001, tracker #880)

This document defines the **decoupled folder structure, file contract, and
build/publish conventions** for the out-of-the-box (OOTB) connectors and
activities that ship with Custos. It is the source of truth for OOTB-001 and
the basis for the developer authoring docs (OOTB-002), the reference
implementations (OOTB-003), and the migration of the existing reference
plugins out of `src/libs/` (OOTB-004).

## 1. Goals and Non-Goals

### Goals

- Keep OOTB connectors and activities **physically decoupled** from platform
   service source (`src/services`, `src/libs`). Nothing in the catalog imports
   platform packages at build time.
- Keep connectors and activities **separated from each other** under a single,
   predictable root.
- Make every extension **independently buildable, testable, and publishable**
   without the platform monorepo's build graph.
- Be **language-agnostic**: the OCI image build recipe is the only required
   build interface. Python is the reference language, not a requirement.

### Non-Goals

- Re-architecting the Connector Service or Activity Runtime Manager contracts.
   Extensions are authored *against* those stable contracts, not the reverse.
- A shared authoring SDK. Deferred — see § 9.
- A public marketplace / registry UI. Future work.

## 2. Decoupling Principles

1. **Contract-only coupling.** An extension depends on the runtime *contract*
   (the connector JSON-on-stdio hook envelope, or the activity `/custos/in`
   `/custos/out` file contract) — never on a platform Python package.
2. **Self-contained folders.** Each extension is one directory that can be
   copied elsewhere and still build and test. Its tests and its documentation
   live inside it.
3. **No reverse imports.** Platform services may reference an extension's
   *artifacts* (e.g. a manifest file path used as a test fixture) but must not
   import an extension's code.
4. **Image is the unit of distribution.** Every extension publishes an OCI
   image plus its manifest; the on-disk source is the buildable origin of that
   image.

## 3. Top-Level Structure

OOTB extensions live under a single repo-root directory, `extensions/`,
separate from `src/`:

```sh
extensions/
  README.md                 # what the catalog is; index of bundled extensions
  connectors/
    <connector-name>/        # one self-contained connector
    ...
  activities/
    <activity-name>/         # one self-contained activity
    ...
```

- `extensions/` is **not** under `src/` and shares no build configuration with
   the platform services.
- `connectors/` and `activities/` are sibling subtrees; they never share code.
- `<name>` is the extension's stable slug (see § 7).

## 4. Connector Extension Layout

A connector is a container-packaged program the Connector Service invokes as
`run --rm -i <image> <hook>` with a JSON request on stdin and a single JSON
response on stdout. Required layout:

```sh
extensions/connectors/<name>/
  connector-manifest.json    # REQUIRED — v1 ConnectorManifest (capabilities, target, credentials, events)
  Containerfile              # REQUIRED — OCI image build recipe (Dockerfile accepted)
  README.md                  # REQUIRED — primary extension doc (see § 6)
  docs/                      # OPTIONAL — extended docs if README grows too large
  app/                       # REQUIRED — implementation (any language)
  tests/                     # REQUIRED — unit + contract tests, self-contained
```

Contract notes:

- `connector-manifest.json` MUST validate against
   [`connector-manifest.v1.schema.json`](../components/connector-service/schemas/connector-manifest.v1.schema.json).
   It is published as the connector-manifest OCI artifact (Referrers API, with
   the digest-derived fallback tag).
- The implementation MUST dispatch the three hooks `bind`, `listen`, `health`
  per the JSON-on-stdio hook wire contract in
  [`docs/developers/connector-plugin-author.md`](../../docs/developers/connector-plugin-author.md)
  (§ 4, The hook wire contract). Connector Service discovery and service
  behavior are described in
   [`design/components/connector-service/design.md`](../components/connector-service/design.md).
- The image entrypoint MUST: read the hook name from argv, read the JSON
   request from stdin, write exactly one JSON response object to stdout, and
   exit 0 even on handled errors (errors are returned as
   `{ "ok": false, "error": { code, detail, data? } }`). This "transport shim"
   guarantees the runtime never has to interpret stderr or non-zero exits.

> The Python reference uses `app/` as the package root containing the
> `handle(hook, request)` dispatcher plus a `__main__` transport shim. Other
> languages provide the equivalent entrypoint; only the wire behavior is
> normative.

## 5. Activity Extension Layout

An activity is a container-packaged program ARM runs in a sandbox; it reads
inputs from `/custos/in` and writes outputs/artifacts to `/custos/out`.
Required layout:

```sh
extensions/activities/<name>/
  activity-manifest.yaml     # REQUIRED — custos.dev/v1 ActivityManifest
  Containerfile              # REQUIRED — OCI image build recipe (Dockerfile accepted)
  README.md                  # REQUIRED — primary extension doc (see § 6)
  docs/                      # OPTIONAL — extended docs if README grows too large
  app/                       # REQUIRED — implementation (any language)
  tests/                     # REQUIRED — unit + contract tests, self-contained
```

Contract notes:

- `activity-manifest.yaml` is a `custos.dev/v1` `ActivityManifest` with
   `metadata` (type, version, namespace, …) and `spec` (runtime, inputs/outputs
   JSON Schema, artifacts, connector slots, resources, isolation, errors). See
   [`design/components/activity-runtime-manager/design.md`](../components/activity-runtime-manager/design.md)
   and the manifest models in
   [`src/services/activity-runtime-manager/src/custos_arm/manifest/models.py`](../../src/services/activity-runtime-manager/src/custos_arm/manifest/models.py).
- The implementation MUST honor the file-based I/O contract: read
   `/custos/in/inputs.json` and `/custos/in/ctx.json`, resolve connector secrets
   from `/custos/in/secrets/<connector>/…` (via the sidecar token when needed),
   and write `/custos/out/outputs.json` plus declared `/custos/out/artifacts/<name>`.
- Manifest-declared artifacts use name-only refs in `outputs.json`; ARM
   finalizes them to artifact-store IDs post-exit. The activity only writes
   files by their declared names.

## 6. Documentation for an Extension

Documentation is organized in **three tiers**, each with a distinct home. The
guiding rule: anything specific to a single extension is **co-located with that
extension** so it moves with the code; anything generic stays centralized.

### 6.1 Per-extension docs — inside the extension folder (REQUIRED)

Each extension's own documentation lives in its directory and travels with it:

```sh
extensions/connectors/<name>/README.md      # primary, REQUIRED
extensions/connectors/<name>/docs/           # OPTIONAL, only if README grows large
extensions/activities/<name>/README.md       # primary, REQUIRED
extensions/activities/<name>/docs/            # OPTIONAL, only if README grows large
```

The `README.md` is the primary doc and MUST cover what is specific to *that*
extension:

- What it does — the external system it brokers (connector) or the unit of work
   it performs (activity).
- **Connector:** declared capabilities, target kind, connection/configuration
   fields, required credentials/secrets, event modes (push/pull) if any.
- **Activity:** inputs/outputs summary, produced artifacts, connector slots it
   binds, supported isolation tier, default resources, documented error codes.
- Build & publish notes (image name, how to build, version history).

Keep the `README.md` self-contained: a third-party author who copies one
extension folder MUST get usable documentation with it, and the platform never
reaches into a service tree to find an extension's docs. Use the optional
`docs/` subfolder only when a single README would become unwieldy.

### 6.2 Generic authoring guides — `docs/developers/` (centralized)

The "how to build *any* connector/activity" guidance is platform-wide, not
per-extension, and stays in the developer docs:

- [`docs/developers/connector-plugin-author.md`](../../docs/developers/connector-plugin-author.md)
- [`docs/developers/activity-author.md`](../../docs/developers/activity-author.md)

These reference the in-folder extension READMEs and use the OOTB extensions as
worked examples (the OOTB-002 work). They MUST NOT duplicate per-extension
reference material.

### 6.3 End-user usage docs — `docs/users/` (centralized)

How an operator *uses* a bundled extension inside a workflow (as opposed to how
it is built) belongs with the other user-facing docs under
[`docs/users/`](../../docs/users/), if and when the OOTB catalog is documented
for end users.

## 7. Naming, Versioning, Namespace

- **Slug** (`<name>`): lowercase kebab-case, stable for the life of the
   extension (e.g. `oci-registry`, `slack-notifier`, `list-tags`). The folder
   name, manifest `metadata.type`, and image repository basename agree.
- **Version**: SemVer in the manifest. The published image is pinned by digest
   at publish time.
- **Namespace**: OOTB extensions authored by the Custos project use the
   `custos.builtin` namespace. Third-party authors use their own namespace and
   `x-<vendor>.*` capability tokens.
- **Capabilities**: connectors declare Tier-1 tokens from
   [`design/architecture/capabilities.md`](capabilities.md) or vendor
   `x-<vendor>.*` tokens.

## 8. Build, Test, Publish

- **Build interface = the image recipe.** Each extension builds with
   `docker build` / `buildah bud` from its own directory using its
   `Containerfile`. No dependency on platform build tooling. (`Dockerfile` is
   accepted; `Containerfile` is the canonical name going forward.)
- **Test interface = the extension's own `tests/`.** Tests must run from inside
   the extension directory with no platform packages on the path. Connector
   tests drive `handle()` and the transport shim; activity tests drive the
   file-contract entrypoint.
- **Manifest validation.** Connector manifests validate against the v1 JSON
   schema; activity manifests parse with the ARM manifest models. CI runs this
   per extension.
- **CI.** A matrix job builds, lints (where applicable), type-checks (where
   applicable), tests, and image-builds each extension, keyed off changes under
   `extensions/**`. This replaces the current `connector-plugins` job that keys
   off `src/libs/connector-plugins/**`.
- **Publish.** Each extension publishes its OCI image and (for connectors) the
   manifest artifact. The on-disk source is the buildable origin.

## 9. Shared SDK — Deferred (No SDK Now)

Decision: __no shared authoring SDK at this time.__ Each extension is fully
standalone and copies the small transport boilerplate (the connector
`__main__` shim is ~70 near-identical lines). Rationale:

- Maximizes decoupling: no shared build/version dependency reintroduced inside
   the catalog.
- A Python-only SDK would not help Go/Rust/other-language authors, conflicting
   with the language-agnostic goal.
- Keeps each extension independently movable and testable (important for the
   low-risk OOTB-004 migration).

A future, optional, **per-language** SDK MAY be added under
`extensions/<lang>-sdk/` if duplication becomes painful — out of scope here.

## 10. Migration of Existing Reference Plugins

The two reference plugins currently under `src/libs/connector-plugins/`
(`oci-registry`, `slack-notifier`) move to `extensions/connectors/<name>/` with
`git mv` (history preserved). Tracked in **OOTB-004 (#884)**. The move requires
updating, in the same change:

- The Connector Service integration test fixture path
   (`_SAMPLE_PLUGINS_DIR` in
   [`test_sample_plugins.py`](../../src/services/connector-service/tests/integration/test_sample_plugins.py)) —
   the test stays in connector-service and references the new location.
- CI path filters and the `connector-plugins` matrix job in
   [`.github/workflows/python-services.yml`](../../.github/workflows/python-services.yml).
- Doc links in
   [`docs/developers/connector-plugin-author.md`](../../docs/developers/connector-plugin-author.md)
   (coordinated with OOTB-002).

## 11. Decisions Record

| Decision | Choice | Notes |
|---|---|---|
| Root directory | `extensions/` | Neutral; avoids collision with the Catalog Service name |
| Connectors vs activities split | `extensions/connectors/` + `extensions/activities/` subfolders | Sibling subtrees, never share code |
| Per-extension documentation | Co-located `README.md` (required) + optional `docs/` | Generic guides in `docs/developers/`, user docs in `docs/users/` (§ 6) |
| Shared SDK | None now | Deferred; optional per-language SDK is future work (§ 9) |
| Language policy | Language-agnostic | Image recipe is the only required build interface; Python is the reference |
| Image recipe filename | `Containerfile` canonical, `Dockerfile` accepted | Minimizes migration churn |
| Existing plugins | Migrate via OOTB-004 (#884) | `git mv`, no `src/libs/connector-plugins/` left behind |

## 12. Acceptance (OOTB-001)

- [x] Top-level structure agreed and documented (`extensions/` + `connectors/`/`activities/`).
- [x] Per-extension required-file contract documented for both connectors and activities.
- [x] Documentation strategy (three tiers) documented (§ 6).
- [x] Decoupling boundary stated (no build-time imports from platform packages).
- [x] Migration decision recorded (OOTB-004).
- [x] Open items (SDK) recorded as deferred.
