# Custos Sample Connector Plugins

Last Updated: 2026-06-27

This directory holds the reference connector plugins shipped with Custos.
They serve two purposes:

1. **Documentation fixtures** — every code snippet in
   [`docs/developers/connector-plugin-author.md`](../../docs/developers/connector-plugin-author.md)
   resolves to a real file in this tree, so plugin authors can copy a
   complete working example rather than splicing fragments together.
2. __Integration test fixtures__ — the connector-service integration
   suite (`tests/integration/test_sample_plugins.py`,
   [CONN-IMPL-031](https://github.com/toddysm/custos/issues/314)) publishes
   each plugin's `connector-manifest.json` to a fixture OCI registry
   (Zot for the Referrers path, distribution for the fallback-tag path),
   resolves it back through the Connector Service's discovery code, and
   re-validates the retrieved manifest body against the v1 schema. Hook
   flows (`bind` / `listen` / `revoke`) are covered by each plugin's
   unit tests under its own `tests/` directory, not by the integration
   suite.

| Plugin | Target kind | Events block | Exercises |
|---|---|---|---|
| [`oci-registry`](oci-registry/) | `oci-registry` | push + pull | Full plugin contract: every capability, both event-delivery modes, the `oci-list-tags-v1` cursor encoding. KMS-backed credentials via Azure Key Vault. |
| [`slack-notifier`](slack-notifier/) | `slack-webhook` | *(absent)* | Minimal sink connector. Exercises the optional-`events` code path. Workload-identity credentials. |
| [`dockerhub`](dockerhub/) | `oci-registry` | *(absent)* | OOTB Docker Hub connector. `x-dapr-secret` credentials; `bind` shapes the data-plane context (`tokenTypeHint: basic`) and `health` does a live unauthenticated `GET /v2/` reachability probe. |
| [`ghcr`](ghcr/) | `oci-registry` | *(absent)* | OOTB GHCR (GitHub Container Registry) connector. Same two-layer token model as `dockerhub`, targeting `ghcr.io` with a GitHub `write:packages` PAT. |

## Plugin contract (v1)

Every plugin image is invoked by the connector-service runtime through
the Plugin Runtime adapter (`custos_connector.runtime`). The contract:

* The image is run with the hook name as its first argv token
   (`docker run --rm -i <image> <hook>`).

* The plugin reads a single JSON request from stdin and writes a single
   JSON response to stdout.

* Hooks: `bind`, `listen`, `health`.

* Request envelope:

```json
{
  "apiVersion": 1,
  "hook": "bind|listen|health",
  "connector": { "type": "...", "version": "...", "imageRef": "...", "digest": "...", "manifest": { ... } },
  "instance":  { "workspaceId": "...", "instanceId": "...", "type": "...", "version": "...", "name": "...", "enabled": true, "status": "active", "healthStatus": "healthy", "leaseTtlSeconds": 600, "targetConfig": { ... }, "credentialsAuthentication": { ... }, "usedCapabilities": [ ... ] },
  "input": { ... }
}
```

* Response envelope (success):

```json
{ "ok": true, "result": { ... } }
```

* Response envelope (failure):

```json
{ "ok": false, "error": { "code": "upstream-unreachable", "detail": "...", "data": { ... } } }
```

See [`docs/developers/connector-plugin-author.md`](../../docs/developers/connector-plugin-author.md)
for the per-hook result schemas and the full error taxonomy.

## Packaging

Each plugin ships:

* `connector-manifest.json` — pinned against the v1 schema at
   `design/components/connector-service/schemas/connector-manifest.v1.schema.json`.
* `pyproject.toml` — Python project metadata; no external runtime deps.
* `src/<plugin>/__main__.py` — the entry point invoked by the Dockerfile's `ENTRYPOINT`.
* `Dockerfile` — `python:3.12-slim` base, single-stage, runs as a
   non-root user.
* `tests/` — local exercise of the plugin's hook handlers with a stubbed
   identity-material bag and `PluginInvoker`-equivalent JSON shapes.
* `README.md` — pointer back to the design document.

The Dockerfiles publish images that conform to the runtime contract
exactly. The integration job builds both images in CI (see
`.github/workflows/python-services.yml` — `connector-plugins (build)`)
so the build step itself is gated.

## Local development

```sh
cd extensions/connectors/oci-registry
python -m oci_registry_plugin bind <<EOF
{ "apiVersion": 1, "hook": "bind", ... }
EOF
```

The plugin tests can be run independently:

```sh
pip install -e extensions/connectors/oci-registry[dev]
pytest extensions/connectors/oci-registry/tests -q
```
