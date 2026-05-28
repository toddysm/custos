# Custos Sample Connector Plugins

Last Updated: 2026-05-27

This directory holds the reference connector plugins shipped with Custos.
They serve two purposes:

1. **Documentation fixtures** — every code snippet in
   [`docs/developers/connector-plugin-author.md`](../../../docs/developers/connector-plugin-author.md)
   resolves to a real file in this tree, so plugin authors can copy a
   complete working example rather than splicing fragments together.
2. **Integration test fixtures** — the connector-service integration
   suite (`tests/integration/test_sample_plugins.py`,
   [CONN-IMPL-031](https://github.com/toddysm/custos/issues/314)) publishes
   each plugin's `connector-manifest.json` to a fixture OCI registry and
   exercises the end-to-end registration + bind + listen + revoke flow
   against it.

| Plugin | Target kind | Events block | Exercises |
|---|---|---|---|
| [`oci-registry`](oci-registry/) | `oci-registry` | push + pull | Full plugin contract: every capability, both event-delivery modes, the `oci-list-tags-v1` cursor encoding. KMS-backed credentials via Azure Key Vault. |
| [`slack-notifier`](slack-notifier/) | `slack-webhook` | *(absent)* | Minimal sink connector. Exercises the optional-`events` code path. Workload-identity credentials. |

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

See [`docs/developers/connector-plugin-author.md`](../../../docs/developers/connector-plugin-author.md)
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
cd src/libs/connector-plugins/oci-registry
python -m oci_registry_plugin bind <<EOF
{ "apiVersion": 1, "hook": "bind", ... }
EOF
```

The plugin tests can be run independently:

```sh
pip install -e src/libs/connector-plugins/oci-registry[dev]
pytest src/libs/connector-plugins/oci-registry/tests -q
```
