# Reference OCI-registry connector plugin

Reference implementation of a full-fat connector plugin targeting an OCI
registry. Exercises every Tier-1 OCI capability advertised in the
Custos capability namespace and both `push` + `pull` event-delivery
modes.

* **Manifest**: [`connector-manifest.json`](connector-manifest.json) — validated
  against `design/components/connector-service/schemas/connector-manifest.v1.schema.json`.
* **Hooks**: implemented in
  [`src/oci_registry_plugin/plugin.py`](src/oci_registry_plugin/plugin.py).
* **Wire contract**: documented in
  [`docs/developers/connector-plugin-author.md`](../../../../docs/developers/connector-plugin-author.md).

## Building the image

```sh
docker build -t custos-sample/oci-registry-plugin:1.0.0 \
    -f src/libs/connector-plugins/oci-registry/Dockerfile \
    src/libs/connector-plugins/oci-registry
```

## Running a hook locally

```sh
docker run --rm -i custos-sample/oci-registry-plugin:1.0.0 health <<'EOF'
{
  "apiVersion": 1,
  "hook": "health",
  "connector": {
    "type": "custos-oci-registry",
    "version": "1.0.0",
    "imageRef": "ghcr.io/example/custos-oci-registry:1.0.0",
    "digest": "sha256:abc",
    "manifest": { "spec": { "target": { "endpoint": "https://registry.example.com" } } }
  },
  "instance": {
    "workspaceId": "ws-1",
    "instanceId": "inst-1",
    "type": "custos-oci-registry",
    "version": "1.0.0",
    "name": "prod",
    "enabled": true,
    "status": "active",
    "healthStatus": "unknown",
    "leaseTtlSeconds": 600,
    "targetConfig": { "repositoryNamespace": "team-a" },
    "credentialsAuthentication": {},
    "usedCapabilities": ["oci.pull"]
  },
  "input": {}
}
EOF
```

## Tests

```sh
pip install -e .[dev]
pytest -q
```
