# Change: target-config-property-bag

Date: 2026-05-16
Type: component-design
Component: connector-service
Sequence: 001
GitHub Issue: —
Status: closed

## Summary

Refactor the connector manifest `target` block to separate kind-agnostic
fields from kind-specific fields. Type-specific properties
(`repositoryNamespace`, `s3Bucket`, `s3Region`, `azureStorageAccount`,
`azureContainer`) are removed from the flat target object and moved into a
generic `target.config` property bag interpreted based on `target.kind`.

This mirrors the existing pattern used by `credentials.authentication`
(generic bag interpreted by `credentials.authenticationType`) and makes the
target model extensible to non-storage target kinds (messaging services,
event brokers, secret stores, etc.) without further bloating the shared
target object.

## Before

`target` carried all type-specific fields flattened on the same level and
relied on `allOf` + `if/then` to forbid the wrong fields per `kind`:

```json
"target": {
  "kind": "oci-registry",
  "endpoint": "https://...",
  "repositoryNamespace": "prod",
  "verifyTls": true
}
```

Per-kind requirements enforced:
- `oci-registry` requires `repositoryNamespace`.
- `azure-blob-storage` requires `azureStorageAccount` and `azureContainer`.
- `amazon-s3-bucket` requires `s3Bucket` and `s3Region`.

## After

`target` keeps only common fields (`kind`, `endpoint`, `verifyTls`) plus a
required `config` object whose closed sub-schema is selected by `kind`.
Field names inside `config` drop the redundant type prefix since `kind`
already disambiguates them.

```json
"target": {
  "kind": "oci-registry",
  "endpoint": "https://...",
  "verifyTls": true,
  "config": {
    "repositoryNamespace": "prod"
  }
}
```

```json
"target": {
  "kind": "amazon-s3-bucket",
  "endpoint": "https://s3.us-east-1.amazonaws.com",
  "verifyTls": true,
  "config": {
    "bucket": "custos-artifacts-prod",
    "region": "us-east-1"
  }
}
```

```json
"target": {
  "kind": "azure-blob-storage",
  "endpoint": "https://custosstorage.blob.core.windows.net",
  "verifyTls": true,
  "config": {
    "storageAccount": "custosstorage",
    "container": "supply-chain-artifacts"
  }
}
```

Per-kind `target.config` schemas (closed, `additionalProperties: false`)
are defined in `$defs` and selected via `allOf` / `if (kind == X) then
config: $ref X-config`.

## Impact

- `connector-manifest.v1.schema.json` — `target` properties restructured;
  new `$defs` entries for `ociRegistryConfig`, `azureBlobStorageConfig`,
  and `amazonS3BucketConfig`.
- All six example manifests under `examples/` updated to the new shape.
- `design.md` Plugin Manifest v1 YAML example and validation requirements
  bullets updated; document bumped to Version 2.
- Plugin authors must be informed before contractVersion `1` ships; the
  manifest contract is still in draft so no backward-compatibility shim is
  needed.
- No impact on `credentials`, `events`, capability tokens, or identity
  models — these blocks are unchanged.

## Related Requirements

(none directly — derives from connector manifest design)
