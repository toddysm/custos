# Connector Manifest Examples

These examples are concrete `ConnectorManifest` payloads that conform to `schemas/connector-manifest.v1.schema.json`.

## Files

- `oci-registry-azure-key-vault-secrets.manifest.json`
- `oci-registry-amazon-kms-secrets.manifest.json`
- `oci-registry-azure-managed-identity.manifest.json`
- `oci-registry-oidc-federated.manifest.json`
- `azure-blob-storage-kms.manifest.json`
- `amazon-s3-bucket-amazon-kms.manifest.json`

## Notes

- Examples cover three `target.kind` values: `oci-registry` (four files), `azure-blob-storage` (`azure-blob-storage-kms.manifest.json`), and `amazon-s3-bucket` (`amazon-s3-bucket-amazon-kms.manifest.json`).
- Target configuration is defined inline at `spec.target` as a property bag with common fields `kind`, required `endpoint`, optional `verifyTls`, and required `config` (see change 001).
- Credential mode is defined inline at `spec.credentials.authenticationType`.
- Credential details are provided inline at `spec.credentials.authentication`.
- Manifest payload is self-contained and does not require external config/secret schema references.
