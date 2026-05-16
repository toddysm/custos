# Connector Manifest Examples

These examples are concrete `ConnectorManifest` payloads that conform to `schemas/connector-manifest.v1.schema.json`.

## Files

- `oci-registry-azure-key-vault-secrets.manifest.json`
- `oci-registry-amazon-kms-secrets.manifest.json`
- `oci-registry-azure-managed-identity.manifest.json`
- `oci-registry-oidc-federated.manifest.json`
- `azure-blob-storage-kms.manifest.json`
- `amazon-s3-bucket-workload.manifest.json`

## Notes

- All examples use `type: oci-registry`.
- Registry target is defined inline at `spec.target`.
- Credential source is defined inline at `spec.credentials`.
- Identity model selection is represented by `spec.identityModels`.
- The OIDC example includes `spec.federatedProviders` because it uses the `federated` identity model.
- Manifest payload is self-contained and does not require external config/secret schema references.
