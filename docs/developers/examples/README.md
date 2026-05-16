# Connector Manifest Examples

Last Updated: 2026-05-16

These are reference connector manifests for the supported combinations of target kind and authentication type. Each file is a complete, schema-valid Connector Manifest v1.

For the field-by-field reference, see [../connections-api.md](../connections-api.md).
For the normative schema, see `design/components/connector-service/schemas/connector-manifest.v1.schema.json` in the repository.

## Index

| File | Target kind | Authentication type | Identity category |
|---|---|---|---|
| [oci-registry-azure-key-vault.json](oci-registry-azure-key-vault.json) | `oci-registry` | `azure-key-vault` | `kms` |
| [oci-registry-azure-managed-identity.json](oci-registry-azure-managed-identity.json) | `oci-registry` | `azure-managed-identity` | `workload` |
| [oci-registry-oidc-federated.json](oci-registry-oidc-federated.json) | `oci-registry` | `oidc` | `federated` |
| [amazon-s3-bucket-amazon-kms.json](amazon-s3-bucket-amazon-kms.json) | `amazon-s3-bucket` | `amazon-kms` | `kms` |
| [azure-blob-storage-azure-key-vault.json](azure-blob-storage-azure-key-vault.json) | `azure-blob-storage` | `azure-key-vault` | `kms` |

## How to use these examples

1. Copy the example that best matches your target and authentication setup.
2. Edit `metadata.type` and `metadata.version` to match your connector.
3. Replace `target.endpoint` and `target.config` fields with values for your environment.
4. Replace `credentials.authentication` fields with references to your real secret store or identity.
5. Trim `spec.capabilities` to only the verbs your connector actually implements.
6. Trim `spec.events.delivery` and `spec.events.produced` to reflect what your connector emits.
7. Validate against the JSON Schema before publishing.
