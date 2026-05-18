# Change: bucket-b-doc-drift

Date: 2026-05-18
Type: component-design
Component: connector-service
Sequence: 012
GitHub Issue: #79, #80, #81, #82, #83, #84
Status: open

## Summary

Bucket B of the design-inconsistency cleanup: bring the connector manifest JSON Schema, the connector-service examples README, the developer-facing examples, and the developer API reference back in sync with prior connector-service change records (002, 004, 005, 008). No new contract decisions are introduced — this is a pure reconciliation pass that closes the drift accumulated as those four changes landed.

## Before

- `connector-manifest.v1.schema.json` listed `events` in `spec.required`, contradicting change 005 (sink-connector events-optional).
- `$defs.capability.description` in the same schema gave `event.push` as an example token, contradicting change 004 (event delivery verbs forbidden from `capabilities`).
- `design/components/connector-service/examples/README.md` still described `spec.identityModels` and `spec.federatedProviders`, both removed in change 002, and claimed "all examples use `type: oci-registry`" even though azure-blob and amazon-s3 examples had been added.
- Change record `2026-05-17-007-sidecar-secret-token-api-contract.md` carried internal `Sequence: 002`, conflicting with `2026-05-16-002-remove-identity-models-and-federated-providers.md`.
- All five pull-mode developer examples under `docs/developers/examples/` omitted the `events.pull` block required by change 008 whenever `delivery` contains `"pull"`; they would fail JSON Schema validation at registration time.
- `docs/developers/connections-api.md` documented `events.delivery` and `events.produced` only, with no mention of the conditionally-required `events.pull` block or the `cursorEncoding` / `initialCursorBehavior` fields. The section also implied `events` was always required.

## After

- `connector-manifest.v1.schema.json`:
  - `spec.required` no longer includes `events`. The existing `allOf/if-then` rule continues to require `events.pull` when `delivery` contains `"pull"`.
  - `$defs.capability.description` references `oci.pull` / `s3.read` and explicitly steers `push` / `pull` to `events.delivery`.
- `design/components/connector-service/examples/README.md` drops the removed identity-model fields, accurately enumerates the three `target.kind` values across the six examples, and references change 001 for the inline target property bag.
- `2026-05-17-007-sidecar-secret-token-api-contract.md` frontmatter is corrected to `Sequence: 007`.
- Five developer example manifests (`amazon-s3-bucket-amazon-kms.json`, `azure-blob-storage-azure-key-vault.json`, `oci-registry-oidc-federated.json`, `oci-registry-azure-key-vault.json`, `oci-registry-azure-managed-identity.json`) now carry the appropriate `events.pull` block (`s3-list-objects-v1`, `azure-blob-list-v1`, or `oci-list-tags-v1` cursor encodings). All eleven manifests across `design/` and `docs/` validate against the v1 schema.
- `docs/developers/connections-api.md` `spec.events` section:
  - Calls out that `events` itself is optional for sink connectors, citing change record 005.
  - Adds a dedicated `events.pull` subsection covering `cursorEncoding` (with migration-trigger semantics) and `initialCursorBehavior` (with the `now` / `beginning` / `custom` enum), plus a complete pull-mode example.
  - Validation checklist is reworked to gate `events.delivery` / `events.produced` items on "if `spec.events` is present" and adds a checklist line for `events.pull`.

## Impact

- A developer copying any pull-mode example from the developer docs now produces a schema-valid manifest. Validation errors at connector registration time will no longer be the developer's first exposure to the `events.pull` requirement.
- Reviewers will no longer have to mentally reconcile schema, prose, and examples: all three agree on the optionality of `events` and the conditional requirement of `events.pull`.
- No connector-service contract or behavior changes. The Connector Service implementation work can rely on the schema as the single normative source.

## Files changed

- `design/components/connector-service/schemas/connector-manifest.v1.schema.json`
- `design/components/connector-service/examples/README.md`
- `design/components/connector-service/changes/2026-05-17-007-sidecar-secret-token-api-contract.md`
- `design/components/connector-service/changes/2026-05-18-012-bucket-b-doc-drift.md` (this file)
- `docs/developers/connections-api.md`
- `docs/developers/examples/amazon-s3-bucket-amazon-kms.json`
- `docs/developers/examples/azure-blob-storage-azure-key-vault.json`
- `docs/developers/examples/oci-registry-azure-key-vault.json`
- `docs/developers/examples/oci-registry-azure-managed-identity.json`
- `docs/developers/examples/oci-registry-oidc-federated.json`

## Related Change Records

- `2026-05-16-002-remove-identity-models-and-federated-providers.md`
- `2026-05-17-004-events-delivery-and-capabilities-separation.md`
- `2026-05-17-005-incon-012-events-block-optional.md`
- `2026-05-17-008-pull-cursor-model.md`
