# Change: remove-identity-models-and-federated-providers

Date: 2026-05-16
Type: component-design
Component: connector-service
Sequence: 002
GitHub Issue: —
Status: closed

## Summary

Remove the redundant `identityModels` and `federatedProviders` top-level
fields from the connector manifest. The identity category (KMS-backed,
workload, federated) is fully derivable from the concrete
`credentials.authenticationType` value, and the v1 schema already
hardcoded that mapping via `allOf` / `if-then` cross-checks. Carrying
both fields forced manifest authors to repeat information already
present in `authenticationType` and was a pure validation tax.

The Connector Service now owns the `authenticationType` → identity
category lookup. Vendor extension auth types (`x-*`) register their
category at plugin-registration time as out-of-band metadata rather than
in the manifest payload.

## Before

```yaml
spec:
  credentials:
    authenticationType: oidc
    authentication:
      issuer: https://token.actions.githubusercontent.com
      audience: https://ghcr.io
      subjectTemplate: repo:my-org/my-repo:ref:{ref}
  identityModels:
    - federated
  federatedProviders:
    - oidc
  events:
    produced:
      - oci.image.pushed
```

Schema required `identityModels` and conditionally `federatedProviders`,
with four `allOf` / `if-then` blocks cross-checking
`authenticationType` against `identityModels` membership.

## After

```yaml
spec:
  credentials:
    authenticationType: oidc
    authentication:
      issuer: https://token.actions.githubusercontent.com
      audience: https://ghcr.io
      subjectTemplate: repo:my-org/my-repo:ref:{ref}
  events:
    produced:
      - oci.image.pushed
```

- `spec.required` no longer lists `identityModels`.
- `identityModels` and `federatedProviders` properties removed from
  `spec.properties`.
- `spec.allOf` cross-check block removed entirely.
- `$defs.identityModel` and `$defs.providerName` removed (unused).
- Identity category lookup table moved into `design.md` as a service
  responsibility, not a manifest field.

## Impact

- `connector-manifest.v1.schema.json` — net removal of ~140 lines
  (properties, required, allOf, $defs).
- All six example manifests under `examples/` have their
  `identityModels` / `federatedProviders` blocks stripped.
- `design.md` — YAML example trimmed; validation requirements rule about
  `federatedProviders` replaced with a note that the Connector Service
  derives the identity category from `authenticationType`; the Identity
  and Credential Model section now documents the explicit mapping
  table; bumped to Version 3.
- Connector Service runtime responsibility added: maintain the
  authenticationType → identity category lookup, and require `x-*`
  extension auth types to register their category at registration time.
- No impact on `target`, `credentials.authentication`, capabilities, or
  events.

## Related Requirements

(none directly — derives from connector manifest design simplification)
