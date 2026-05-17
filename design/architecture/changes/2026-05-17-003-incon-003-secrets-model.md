# Change: incon-003-secrets-model

Date: 2026-05-17
Type: architecture
Sequence: 003
GitHub Issue: #28
Status: open

## Summary

Remove the `secrets` field from the `ConnectorContext` example in `design/architecture/overview.md` § Connector Contract v1, and document the two credential delivery paths actually locked by the Connector Service and ARM designs: **sidecar API** for live token resolution (with lease + audit), and **filesystem mount** at `/custos/in/secrets/<connector-name>/<key>` for materialized credentials. Add forward references to the normative specs.

## Before

```json
{
  "connectorType": "oci-registry",
  "instanceId": "prod-registry",
  "endpoints": { "api": "https://registry.example.com" },
  "secrets": { "auth": "secret-handle://..." },
  "capabilities": ["push", "pull", "tag", "copy"],
  "version": "1"
}
```

Implied that opaque secret handles live inside the context object and travel through `ctx.json`. No mention of sidecar or filesystem mount.

## After

```json
{
  "connectorType": "oci-registry",
  "instanceId": "prod-registry",
  "endpoints": { "api": "https://registry.example.com" },
  "capabilities": ["push", "pull", "tag", "copy"],
  "version": "1"
}
```

Followed by an explicit "Credentials are not in `ConnectorContext`" paragraph describing the two delivery paths (sidecar RPC with lease scope and audit; tmpfs mount at `/custos/in/secrets/<connector-name>/<key>`) and pointing to:
- `design/components/connector-service/design.md` § Secret and Token Flow to Activities
- `design/components/activity-runtime-manager/design.md` § Activity Contract v1

## Impact

- Closes the three-way disagreement between overview, Connector Service, and ARM on how activities obtain credentials.
- Activity authors will now build against the correct model (sidecar / tmpfs files), not the discarded "handle-in-context" model.
- ARM implementers have an unambiguous signal that `ctx.json` carries no `secrets` field.
- Sets a clean baseline for the upcoming Workflow Service design — step compilation does not need to thread secret handles through workflow context.

## Related Requirements

- Connector Service design § Secret and Token Flow to Activities (authoritative)
- ARM design § Activity Contract v1, sandbox layout (authoritative)
- ADR (secret delivery) — pending
- Issues: #28 (this change), #29 (INCON-004, capabilities namespacing — same Connector Contract section), #34 (INCON-009, secret path format)
