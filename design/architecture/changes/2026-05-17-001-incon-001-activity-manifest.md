# Change: incon-001-activity-manifest

Date: 2026-05-17
Type: architecture
Sequence: 001
GitHub Issue: #26
Status: open

## Summary

Replace the stale Activity Contract v1 manifest example in `design/architecture/overview.md` with an ARM-aligned `ActivityManifest` schema, and add a forward reference to `design/components/activity-runtime-manager/design.md` § Activity Manifest v1 as the normative source. The previous example used `kind: ActivityType`, nested `spec.versions[]`, flat `requiresConnectorTypes`, external schema file references, capability-style artifact lists, bare `timeoutSeconds`, and omitted required fields (`runtime.digest`, namespace, isolation hints, error catalog). All would fail validation against the Catalog Service, ARM, and CLI publish flow.

## Before

`overview.md` § Activity Contract v1 manifest example:

```yaml
apiVersion: custos.dev/v1
kind: ActivityType
metadata:
  name: vuln-scan
spec:
  versions:
    - version: 2
      runtime: oci-container
      image: ghcr.io/custos/activities/vuln-scan:2.3.1
      requiresConnectorTypes: [oci-registry]
      inputsSchema: ./schemas/vuln-scan.inputs.json
      outputsSchema: ./schemas/vuln-scan.outputs.json
      capabilities:
        produces: [sbom, vuln-report]
      resources:
        cpu: 1
        memory: 1Gi
      timeoutSeconds: 900
```

Trailing note: "deeper specification ... is deferred to a dedicated component-design session — see issue #6 / TODO-004".

## After

`overview.md` § Activity Contract v1 manifest example now mirrors the ARM-locked schema (`kind: ActivityManifest`, `metadata.namespace`, `metadata.version` flat, `spec.contractVersion`, `spec.runtime.{kind,image,digest,isolation}`, inline JSON Schema for `spec.inputs`/`spec.outputs`, `spec.outputs.artifacts[]`, named `spec.connectors[]` slots, `spec.resources.timeout` ISO-8601, `spec.errors[]`). A "Key contract points" bullet list summarises namespace tiers, required digest, isolation hints, fully-qualified workflow references, and JSON-on-wire format, and links to `design/components/activity-runtime-manager/design.md#activity-manifest-v1` as the normative spec.

## Impact

- Eliminates the highest-severity architecture/component inconsistency for activities.
- Developers, contributors, and tools reading the overview will now build against the schema enforced by Catalog publish validation, ARM execution validation, and the CLI publish flow.
- Unblocks the upcoming Workflow Service detailed design — workflow compile-time type-checking can reference a single coherent manifest schema.
- Does not alter the conceptual ER diagram (`ActivityType → ActivityVersion`) — that remains the platform's logical model; only the manifest contract example was stale.

## Related Requirements

- REQ-023 (action contract), REQ-039 (sandbox tech)
- ADR-008 (exit codes)
- Issues: #26 (this change), #6 (original activity contract TODO), #27 (INCON-002, follow-up: short-form refs in examples)
