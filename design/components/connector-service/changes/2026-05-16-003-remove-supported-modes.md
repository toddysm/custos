# Change: remove-supported-modes

Date: 2026-05-16
Type: component-design
Component: connector-service
Sequence: 003
GitHub Issue: —
Status: closed

## Summary

Remove the `supportedModes` field from the connector manifest. Trigger
delivery direction is already encoded by the `event.push` and
`event.pull` capability tokens, which the Trigger Service must already
consume to set up listen streams. `supportedModes` carried no additional
signal and duplicated the same fact in a coarser enum form (`push` /
`pull`) that overlapped confusingly with data-plane `oci.push` /
`oci.pull` semantics.

## Before

```yaml
spec:
  capabilities:
    - oci.pull
    - oci.push
    - event.push
    - event.pull
  supportedModes:
    - push
    - pull
```

Every manifest declared both:
- `event.push` capability ↔ `push` in `supportedModes`
- `event.pull` capability ↔ `pull` in `supportedModes`

## After

```yaml
spec:
  capabilities:
    - oci.pull
    - oci.push
    - event.push
    - event.pull
```

- `spec.required` no longer lists `supportedModes`.
- `supportedModes` property removed from `spec.properties`.
- Trigger Service derives delivery mode support from the presence of
  `event.push` / `event.pull` capability tokens.

## Impact

- `connector-manifest.v1.schema.json` — `supportedModes` removed from
  required list and properties (~12 lines).
- All six example manifests under `examples/` have their
  `supportedModes` blocks stripped.
- `design.md` — YAML example trimmed; bumped to Version 4. No validation
  rule referenced `supportedModes` directly, so no rule wording change
  needed.
- Trigger Service: subscribe/listen logic reads only the capability
  tokens; no separate mode field to consult.
- Instance-level gating (operator wants to disable push delivery even
  though the type supports it) remains a connector instance config
  concern, not a manifest concern.

## Related Requirements

(none directly — manifest simplification)
