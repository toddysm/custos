# Change: incon-010-capability-namespacing

Date: 2026-05-17
Type: component-design
Component: activity-runtime-manager
Sequence: 001
GitHub Issue: #35
Status: open

## Summary

Align the Activity Manifest v1 example and the `spec.connectors[]` field reference in `design/components/activity-runtime-manager/design.md` with the Connector Service authoritative naming rule: capability tokens are **dot-delimited, namespace-prefixed, lowercase** (e.g. `oci.pull`, `oci.push`, `s3.read`). Bare tokens (`pull`, `push`) collided with the `events.delivery` vocabulary and would not match the Binder's capability check at step bind time.

## Before

Activity Manifest v1 example:

```yaml
connectors:
  - name: registry
    type: oci-registry
    required: true
    capabilities: [pull]            # advisory; connector enforces
```

Field reference row for `capabilities`:

> Advisory list (e.g. `[pull]`, `[push]`); the connector itself enforces.

## After

Activity Manifest v1 example:

```yaml
connectors:
  - name: registry
    type: oci-registry
    required: true
    capabilities: [oci.pull]        # advisory; connector enforces. Tokens MUST be dot-namespaced.
```

Field reference row:

> Advisory list of **data-plane verbs** the activity needs from the bound connector (e.g. `[oci.pull]`, `[oci.push, oci.list-tags]`, `[s3.read]`). Tokens MUST follow the dot-delimited lowercase convention defined by the Connector Service. The connector itself enforces; the Binder fails bind if a required capability is missing on the bound connector type version. `event.*` and bare tokens like `pull`/`push` are not valid here.

## Impact

- Activity authors will declare connector capability requirements in the dot-namespaced form the Binder actually checks.
- Eliminates the last bare-token capability example in component-level design — the architecture overview was already fixed by `2026-05-17-004-incon-004-capabilities.md`.
- Removes the bare-`push`/`pull` collision that conflated data-plane verbs with event delivery modes.

## Related Requirements

- `design/components/connector-service/design.md` § Capabilities and Events (authoritative naming rule)
- `design/architecture/changes/2026-05-17-004-incon-004-capabilities.md` (same fix at architecture level)
- Connector Service Change 004 (events-delivery-and-capabilities-separation)
- Issues: #35 (this change), #29 (INCON-004, related architecture fix)
