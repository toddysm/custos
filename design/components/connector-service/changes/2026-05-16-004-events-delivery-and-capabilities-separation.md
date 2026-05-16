# Change: events-delivery-and-capabilities-separation

Date: 2026-05-16
Type: component-design
Component: connector-service
Sequence: 004
GitHub Issue: —
Status: closed

## Summary

Separate data-plane verbs from event-stream concerns in the connector
manifest. The `event.push` and `event.pull` tokens are removed from
`capabilities` and replaced by an explicit `events.delivery` array on
the `events` block. `capabilities` now contains only data-plane verbs
(`oci.pull`, `s3.read`, etc.) consumed by the activity Binder; all
event-stream metadata (delivery mechanisms + produced event catalog)
lives in `events`, consumed by the Listen Manager, Trigger Service, and
Workflow validator.

A new `Capabilities and Events` section in `design.md` documents the
intent, consumer, and lifecycle timing for each of the three fields.

## Before

```yaml
spec:
  capabilities:
    - oci.pull
    - oci.push
    - event.push
    - event.pull
  events:
    produced:
      - oci.image.pushed
      - oci.tag.updated
```

Capabilities mixed data-plane verbs and event delivery modes in a single
flat list. Every consumer (Binder, Listen Manager, Trigger Service) had
to filter by `event.*` prefix to recover the dimension it cared about.

## After

```yaml
spec:
  capabilities:
    - oci.pull
    - oci.push
  events:
    delivery:
      - push
      - pull
    produced:
      - oci.image.pushed
      - oci.tag.updated
```

- `capabilities` contains only data-plane verbs. `event.*` tokens are
  explicitly disallowed by the validation rules.
- `events.delivery` is a required array drawn from `["push", "pull"]`
  declaring which delivery mechanisms the Listen Manager must wire up.
- `events.produced` remains the catalog of normalized event types.

## Impact

- `connector-manifest.v1.schema.json`: `events.required` now lists
  `delivery` and `produced`; `events.properties` gains a `delivery`
  array with enum-constrained items.
- All six example manifests under `examples/`: `event.push` /
  `event.pull` removed from `capabilities`; `events.delivery` added
  matching the prior implicit modes.
- `design.md`: new `Capabilities and Events` section documents purpose,
  consumers, and lifecycle timing for `capabilities`, `events.delivery`,
  and `events.produced`. YAML example updated. Validation rules updated.
  Bumped to Version 5.
- Binder (Connector Service): reads only data-plane verbs from
  `capabilities`. No `event.*` filtering needed.
- Listen Manager (Connector Service): reads `events.delivery` directly.
- Workflow validator: reads `events.produced` unchanged.

## Related Requirements

(none directly — manifest semantics clarification)
