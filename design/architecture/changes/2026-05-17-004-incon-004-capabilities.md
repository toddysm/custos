# Change: incon-004-capabilities

Date: 2026-05-17
Type: architecture
Sequence: 004
GitHub Issue: #29
Status: open

## Summary

Update the `ConnectorContext` example in `design/architecture/overview.md` § Connector Contract v1 to use **dot-delimited, namespace-prefixed** capability tokens (`oci.pull`, `oci.push`, `oci.tag`, `oci.copy`) per the Connector Service authoritative naming rule. Add a sentence clarifying that `capabilities` carries only data-plane verbs and that event delivery modes live in `events.delivery` on the connector manifest — restoring the data-plane / event-plane separation locked by Connector Service changes 003 and 004.

## Before

```json
"capabilities": ["push", "pull", "tag", "copy"]
```

Bare tokens collided with the `events.delivery: ["push", "pull"]` vocabulary, recreating the ambiguity that Connector Service changes 003 and 004 were specifically introduced to remove.

## After

```json
"capabilities": ["oci.pull", "oci.push", "oci.tag", "oci.copy"]
```

Followed by:

> `capabilities` lists only **data-plane verbs** in dot-delimited lowercase form (e.g. `oci.pull`, `oci.push`, `s3.read`, `blob.write`). Event-stream concerns — including whether a connector delivers trigger events via inbound webhook or polling — live in a separate `events.delivery` field on the connector manifest and **must not** appear in `capabilities`. See `design/components/connector-service/design.md` § Capabilities and Events for the full treatment.

## Impact

- Eliminates the four-token capability mismatch between the overview and the Connector Service authoritative spec.
- Activity authors will declare connector capability requirements in the dot-namespaced form the Binder actually checks against.
- Connector plugin authors will write manifests that pass schema validation at registration.
- Removes the bare-`push`/`pull` collision that conflated data-plane verbs with event delivery modes.
- Leaves INCON-010 (ARM manifest `capabilities: [pull]` vs `[oci.pull]`) as a separate fix in the ARM component design.

## Related Requirements

- Connector Service design § Capabilities and Events (authoritative)
- Connector Service Change 003 (remove-supported-modes)
- Connector Service Change 004 (events-delivery-and-capabilities-separation)
- Issues: #29 (this change), #28 (INCON-003, same section), #35 (INCON-010, related fix in ARM manifest)
