# Change: incon-012-events-block-optional

Date: 2026-05-17
Type: component-design
Component: connector-service
Sequence: 005
GitHub Issue: #37
Status: open

## Summary

Make the `events` block on the connector manifest **optional** so that sink/data-plane-only connectors (e.g. Slack/Teams notification, write-only blob targets) can omit it entirely. When the block IS present, both `events.delivery` and `events.produced` must each have at least one entry. Adds a sink-connector example to the design and tightens the validation prose accordingly. This is the Connector Service design source-of-truth update behind INCON-012; the normative JSON Schema and existing OCI example files in `schemas/` and `examples/` should be updated in a follow-on schema-sync change.

## Before

Validation prose treated `events` as if it were always present:

> - `events.delivery` enumerates the delivery mechanisms ... At least one entry is required.
> - `events.produced` enumerates the catalog of normalized event types ...

Section heading prose:

> An array drawn from `["push", "pull"]`. Declares the delivery mechanisms the connector supports.

Only one manifest example existed (OCI registry, with `events`).

## After

Validation prose:

> - The `events` block is **optional**. Sink/data-plane-only connectors (e.g. Slack, Teams, Email notification connectors, write-only blob targets) omit it entirely. The Listen Manager treats connector type versions with no `events` block as non-event-producing and skips trigger wiring for them.
> - When the `events` block is present:
>   - `events.delivery` ... at least one entry is required.
>   - `events.produced` ... at least one entry is required.

`events.delivery — how events arrive` section prose:

> The `events` block as a whole is optional; connectors that do not produce events (sinks, notification targets, write-only data planes) omit it. When present, `events.delivery` is an array drawn from `["push", "pull"]` ...

Added a complete sink-connector example (Slack notifier) immediately after the OCI registry example showing a manifest with no `events` block.

## Impact

- Connector plugin authors can now ship pure-sink connectors (notification, write-only storage) without inventing placeholder `events.delivery` / `events.produced` arrays.
- Listen Manager has an explicit, documented skip path for non-event-producing connector type versions.
- Binder behavior unchanged — `capabilities` is still authoritative at step bind time regardless of whether `events` is present.
- Closes a long-standing ambiguity flagged across reviews of the Slack/Teams notification activity proposals.
- Normative JSON Schema (`schemas/connector-manifest.v1.schema.json`) and `examples/` need a follow-on sync change to make `events` optional and add the sink example; tracked separately so this PR stays scoped to design prose.

## Related Requirements

- `design/components/connector-service/design.md` § Plugin Manifest v1, § Capabilities and Events
- Connector Service Change 003 (remove-supported-modes)
- Connector Service Change 004 (events-delivery-and-capabilities-separation)
- Issues: #37 (this change)
