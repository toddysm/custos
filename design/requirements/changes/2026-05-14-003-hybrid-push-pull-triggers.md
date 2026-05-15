# Requirements Change: Hybrid Push/Pull Trigger Ingestion

Date: 2026-05-14
Author: Custos design
Status: Proposed
Related Issue: pending

## Summary

Add an explicit, first-class requirement that trigger ingestion supports **both push (event/webhook) and pull (polling)** modes for external sources that can fire workflows. Users or operators choose the mode per trigger; polling is the fallback when a source cannot push events reliably (or at all).

This generalizes TODO-003 (which was scoped to OCI registries) to a platform-wide capability.

## New Requirements

| ID | Requirement | Priority | Status | Added |
|---|---|---|---|---|
| REQ-079 | Trigger ingestion must support both push (event/webhook) and pull (polling) modes for any external source that can fire workflows (OCI registries, storage, databases, generic HTTP endpoints, etc.). Mode is selectable per trigger; polling is the fallback when the source does not reliably push events. Both modes deliver into the same normalized event pipeline and the same dedup/idempotency layer. | High | Open | 2026-05-14 |

## Rationale

- Not every external system pushes events. Many OCI registries, storage backends, and SaaS systems either lack webhooks or have unreliable/asymmetric delivery.
- Forcing webhook-only ingestion would limit which sources can trigger workflows and force users to build sidecar pollers themselves.
- Treating polling as a first-class trigger mode keeps the platform usable on day one against any source, and lets connector plugins declare which mode(s) they support via capabilities.

## Impact

- **Requirements doc:** add REQ-079, bump version 3 → 4.
- **Architecture (overview.md):**
  - Trigger Pipeline section: clarify that receivers come in two flavors (push / pull) for every source category, not just registries.
  - Connector Contract v1: `describe()` and `listen()` cover this — `listen()` can be implemented as a long-poll loop; capabilities advertise `push`, `pull`, or both.
  - Trigger Service (COMP-004): pollers are a first-class peer of webhook receivers.
- **Milestones:** registry polling alongside registry webhooks lands in M2 (already implied by TODO-003; now explicit at the platform level). Generic source polling for other connector types follows the connector-extensibility track (M2+).
- **TODO-003** (registry-specific push vs. pull decision) remains open as the concrete realization for OCI registries.

## Open Questions

- Default polling intervals and backoff strategy per source category.
- How to surface "missed events" semantics when transitioning a trigger between push and pull modes.
- Whether per-trigger polling state belongs in `MetadataStoreProvider` or a dedicated trigger-state store.
