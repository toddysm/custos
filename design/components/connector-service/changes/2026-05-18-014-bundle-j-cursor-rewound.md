# Change: bundle-j-cursor-rewound

Date: 2026-05-18
Type: component-design
Component: connector-service
Sequence: 014
GitHub Issue: #103
Status: open

## Summary

Aligned the Connector Service admin-rewind documentation with SPL's `rewindCursor` audit-emitter model and removed a stale reference to a Trigger Service dedup-clear admin API that does not exist in v1. The audit event taxonomy is now split cleanly between `cursor.advanced` (emitted only on normal ticks via `commitCursor`) and `cursor.rewound` (emitted only by SPL's `rewindCursor(instanceId, newValue, actor, reason)` for operator-initiated rewinds). The v1 dedup-replay workaround (wait for `DedupKey` TTL or rewind past the dedup window) is documented in place, and a Trigger Service TODO-007 placeholder is referenced for the future selective dedup-clear admin API.

## Before

- The admin rewind procedure stated that the operator "clears matching Trigger Service `DedupKey` entries" as part of replay, implying an admin API that has no design or implementation in v1.
- The audit events table conflated tick-driven cursor advance and operator-driven rewind under a single naming convention, which contradicted SPL's normative split (`cursor.advanced` vs `cursor.rewound`).

## After

- Admin rewind paragraph rewritten to call SPL's `rewindCursor(instanceId, newValue, actor, reason)` as the single audit-emitting entry point; the SPL adapter emits exactly one `cursor.rewound` event with `{from, to, actor, reason}` context.
- The "operator clears matching Trigger Service `DedupKey` entries" sentence is removed. In its place, the design documents the v1 workaround (wait for the dedup window TTL, or rewind to a point before the window) and references Trigger Service TODO-007 (selective dedup-clear admin API, deferred to M2+).
- Audit events table split:
  - `cursor.advanced` — emitted on normal tick commits via `commitCursor`.
  - `cursor.rewound` — emitted exclusively by SPL's `rewindCursor` on operator-initiated rewinds; carries `{from, to, actor, reason}`.

## Impact

- Audit consumers must distinguish `cursor.advanced` from `cursor.rewound` (already implicit in SPL emitters; now normatively documented on the producer side).
- Operators executing a rewind in v1 must accept the documented dedup-replay workaround until Trigger Service TODO-007 lands.
- No SPL interface changes — this is a documentation alignment.

## Files changed

- `design/components/connector-service/design.md` v8 → v9 (Admin rewind paragraph; Audit events table; Change History)

## Related Change Records

- Trigger Service: `2026-05-18-005-bundle-j-cursor-rewound.md` (companion — adds TODO-007 for the dedup-clear admin API).
