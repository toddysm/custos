# Change: lease-expiry-operator-ux

Date: 2026-05-17
Type: component-design
Component: connector-service
Sequence: 011
GitHub Issue: #63
Status: open

## Summary

Locks the operator-facing surface for credential lease lifecycle: revoke flows (single lease / per-instance / per-run), pull-loop pause/resume, force-health-check, audit query, and the sidecar-side mTLS control-channel API that ARM and Connector Service use to push revoke signals into the sidecar. Establishes the live-state-from-sidecars vs audit-history-from-Observability split. Closes the last v1 design item for COMP-005; component status flips from `Defined` to `Designed`.

## Before

The sidecar contract (closed via #57) locked the *activity-facing* lease lifecycle — TTLs, refresh, release, audit events, the failure-mode table activities consume. It also mentioned that revocation flows through a "separate ARM → sidecar control channel" but did not specify that channel's transport, auth model, request/response shape, or idempotency semantics. There were no operator-facing endpoints documented for:

- Revoking a specific lease, all leases on a connector instance, or all leases for a run.
- Pausing/resuming an instance's pull loop independent of revoke.
- Listing currently-active leases.
- Querying lease audit history.
- Force-invoking a health check.

`todos.md` carried the matching open item. The component status remained `Defined` because of this gap (plus the now-closed capability governance and fallback tag naming items).

## After

§ Public Interface gains 11 new admin endpoints. § Secret and Token Flow to Activities → Revocation expands with a new `Sidecar revoke control-channel API` sub-section specifying the mTLS transport, JSON request/response shape, idempotency contract, and failure modes. Lease audit event taxonomy gains `lease.revoke-requested` (fires before the sidecar call, distinct from `lease.revoked` which fires after the sidecar acks).

A new top-level § Operator Admin Surface section ties everything together:

- **Live state vs audit history split**: live state read from sidecars via fan-out RPC (eventually consistent, fast, no history); audit history read from Observability Service's audit store (durable, queryable, slower). Connector Service does not maintain its own audit store.
- **Revoke selectors**: three selectors (single lease, all-on-instance, all-for-run), each backed by a dedicated endpoint. All revoke endpoints require a mandatory `reason` field; missing/empty returns 400. Reason flows to `lease.revoke-requested` audit, the sidecar control-channel call, and per-lease `lease.revoked` events.
- **Pull-loop lifecycle**: pause/resume/force-health-check. Pause preserves the cursor; cursor-rewind is a separate operation. Pause does not revoke active leases.
- **Permission model**: names three permissions (`connector:read`, `audit:read`, `admin:connector`) defined by COMP-002 Auth Service. Connector Service enforces by name only; role hierarchy belongs to Auth Service.
- **Audit events**: documents the four new operator-facing audit events (`lease.revoke-requested`, `connector.pull-loop.paused`, `connector.pull-loop.resumed`, `connector.health-check.invoked`).

`todos.md`: closes the lease-expiry item; the only remaining open items are the implementation-track connector test harness and manifest example sync. Component registry: COMP-005 flips `Defined` → `Designed`.

## Key Decisions Locked This Session

1. **Live state from sidecars (fan-out), not from a central Connector Service DB.** Sidecar in-memory lease tracking is already authoritative; mirroring it into a central DB introduces dual-write and consistency problems for no real benefit at M1 scale. Cost: list endpoints fan out across N sidecars and are eventually consistent during restarts. An aggregator becomes worthwhile only at thousands-of-concurrent-leases scale, which v1 does not anticipate.
2. **`reason` is mandatory on all revoke endpoints.** Operator-driven revoke without a recorded reason is an auditability hole. Empty/missing → 400. Reason flows to three audit events (`lease.revoke-requested`, the sidecar call, per-lease `lease.revoked`).
3. **Two audit events per revoke (`requested` + `revoked`), not one.** The operator-side request and the sidecar-side result are temporally and causally distinct. Separating them lets investigators reconstruct who-requested-when independently of which-leases-actually-got-revoked (some may have been already-expired).
4. **mTLS on the sidecar control channel, separate port from the activity-facing UDS.** The activity-facing API stays a pure pull surface (no callbacks, no inbound). mTLS via cluster-issued workload certs avoids the SPIFFE/SPIRE dependency (REQ-059 is M3). Default port 9443.
5. **Per-lease idempotent ack from the sidecar.** Revoking a lease that's `not-found` or `already-expired` is not an error — the operator's intent is "ensure these leases are not active", and that's satisfied either way. Returning per-lease status lets operator tooling display which leases were already gone.
6. **Pause/resume preserves the cursor; rewind is a separate operation.** Two independent verbs because operators want them independently: "stop pulling while I investigate" (pause) is distinct from "reset position" (rewind). Forcing them together would mean every investigation-pause loses the cursor.
7. **Single audit-query endpoint, wrapper over Observability Service.** Connector Service does not own an audit store; Observability Service already needs the audit query API for REQ-038. The Connector Service endpoint is a thin domain-filtered wrapper that pre-restricts to lease-related event kinds, so operators don't need to know the full Observability schema.
8. **Permission names declared here; role hierarchy lives in Auth Service.** This design names three permissions and notes which endpoints they gate. The mapping of permissions to roles, role inheritance, and assignment workflow is COMP-002's surface and is not re-litigated here.

## Impact

- ARM has a complete control-channel contract to code against; the sidecar pod template can specify the mTLS port and cert mounts without further design input.
- Operators get a complete surface for credential-incident response: targeted revoke, blast-radius-scoped revoke, audit forensics.
- Pull-loop lifecycle ops (pause/resume) fill the gap between cursor-rewind and full disable/enable for non-emergency operator work.
- Connector Service `todos.md` reduced from 2 design-track open items to 0. Only implementation-track items remain (connector test harness, manifest example sync), neither of which blocks `Designed` status.
- **COMP-005 status: `Defined` → `Designed`.** All v1 design questions resolved.

## Out of Scope (Deferred)

- Cross-workspace bulk operations (revoke leases across multiple workspaces in one call) — M2+ multi-tenant ops concern.
- Lease/cursor admin webhooks (push notifications to operator tooling) — operators query/poll in v1.
- Automatic revoke triggers tied to secret rotation events (rotate connector secret → auto-revoke all leases on that instance) — interesting but needs more design; v1 keeps revoke explicitly operator-driven.
- Bulk pull-loop pause across instances — operators iterate; v1 keeps it per-instance.
- Live-state aggregator service for sub-100ms list latency at scale — added later if N-sidecar fan-out becomes a bottleneck.
- Sidecar control-channel migration to SPIFFE/SPIRE identities — defers with the rest of REQ-059 to M3.

## Related Requirements

- REQ-037 (secrets management; never stored in workflow defs) — operator-facing revoke completes the lifecycle implied by REQ-037.
- REQ-038 (audit log for every secret access) — `lease.revoke-requested` and the existing six lease audit events satisfy operator forensics.
- REQ-059 (SPIFFE/SPIRE workload identity) — explicitly deferred to M3 for the sidecar control channel; v1 uses cluster-issued workload certs.
- ADR-008 (exit codes / failure classes) — 410 Gone on revoked leases (already mapped) is the activity-visible surface of the operator revoke flow.
- ADR-010 (audit retention / tamper-evidence) — `lease.revoke-requested` carries operator identity; retention rules in ADR-010 apply uniformly.
