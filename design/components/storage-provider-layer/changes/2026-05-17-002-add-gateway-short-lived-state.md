# Change: add-gateway-short-lived-state

Date: 2026-05-17
Type: component-design
Component: storage-provider-layer
Sequence: 002
GitHub Issue: #70
Status: open

## Summary

Adds two short-lived-state entities to `MetadataStoreProvider` to back the API Gateway: `IdempotencyRecord` (write-endpoint dedup per RFC 9110 `Idempotency-Key`) and `DeviceCodeSession` (OIDC device-code flow for the CLI per RFC 8628). Bumps `MetadataStoreProvider` to schema revision 3.

## Context

The COMP-001 API Gateway design session (2026-05-17) locked two decisions that require persisted state shared across gateway replicas:

- **Decision Q2 (idempotency storage)**: store idempotency records in the SPL `MetadataStoreProvider` rather than in-memory or Redis. Survives restarts, works across replicas, and adds no new infra dependency.
- **Decision Q7 (CLI auth)**: ship the OIDC device-code flow in M1 alongside service tokens. Device-code sessions are short-lived (15 min TTL) but must persist across the polling window and across gateway replicas.

## Impact

- **MetadataStoreProvider contract**: gains three idempotency methods (`reserveIdempotencyRecord`, `completeIdempotencyRecord`, `deleteExpiredIdempotencyRecords`) and five device-code methods (`putDeviceCodeSession`, `getDeviceCodeSessionByDeviceCode`, `getDeviceCodeSessionByUserCode`, `completeDeviceCodeSession`, `deleteExpiredDeviceCodeSessions`). All workspace-scoped except `DeviceCodeSession`, which is keyed by `(deviceCode)` / `(userCode)` because the session predates the user picking a workspace.
- **Schema revision**: `MetadataStoreProvider:3` covers both entities. The migration runner refuses startup if the active adapter does not declare revision 3.
- **Reserve semantics**: `reserveIdempotencyRecord` is atomic — it both inserts the in-progress row and reports back whether the key was already taken (and in what state). Postgres adapter implements via `INSERT ... ON CONFLICT DO NOTHING RETURNING ...` paired with a follow-up read; other backends may use compare-and-swap.
- **Sweeper**: both entity families need a TTL sweep. Reuses the existing sweeper pattern from `deleteExpiredServiceTokens` / `deleteResumeSubscription`.
- **No cross-interface impact**: no change to `DefinitionStoreProvider`, `CatalogStoreProvider`, `ArtifactStoreProvider`, or `AuthStoreProvider`.

## Before / After

**Before**: `MetadataStoreProvider` held only durable runtime state (runs, steps, subscriptions, cursors, audit). Gateway had no persistence story.

**After**: `MetadataStoreProvider` also holds two short-lived-state families with explicit TTL sweepers. The API Gateway is fully replicated and stateless apart from the rate-limiter token bucket (which is intentionally per-replica in v1).

## Files Changed

- `design/components/storage-provider-layer/design.md` — entity-to-interface map, public interface (gateway short-lived state section), migration revision table, change history.
- `design/components/api-gateway/design.md` — added in the parallel API Gateway session; references this new interface surface as its sole persistence boundary.

## Open Follow-ups

- Conformance test suite skeleton must cover both new entity families.
- The atomic reserve semantics need a concrete error mapping in the Postgres adapter (advisory lock vs `ON CONFLICT` race window).
