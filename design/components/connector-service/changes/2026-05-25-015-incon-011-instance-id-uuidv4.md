# Change: incon-011-instance-id-uuidv4

Date: 2026-05-25
Type: component-design
Component: connector-service
Sequence: 015
GitHub Issue: #294
Status: open

## Summary

Align `ConnectorInstance.instanceId` server-side generation with the platform
convention used elsewhere in the codebase: **UUIDv4** (via `uuid.uuid4()`).
Issue #294's original acceptance criteria called for ULID; this change
supersedes that requirement. The acceptance criteria on #294 is updated to
match, and the implementation in `custos_connector.instances.InstanceService`
remains as shipped.

## Before

- Issue #294 acceptance criteria: "Server-side `instanceId` generation (ULID)
  on create; client-supplied IDs rejected."
- No corresponding requirement appears in
  `design/components/connector-service/design.md`; the design is silent on
  ID format.
- No ULID library is referenced anywhere in the repo. All other
  server-generated identifiers — `ServiceTokenId`, `RoleBindingId`,
  `event_id` in audit emitters across catalog/auth/connector services,
  CallContext `jti` — use `uuid.uuid4()`.

## After

- `ConnectorInstance.instanceId` is generated server-side as `str(uuid4())`
  in `InstanceService.create()`. Client-supplied IDs continue to be rejected
  (`create()` does not accept an `instance_id` parameter).
- `custos_spl.ids.ConnectorInstanceId` remains an opaque `NewType("…", str)`,
  so consumers continue to treat the value as an opaque string.
- Issue #294 acceptance criteria updated to: "Server-side `instanceId`
  generation (UUIDv4) on create; client-supplied IDs rejected."

## Rationale

- **Consistency** with the established platform pattern (all other
  server-generated IDs use UUIDv4).
- **No new dependencies** — `uuid` is in the standard library; ULID would
  require pulling in `python-ulid` (or similar) for a single identifier.
- **Opaqueness** is preserved either way; the `ConnectorInstanceId` NewType
  hides the underlying format from consumers, so a future migration to ULID
  is straightforward if/when sortability becomes a requirement (e.g. for
  cursor pagination on instance lists — currently handled via
  `(created_at DESC, instance_id ASC)` indexes in the Postgres adapter).

## Impact

- No SPL interface changes.
- No Postgres schema changes (column is `text` either way).
- Operator-visible ID format changes from "26-char base32" to "36-char
  hyphenated UUID" relative to the original acceptance criteria; no
  operator-facing documentation referenced the ULID format, so no docs
  update is required.

## Files changed

- This change record (new).
- GitHub Issue #294 body: acceptance criteria line updated from "(ULID)"
  to "(UUIDv4)".
