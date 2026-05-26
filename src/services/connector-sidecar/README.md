# Custos Connector Sidecar (COMP-005 sidecar, Phase H)

Per-pod secret-bridge that serves activity-token leases to an activity
container over a Unix Domain Socket.

The sidecar is co-deployed by the Activity Runtime Manager (ARM) into
the same pod as the activity container. It exposes three endpoints on
a UDS at `/custos/run/connector.sock` (mode `0600`, owner `sidecar
UID`, group `activity UID`):

| Method | Path | Purpose |
|---|---|---|
| `GET`  | `/v1/token`         | Mint a lease + upstream credential for a slot/purpose. |
| `POST` | `/v1/token/refresh` | Re-mint while keeping the lease id stable. |
| `POST` | `/v1/token/release` | Best-effort release. |

Every request must carry `Custos-Sidecar-Token: <bootstrap>` — an
HMAC-SHA256-signed envelope ARM minted at sidecar start, bound to the
specific `(runId, stepId, attempt)` triple.

Lease bookkeeping (capacity tracking + audit emission) is delegated
to the Connector Service over `/internal/v1/leases:{issue,refresh,release}`.
The sidecar mints the upstream credential locally (KMS integration
arrives in a follow-up ticket; CONN-IMPL-019 ships a stub minter).

See [design/components/connector-service/design.md § Secret and Token
Flow to
Activities](../../../design/components/connector-service/design.md#secret-and-token-flow-to-activities)
for the normative contract.

## Phase H scope split

| Issue | Title | Ships |
|---|---|---|
| #302 (CONN-IMPL-019) | Sidecar UDS server | This package + integration harness |
| #303 (CONN-IMPL-020) | Sidecar mTLS revoke control-channel | Adds `/sidecar-admin/v1/revoke` |
| #304 (CONN-IMPL-021) | Container image + standalone integration | Dockerfile + e2e wiring |
