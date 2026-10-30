# custos-connector-service

Custos Connector Service (COMP-005).

Connector Service is the access broker between workflows / activities and
external systems. It owns the connector type registry, the connector
instance lifecycle, capability matching at publish, context issuance to
running activities (via the secret-bridge sidecar), and the trigger
listen / pull streams that feed the Trigger Service.

## Status

**Implemented** — the CONN-IMPL-000 milestone ([#318](https://github.com/toddysm/custos/issues/318))
is complete; all 34 child tasks (CONN-IMPL-001 … CONN-IMPL-034) are merged and
the tracking issue is closed. The service implements the full design: the
ConnectorManifest v1 validator + normalizer + OCI Referrers discovery, the
Plugin Loader / connector-type registry, the ConnectorInstance lifecycle +
config validator + activation/health, per-category identity resolvers, the
Context Binder (`BindForStep`) + Lease Manager (TTL precedence, concurrent-lease
cap), the secret-bridge sidecar (UDS token API + mTLS control channel +
container image), the pull cursor + pull-loop scheduler + Listen Manager + Event
Normalizer, the operator revoke flows, the FastAPI REST surface + Internal RPCs
(`BindForStep`, `ValidateConnector`, `SubscribeEvents`, `RefreshLease`), SPL
provider wiring, call-context middleware + permission enforcement, and
observability + audit emission. Backed by a unit + integration suite at the
≥90 % coverage gate (CONN-IMPL-030/031), sample reference plugins
(CONN-IMPL-032), and developer docs
([`docs/developers/connector-plugin-author.md`](../../../docs/developers/connector-plugin-author.md),
CONN-IMPL-033).

Tracking issue: [#318](https://github.com/toddysm/custos/issues/318) (CONN-IMPL-000).

Design reference:
[`design/components/connector-service/design.md`](../../../design/components/connector-service/design.md).

## Configuration

Per the design (§ Internal Structure, § Identity and Credential Model,
§ Operator Admin Surface, § Pull Cursor Model), the runtime reads the
following `CONN_*` environment variables.

| Variable | Required | Default | Description |
|---|---|---|---|
| `CONN_CATALOG_STORE` | Yes | — | `CatalogStoreProvider` binding (Postgres DSN in v1). Used for the `ConnectorTypeVersion` registry. |
| `CONN_METADATA_STORE` | Yes | — | `MetadataStoreProvider` binding (Postgres DSN in v1). Used for `ConnectorInstance`, leases, and pull cursors. |
| `CONN_AUTHZ_ENDPOINT` | Yes (prod) | — | AuthN/AuthZ Service endpoint. Empty switches the call-context middleware to a dev-shim and refuses to start when `ENVIRONMENT=production`. |
| `CONN_CATALOG_ENDPOINT` | Yes | — | Catalog Service endpoint for `ResolveConnectorTypeRef` lookups. |
| `CONN_OCI_REFERRERS_TIMEOUT_MS` | No | `5000` | OCI Referrers API per-request timeout. |
| `CONN_PUBLISH_MAX_BODY_MB` | No | `4` | Maximum ConnectorManifest size at registry write. |
| `CONN_SIDECAR_DEFAULT_TTL` | No | `600` (10 min) | Platform-wide default lease TTL when none is specified. |
| `CONN_SIDECAR_MTLS_ISSUER` | Yes (prod) | — | Issuer name for the sidecar control-channel mTLS certs. |
| `CONN_LEASE_MAX_CONCURRENT` | No | `16` | Per-step concurrent-lease cap. |
| `CONN_PULL_LOOP_MIN_INTERVAL_SEC` | No | `10` | Minimum interval between pull-loop ticks. |
| `CONN_DURABLE_PULL_ENABLED` | No | `false` | Wire durable policy, shared due-time/cursor ownership and private demand reconciliation; requires acknowledged publication and authenticated Trigger identity. |
| `CONN_PULL_DEFAULT_INTERVAL_SEC` | No | `60` | Default for pull starts/waits without an explicit interval. Keep aligned with Trigger's `TRIGGER_POLLER_DEFAULT_INTERVAL_SECONDS`. |
| `ENVIRONMENT` | No | `development` | Operational env tag. The dev-shim refuses to start when this is `production` (case-insensitive). |
| `APP_API_TOKEN` | For private Trigger RPCs | unset | Token shared only with the local Dapr sidecar; empty/blank fails closed. |
| `CONN_TRIGGER_APP_ID` | For private Trigger RPCs | `trigger-service` | Exact authenticated caller application ID, not an HTTP endpoint. |
| `CONN_TRIGGER_NAMESPACE` | For private Trigger RPCs | unset | Exact authenticated caller namespace; required independently of application ID. |

## Private webhook verification

`POST /VerifyWebhook` implements the version-one internal contract for the exact
workspace/connector-instance pair. It requires the authenticated Trigger service
through Dapr 1.18.6+ with mutual Transport Layer Security, even in development.
It does not publish events, dispatch workflows or advance cursors. Credential
resolution may emit the registry's existing audit/telemetry records.

The request contains `schemaVersion: "1"`, `workspaceId`, `connectorInstanceId`,
`bodyBase64` and `headers`. The encoded request is capped at two MiB, decoded
vendor bytes at one MiB, and normalized header names/values plus `": "`/CRLF
overhead at 16 KiB. Verified batches are limited to **1,000 events**, including
invalid entries, to bound response amplification. Larger batches receive
413 `webhook.too_large` before per-index results are constructed; senders must
split batches while retaining their stable event IDs. Duplicate/private identity headers, duplicate JSON keys,
invalid encodings and nonfinite JSON values are rejected. Raw vendor bytes are
authenticated before vendor JSON is parsed.
JSON nesting is bounded at 64 levels (root depth zero); deeper wrappers or verified
vendor payloads return sanitized 400 errors before normalization or serialization.

Add a `webhookVerification` entry to the instance's existing
`credentials_authentication` map, alongside its normal activity credentials:

```json
{
  "webhookVerification": {
    "scheme": "hmac-sha256",
    "authenticationType": "azure-key-vault",
    "authentication": {
      "vaultUri": "https://example.vault.azure.net",
      "secretName": "registry-webhook"
    },
    "materialKey": "secret",
    "signatureHeader": "x-hub-signature-256"
  }
}
```

The referenced existing identity resolver returns the material selected by
`materialKey`. Store only references here, never a signing value. HMAC-SHA256
uses `sha256=<hex>` over the exact raw body; the default header is
`x-custos-signature`. `scheme: "bearer"` instead compares the external
`headers.authorization` bearer using a constant-time comparison. It is not a
Custos identity. Both modes resolve fresh material on each verification, bypassing
old registry cache entries so secret rotation needs no Trigger change.
Fresh reads also bypass cache-miss serialization and do not replace cached
binding credentials; upstream clients retain their configured request timeouts.
Missing or unavailable material fails closed; no alternate scheme is inferred from headers.
Azure Key Vault's enabled, not-before and expiry attributes are enforced before
material is returned; forced refresh never extends an expired secret's lifetime.
Vendor timestamp canonicalization and non-SHA256 signature schemes are not
implemented by these built-in verifiers.

The caller supplies a vendor batch `{"events":[...]}` whose event types are in
the instance's immutable connector manifest. Success returns `schemaVersion`,
`receivedCount`, `events: [{"index":0,"event":{...}}]` and
`rejections: [{"index":1,"code":"missing-event-id"}]`. Every original index
occurs exactly once and valid events retain input order. Rejections contain
only an index and one of `missing-event-id`, `missing-event-type`,
`unknown-event-type`, or `malformed`. Even an all-invalid verified batch returns
this complete partition.

Failures use the existing `{"error":{"code","detail"}}` envelope: 401 for invalid
or missing signatures; 404 for unknown/inactive/non-push pairs; 400 for malformed
wrappers or verified whole payloads (including empty batches); 413 for limits;
503 for unavailable configuration, credentials or storage. Mesh failures are
independently 401/403/503. Credentials, signatures and raw transport headers are
not copied into normalized envelopes or rejection metadata.

Scoped Gateway/Trigger routing landed in
[#1011](https://github.com/toddysm/custos/issues/1011). Automatic production
activation remains [#1017](https://github.com/toddysm/custos/issues/1017); deployed
certification remains [#1018](https://github.com/toddysm/custos/issues/1018).

## Durable pull controls and demand

`CONN_DURABLE_PULL_ENABLED=true` assembles the existing single `PullLoopScheduler`,
`CursorService`, `ListenManager` and acknowledged publisher bridge with the
optional `ConnectorPullControlProvider`. It requires metadata revision **11**;
the shared metadata minimum stays **4**. A no-op publisher cannot be used in this
mode. The flag is default-off: legacy embedded collaborators retain their former
behavior until explicitly upgraded.

Controls persist operator pause, revision, next eligible time, update actor/time,
shared consecutive failures and separate native/Trigger interval contributions.
Demand is the union of active pull-start subscriptions and unexpired associated
pull waits, plus explicit Connector listen registrations. The shortest requested
interval wins subject to the Connector floor. Native deactivation removes only
its own contribution; it never deletes another subscription's or wait's demand.
Subscription pause, deletion, cancellation and expiry likewise cannot clear an
operator pause. Reconciliation rebuilds bounded pages of local registrations
and excludes disabled/inactive instances.

In durable mode, `POST .../pull-loop:pause` and `POST .../pull-loop:resume` commit
policy and the existing audit/outbox event **atomically before acknowledgment**,
including without a local registration. Resume records intent; it does not
shorten a reserved interval. New server-generated instance IDs initialize policy
before the instance becomes visible. Existing policy is never overwritten by
initialization.

Stop/drain legacy pollers before migration and enable durable mode consistently
across replicas. Revision 10 marks known legacy instances paused. Missing legacy
controls also remain blocked; reconciliation never guesses that an active
subscription means the operator intended polling to resume. Revision 11 fences
existing cursor holders, even expired ones, with an unknown legacy executor
locator. Operators must
explicitly confirm resume through the protected admin endpoint.

Each upstream call requires one atomic storage operation checking pause, demand,
database time and cursor ownership. A fresh opaque token fences every attempt,
including re-acquisition by the same process. The operation also persists an
invocation fence and owner **before** starting the plugin. Successful publication/commit
releases ownership but retains the shared interval reservation. Failure keeps
the previous cursor and persists capped exponential backoff; another replica
cannot bypass either reservation. Interval changes apply to future reservations.
The complete plugin/publication/commit path is bounded inside the lease, with
five seconds reserved for container cleanup and another five for failure
finalization. The remaining budget reaches the plugin runner. Each Docker
invocation has a uniquely owned container; timeout and cancellation wait for
the tracked CLI and container cleanup before ownership is released. If cleanup
cannot be confirmed, durable mode attempts an audited safety pause and reports
failures explicitly. The pre-existing invocation fence remains in either case,
blocking new polling **beyond lease and due-time expiry**. Ordinary resume,
registration changes and reconciliation never clear that fence. A verified
stopped owner can finalize its matching attempt even after lease expiry; a stale
owner cannot clear a recovered or replaced invocation.

For recovery, inspect `GET /v1/workspaces/{ws}/connectors/{id}/cursor` with
`connector:read`. Its durable-mode `pullControl` object includes `invocationId`,
`invocationOwner`, `containerName`, `operatorPaused`, `nextEligibleAt` and `revision`.
Locate the named container on that executor. Stop/drain the **old executor as well
as the invocation**, proving neither can resume or launch late; a momentarily
absent container is insufficient. Legacy fences have `containerName: null` and
`invocationOwner: "legacy:unknown"`: drain all possible legacy owners instead of
guessing a container name.

After ownership expires, an `admin:connector` operator can send the observed ID to
`POST /v1/workspaces/{ws}/connectors/{id}/pull-loop:resume`:

```json
{
  "invocationId": "pull:observed-attempt-token",
  "confirmedStopped": true,
  "reason": "Previous executor drained and invocation termination verified"
}
```

Recovery commits confirmation/audit, fence removal and resume atomically without
advancing the cursor or shortening its reservation. A live owner or stale ID
returns 409; malformed or incomplete confirmation returns 422; unavailable storage
returns 503 without recovery. Reinspect after a conflict. A bodyless resume only
changes ordinary pause intent; it is **not** invocation recovery. Unclean crashes
therefore fail closed until termination is confirmed, rather than automatically
retrying after lease expiry.

Local projections retain applied policy revisions after removal and fence
enumeration cleanup with registration generations. An older refresh cannot
delete a newer native activation. Each scheduler pass reads registered controls
in workspace/key batches of at most 100, rather than one transaction per instance;
the final per-tick admission remains atomic in storage.

Private `POST /ReconcilePullDemand` accepts
`{"schemaVersion":"1","workspaceId":"team-a"}` and returns
`{"schemaVersion":"1","registered":N}`. It accepts only the authenticated Trigger
app/namespace after local sidecar-token verification, including in development.
Connector reads current stored intent after serialization; callers cannot supply
an arbitrary interval list or clear pause. The count includes paused advisory
registrations and is not a promise that upstream polling has started. Commands
are bounded to 16 KiB and 60 seconds; Trigger's client allows a 70-second total
budget. Invalid commands return 400, oversized commands 413, unavailable
storage/reconciliation 503, and mesh failures 401/403/503.

These are default-off collaborators and an authenticated command seam, not a
second upstream timer. Production lifecycle/workspace discovery, invoking
reconciliation on restart and intent changes, and remaining production
call-context wiring stay in [#1017](https://github.com/toddysm/custos/issues/1017).
Deployment and resilience certification stay in
[#1018](https://github.com/toddysm/custos/issues/1018).

Process bind:

| Variable | Default | Purpose |
|---|---|---|
| `HOST` | `0.0.0.0` | Address the uvicorn process binds to. |
| `PORT` | `8080` | Port the uvicorn process listens on. |

## Local development

```bash
cd src/services/connector-service
pip install -e ".[dev]"
ruff check .
ruff format --check .
mypy src tests
pytest -q
```

`python -m custos_connector` starts the FastAPI app with the full REST +
Internal RPC surface plus the `/healthz` and `/readyz` probes.

## Layout

```
src/services/connector-service/
├── README.md
├── pyproject.toml
├── openapi.json
├── src/
│   └── custos_connector/
│       ├── __init__.py        # create_app() factory (full app wiring)
│       ├── __main__.py        # uvicorn entry point
│       ├── api/               # FastAPI REST surface + Internal RPC routers
│       ├── manifest/          # ConnectorManifest validator + normalizer
│       ├── loader/            # Plugin Loader + connector-type registry
│       ├── instances/         # ConnectorInstance lifecycle + config validator
│       ├── identity/          # per-category identity resolvers
│       ├── binding/           # Context Binder (BindForStep)
│       ├── lease/             # Lease Manager + audit emission
│       ├── sidecar_admin/     # secret-bridge sidecar control surface
│       ├── cursor/            # pull cursor lifecycle + admin
│       ├── scheduler/         # pull-loop scheduler
│       ├── listen/            # Listen Manager + Event Normalizer
│       ├── validate/          # shared validation helpers
│       ├── middleware/        # call-context middleware + permissions
│       ├── providers.py       # SPL provider wiring + schema-revision gate
│       ├── runtime.py         # plugin runtime adapter
│       ├── _telemetry.py      # OpenTelemetry hooks
│       ├── audit.py           # audit event emission
│       └── py.typed
└── tests/
```
