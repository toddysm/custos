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
| `ENVIRONMENT` | No | `development` | Operational env tag. The dev-shim refuses to start when this is `production` (case-insensitive). |
| `APP_API_TOKEN` | For `VerifyWebhook` | unset | Token shared only with the local Dapr sidecar; empty/blank fails closed. |
| `CONN_TRIGGER_APP_ID` | For `VerifyWebhook` | `trigger-service` | Exact authenticated caller application ID, not an HTTP endpoint. |
| `CONN_TRIGGER_NAMESPACE` | For `VerifyWebhook` | unset | Exact authenticated caller namespace; required independently of application ID. |

## Private webhook verification

`POST /VerifyWebhook` implements the version-one internal contract for the exact
workspace/connector-instance pair. It requires the authenticated Trigger service
through Dapr 1.18.6+ with mutual Transport Layer Security, even in development.
It does not publish events, dispatch workflows or advance cursors. Credential
resolution may emit the registry's existing audit/telemetry records.

The request contains `schemaVersion: "1"`, `workspaceId`, `connectorInstanceId`,
`bodyBase64` and `headers`. The encoded request is capped at two MiB, decoded
vendor bytes at one MiB, and normalized header names/values plus `": "`/CRLF
overhead at 16 KiB. Duplicate/private identity headers, duplicate JSON keys,
invalid encodings and nonfinite JSON values are rejected. Raw vendor bytes are
authenticated before vendor JSON is parsed.

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

This adds a verification seam, not public gateway routing or automatic receiver
activation. Those remain [#1011](https://github.com/toddysm/custos/issues/1011)
and [#1017](https://github.com/toddysm/custos/issues/1017); full deployed
certification remains [#1018](https://github.com/toddysm/custos/issues/1018).

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
