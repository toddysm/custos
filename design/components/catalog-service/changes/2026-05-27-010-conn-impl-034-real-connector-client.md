# 2026-05-27-010 — CONN-IMPL-034 / CS-IMPL-023: real Connector Service client

**Phase:** M (final phase of #318 connector umbrella)
**Issues:** closes #317 (CONN-IMPL-034), closes #224 (CS-IMPL-023)

## Summary

Replaces the catalog's offline `StubConnectorClient` with a live HTTP
client backed by the Connector Service `ValidateConnector` Internal RPC
(CONN-IMPL-027). The catalog now consults the real Connector Service at
workflow publish time to confirm every `connector:` reference resolves
to a live instance in the target workspace, with failure modes mapped
per design § Failure Modes.

## Decisions

### 1. Use `ValidateConnector` (not `GET workspaces/.../connectors/<id>`)

Per #317 scope the catalog uses the **internal** RPC contract surfaced
by CONN-IMPL-027 — `POST /internal/v1/connectors:validate` with body
`{"mode": "instance", "connectorInstanceId": <name>}` — not the public
workspace-scoped read endpoint. Rationale:

* Internal RPC is what Connector Service exposes for control-plane
  callers and pins the permission to `connector:validate` (vs the
  broader `connector:read`).
* The contract is single-purpose (existence + manifest re-check) so
  the wire shape exactly matches the catalog's question.

### 2. Per-request call-context forwarding via factory pattern

The `ConnectorClient` Protocol stays single-method
(`exists_connector_instance(workspace_id, name)`). To carry the
inbound `x-custos-callctx` header to Connector Service without
threading it through every caller, we add:

* `ConnectorClientFactory` — process-wide owner of the shared
  `httpx.AsyncClient` pool + the negative-result cache.
* `ConnectorClientFactory.for_request(callctx_header_value=...)`
  returns a per-request `HttpConnectorClient` bound to that header.
* `get_connector_client(request)` reads the header off the inbound
  request and asks the factory for a request-scoped view.

This keeps the resolver code unchanged (still consumes
`ConnectorClient`) and avoids leaking the call context into the
protocol — important because `StubConnectorClient` is workspace-only.

### 3. Status-code mapping

| Connector Service response | Catalog behaviour | API outcome |
|---|---|---|
| `200` | Return `True` (instance exists) | Publish proceeds. |
| `400` | Return `True` (existence satisfied; manifest drift) + INFO log. Runtime re-validates per step. | Publish proceeds. |
| `404` | Return `False` + record in negative cache | `400 catalog.publish.resolve` with `resolve.connector_instance_missing`. |
| `5xx` | Raise `ConnectorServiceUnavailable(status_code=...)` | `503 catalog.dependency_unavailable`. |
| Other 4xx (401/403/etc.) | Raise `ConnectorServiceUnavailable` | `503 catalog.dependency_unavailable` (catalog mis-wired or auth gap; not a publish-input problem). |
| `httpx.TimeoutException`, `httpx.TransportError` | Raise `ConnectorServiceUnavailable` | `503 catalog.dependency_unavailable`. |

400 is treated as "exists with config drift" because the catalog's
contract is *existence*, not manifest validity. Connector Service
re-validates the manifest at runtime per step, so swallowing the 400
here cannot mask a runtime error.

### 4. Negative-result cache scope

Cache only the **404** outcome, with a short TTL
(`CAT_CONNECTOR_NEGATIVE_CACHE_TTL_SECONDS`, default 5 s):

* 404 is an authoritative answer — Connector Service knows the
  instance is absent. Caching protects Connector Service when a
  misconfigured caller hammers the publish endpoint with the same
  missing reference.
* 200 / 400 results are NOT cached so a newly-registered instance
  becomes visible to the catalog immediately.
* 5xx results are NOT cached — they're transient and must be surfaced
  to the caller so retries reach a healthy node.

The cache is per-process; we accept the brief inconsistency window
because publishes are batch-bounded (the resolver de-duplicates inside
one publish via `collect_connector_instance_calls`).

### 5. `ConnectorServiceUnavailable` inherits `RuntimeError`

The exception does **not** subclass `ResolveError` / `ValueError`. The
publish pipeline's `except ResolveError:` blocks in
`DefinitionManager._resolve_refs` and `TemplateManager._resolve_refs`
deliberately let `ConnectorServiceUnavailable` propagate unchanged, so
the FastAPI exception handler renders the 503 envelope without it
being re-shaped as a publish-validation 400.

### 6. `StubConnectorClient` retained behind a feature flag

`CAT_USE_STUB_CONNECTOR_CLIENT=true` swaps the live factory for the
offline stub. Production must leave the flag false; the stub logs a
WARNING per process and per publish batch so operators see it clearly.
Use cases:

* Airgapped / offline test environments without Connector Service.
* Local development against a partial stack.

## Configuration

Three new env vars wire the live client (see `README.md` § Configuration):

* `CAT_CONNECTOR_TIMEOUT_SECONDS` — per-call timeout (default `2.0`).
* `CAT_CONNECTOR_NEGATIVE_CACHE_TTL_SECONDS` — 404 cache TTL
  (default `5.0`; `0` disables caching).
* `CAT_USE_STUB_CONNECTOR_CLIENT` — feature flag for the offline
  stub (default `false`).

The Helm chart exposes the same three as `connector.timeoutSeconds`,
`connector.negativeCacheTtlSeconds`, `connector.useStubClient`.

## Test strategy

* **Unit:** `tests/test_connector_client.py` covers status-code
  mapping (200/400/404/5xx/401/403), `httpx.TimeoutException`,
  `httpx.TransportError`, the negative cache (hit, miss, TTL=0
  disabled), call-context header forwarding, and the
  `build_connector_client_factory(use_stub=…)` toggle. All paths use
  `httpx.MockTransport` so no sockets open.
* **Integration:** `tests/integration/test_connector_wire.py` builds a
  tiny Starlette test double exposing
  `POST /internal/v1/connectors:validate`, mounts it via
  `httpx.ASGITransport`, injects the resulting factory into
  `create_app(connector_factory=…)`, and exercises the four wire
  outcomes against the real catalog publish pipeline backed by a
  Postgres testcontainer.

## Files touched

* New: `src/custos_catalog/clients/__init__.py`,
  `src/custos_catalog/clients/connector.py`.
* `src/custos_catalog/resolve.py` — `ConnectorClient`,
  `StubConnectorClient`, `ConnectorServiceUnavailable` re-exported
  from `clients.connector` for API stability.
* `src/custos_catalog/settings.py` — new env vars + `_opt_float` /
  `_opt_bool` helpers.
* `src/custos_catalog/__init__.py` — lifespan builds the factory and
  closes it on shutdown; `create_app(connector_factory=…)` test seam.
* `src/custos_catalog/api/dependencies.py` — `get_connector_client`
  consumes the factory and forwards the callctx header.
* `src/custos_catalog/api/errors.py` — registers
  `ConnectorServiceUnavailable` → 503 handler.
* `README.md` + `deploy/helm/charts/catalog-service/{values,
  templates/configmap}.yaml` + chart README — new env vars / values.
* `design/architecture/components.md` — COMP-005 flipped to
  `Implemented` (Phase M completes the #318 umbrella).
