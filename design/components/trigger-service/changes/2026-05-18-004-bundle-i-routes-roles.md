# Change: bundle-i-routes-roles

Date: 2026-05-18
Type: component-design
Component: trigger-service
Sequence: 004
GitHub Issue: #99
Status: open

## Summary

Aligned Trigger Service public REST API with the workspace-scoped URL convention `/v1/workspaces/{workspaceId}/...`, and relocated the public webhook ingress description to the API Gateway design (single source of truth for gateway-mounted public paths). The Generic Webhook Receiver inside Trigger Service is now described as the subscription demux: API Gateway accepts `POST /v1/webhooks/{connectorInstanceId}`, terminates TLS, and **forwards anonymously** (no call-context, no HMAC/token verification). HMAC or token verification is performed by Trigger Service's Generic Webhook Receiver (or its connector plugin per TODO-006) against the connector-instance config, and then the receiver fans the validated payload out to all matching subscriptions on that instance.

## Before

- Trigger CRUD endpoints in the Public Interface section were rooted at `/v1/triggers/...` with workspace passed implicitly via call context, inconsistent with workspace-scoped paths used by other components.
- The webhook ingress row sat in the Trigger Service REST table as if Trigger Service owned the public mount point, conflicting with the API Gateway design which terminates `/v1/webhooks/{connectorInstanceId}` at the gateway.
- Module Responsibilities described the Generic Webhook Receiver as the public HTTP entry, blurring where the public mount lives (gateway vs. Trigger Service) and where HMAC/token verification runs.

## After

- Trigger CRUD and manual fire endpoints are workspace-scoped:
  - `POST   /v1/workspaces/{ws}/triggers`
  - `GET    /v1/workspaces/{ws}/triggers/{id}`
  - `PATCH  /v1/workspaces/{ws}/triggers/{id}`
  - `DELETE /v1/workspaces/{ws}/triggers/{id}`
  - `POST   /v1/workspaces/{ws}/triggers/{id}:fire`
- The webhook ingest row is removed from the Trigger Service REST table; a short paragraph points to API Gateway design § Webhook Pass-through as authoritative.
- Module Responsibilities split the responsibility cleanly: API Gateway owns the connector-instance-scoped public URL, terminates TLS, and forwards the request anonymously (no call-context, no signature checks) — matching the gateway design's Webhook Pass-through section. Trigger Service's Generic Webhook Receiver owns HMAC/token verification against the connector-instance config (or delegates to the connector plugin per TODO-006) and then de-multiplexes the validated payload to all matching subscriptions on that instance.

## Impact

- Routing / Gateway: HTTPRoute updates needed so workspace-scoped trigger paths land on Trigger Service; webhook pass-through path already documented in gateway design.
- Authz: workspace-scoped paths make role-binding scope checks straightforward (workspace ID extracted from URL).
- Client SDKs: trigger admin endpoints now require workspace in path.

## Files changed

- `design/components/trigger-service/design.md` v4 → v5 (Module Responsibilities table; Public Interface § REST API; Change History)

## Related Change Records

- API Gateway: `2026-05-18-002-bundle-i-routes-roles.md` (companion entry; observability route paths)
