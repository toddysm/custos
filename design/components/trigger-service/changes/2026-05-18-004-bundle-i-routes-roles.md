# Change: bundle-i-routes-roles

Date: 2026-05-18
Type: component-design
Component: trigger-service
Sequence: 004
GitHub Issue: #99
Status: open

## Summary

Aligned Trigger Service public REST API with the workspace-scoped URL convention `/v1/workspaces/{workspaceId}/...`, and relocated the public webhook ingress description to the API Gateway design (single source of truth for gateway-mounted public paths). The Generic Webhook Receiver inside Trigger Service is now described as the downstream demux: API Gateway accepts `POST /v1/webhooks/{connectorInstanceId}`, validates HMAC/token per connector instance, then routes to the Trigger Service receiver which fans out to all matching subscriptions.

## Before

- Trigger CRUD endpoints in the Public Interface section were rooted at `/v1/triggers/...` with workspace passed implicitly via call context, inconsistent with workspace-scoped paths used by other components.
- The webhook ingress row sat in the Trigger Service REST table as if Trigger Service owned the public mount point, conflicting with the API Gateway design which terminates `/v1/webhooks/{connectorInstanceId}` at the gateway.
- Module Responsibilities described the Generic Webhook Receiver as the public HTTP entry, ambiguous about which component validates HMAC/token versus which performs subscription demux.

## After

- Trigger CRUD and manual fire endpoints are workspace-scoped:
  - `POST   /v1/workspaces/{ws}/triggers`
  - `GET    /v1/workspaces/{ws}/triggers/{id}`
  - `PATCH  /v1/workspaces/{ws}/triggers/{id}`
  - `DELETE /v1/workspaces/{ws}/triggers/{id}`
  - `POST   /v1/workspaces/{ws}/triggers/{id}:fire`
- The webhook ingest row is removed from the Trigger Service REST table; a short paragraph points to API Gateway design § Webhook Pass-through as authoritative.
- Module Responsibilities split the responsibility cleanly: API Gateway owns the connector-instance-scoped public URL and HMAC/token validation; Trigger Service's Generic Webhook Receiver consumes the validated payload and demultiplexes to matching subscriptions.

## Impact

- Routing / Gateway: HTTPRoute updates needed so workspace-scoped trigger paths land on Trigger Service; webhook pass-through path already documented in gateway design.
- Authz: workspace-scoped paths make role-binding scope checks straightforward (workspace ID extracted from URL).
- Client SDKs: trigger admin endpoints now require workspace in path.

## Files changed

- `design/components/trigger-service/design.md` v4 → v5 (Module Responsibilities table; Public Interface § REST API; Change History)

## Related Change Records

- API Gateway: `2026-05-18-002-bundle-i-routes-roles.md` (companion entry; observability route paths)
