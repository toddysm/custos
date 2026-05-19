# Change: bundle-i-routes-roles

Date: 2026-05-18
Type: component-design
Component: api-gateway
Sequence: 002
GitHub Issue: #99
Status: open

## Summary

Expanded the Observability row in the Public Routes table so the workspace-scoped audit, log, and metric routes mounted at the gateway match the Observability Service's published API surface and the permission registry. No new routes added — this is documentation alignment so the gateway design enumerates exactly what callers can hit.

## Before

The Observability row in the Public Routes table covered audit search at a high level but did not enumerate the run-scoped log tail / historical log query / metric series paths, leaving callers and operators unsure which surfaces the gateway forwards versus which are internal-only.

## After

The Observability row reads:

| Observability | `/v1/workspaces/{ws}/audit/*`, `/v1/workspaces/{ws}/runs/{id}/logs/*`, `/v1/workspaces/{ws}/runs/{id}/metrics` | Audit search and single-event lookup; run-scoped log tail (`/logs/tail`) and historical log query (`/logs`); run-scoped metric series. |

## Impact

- HTTPRoute config: confirm both `runs/{id}/logs` and `runs/{id}/logs/tail` resolve to Observability Service backends.
- Authz check on these paths uses workspace ID from URL and consults `audit:read`, `logs:read`, `metrics:read` permissions (the latter two added to `workspace.viewer` in companion auth-service change).

## Files changed

- `design/components/api-gateway/design.md` v1 → v2 (Public Routes table Observability row; Change History)

## Related Change Records

- Trigger Service: `2026-05-18-004-bundle-i-routes-roles.md` (companion entry; workspace-scoped trigger paths)
- Auth Service: `2026-05-18-001-bundle-i-routes-roles.md` (companion entry; `logs:read`/`metrics:read` added to `workspace.viewer`)
