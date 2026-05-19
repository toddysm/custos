# Change: bundle-i-routes-roles

Date: 2026-05-18
Type: component-design
Component: auth-service
Sequence: 001
GitHub Issue: #102
Status: open

## Summary

Added `logs:read` and `metrics:read` to the `workspace.viewer` built-in role so the role bindings match the permissions enumerated by the Observability Service's permission registry. Kept the permissions distinct (not folded into `run:read`) so service accounts created for narrower scopes (e.g., log-shipping integrations) can be granted only the permissions they need.

## Before

`workspace.viewer` granted: `workflow:read`, `template:read`, `connector:read`, `audit:read`, `run:read`.

Observability Service's registry declared `logs:read` and `metrics:read` as required permissions for its `/logs*` and `/metrics` routes, but no built-in role granted them — every viewer call to those routes would be denied unless an operator hand-crafted a custom role.

## After

`workspace.viewer` grants: `workflow:read`, `template:read`, `connector:read`, `audit:read`, `run:read`, `logs:read`, `metrics:read`.

Considered folding both into `run:read`. Rejected because permissions are the unit of authorization in the registry — a service account that only needs to ship logs should be grantable `logs:read` without also handing it the ability to read run metadata.

## Impact

- All current `workspace.viewer` bindings transparently gain log + metric read access at next token refresh.
- Other built-in roles (`workspace.author`, `workspace.operator`, `workspace.admin`) inherit viewer's permissions per the role hierarchy in `Built-in Roles` so they pick these up automatically.

## Files changed

- `design/components/auth-service/design.md` v1 → v2 (Built-in Roles table `workspace.viewer` row; Change History)

## Related Change Records

- API Gateway: `2026-05-18-002-bundle-i-routes-roles.md` (companion entry; routes that now resolve under the new permissions)
