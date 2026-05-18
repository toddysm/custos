# API Gateway TODOs

Last Updated: 2026-05-17

## Open

- [ ] Specify the per-component thin client package layout (`custos-<service>-routes`) that the gateway imports for route + Pydantic model registration.
- [ ] Define the OpenAPI extension schema (`x-custos-required-permission`, `x-custos-idempotent`) and pin its semver.
- [ ] Decide whether the OIDC device-code landing page is server-rendered by the gateway or a redirect into the Web UI (M1 vs M2).
- [ ] Specify the coordinated rate-limiter design for M2 (Dapr state-backed or Redis).
- [ ] Specify the multi-region routing model (M2+).
- [ ] Add a conformance test suite that exercises every cross-cutting middleware against a stub downstream.

## Closed

_(none yet)_
