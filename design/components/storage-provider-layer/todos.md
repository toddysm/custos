# Storage Provider Layer TODOs

Last Updated: 2026-05-17

## Open

- [ ] Define exact schema-revision policy for adapter upgrades that span multiple revisions in one platform release.
- [ ] Specify the audit outbox draining contract (LISTEN/NOTIFY vs polling cadence, batch size, redelivery guarantees) when Observability Service detailed design starts.
- [ ] Specify the static lint rule that enforces `workspaceId` on every adapter query (tooling task; M1 implementation track).
- [ ] Add a conformance test suite skeleton that any adapter must pass.

## Closed

_(none yet)_
