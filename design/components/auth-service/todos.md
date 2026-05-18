# Auth Service TODOs

Last Updated: 2026-05-17

## Open

- [ ] Define the exact JWT claim shape for signed call contexts (claim names, audience, signing algorithm — proposed EdDSA).
- [ ] Specify the OIDC issuer config schema for `CUSTOS_AUTH_OIDC_ISSUERS` (per-issuer provisioning policy options).
- [ ] Specify the **GitHub OIDC preset** (default issuer URL, JWKS endpoint, audience claim shape, GitHub Actions `aud`/`sub`/`repository` claim handling for workload tokens, human-login vs workload-token distinction — **M1, P0**).
- [ ] Specify the **Azure Entra ID OIDC preset** (default authority URL, tenant-vs-multitenant audience handling, group-claim → role-binding mapping rules — **M1, P0**).
- [ ] Cross-region replication strategy for Auth Service state (multi-region M2+).
- [ ] Custom role authoring API (M2+).
- [ ] SPIFFE/SPIRE cutover plan (M2/M3).

## Closed

_(none yet)_
