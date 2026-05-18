# Change: bucket-a-alignment

Date: 2026-05-18
Type: architecture
Sequence: 010
GitHub Issues: #87, #91, #96, #97
Status: open

## Summary

Bucket A of the design-inconsistency cleanup: refresh `design/README.md`, assign REQ-080/REQ-081 to M2, document the seven Storage Provider Layer interfaces in the architecture overview and component registry, and add a contract-vs-implementation framing that reconciles the M1 milestone scope with the more aggressive surface defined by the component designs (Auth/OIDC, API Gateway device-code, Trigger Service receivers, observability traces, Web UI in the reference deployment).

## Before

- `design/README.md` listed only two active component design sessions (Connector, Trigger) and stale revisions for Requirements (rev 4 / 2026-05-14) and Architecture (rev 2 / 2026-05-14). Nine components were already designed; ARM was missing entirely.
- REQ-080 (internal workflow-to-workflow triggers) and REQ-081 (dual-purpose event delivery) had status `Open` but no milestone row — implementation planning could not pull them in.
- Architecture overview's "Storage Provider Contract" listed four interfaces. The SPL component design had grown to seven (`AuthStoreProvider`, `LogQueryProvider`, `MetricsQueryProvider` were added with Auth and Observability). The component map had no edges into SPL from Auth or Observability. The COMP-008 row in the registry described responsibility as "definitions, metadata, catalog, and artifacts" only.
- Requirements M1 scope conflicted with component designs in five areas: OIDC (Auth design has it M1, reqs say M3); API Gateway device-code routes (in M1 set, reqs say M3); Trigger Service receivers (full surface designed, reqs say manual-only M1); observability traces (in v1 table, reqs defer to M2); Web UI / reference deployment (chart claims "all ten components", reqs and `web-ui.enabled` say deferred). Implementation planning could not use "M1" consistently.

## After

- `design/README.md` reflects revision 5 of requirements (2026-05-18), revision 3 of architecture (2026-05-18), a complete Components table for COMP-001..009 with COMP-010 marked deferred, and recent-changes rows for 2026-05-15..18.
- REQ-080 and REQ-081 are both assigned to **M2** (Triggers & action breadth). REQ-080 lands with the extensible connector model that already targets M2; REQ-081 ships with the Trigger Service Dispatcher's first non-manual receivers (scheduled, webhook).
- Architecture overview's "Storage Provider Contract" lists seven interfaces with a one-paragraph note explaining when the extra three were added and why. COMP-008 diagram in components.md adds `AuthIfc`, `LogQIfc`, `MetricsQIfc` nodes and Postgres / Loki / Prometheus adapters, and feeds `AuthIfc` into the migration runner. The COMP-008 row in the registry now reads "definitions, metadata, catalog, artifacts, auth state, and log/metric query". The component map gains `Auth → Store` and `Obs → Store` edges.
- Requirements gains a **Contract vs implementation** preamble: component designs define v1 contracts; the milestone table tracks the first implementation milestone. M1 row clarifies that OIDC/RBAC are contract-locked but disabled (API tokens only), traces are deferred to M2, and the Web UI is not deployed in M1. M2 row absorbs REQ-080/REQ-081 and the initial Web UI deployment.
- Component designs gain matching M1 implementation notes:
  - **Auth Service** — OIDC presets and RBAC are contract-locked, but M1 ships API tokens + service tokens only; OIDC code paths disabled.
  - **API Gateway** — device-code routes are wired but return 503 in M1; live in M3.
  - **Trigger Service** — manual API trigger only in M1; scheduled / webhook / vendor push / poll / internal receivers are contract-locked, stubbed.
- **Reference deployment** updated: M1 chart deploys nine components (COMP-001..009), `web-ui.enabled` defaults to false, OIDC issuer is no longer an M1 prerequisite (M3 onward).

## Impact

- Implementation planning can now use "M1" without ambiguity — for each requirement, the milestone table is authoritative for "when does the code path light up"; the component design is authoritative for "what is the shape it must have when it does".
- Reviewers reconciling component designs against the requirements milestone table have a documented decision rule for apparent mismatches.
- SPL implementation work in M1 covers all seven interfaces' migrations (Auth, log query and metrics query schemas land in M1 even though Auth and Observability features themselves come online later).

## Files changed

- `design/README.md`
- `design/requirements/requirements.md`
- `design/architecture/overview.md`
- `design/architecture/components.md`
- `design/architecture/reference-deployment.md`
- `design/components/auth-service/design.md`
- `design/components/api-gateway/design.md`
- `design/components/trigger-service/design.md`
- `design/architecture/changes/2026-05-18-010-bucket-a-alignment.md` (this file)
