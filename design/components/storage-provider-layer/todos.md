# Storage Provider Layer TODOs

Last Updated: 2026-06-06 (all M1 implementation issues SPL-001..023 merged and closed; #115–#137 CLOSED)

## Open — Design

- [ ] Define exact schema-revision policy for adapter upgrades that span multiple revisions in one platform release.

## Implementation — M1 (complete)

Tracked as GitHub issues under labels `type:implementation, component:storage-provider-layer, phase:implementation`. All M1 issues are merged and closed; the `custos-spl` package under `src/libs/storage-provider-layer` ships the seven provider interfaces plus the Postgres, CSI/PVC, S3, Loki/OpenSearch, and Prometheus adapters.

### A. Foundations

- [x] SPL-001: Scaffold custos-spl Python package (#115)
- [x] SPL-002: Define error taxonomy and shared types (#116)

### B. Interface contracts

- [x] SPL-003: DefinitionStoreProvider and CatalogStoreProvider interfaces (#117)
- [x] SPL-004: MetadataStoreProvider interface — full surface (#118)
- [x] SPL-005: ArtifactStoreProvider interface (#119)
- [x] SPL-006: AuthStoreProvider interface (#120)
- [x] SPL-007: LogQueryProvider and MetricsQueryProvider interfaces (#121)

### C. Cross-cutting

- [x] SPL-008: Workspace-scoping middleware and adapter SQL lint rule (#122)
- [x] SPL-009: Audit Partition Enforcer (#123)
- [x] SPL-010: Transaction model — withTransaction, opaque handles (#124)
- [x] SPL-011: Migration runner and custos migrate up CLI (#125)

### D. Postgres adapter

- [x] SPL-012: Postgres adapter — DefinitionStoreProvider and CatalogStoreProvider (#126)
- [x] SPL-013: Postgres adapter — MetadataStore runtime/trigger/cursors (#127)
- [x] SPL-014: Postgres adapter — MetadataStore gateway short-lived state (#128)
- [x] SPL-015: Postgres adapter — MetadataStore audit outbox writer and drain (#129)
- [x] SPL-016: Postgres adapter — AuthStoreProvider (#130)
- [x] SPL-017: Postgres migrations rev 1-4 (#131)

### E. Artifact adapters

- [x] SPL-018: CSI/PVC ArtifactStore adapter (#132)
- [x] SPL-019: S3 ArtifactStore adapter — optional (#133)

### F. Query facade adapters

- [x] SPL-020: Log query adapters — loki, opensearch, noop (#134)
- [x] SPL-021: Metrics query adapters — prometheus, noop (#135)

### G. Quality

- [x] SPL-022: Conformance test suite (#136)
- [x] SPL-023: Integration test pass — Postgres + CSI + Loki + Prometheus (#137)

## Closed

_All M1 implementation issues (SPL-001..023, #115–#137) merged and closed._
