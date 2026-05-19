# Storage Provider Layer TODOs

Last Updated: 2026-05-18

## Open — Design

- [ ] Define exact schema-revision policy for adapter upgrades that span multiple revisions in one platform release.

## Open — Implementation

Tracked as GitHub issues under labels `type:implementation, component:storage-provider-layer, phase:implementation`.

### A. Foundations

- [ ] SPL-001: Scaffold custos-spl Python package (#115)
- [ ] SPL-002: Define error taxonomy and shared types (#116)

### B. Interface contracts

- [ ] SPL-003: DefinitionStoreProvider and CatalogStoreProvider interfaces (#117)
- [ ] SPL-004: MetadataStoreProvider interface — full surface (#118)
- [ ] SPL-005: ArtifactStoreProvider interface (#119)
- [ ] SPL-006: AuthStoreProvider interface (#120)
- [ ] SPL-007: LogQueryProvider and MetricsQueryProvider interfaces (#121)

### C. Cross-cutting

- [ ] SPL-008: Workspace-scoping middleware and adapter SQL lint rule (#122)
- [ ] SPL-009: Audit Partition Enforcer (#123)
- [ ] SPL-010: Transaction model — withTransaction, opaque handles (#124)
- [ ] SPL-011: Migration runner and custos migrate up CLI (#125)

### D. Postgres adapter

- [ ] SPL-012: Postgres adapter — DefinitionStoreProvider and CatalogStoreProvider (#126)
- [ ] SPL-013: Postgres adapter — MetadataStore runtime/trigger/cursors (#127)
- [ ] SPL-014: Postgres adapter — MetadataStore gateway short-lived state (#128)
- [ ] SPL-015: Postgres adapter — MetadataStore audit outbox writer and drain (#129)
- [ ] SPL-016: Postgres adapter — AuthStoreProvider (#130)
- [ ] SPL-017: Postgres migrations rev 1-4 (#131)

### E. Artifact adapters

- [ ] SPL-018: CSI/PVC ArtifactStore adapter (#132)
- [ ] SPL-019: S3 ArtifactStore adapter — optional (#133)

### F. Query facade adapters

- [ ] SPL-020: Log query adapters — loki, opensearch, noop (#134)
- [ ] SPL-021: Metrics query adapters — prometheus, noop (#135)

### G. Quality

- [ ] SPL-022: Conformance test suite (#136)
- [ ] SPL-023: Integration test pass — Postgres + CSI + Loki + Prometheus (#137)

## Closed

_(none yet)_
