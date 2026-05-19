"""Backend adapters implementing the provider interfaces.

Populated by:
- SPL-012 — Postgres adapter for Definition + Catalog stores
- SPL-013 — Postgres adapter for MetadataStore runtime/trigger/cursors
- SPL-014 — Postgres adapter for MetadataStore gateway short-lived state
- SPL-015 — Postgres adapter for MetadataStore audit outbox writer + drain
- SPL-016 — Postgres adapter for AuthStoreProvider
- SPL-018 — CSI/PVC ArtifactStore adapter
- SPL-019 — S3 ArtifactStore adapter (optional)
- SPL-020 — Log query adapters (loki, opensearch, noop)
- SPL-021 — Metrics query adapters (prometheus, noop)
"""
