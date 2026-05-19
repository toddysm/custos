"""Cross-cutting middleware: workspace scoping, audit partition enforcement.

Populated by:
- SPL-008 — Workspace-scoping middleware + adapter SQL lint rule
- SPL-009 — Audit Partition Enforcer (custos_audit schema, append-only DDL)
- SPL-010 — Transaction model (withTransaction, opaque handles)
"""
