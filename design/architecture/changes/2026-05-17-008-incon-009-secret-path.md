# Change: incon-009-secret-path

Date: 2026-05-17
Type: architecture
Sequence: 008
GitHub Issue: #34
Status: open

## Summary

Align the activity sandbox secret path between `design/architecture/overview.md` (§ Activity Contract v1 filesystem table) and `design/components/activity-runtime-manager/design.md` (§ Activity Contract v1 sandbox layout) on the two-level, connector-namespaced form `/custos/in/secrets/<connector-name>/<key>` — the format already locked in ARM's § No `spec.secrets[]` in v1. Both the overview's filesystem layout table and ARM's sandbox layout table previously used the flat placeholder `/custos/in/secrets/<name>`, which silently contradicted the normative two-level path.

## Before

Overview (§ Activity Contract v1, filesystem table):

| Path | Purpose |
|---|---|
| `/custos/in/secrets/<name>` | Mounted secret materials (per binding, never logged) |

ARM (§ Activity Contract v1, sandbox layout table):

| Path | Direction | Owner | Description |
|---|---|---|---|
| `/custos/in/secrets/<name>` | orchestrator → activity | ARM writes | One file per injected secret. Plaintext credentials live ONLY here, never in `inputs.json`. Read-only, tmpfs-mounted. |

Both used a flat `<name>` placeholder.

## After

Overview:

| `/custos/in/secrets/<connector-name>/<key>` | Mounted secret materials, namespaced by activity-manifest connector slot name (matches `spec.connectors[].name`). One file per key. Read-only tmpfs. Never logged. |

ARM:

| `/custos/in/secrets/<connector-name>/<key>` | orchestrator → activity | ARM writes | One file per injected secret, namespaced under the activity-manifest connector slot name (matches `spec.connectors[].name`). Plaintext credentials live ONLY here, never in `inputs.json`. Read-only, tmpfs-mounted. See § No `spec.secrets[]` in v1 for the populating-rule. |

## Impact

- Activity authors reading either document now see the same path layout that ARM actually writes.
- Eliminates the namespace-collision risk for activities binding two connector slots (e.g. `source` + `destination`) that share a key like `token`.
- Aligns the architecture overview and ARM design with the already-locked Connector Service `bind()` and credential delivery model.

## Related Requirements

- `design/components/activity-runtime-manager/design.md` § No `spec.secrets[]` in v1 (authoritative)
- `design/architecture/changes/2026-05-17-003-incon-003-secrets-model.md` (sidecar + tmpfs credential delivery)
- Issues: #34 (this change); related: #28 (INCON-003, ConnectorContext secrets)
