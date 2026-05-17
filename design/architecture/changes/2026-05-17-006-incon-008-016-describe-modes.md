# Change: incon-008-016-describe-modes

Date: 2026-05-17
Type: architecture
Sequence: 006
GitHub Issues: #33, #42
Status: open

## Summary

Bundle two related fixes touching the same Connector Service changes 003 / 004:

- **INCON-008** (#33, MEDIUM) — Connector Contract hook table in `design/architecture/overview.md` described `describe()` as returning only "connector type, supported capabilities, config schema." After the capabilities/events separation it must also return `events.delivery` and `events.produced`. The `bind()` description still mentioned "opaque secret handles" in the ConnectorContext — stale after INCON-003.
- **INCON-016** (#42, LOW) — Trigger Pipeline explanatory text restated that connector types declare modes in `describe()`, with no mention that the declaration is static in the manifest at plugin registration time and constrains trigger configuration.

## Before

Hook table:

| `describe()` | Return connector type, supported capabilities, config schema |
| `bind(instance) -> ConnectorContext` | Produce a context activities can use (endpoints, opaque secret handles, capabilities) |

Trigger Pipeline paragraph:

> Connector types declare delivery modes in `events.delivery` on the connector manifest (`push`, `pull`, or both); trigger configuration selects the active mode per instance.

## After

Hook table:

| `describe()` | Return connector type, data-plane capabilities (dot-namespaced verbs), event delivery modes (`events.delivery`), event catalog (`events.produced`), and config schema |
| `bind(instance) -> ConnectorContext` | Produce a context activities can use (endpoints, capabilities, metadata). Credential material is **not** placed in the context — it flows via the connector sidecar API or `/custos/in/secrets/` mount |

Plus a sentence directly under the table:

> The normative definition of the connector manifest (capabilities, `events.delivery`, `events.produced`, config schema) lives in `design/components/connector-service/design.md` § Capabilities and Events.

Trigger Pipeline paragraph:

> Connector types declare their supported delivery modes (`push`, `pull`, or both) in the plugin manifest's `events.delivery` field, registered once at plugin registration time and static for the lifetime of a `ConnectorTypeVersion`. Trigger configuration selects the active mode per subscription, constrained to the modes the connector type version declares; workflow authors specify `mode: push` or `mode: pull` per trigger. ... See `design/components/connector-service/design.md` § Capabilities and Events for the full treatment.

## Impact

- Connector plugin authors now see in the overview's hook table that `describe()` must surface `events.delivery` and `events.produced`, not just data-plane capabilities.
- `bind()` description no longer contradicts INCON-003's secrets-out-of-context fix.
- Trigger pipeline text correctly conveys that delivery modes are a static, registration-time manifest property — not a runtime `describe()` call result — and that trigger configuration is constrained by that declaration.
- Closes two of the seven remaining MEDIUM/LOW inconsistencies in one PR; both touched the same change-003/004 lineage.

## Related Requirements

- `design/components/connector-service/design.md` § Capabilities and Events (authoritative)
- Connector Service change 003 (remove-supported-modes)
- Connector Service change 004 (events-delivery-and-capabilities-separation)
- Issues: #33 (INCON-008), #42 (INCON-016)
- Related: #28 (INCON-003, secrets context), #29 (INCON-004, capability naming), #30 (INCON-005, trigger pipeline diagram)
