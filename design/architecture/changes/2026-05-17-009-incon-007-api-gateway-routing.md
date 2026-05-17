# Change: incon-007-api-gateway-routing

Date: 2026-05-17
Type: architecture
Sequence: 009
GitHub Issue: #32
Status: open

## Summary

Two fixes resolving INCON-007:

1. Add `API --> Conn` to the Component Map in `design/architecture/overview.md` so the diagram reflects the Connector Service's documented user-facing REST API (`/v1/workspaces/{ws}/connectors`, etc.). The Component Map previously listed only Auth, WF, Trig, Cat.
2. Add the missing `COMP-001 → COMP-005` row to the Component Relationships table in `design/architecture/components.md` so component-design audits include the API/auth surface for connector management.

Also clarifies the divergence between the Component Map and the Deployment Model by adding a caption to the Deployment Model: its `API --> <svc>` edges describe deployment topology and ingress reachability, not the set of services that currently expose user-facing REST through the gateway. The Component Map is authoritative for "which services route REST through the API Gateway today."

## Before

Component Map edges from `API`:

```
API --> Auth
API --> WF
API --> Trig
API --> Cat
```

Component Relationships table rows from COMP-001:

| COMP-001 | COMP-002 | Delegates authentication and authorization checks |
| COMP-001 | COMP-003 | Starts and manages workflow runs |
| COMP-001 | COMP-004 | Registers and manages trigger configurations |
| COMP-001 | COMP-007 | Creates/reads workflow definitions and templates |

Connector Service was missing from both.

Deployment Model has eight `API --> <svc>` edges (all services), with no caption explaining why this differs from the Component Map.

## After

Component Map adds `API --> Conn`.

Component Relationships table adds:

| COMP-001 | COMP-005 | Manages connector type registration and connector instance lifecycle |

Deployment Model gains a caption distinguishing topology edges from REST routing, and stating that additional Component Map edges will be added when other services document public interfaces.

## Why not also add API → ARM / Obs / Storage to the Component Map

Audit of the three component designs (`activity-runtime-manager`, `observability-audit-service`, `storage-provider-service`) shows none currently document a user-facing REST API. ARM's § Public Interface is marked `(pending)`; the other two have no public interface section drafted. Their Component Map edges will be added when their component designs declare those interfaces.

## Impact

- Routing-layer implementers have an authoritative source of truth: the Component Map.
- The architecture-level relationships table no longer misses the COMP-001 → COMP-005 path, so component-design sessions reviewing API Gateway scope will surface connector management.
- Future component designs that add public REST APIs (e.g. Workflow Service when its design lands) will trigger a paired Component Map edit; the caption documents that contract.

## Related Requirements

- `design/components/connector-service/design.md` § Public Interface (REST API via API Gateway)
- `design/components/trigger-service/design.md` § Public Interface (REST API mounted under API Gateway)
- Issues: #32 (this change)
