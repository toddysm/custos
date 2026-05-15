# Component Registry: Custos

Last Updated: 2026-05-14

## Components

| ID | Name | Slug | Responsibility | Tech Stack | Status |
|---|---|---|---|---|---|
| COMP-001 | API Gateway | api-gateway | Unified entrypoint for UI, CLI, SDK, and automation APIs | Python, FastAPI, Dapr sidecar | Defined |
| COMP-002 | AuthN/AuthZ Service | auth-service | OIDC login, service token auth, RBAC enforcement | Python, OIDC libs, Dapr | Defined |
| COMP-003 | Workflow Service | workflow-service | Workflow validation, versioning, orchestration lifecycle | Python, Dapr Workflow | Defined |
| COMP-004 | Trigger Service | trigger-service | Manual/scheduled/registry trigger ingestion and normalization | Python, Dapr Pub/Sub | Defined |
| COMP-005 | Connector Service | connector-service | Connector runtime and connector plugin loading/execution | Python plugin runtime, Dapr Secrets | Defined |
| COMP-006 | Activity Runtime Manager | activity-runtime-manager | Activity resolution, execution, lifecycle, and result mapping | Python, Kubernetes Jobs, Dapr | Defined |
| COMP-007 | Definition/Template/Catalog Service | catalog-service | Workflow definitions, template lifecycle, activity/connector catalog metadata | Python, provider abstractions | Defined |
| COMP-008 | Storage Provider Layer | storage-provider-layer | Abstraction and adapters for definitions, metadata, catalog, and artifacts | Python interfaces, provider plugins | Defined |
| COMP-009 | Observability and Audit Service | observability-audit-service | Structured execution events, audit records, logs/traces/metrics export | OpenTelemetry, Kubernetes logging stack | Defined |
| COMP-010 | Web UI and Template Designer | web-ui | Workflow authoring, template authoring, run inspection and ops UX | React, TypeScript | Defined |

## Component Relationships

| From | To | Relationship |
|---|---|---|
| COMP-010 | COMP-001 | UI calls REST APIs |
| COMP-001 | COMP-002 | Delegates authentication and authorization checks |
| COMP-001 | COMP-003 | Starts and manages workflow runs |
| COMP-001 | COMP-004 | Registers and manages trigger configurations |
| COMP-001 | COMP-007 | Creates/reads workflow definitions and templates |
| COMP-003 | COMP-005 | Resolves and uses connector instances |
| COMP-003 | COMP-006 | Schedules and observes activity execution |
| COMP-003 | COMP-008 | Reads/writes workflow state and metadata via abstractions |
| COMP-003 | COMP-009 | Emits execution telemetry and audit events |
| COMP-006 | COMP-005 | Uses connector contexts for activity calls |
| COMP-006 | COMP-008 | Persists step outputs and artifacts via abstractions |
| COMP-007 | COMP-008 | Stores definitions, templates, and catalog entries via provider interfaces |
| COMP-009 | COMP-008 | Persists audit metadata and references |
