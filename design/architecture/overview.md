# Architecture Overview: Custos

Last Updated: 2026-05-14
Version: 1
Status: Draft

## Summary

Custos is a Kubernetes-native workflow platform for supply-chain security operations, built around durable orchestration with Dapr Workflow and strict extensibility boundaries for connectors, activities, and storage providers.

The architecture is intentionally split into a thin control plane and a pluggable extension plane. The platform must run fully self-contained on a single Kubernetes cluster by default, while still allowing optional external/cloud integrations.

## System Context

```mermaid
graph TD
    User([Security Analyst / DevOps / Developer]) -->|UI / API| Custos[Custos Platform]

    Registry[OCI Registry] -->|events| Custos
    Scheduler[Scheduled Trigger] -->|cron| Custos
    CI[CI/CD System] -->|manual trigger| Custos

    Custos -->|read/write artifacts| Registry
    Custos -->|policy evaluation| Policy[Policy Engine]
    Custos -->|notifications| Notify[Webhook / Slack / Teams / Email / PagerDuty]
    Custos -->|identity| IdP[OIDC / Service Identity]

    Ext[External Systems] <-->|optional via connectors| Custos
```

## Component Map

```mermaid
graph TD
    subgraph Custos Control Plane
        UI[Web UI + Template Designer]
        API[API Gateway]
        Auth[AuthN/AuthZ Service]
        WF[Workflow Service]
        Trig[Trigger Service]
        Conn[Connector Service]
        Act[Activity Runtime Manager]
        Cat[Definition/Template/Catalog Service]
        Obs[Observability and Audit Service]
        Store[Storage Provider Layer]
    end

    subgraph Dapr Runtime
        DWF[Dapr Workflow]
        DPUB[Dapr Pub/Sub]
        DSEC[Dapr Secrets API]
    end

    subgraph In-Cluster Dependencies (Default)
        SQL[(In-cluster PostgreSQL)]
        REDIS[(In-cluster Redis)]
        ART[(Kubernetes-backed Artifact Store)]
        LOG[(Kubernetes Logging Stack)]
    end

    subgraph Extension Plane
        CPlugins[Connector Plugins]
        APlugins[Activity Plugins]
        SPlugins[Storage Provider Plugins]
        LPlugins[Log Exporter Plugins]
    end

    UI --> API
    API --> Auth
    API --> WF
    API --> Trig
    API --> Cat

    WF --> DWF
    Trig --> DPUB
    WF --> Conn
    WF --> Act
    WF --> Cat
    WF --> Obs
    WF --> Store

    Conn --> CPlugins
    Act --> APlugins
    Store --> SPlugins
    Obs --> LPlugins

    Store --> SQL
    Store --> REDIS
    Store --> ART
    Obs --> LOG

    Conn --> DSEC
    Act --> DSEC
```

## Deployment Model

```mermaid
graph LR
    User([User / Automation]) --> Ingress[Ingress / API Endpoint]

    subgraph Single Kubernetes Cluster
        Ingress --> UI[UI Pod]
        Ingress --> API[API Pod]

        API --> Auth[Auth Service + Dapr]
        API --> WF[Workflow Service + Dapr]
        API --> Trig[Trigger Service + Dapr]
        API --> Cat[Catalog/Template Service + Dapr]
        API --> Obs[Observability/Audit Service + Dapr]
        API --> Conn[Connector Service + Dapr]
        API --> Act[Activity Runtime Manager + Dapr]
        API --> Store[Storage Provider Service + Dapr]

        WF --> Redis[(Redis StatefulSet)]
        Store --> Pg[(PostgreSQL StatefulSet)]
        Store --> PV[(Persistent Volumes / CSI)]
        Obs --> KLog[(Kubernetes Logging Pipeline)]

        Act --> Jobs[Kubernetes Jobs/Pods for OCI activities]
    end

    Conn --> OptExt[Optional External/Cloud Systems]
    Store --> OptStore[Optional External Storage/DB]
    Obs --> OptLog[Optional External Log Sinks]
```

## Key Data Flows

### Flow: Registry Event to Policy Gate

```mermaid
sequenceDiagram
    participant Registry
    participant Trigger as Trigger Service
    participant Workflow as Workflow Service
    participant Connector as Connector Service
    participant Activity as Activity Runtime Manager
    participant Storage as Storage Provider Layer

    Registry->>Trigger: push/tag event
    Trigger->>Workflow: normalized trigger payload
    Workflow->>Connector: resolve registry connector
    Workflow->>Activity: run signature check activity
    Activity-->>Workflow: result
    Workflow->>Activity: run vulnerability scan activity
    Activity-->>Workflow: result
    Workflow->>Storage: persist run state + outputs
    Workflow-->>Trigger: run terminal status
```

### Flow: Create Workflow from Template

```mermaid
sequenceDiagram
    actor User
    participant UI as Web UI
    participant API as API Gateway
    participant Catalog as Definition/Template Service
    participant Storage as Storage Provider Layer

    User->>UI: choose template
    UI->>API: submit template inputs
    API->>Catalog: validate placeholders
    Catalog->>Storage: load template + schema
    Storage-->>Catalog: template payload
    Catalog-->>API: compiled workflow definition
    API->>Storage: persist workflow version
    API-->>UI: workflow created
```

### Flow: Create Template from Existing Workflow

```mermaid
sequenceDiagram
    actor User
    participant API as API Gateway
    participant Catalog as Definition/Template Service
    participant Storage as Storage Provider Layer

    User->>API: save-as-template(workflow version)
    API->>Catalog: remove selected concrete values
    Catalog->>Storage: persist template + required placeholders
    Storage-->>Catalog: template version id
    Catalog-->>API: template published
```

## Architecture Decisions

| ID | Decision | Rationale | Date |
|---|---|---|---|
| ADR-001 | Keep control plane thin and extension-driven | Prevent domain logic from hard-coding into the core platform | 2026-05-14 |
| ADR-002 | Single-cluster self-contained deployment is the hard baseline | Satisfies portability and self-hosting requirement (REQ-077) | 2026-05-14 |
| ADR-003 | Introduce storage provider abstractions for definitions, catalog, metadata, and artifacts | Avoid hard lock-in to one database or object store (REQ-048, REQ-050) | 2026-05-14 |
| ADR-004 | Use Kubernetes-native logging/audit pipeline by default with pluggable exporters | Keeps default ops model cluster-local while enabling cloud sinks later (REQ-078) | 2026-05-14 |
| ADR-005 | Separate connectors from activities | Connectors model access; activities model operations (REQ-074, REQ-075) | 2026-05-14 |
| ADR-006 | Support first-class workflow templates | Speeds workflow authoring and reuse (REQ-076) | 2026-05-14 |

## Open TODOs

- [ ] Finalize activity isolation strategy for OCI activity pods (issue #4)
- [ ] Finalize registry trigger strategy (webhook vs polling mix) (issue #5)
- [ ] Finalize versioned activity contract schema and compatibility rules (issue #6)

## Change History

| Date | Change | GitHub Issue |
|---|---|---|
| 2026-05-14 | Initial architecture draft with single-cluster baseline, templates, and provider abstractions | — |
