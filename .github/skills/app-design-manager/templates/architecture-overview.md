# Architecture Overview: <Project Name>

Last Updated: YYYY-MM-DD
Version: 1
Status: Draft

## Summary

<2-3 sentence description of the overall architecture approach>

## System Context

```mermaid
graph TD
    User([User]) -->|uses| App[Application]
    App -->|integrates with| ExtAPI[External System]
    App -->|stores data in| DB[(Database)]
```

## Component Map

```mermaid
graph TD
    subgraph Application
        UI[Frontend] --> API[API Gateway]
        API --> SvcA[Service A]
        API --> SvcB[Service B]
        SvcA --> DB[(Database)]
        SvcB --> DB
    end
```

## Deployment Model

```mermaid
graph LR
    subgraph Cloud[Cloud Provider / Region]
        LB[Load Balancer] --> App[App Servers]
        App --> Cache[Cache]
        App --> DB[(Database)]
    end
    User([User]) --> LB
```

## Key Data Flows

### Flow: <Primary User Flow Name>

```mermaid
sequenceDiagram
    actor User
    participant API as API Gateway
    participant Svc as Service
    participant DB as Database

    User->>API: request
    API->>Svc: process(data)
    Svc->>DB: query
    DB-->>Svc: result
    Svc-->>API: response
    API-->>User: response
```

## Architecture Decisions

| ID | Decision | Rationale | Date |
|---|---|---|---|
| ADR-001 | | | YYYY-MM-DD |

## Open TODOs

<!-- - [ ] TODO-NNN: description (added YYYY-MM-DD) -->

## Change History

| Date | Change | GitHub Issue |
|---|---|---|
| YYYY-MM-DD | Initial architecture | — |
