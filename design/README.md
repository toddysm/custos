# Custos — Design Documentation

Last Updated: 2026-06-30

## Overview

Custos is a pluggable workflow orchestrator for supply-chain security operations on OCI artifacts. Users (security analysts, DevOps engineers, PMs, developers) author workflows in YAML or via a visual designer; the orchestrator executes them durably on top of Dapr Workflow, dispatching steps to pluggable actions (OCI containers, HTTP webhooks, WASM modules). Built-in actions cover common supply-chain operations (vulnerability scan, SBOM, signature verification, attestation, policy eval, image promotion), and a stable orchestrator–action contract lets users add their own.

## Design Phases

| Phase | Status | Last Updated |
|---|---|---|
| Requirements | In Progress (revision 5) | 2026-05-18 |
| Architecture | In Progress (revision 3, detailed) | 2026-05-18 |
| Component Designs | In Progress | 2026-05-18 |
| Implementation | Started (scaffold + Helm) | 2026-05-18 |

## Quick Links

- [Requirements](requirements/requirements.md)
- [Architecture Overview](architecture/overview.md)
- [Component Registry](architecture/components.md)
- [Capabilities Registry](architecture/capabilities.md)
- [Reference Deployment](architecture/reference-deployment.md)
- [Out-of-the-Box Catalog (Connectors & Activities)](architecture/ootb-catalog.md)
- [OOTB Publishing & Onboarding](architecture/ootb-publishing-onboarding.md)

## Components

| ID | Name | Slug | Design | Status |
|---|---|---|---|---|
| COMP-001 | API Gateway | api-gateway | [design.md](components/api-gateway/design.md) | Designed |
| COMP-002 | AuthN/AuthZ Service | auth-service | [design.md](components/auth-service/design.md) | Designed |
| COMP-003 | Workflow Service | workflow-service | [design.md](components/workflow-service/design.md) | Designed |
| COMP-004 | Trigger Service | trigger-service | [design.md](components/trigger-service/design.md) | Designed |
| COMP-005 | Connector Service | connector-service | [design.md](components/connector-service/design.md) | Designed |
| COMP-006 | Activity Runtime Manager | activity-runtime-manager | [design.md](components/activity-runtime-manager/design.md) | Designed |
| COMP-007 | Definition/Template/Catalog Service | catalog-service | [design.md](components/catalog-service/design.md) | Designed |
| COMP-008 | Storage Provider Layer | storage-provider-layer | [design.md](components/storage-provider-layer/design.md) | Designed |
| COMP-009 | Observability and Audit Service | observability-audit-service | [design.md](components/observability-audit-service/design.md) | Designed |
| COMP-010 | Web UI and Template Designer | web-ui | — | Defined, deferred to M2+ |
| COMP-011 | Local Dev & Test CLI (`custosctl`) | custosctl | [design.md](components/custosctl/design.md) | Designed (0.2) |

## Recent Changes

| Date | Change | Issue |
|---|---|---|
| 2026-06-30 | Local Dev & Test CLI (`custosctl`) component design — target-aware local (kind) + remote deploy and extension lifecycle (0.2, GHCR-only) | #951 |
| 2026-06-27 | OOTB Publishing & Onboarding: per-extension publish workflows, `scripts/seed-ootb.sh` onboarding, end-to-end runbook, and author-guide + skill requirements | #944 |
| 2026-06-25 | Out-of-the-Box Catalog structure design: decoupled `extensions/` root for connectors & activities, no-SDK/language-agnostic conventions, plugin migration plan | #880, #881, #884 |
| 2026-05-18 | Bucket A design alignment: design/README.md refresh, REQ-080/081 assigned to M2, SPL documented as 7 interfaces, M1 scope reconciliation (contract vs implementation) | #87, #91, #96, #97 |
| 2026-05-18 | Repository scaffold + Helm umbrella chart and 11 subcharts | #77 |
| 2026-05-17 | Reference Deployment doc; Capabilities Registry; architecture overview updates for connector/trigger/activity contracts | _pending_ |
| 2026-05-17 | Observability and Audit Service component design | _pending_ |
| 2026-05-17 | Storage Provider Layer component design (7 interfaces: definitions, catalog, metadata, artifacts, auth, log query, metric query) | _pending_ |
| 2026-05-16 | Activity Runtime Manager component design; Catalog Service component design; API Gateway, Auth Service, Workflow Service designs | _pending_ |
| 2026-05-16 | Trigger Service component design draft; added REQ-080 (internal workflow-to-workflow trigger) and REQ-081 (dual-purpose event delivery for start and resume) | _pending_ |
| 2026-05-15 | Connector Service component design draft (plugin manifest, identity model) | — |
| 2026-05-14 | Added hybrid push/pull trigger ingestion requirement (REQ-079) and aligned trigger pipeline | _pending_ |
| 2026-05-14 | Detailed architecture: principles, domain model, schemas, contracts, security/observability/trigger pipeline | — |
| 2026-05-14 | Added workflow templates and single-cluster deployment/storage/logging requirements | _pending_ |
| 2026-05-14 | Added workflow primitives, extensible connectors, and pluggable activity requirements | #8 |
| 2026-05-13 | Initial requirements drafted | #2 |
