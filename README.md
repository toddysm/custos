<p align="center">
  <img src="media/custos-icon.png" alt="Custos" width="160" />
</p>

<p align="center"><sub>Icon by <a href="https://www.instagram.com/graffitonic/">@graffitonic</a> on Instagram.</sub></p>

# Custos

![GitHub issues](https://img.shields.io/github/issues-raw/toddysm/custos?link=https%3A%2F%2Fgithub.com%2Ftoddysm%2Fcustos%2Fissues)
![GitHub pull requests](https://img.shields.io/github/issues-pr-raw/toddysm/custos?link=https%3A%2F%2Fgithub.com%2Ftoddysm%2Fcustos%2Fpulls)

> **Custos** (Latin: *guardian*, *keeper*) — *Custos verifies, isolates, and orchestrates.*

Custos is a **pluggable workflow orchestrator for supply-chain security operations on OCI artifacts**. Security analysts, DevOps engineers, and developers define workflows in YAML (or via a visual designer) that operate on container images and other cloud-native artifacts stored in OCI-compliant registries. The orchestrator executes them durably on top of [Dapr Workflow](https://docs.dapr.io/developing-applications/building-blocks/workflow/); all real work happens inside independently versioned and deployable *actions* that conform to a stable orchestrator–action contract.

## 🚀 Get started

**New to Custos? [Deploy it on your laptop in minutes](docs/users/evaluation/local-cluster.md).** The local-cluster quickstart stands the platform up on **kind** or **Docker Desktop Kubernetes** with locally built images, then walks you through running your first workflow.

- 📘 [Local-cluster quickstart](docs/users/evaluation/local-cluster.md) — fastest way to try Custos
- 📚 [User & evaluation guide](docs/users/README.md) — deploy, verify, and operate the platform
- 🧩 [Developer guide](docs/developers/README.md) — write connectors, activities, and plugins

## What Custos does

- **Trigger** workflows on OCI registry events (push, delete, tag), on a cron schedule, manually via API, or by polling any external source.
- **Orchestrate** sequential DAGs, parallel fan-out/fan-in branches, conditional steps, loops, retry policies, and human-in-the-loop approval gates.
- **Execute** work inside sandboxed OCI container actions, HTTP webhook actions, or WebAssembly modules — all via a shared, versioned contract.
- **Connect** to any OCI-compliant registry and, via pluggable connectors, to storage accounts, databases, and other external systems.
- **Audit and observe** every run, step, secret access, and authz decision with correlated logs, Prometheus metrics, OpenTelemetry traces, and an append-only audit stream.

Built-in actions cover the common supply-chain security operations: vulnerability scanning, SBOM generation, signature verification, attestation, policy evaluation, and image promotion. Custom and third-party actions work through the same contract as built-ins.

## Key features

| Feature | Detail |
|---|---|
| Durable execution | Built on Dapr Workflow — runs survive orchestrator crashes and resume from the last completed step |
| Pluggable actions | OCI container, HTTP webhook, and WASM runtimes; independently versioned and deployable |
| Pluggable connectors | OCI registries today; extensible to any external system via the connector contract |
| Pluggable storage | Datastore-agnostic for definitions, catalog, metadata, and artifacts; in-cluster PostgreSQL + CSI by default |
| Workflow templates | Parameterized templates with typed placeholders; round-trip between workflow and template |
| Hybrid triggers | Push (webhook) and pull (polling) receivers per trigger, both feeding a shared dedup/idempotency pipeline |
| Cloud-agnostic | No mandatory cloud-provider dependency; runs self-contained on any conformant Kubernetes cluster |
| Open source | Apache 2.0 |

## Architecture

Custos is a Kubernetes-native set of microservices split into a thin **control plane** and a pluggable **extension plane**.

```
Users / CI/CD  ──▶  API Gateway  ──▶  Workflow Service  ──▶  Dapr Workflow
                                            │
                        ┌───────────────────┼───────────────────┐
                        ▼                   ▼                   ▼
               Connector Service   Activity Runtime Mgr   Trigger Service
                        │                   │
               Connector Plugins     Activity Plugins
                (OCI Registry, …)  (vuln-scan, SBOM, …)
```

- **Workflow Service** — owns the orchestration state machine (Dapr Workflow).
- **Connector Service** — access broker between workflows/activities and external systems; issues `ConnectorContext` to activities; manages push and pull trigger streams.
- **Activity Runtime Manager** — resolves connector context, launches sandboxed activity pods, captures outputs and artifacts.
- **Trigger Service** — normalizes events from push and pull receivers, matches against registered triggers, deduplicates, and dispatches run requests.
- **Definition/Template/Catalog Service** — stores versioned workflow definitions, templates, activity types, and connector types.
- **Storage Provider Layer** — pluggable adapters for definitions, metadata, and artifact storage.

Full architecture documentation is in [design/architecture/overview.md](design/architecture/overview.md) and [design/architecture/components.md](design/architecture/components.md).

## Technology stack

| Layer | Choice | Reason |
|---|---|---|
| Orchestration engine | [Dapr Workflow](https://docs.dapr.io/developing-applications/building-blocks/workflow/) | Durable execution, replay, pluggable state stores |
| Implementation language | Python | Team familiarity; Dapr Python SDK parity verified |
| Frontend | React + TypeScript | Required for visual workflow designer |
| Default metadata store | PostgreSQL (in-cluster) | Datastore-agnostic abstraction; portable baseline |
| Default ephemeral state | Redis (Dapr state + pub/sub) | Standard Dapr-supported backend |
| Deployment | Kubernetes (any conformant cluster) + Helm | Cloud-agnostic; Dapr sidecar model |
| Secrets | Dapr Secrets API (Kubernetes Secrets by default) | Pluggable backends without code changes |
| Observability | OpenTelemetry + Prometheus | Correlated logs, metrics, and traces |

## Deployment model

Custos runs fully self-contained on a **single Kubernetes cluster** with no mandatory off-cluster dependencies. The default Helm chart deploys all control-plane services, Dapr, PostgreSQL, Redis, a CSI-backed artifact store, and an in-cluster logging pipeline.

Operators can selectively "go external" by swapping storage provider adapters or adding log/connector plugins — without changing the platform core.

Target cluster environments: AKS, EKS, GKE, k3s, OpenShift, and vanilla Kubernetes.

## Roadmap

| Milestone | Target | Focus |
|---|---|---|
| **M1 — Core engine** | 2026-08-13 | Dapr-Workflow-backed engine; YAML DAG workflows; manual API trigger; vuln-scan + signature-verify actions; run inspection; minimal auth; in-cluster storage defaults; Helm chart |
| **M2 — Triggers & action breadth** | +6 months | Scheduled + registry webhook triggers; extensible connectors; SBOM, attestation, policy-eval, and image-promotion actions; notifications; OTel tracing |
| **M3 — UX, security, multi-tenancy** | +9–12 months | Visual designer; OIDC + RBAC; pluggable secrets; approval gates; HTTP webhook action runtime; workflow defs as OCI artifacts |
| **M4+ — Hardening** | beyond | WASM action runtime; re-run with modified inputs; full SLA and scale testing |

## Design documentation

| Document | Description |
|---|---|
| [design/requirements/requirements.md](design/requirements/requirements.md) | Full functional, non-functional, and technology requirements |
| [design/architecture/overview.md](design/architecture/overview.md) | Architecture overview, domain model, contracts, ADRs |
| [design/architecture/components.md](design/architecture/components.md) | Detailed component map and descriptions |
| [design/README.md](design/README.md) | Design documentation index and status |

## User and developer documentation

| Document | Description |
|---|---|
| [docs/README.md](docs/README.md) | Documentation index across audiences |
| [docs/users/evaluation/local-cluster.md](docs/users/evaluation/local-cluster.md) | Getting-started quickstart — deploy on a local kind / Docker Desktop Kubernetes cluster |
| [docs/developers/README.md](docs/developers/README.md) | Plugin, connection, and activity developer guide |
| [docs/developers/connections-api.md](docs/developers/connections-api.md) | Connector Manifest v1 reference for connection developers |

## Repository layout

```
custos/
├── design/              # design documents (requirements, architecture, components)
├── docs/                # user / developer / contributor documentation
├── src/
│   ├── services/        # one folder per deployable service (8 services)
│   ├── libs/            # shared Python libraries (SPL, common, callctx)
│   └── jobs/            # Helm-invoked Jobs (migrate, bootstrap)
├── deploy/
│   ├── helm/
│   │   ├── custos/      # umbrella chart (4 profile values files)
│   │   └── charts/      # per-component + dependency subcharts
│   ├── offline/         # air-gapped install bundle recipe
│   └── alert-rules/     # default observability alert rules
├── tests/               # helm-render + integration tests
├── scripts/             # helper scripts
└── .github/workflows/   # CI (helm lint/test)
```

## Helm

The umbrella chart at `deploy/helm/custos/` installs the currently wired platform components — Custos services, CloudNativePG, MinIO where enabled by the selected values file, External Secrets Operator, plus Envoy Gateway resources, the SPL migration Job, and the bootstrap Job.

Four profile values files cover the supported topologies (see [design/architecture/reference-deployment.md](design/architecture/reference-deployment.md)):

| File | Topology | Profile |
|---|---|---|
| `values-connected-eval.yaml` | Connected | Eval defaults |
| `values-connected-ha.yaml` | Connected | HA-oriented defaults with MinIO enabled |
| `values-airgapped-eval.yaml` | Air-gapped | Eval defaults for air-gapped environments |
| `values-airgapped-ha.yaml` | Air-gapped | HA-oriented defaults for air-gapped environments with MinIO enabled |

Replica counts are configured per service/subchart values; selecting an HA profile does not by itself force every Custos service to run 3 replicas.

```bash
make lint        # helm lint umbrella chart against all 4 profiles
make template    # render manifests into build/ for all 4 profiles
make bundle      # build air-gapped offline tarball (stub)
```

## License

Apache 2.0 — see [LICENSE](LICENSE).
