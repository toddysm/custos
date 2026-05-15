# Requirements: Custos

Last Updated: 2026-05-14
Version: 2

## Project Goal

Custos is a **pluggable workflow orchestrator for supply-chain security operations on OCI artifacts**. It enables security analysts, DevOps engineers, product managers, and developers to define and run arbitrary workflows that operate on container images and other cloud-native artifacts stored in OCI-compliant registries.

Workflows can be authored programmatically (YAML/SDK) or visually (designer UI). The orchestrator manages execution; all real work happens inside actions ("activities") that conform to a stable orchestrator–action contract, allowing custom and third-party actions without engine changes.

**Primary users:** security analysts, DevOps engineers, product managers, developers/engineers — both interactive (UI/CLI) and programmatic (API, CI/CD systems, automated agents).

**Success metrics for v1:**
1. **Time to create a workflow** — a typical user can author, register, and run a new workflow in minutes (target: <15 min for a 3-step workflow from blank slate).
2. **Execution reliability** — workflows complete with deterministic outcomes; transient failures are retried; the orchestrator survives crashes without losing in-flight workflow state (target: ≥99.9% of well-formed workflow runs reach a terminal state without operator intervention).

## Functional Requirements

| ID | Requirement | Priority | Status | Added |
|---|---|---|---|---|
| REQ-001 | Define workflows declaratively in YAML/JSON, addressable by name + version | High | Open | 2026-05-13 |
| REQ-002 | Programmatic SDK for authoring workflows (Python, matching orchestrator language) | High | Open | 2026-05-13 |
| REQ-003 | Visual workflow designer in the web UI (drag-and-drop graph editor) | Medium | Open | 2026-05-13 |
| REQ-004 | Trigger: manual / on-demand via REST API and UI | High | Open | 2026-05-13 |
| REQ-005 | Trigger: scheduled (cron-style expressions, per workflow) | High | Open | 2026-05-13 |
| REQ-006 | Trigger: OCI registry events (push, delete, tag) via inbound webhook receiver | High | Open | 2026-05-13 |
| REQ-007 | Execution: sequential DAG with explicit step dependencies | High | Open | 2026-05-13 |
| REQ-008 | Execution: parallel branches with fan-out / fan-in | High | Open | 2026-05-13 |
| REQ-009 | Execution: conditional steps (if / when / unless expressions over prior step outputs) | High | Open | 2026-05-13 |
| REQ-010 | Execution: configurable retry and error-handling policies per step | High | Open | 2026-05-13 |
| REQ-011 | Execution: loops / iteration over artifact lists | Medium | Open | 2026-05-13 |
| REQ-012 | Execution: approval / human-in-the-loop gates | Medium | Open | 2026-05-13 |
| REQ-013 | Action runtime: OCI container actions (action = image + manifest, run in a sandbox) | High | Open | 2026-05-13 |
| REQ-014 | Action runtime: HTTP webhook actions (action = remote URL with request/response contract) | Medium | Open | 2026-05-13 |
| REQ-015 | Action runtime: WebAssembly module actions | Low | Open | 2026-05-13 |
| REQ-016 | Built-in action: vulnerability scanning (Trivy/Grype-style adapter) | High | Open | 2026-05-13 |
| REQ-017 | Built-in action: SBOM generation/extraction | High | Open | 2026-05-13 |
| REQ-018 | Built-in action: signature verification (cosign / notation) | High | Open | 2026-05-13 |
| REQ-019 | Built-in action: attestation creation (in-toto / SLSA-format) | Medium | Open | 2026-05-13 |
| REQ-020 | Built-in action: policy evaluation (delegates to configured policy engine) | Medium | Open | 2026-05-13 |
| REQ-021 | Built-in action: image promotion / copy across registries (ORAS-style) | Medium | Open | 2026-05-13 |
| REQ-022 | Custom user-supplied actions (container-based) must work via the same contract as built-ins | High | Open | 2026-05-13 |
| REQ-023 | Stable orchestrator ↔ action contract (versioned input/output schema, exit codes, log streaming) | High | Open | 2026-05-13 |
| REQ-024 | Action catalog / registry: actions are discoverable and versioned | High | Open | 2026-05-13 |
| REQ-025 | Workflow versioning: editing a workflow produces a new immutable version; runs reference a specific version | High | Open | 2026-05-13 |
| REQ-026 | Workflow run inspection: per-run timeline, step inputs/outputs, logs, status | High | Open | 2026-05-13 |
| REQ-027 | Cancel a running workflow run | High | Open | 2026-05-13 |
| REQ-028 | Re-run a completed workflow run (with same or modified inputs) | Medium | Open | 2026-05-13 |
| REQ-029 | Workflow definitions can be stored as OCI artifacts in a registry (versioning by digest/tag) | Medium | Open | 2026-05-13 |
| REQ-073 | The workflow platform must provide first-class workflow primitives for eventing, orchestration, and common control constructs such as branches, loops, fan-out/fan-in, conditions, retries, and step coordination | High | Open | 2026-05-14 |
| REQ-074 | The platform must provide an extensible connector model so initial connections can target OCI registries and later be extended to storage accounts, databases, and other external systems without redesigning the core platform | High | Open | 2026-05-14 |
| REQ-075 | Activities must be independently pluggable, packaged, versioned, and deployable so new activities can be added without requiring a platform upgrade or code change in the core orchestrator | High | Open | 2026-05-14 |

## Non-Functional Requirements

| ID | Requirement | Target | Status | Added |
|---|---|---|---|---|
| REQ-030 | Throughput | 1K–10K workflow runs/day across dozens of teams | Open | 2026-05-13 |
| REQ-031 | Scheduling latency | p95 < 1s between step completion and next step dispatch | Open | 2026-05-13 |
| REQ-032 | Availability | 99.9% control plane uptime | Open | 2026-05-13 |
| REQ-033 | Durability | If orchestrator crashes mid-run, the run resumes from the last completed step (durable state, idempotent step boundary) | Open | 2026-05-13 |
| REQ-034 | AuthN: OIDC for human users (any compliant provider) | All UI/API access | Open | 2026-05-13 |
| REQ-035 | AuthN: service accounts / API tokens for programmatic clients | Required | Open | 2026-05-13 |
| REQ-036 | AuthZ: RBAC over workflows, actions, runs, registries, and secrets | Required | Open | 2026-05-13 |
| REQ-037 | Secrets management: action credentials and registry auth retrieved from a pluggable secrets backend; never stored in workflow definitions | Required | Open | 2026-05-13 |
| REQ-038 | Audit log: every workflow run, every authz decision, every secret access logged with actor, time, and resource | Required | Open | 2026-05-13 |
| REQ-039 | Sandboxed action execution: actions run isolated from host (no escape, no access to orchestrator memory/filesystem) | Required | Open | 2026-05-13 |
| REQ-040 | Per-workflow execution logs accessible via API and UI | Required | Open | 2026-05-13 |
| REQ-041 | Per-action step logs and produced artifacts retained per configured retention policy | Required | Open | 2026-05-13 |
| REQ-042 | Metrics: Prometheus / OpenMetrics endpoint exposing engine and per-action metrics | Required | Open | 2026-05-13 |
| REQ-043 | Distributed tracing: OpenTelemetry spans for workflow run → step → action call chain | Required | Open | 2026-05-13 |
| REQ-044 | Alerting hooks: emit events on configurable conditions (run failed, SLO breach, etc.) | Required | Open | 2026-05-13 |

## Technology Constraints

| ID | Constraint | Reason | Added |
|---|---|---|---|
| REQ-045 | Orchestrator and built-in actions implemented in Python | Team familiarity; matches Dapr Python SDK | 2026-05-13 |
| REQ-046 | Workflow engine built on Dapr Workflow (durable execution actor model) | Provides durability, replay, and pluggable state stores out of the box | 2026-05-13 |
| REQ-047 | Frontend implemented in React + TypeScript | Modern ecosystem; required for visual designer (React Flow / similar) | 2026-05-13 |
| REQ-048 | Persistent state: PostgreSQL (workflow defs, run metadata, audit log, RBAC) | Mature relational store; strong durability | 2026-05-13 |
| REQ-049 | Ephemeral state / queues: Redis (Dapr state and pub/sub component) | Standard Dapr-supported backend | 2026-05-13 |
| REQ-050 | Large artifacts and step logs: S3-compatible object storage | Cheap, durable storage for variable-sized blobs | 2026-05-13 |
| REQ-051 | Workflow definitions MAY be stored as OCI artifacts in an OCI registry | Enables registry-native workflow distribution | 2026-05-13 |
| REQ-052 | All services run on Kubernetes (any conformant cluster, including AKS/EKS/GKE) | Required by Dapr sidecar model; cloud-agnostic | 2026-05-13 |
| REQ-053 | Cloud-agnostic: no hard dependency on any single cloud provider's services | Portability across Azure / AWS / GCP / on-prem | 2026-05-13 |
| REQ-054 | Open source under Apache 2.0 from day one | Maximize adoption and contribution | 2026-05-13 |

## Deployment Model

Custos is deployed as a set of containerized microservices on **Kubernetes**, with **Dapr sidecars** providing workflow durability, state management, pub/sub, and secrets abstraction.

- **Target environments:** any conformant Kubernetes cluster, including managed offerings (AKS, EKS, GKE) and self-managed (vanilla, k3s, OpenShift).
- **Cloud posture:** cloud-agnostic. No hard dependency on a single cloud's proprietary services. Cloud-specific integrations (managed Postgres, managed Redis, S3-vs-Blob-vs-GCS, secrets backends) are configured through Dapr components.
- **Distribution:**
  - Container images per service (orchestrator, API, UI, webhook receiver, scheduler, etc.)
  - Helm chart for installation
  - Dapr component manifests (state store, pub/sub, secrets) shipped as templates
- **Target OCI registries:** any OCI-compliant registry (Docker Hub, GHCR, ACR, ECR, GAR, Harbor, Zot, etc.). Distribution spec v1.1 with Referrers API preferred; v1.0 fallback supported via subject-manifest tag scheme.

## Integrations

| ID | System | Purpose | Status | Added |
|---|---|---|---|---|
| REQ-055 | OCI Distribution spec v1.1 (preferred) with v1.0 fallback | Read/write artifacts and referrers across registries | Open | 2026-05-13 |
| REQ-056 | OIDC providers (generic) | Human user authentication | Open | 2026-05-13 |
| REQ-057 | GitHub OIDC | CI-issued workload tokens for programmatic clients | Open | 2026-05-13 |
| REQ-058 | Azure AD / Entra ID | Enterprise SSO option | Open | 2026-05-13 |
| REQ-059 | SPIFFE / SPIRE workload identity | Service-to-service identity inside the cluster | Open | 2026-05-13 |
| REQ-060 | Kubernetes Secrets (baseline secrets backend) | Default secrets store | Open | 2026-05-13 |
| REQ-061 | Dapr Secrets API (abstracts Vault, Key Vault, AWS SM, GCP SM, etc.) | Pluggable secrets backends without code changes | Open | 2026-05-13 |
| REQ-062 | OPA (Rego) policy engine | Built-in policy-eval action backend | Open | 2026-05-13 |
| REQ-063 | CUE policy engine | Alternative policy backend | Open | 2026-05-13 |
| REQ-064 | CEL policy engine | Alternative policy backend | Open | 2026-05-13 |
| REQ-065 | Kyverno policy engine | Alternative policy backend | Open | 2026-05-13 |
| REQ-066 | Custom HTTP policy endpoint | Allow arbitrary remote policy evaluators | Open | 2026-05-13 |
| REQ-067 | Notification: generic webhook | Universal notification channel | Open | 2026-05-13 |
| REQ-068 | Notification: Slack | Common chat ops channel | Open | 2026-05-13 |
| REQ-069 | Notification: Microsoft Teams | Enterprise chat ops channel | Open | 2026-05-13 |
| REQ-070 | Notification: Email (SMTP) | Baseline notification | Open | 2026-05-13 |
| REQ-071 | Notification: PagerDuty | On-call escalation | Open | 2026-05-13 |
| REQ-072 | All other supply-chain tooling (Trivy, Grype, Syft, cosign, notation, Copa, ORAS, in-toto, GitHub APIs, etc.) is integrated **via custom or built-in actions**, NOT first-class engine dependencies | Keeps the orchestrator integration-agnostic and the action contract authoritative | Open | 2026-05-13 |

## Timeline & Milestones

**Team & horizon:** solo, nights-and-weekends, **3 months to v1 (M1)**. The ambition is large for this capacity; v1 scope is intentionally minimal. M2 and M3 extend beyond the 3-month horizon.

> **Scope-vs-capacity note:** with a solo nights-and-weekends team, the full requirements set above is realistically a multi-quarter program. v1 (M1) MUST stay limited to the items marked High and required for an end-to-end demo. Items marked Medium/Low are scoped into M2/M3.

| Milestone | Target | Scope | Dependencies |
|---|---|---|---|
| M1 — Core engine | +3 months (≈ 2026-08-13) | Dapr-Workflow-backed engine; YAML-defined DAG workflows and core workflow primitives (REQ-001, REQ-007–010, REQ-025, REQ-073); manual API trigger (REQ-004); 2 built-in actions (vuln scan REQ-016, signature verify REQ-018); OCI container action runtime and independently pluggable activities (REQ-013, REQ-022, REQ-023, REQ-075); run inspection (REQ-026, REQ-027); minimal auth (REQ-035 API tokens); Postgres + Redis + S3 (REQ-048–050); basic logs + Prometheus metrics (REQ-040, REQ-042); Helm chart (subset of REQ-052); audit log skeleton (REQ-038) | None |
| M2 — Triggers & action breadth | +6 months | Scheduled trigger (REQ-005); registry webhook trigger (REQ-006); extensible connector model beyond registries (REQ-074); SBOM action (REQ-017); attestation action (REQ-019); policy eval action with OPA backend (REQ-020, REQ-062, REQ-066); image promotion action (REQ-021); generic webhook + Slack notifications (REQ-067, REQ-068); OpenTelemetry tracing (REQ-043); SDK for action authors (REQ-022 hardening) | M1 |
| M3 — UX, security, multi-tenancy | +9–12 months | Visual designer (REQ-003); OIDC for users (REQ-034, REQ-056); RBAC (REQ-036); full secrets backend pluggability via Dapr (REQ-037, REQ-061); approval gates (REQ-012); loops (REQ-011); HTTP webhook action runtime (REQ-014); workflow defs as OCI artifacts (REQ-029, REQ-051); remaining notification channels; remaining policy engines; SPIFFE/SPIRE (REQ-059); GitHub OIDC and Entra ID (REQ-057, REQ-058) | M2 |
| M4+ — Hardening & advanced | beyond | WebAssembly action runtime (REQ-015); re-run with modified inputs (REQ-028); full SLA achievement (REQ-032); scale testing to upper bound of REQ-030 | M3 |

## Open TODOs

- [ ] TODO-002: Decide action sandbox technology (gVisor, Kata, plain runc with seccomp/AppArmor, or Kubernetes Jobs only) — REQ-039 (added 2026-05-13, issue #4)
- [ ] TODO-003: Decide trigger receiver design for REQ-006 — pull (poll) vs. push (webhook from registry) per registry vendor; not all registries push events uniformly (added 2026-05-13, issue #5)
- [ ] TODO-004: Define minimal viable action contract schema (inputs, outputs, secrets injection, log streaming, exit codes) — REQ-023; this is the most load-bearing contract in the system (added 2026-05-13, issue #6)

## Resolved TODOs

- [x] TODO-001: Confirm Dapr Workflow's Python SDK feature parity with the .NET/Go SDKs (suspend/resume, sub-orchestrations, continue-as-new) before locking in REQ-046 — verified and closed on 2026-05-14 (issue #3)

## Change History

| Date | Change | GitHub Issue |
|---|---|---|
| 2026-05-13 | Initial requirements | #2 |
| 2026-05-14 | Verified Dapr Workflow Python SDK parity and closed TODO-001 | #3 |
| 2026-05-14 | Added workflow primitives, extensible connectors, and independently pluggable activity requirements | #8 |
