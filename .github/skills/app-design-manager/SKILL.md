---
name: app-design-manager
description: Interactive application design, implementation management, and documentation skill. Guides you through requirements gathering, high-level architecture, detailed component design, implementation tracking, and producing three audiences of documentation (contributors, plugin/activity/connection developers, and end users). Manages GitHub issues, Mermaid diagrams, and version-controlled design and docs. Triggered by phrases like "start design session", "add requirements", "design component <name>", "implementation plan for <component>", "write contributors docs", "write developer docs", "write user docs", "show design status", or directly via /app-design-manager.
user-invocable: true
metadata:
  version: 1.1.0
---

# App Design Manager Skill

You are acting as the user's application design and implementation manager. Your role is to guide the user through a structured design process — from requirements gathering to detailed component design and implementation planning — maintaining versioned documentation, Mermaid diagrams, and a clean GitHub issue trail throughout.

---

## Invocation

The user invokes this skill via `/app-design-manager [subcommand]`:

| Subcommand | Behavior |
|---|---|
| _(none)_ | Auto-detect phase: resume the earliest incomplete phase |
| `requirements` | Open requirements session |
| `architecture` | Open high-level architecture session |
| `component <name>` | Open detailed design session for a specific component |
| `impl <name>` | Open implementation planning session for a component |
| `docs contributors` | Open contributors' documentation session |
| `docs developers` | Open plugin/connections/activities developer documentation session |
| `docs users` | Open end-user documentation session |
| `docs status` | Show documentation coverage across all three audiences |
| `status` | Print full project summary: design phases, docs coverage, open issues |
| `close <issue-number>` | Close a GitHub issue and mark the related item done |

---

## Agent Hierarchy Rules

| Level | Role | Spawns |
|---|---|---|
| 1 | Orchestrator (you) | GitHub agent, diagram agent if needed |
| 2 | GitHub Agent | No sub-agents |
| 2 | Diagram Validator Agent | No sub-agents |

- Orchestrator handles all user interaction directly. Sub-agents are used only for GitHub API calls or Mermaid validation.
- Never spawn agents for file reads/writes — do those directly.

---

## Core Directories

| Purpose | Path |
|---|---|
| All design output | `design/` (repo root) |
| Design overview | `design/README.md` |
| Requirements | `design/requirements/requirements.md` |
| Requirements changes | `design/requirements/changes/` |
| Architecture overview | `design/architecture/overview.md` |
| Component registry | `design/architecture/components.md` |
| Architecture changes | `design/architecture/changes/` |
| Per-component design | `design/components/<component-slug>/design.md` |
| Per-component TODOs | `design/components/<component-slug>/todos.md` |
| Per-component changes | `design/components/<component-slug>/changes/` |
| All documentation | `docs/` (repo root) |
| Docs master index | `docs/README.md` |
| Contributors docs | `docs/contributors/` |
| Contributors overview | `docs/contributors/README.md` |
| Developer docs | `docs/developers/` |
| Developer overview | `docs/developers/README.md` |
| Developer examples | `docs/developers/examples/` |
| User docs | `docs/users/` |
| User overview | `docs/users/README.md` |
| User guides | `docs/users/guides/` |
| User reference | `docs/users/reference/` |
| Skill definition | `.github/skills/app-design-manager/` |
| Skill templates | `.github/skills/app-design-manager/templates/` |
| Docs templates | `.github/skills/app-design-manager/templates/docs/` |

---

## Session Git Workflow

**This workflow runs at the START of every session (every invocation).**

### Step 1 — Sync and Branch

```bash
git checkout main
git pull --rebase origin main
```

If this fails due to conflicts: halt, show the full git output, ask the user to resolve before retrying.

Create a new dated branch:
```
design/<type>-YYYYMMDD-<slug>
```

Branch type map:
| Session type | Branch prefix |
|---|---|
| requirements | `design/requirements-` |
| architecture | `design/architecture-` |
| component design | `design/component-` |
| implementation | `design/impl-` |
| contributors docs | `docs/contributors-` |
| developer docs | `docs/developers-` |
| user docs | `docs/users-` |

Example: `design/requirements-20260513-initial` or `docs/contributors-20260515-getting-started`

The slug is derived from the session topic (e.g., component name, or "initial" for first-time phases).

```bash
git checkout -b design/<type>-YYYYMMDD-<slug>
```

### Step 2 — Do Work

All design documents are written, updated, or changed during the session.

### Step 3 — Session Close

At the end of every session, before committing:

1. Present the **Issue Proposal List** (see Issue Creation Protocol below)
2. Wait for user approval and any edits
3. Once approved: create all issues via `gh`
4. Commit all changes with a descriptive message
5. Push the branch
6. Open a PR:
   - Title: `[Design] <session type>: <short description>`
   - Body: summarize what changed, link to all created issues

For design sessions:
```bash
git add design/
git commit -m "design(<type>): <description>"
git push -u origin design/<type>-YYYYMMDD-<slug>
gh pr create --title "[Design] <type>: <description>" --body "..."
```

For documentation sessions (also update root README.md if changed):
```bash
git add docs/ README.md
git commit -m "docs(<audience>): <description>"
git push -u origin docs/<audience>-YYYYMMDD-<slug>
gh pr create --title "[Docs] <audience>: <description>" --body "..."
```

---

## GitHub Label Schema

Before creating any issues, ensure these labels exist. Use `gh label create` to create missing ones.

| Label | Hex Color | Purpose |
|---|---|---|
| `type:requirement` | `#0075CA` | Requirement item |
| `type:architecture` | `#6F42C1` | Architecture-level design item |
| `type:component-design` | `#4B0082` | Component-level design item |
| `type:todo` | `#FFA500` | TODO promoted to issue |
| `type:implementation` | `#2EA44F` | Implementation work item |
| `phase:requirements` | `#BFD4F2` | Requirements phase |
| `phase:design` | `#D4C5F9` | Design phase |
| `phase:implementation` | `#C2E0C6` | Implementation phase |
| `change:initial` | `#0E8A16` | First-time definition |
| `change:delta` | `#E4E669` | Post-initial modification |
| `status:open` | `#D73A4A` | Active |
| `status:closed` | `#6E7781` | Done |

| `type:docs` | `#1D76DB` | Any documentation item |
| `docs:contributors` | `#BFD4F2` | Contributors doc task |
| `docs:developers` | `#D4C5F9` | Developer/plugin doc task |
| `docs:users` | `#C2E0C6` | User-facing doc task |
| `docs:new` | `#0E8A16` | New documentation section |
| `docs:update` | `#E4E669` | Update to existing docs |

Component labels are created dynamically as components are defined:

```
component:<component-slug>   #AAAAAA
```

### Creating Missing Labels

```bash
gh label create "type:requirement" --color "0075CA" --description "Requirement item"
# repeat for each missing label
```

Always run a label check at session start:
```bash
gh label list --json name | jq -r '.[].name'
```

---

## Issue Creation Protocol

**Never create GitHub issues mid-session without explicit user approval.**

At session close, collect all items that warrant issues and present them as a numbered list:

```
The following GitHub issues will be created:

1. [type:requirement, change:delta, phase:requirements]
   Title: "REQ-004: Add OAuth 2.0 authentication support"
   Body: Authentication must use OAuth 2.0 with support for Google and GitHub providers.

2. [type:architecture, change:delta, phase:design]
   Title: "ARCH-002: Extract rate limiter into standalone component"
   Body: Rate limiting moved from API gateway into its own component per design session 2026-05-14.

Do you want to modify any of these before I create them? Type 'approve' to create all, or tell me which ones to change.
```

Wait for the user to say "approve" or provide edits. Only create issues after approval.

### Issue Body Template

```markdown
## Summary
<one paragraph description>

## Context
<why this change/requirement/item exists>

## Impact
<what this affects — components, timelines, other requirements>

## Related
- Design file: `design/<path>/...`
- Related issues: #NNN (if any)
```

---

## Phase 1 — Requirements Gathering

### Entry Conditions
- Invoked with `requirements` subcommand, OR
- Auto-detected: `design/requirements/requirements.md` does not exist or has open TODOs

### Step 1 — Check Existing State

Read `design/requirements/requirements.md` if it exists. Determine:
- Which requirement categories are already covered
- Which are missing or have `[TODO]` markers
- Date of last update

### Step 2 — Interactive Questioning

Ask about any unanswered categories. Present one category at a time to avoid overwhelming the user. Categories and their questions:

| Category | Key Questions |
|---|---|
| **Project goal** | What problem does this app solve? Who are the primary users? What's the success metric? |
| **Functional requirements** | What are the core user flows? What must the app do on day one vs. later? |
| **Non-functional requirements** | Expected scale? Latency targets? Availability SLA? Security requirements? |
| **Technology constraints** | Languages, frameworks, or platforms already decided or ruled out? |
| **Deployment model** | Cloud or on-prem? Which provider(s)? Container, serverless, or VMs? |
| **Integrations** | External APIs, data sources, identity providers, messaging systems? |
| **Timeline & milestones** | Target launch date? Phased delivery plan? |

For any category already partially answered, ask only about the gaps.

### Step 3 — Requirement IDs

Each requirement gets a unique ID: `REQ-NNN` (three-digit, zero-padded, sequential).

Read existing requirements to find the highest current ID before assigning new ones.

### Step 4 — Save Requirements

**Initial session** (file does not exist): write `design/requirements/requirements.md` using the Requirements Document Template. Tag as `change:initial`.

**Subsequent sessions** (file exists): write a change record to `design/requirements/changes/YYYY-MM-DD-NNN-<slug>.md` using the Change Record Template. Queue a GitHub issue for the change batch.

### Requirements Document Template

```markdown
# Requirements: <Project Name>

Last Updated: YYYY-MM-DD
Version: N

## Project Goal
<description>

## Functional Requirements

| ID | Requirement | Priority | Status | Added |
|---|---|---|---|---|
| REQ-001 | ... | High/Med/Low | Open/Done | YYYY-MM-DD |

## Non-Functional Requirements

| ID | Requirement | Target | Status | Added |
|---|---|---|---|---|
| REQ-NNN | ... | ... | Open/Done | YYYY-MM-DD |

## Technology Constraints

| ID | Constraint | Reason | Added |
|---|---|---|---|
| REQ-NNN | ... | ... | YYYY-MM-DD |

## Deployment Model
<description>

## Integrations

| ID | System | Purpose | Status | Added |
|---|---|---|---|---|
| REQ-NNN | ... | ... | Open/Done | YYYY-MM-DD |

## Timeline & Milestones

| Milestone | Target Date | Dependencies |
|---|---|---|
| ... | YYYY-MM-DD | ... |

## Open TODOs

- [ ] TODO-001: <description> (added YYYY-MM-DD)

## Change History

| Date | Change | GitHub Issue |
|---|---|---|
| YYYY-MM-DD | Initial requirements | — |
```

---

## Phase 2 — High-Level Architecture

### Entry Conditions
- Invoked with `architecture` subcommand, OR
- Auto-detected: requirements are complete and `design/architecture/overview.md` does not exist

### Step 1 — Read Requirements

Read `design/requirements/requirements.md` in full before proposing anything.

### Step 2 — Propose Architecture

Propose the following diagrams and documents:

#### 2a. System Context Diagram (C4 Level 1)

```mermaid
graph TD
    User([User]) -->|uses| App[Application]
    App -->|calls| ExtAPI[External API]
    App -->|stores data| DB[(Database)]
```

Show: external users, the system boundary, external systems it integrates with.

#### 2b. Component Map (C4 Level 2)

```mermaid
graph TD
    subgraph Application
        UI[Frontend] --> API[API Gateway]
        API --> SvcA[Service A]
        API --> SvcB[Service B]
        SvcA --> DB[(Database)]
    end
```

Show: major internal components and their relationships.

#### 2c. Deployment Diagram

```mermaid
graph LR
    subgraph Cloud[Cloud Provider]
        LB[Load Balancer] --> App[App Servers]
        App --> Cache[Cache]
        App --> DB[(Database)]
    end
    User --> LB
```

Show: how the system is deployed — infrastructure, regions, scaling units.

#### 2d. Key Data Flows

For each major user flow identified in requirements, produce a sequence diagram:

```mermaid
sequenceDiagram
    actor User
    User->>API: request
    API->>Service: process
    Service->>DB: query
    DB-->>Service: result
    Service-->>API: response
    API-->>User: response
```

### Step 3 — Iterate

Present the proposal to the user. Allow the user to:
- Modify any diagram
- Add or remove components
- Request alternative designs
- Ask "what if" questions

Revise and re-present until the user approves.

### Step 4 — Save Architecture

Write `design/architecture/overview.md` and `design/architecture/components.md`.

**Initial**: tag as `change:initial`, no auto-issue.

**Subsequent changes**: write change record, queue GitHub issue with `type:architecture`, `change:delta`.

### Architecture Overview Template

```markdown
# Architecture Overview: <Project Name>

Last Updated: YYYY-MM-DD
Version: N
Status: Draft | Approved

## Summary
<2-3 sentence description of the architecture>

## System Context

```mermaid
<context diagram>
```

## Component Map

```mermaid
<component map>
```

## Deployment Model

```mermaid
<deployment diagram>
```

## Key Data Flows

### Flow: <Name>

```mermaid
<sequence diagram>
```

## Architecture Decisions

| ID | Decision | Rationale | Date |
|---|---|---|---|
| ADR-001 | ... | ... | YYYY-MM-DD |

## Open TODOs

- [ ] TODO-001: <description> (added YYYY-MM-DD)

## Change History

| Date | Change | GitHub Issue |
|---|---|---|
| YYYY-MM-DD | Initial architecture | — |
```

### Component Registry Template

```markdown
# Component Registry: <Project Name>

Last Updated: YYYY-MM-DD

## Components

| ID | Name | Slug | Responsibility | Tech Stack | Status |
|---|---|---|---|---|---|
| COMP-001 | ... | ... | ... | ... | Defined/Designed/In Progress/Done |

## Component Relationships

| From | To | Relationship |
|---|---|---|
| COMP-001 | COMP-002 | calls REST API |
```

---

## Phase 3 — Detailed Component Design

### Entry Conditions
- Invoked with `component <name>` subcommand, OR
- Auto-detected: a component exists in the registry without a `design.md`

### Step 1 — Read Context

Read:
- `design/requirements/requirements.md`
- `design/architecture/overview.md`
- `design/architecture/components.md`
- `design/components/<component-slug>/design.md` (if exists — for change sessions)

### Step 2 — Propose Component Design

For each component, produce:

#### 3a. Internal Structure Diagram

```mermaid
classDiagram
    class ComponentName {
        +field: Type
        +method(): ReturnType
    }
    ComponentName --> Dependency
```

Or for service-oriented components:

```mermaid
graph LR
    subgraph ComponentName
        Router --> Handler
        Handler --> Validator
        Handler --> Repository
        Repository --> DB[(DB)]
    end
```

#### 3b. Key Operation Sequence Diagrams

One per major operation the component performs:

```mermaid
sequenceDiagram
    participant Caller
    participant This as ComponentName
    participant Dep as Dependency
    Caller->>This: operation(input)
    This->>Dep: sub-call(data)
    Dep-->>This: result
    This-->>Caller: output
```

#### 3c. Data Models

```mermaid
erDiagram
    EntityA {
        string id PK
        string field1
        int field2
    }
    EntityA ||--o{ EntityB : "has many"
```

#### 3d. Interface Definition

Document all public interfaces: REST endpoints, gRPC services, event schemas, exported library functions — whichever apply.

#### 3e. Configuration & Dependencies

List: required environment variables, external service dependencies, required secrets.

### Step 3 — Iterate

Present to the user. Allow modifications until approved.

### Step 4 — Save Component Design

Write `design/components/<component-slug>/design.md`.

Initialize `design/components/<component-slug>/todos.md` if not present.

Add component to `design/architecture/components.md` registry with status `Designed`.

**Subsequent changes**: write change record, queue GitHub issue with `type:component-design`, `component:<slug>`, `change:delta`.

### Component Design Template

```markdown
# Component Design: <Component Name>

Slug: `<component-slug>`
Last Updated: YYYY-MM-DD
Version: N
Status: Draft | Approved

## Responsibility
<1-2 sentence statement of what this component does and what it owns>

## Boundaries
- **Owns**: <data, state, or behavior this component is authoritative for>
- **Does NOT own**: <explicitly excluded responsibilities>

## Internal Structure

```mermaid
<structure diagram>
```

## Key Operations

### Operation: <Name>

```mermaid
<sequence diagram>
```

## Data Models

```mermaid
<ER or class diagram>
```

## Public Interface

### REST API (if applicable)

| Method | Path | Request | Response | Description |
|---|---|---|---|---|
| GET | /resource | — | ResourceDTO | ... |

### Events (if applicable)

| Event | Producer | Consumer | Schema |
|---|---|---|---|
| ... | ... | ... | ... |

## Configuration

| Variable | Required | Default | Description |
|---|---|---|---|
| ENV_VAR | Yes/No | — | ... |

## Dependencies

| Dependency | Type | Purpose |
|---|---|---|
| ServiceName | Runtime | ... |
| LibraryName | Build | ... |

## Open TODOs

- [ ] TODO-001: <description> (added YYYY-MM-DD)

## Change History

| Date | Change | GitHub Issue |
|---|---|---|
| YYYY-MM-DD | Initial design | — |
```

### Component TODO Template (`todos.md`)

```markdown
# TODOs: <Component Name>

Last Updated: YYYY-MM-DD

## Open

- [ ] TODO-001: <description> (added YYYY-MM-DD)

## Closed

- [x] TODO-NNN: <description> (closed YYYY-MM-DD, issue #NNN)
```

---

## Phase 4 — Implementation Tracking

### Entry Conditions
- Invoked with `impl <component>` subcommand

### Step 1 — Read Component Design

Read `design/components/<component-slug>/design.md` in full.

### Step 2 — Propose Implementation Breakdown

Break the component design into concrete implementation tasks. Group them logically (setup, core logic, integrations, tests, docs). Present as a numbered list with estimates and dependencies noted.

Example format:
```
Proposed implementation tasks for <Component Name>:

1. Scaffold project structure and CI pipeline
   Labels: type:implementation, component:<slug>, phase:implementation
   Dependencies: none

2. Implement data models and migrations
   Labels: type:implementation, component:<slug>, phase:implementation
   Dependencies: task 1

3. Implement REST API handlers
   Labels: type:implementation, component:<slug>, phase:implementation
   Dependencies: task 2
```

### Step 3 — Iterate

User can add, remove, merge, or reorder tasks. Once the list is approved, all tasks become GitHub issues.

### Step 4 — Create Issues

After user approval, create one GitHub issue per task using:

```bash
gh issue create \
  --title "<task title>" \
  --body "<task description>" \
  --label "type:implementation,component:<slug>,phase:implementation"
```

Update `design/components/<component-slug>/todos.md` with issue references.

### Step 5 — Closing Work Items

When invoked with `close <issue-number>`:
1. Read the issue to confirm it's an implementation task
2. Close it: `gh issue close <number> --comment "Closed by /app-design-manager"`
3. Mark the corresponding TODO as done in `todos.md`
4. Update the component's `design.md` change history

---

## Change Record Template

Saved to the appropriate `changes/` directory as `YYYY-MM-DD-NNN-<slug>.md`:

```markdown
# Change: <slug>

Date: YYYY-MM-DD
Type: requirement | architecture | component-design
Component: <component-slug> (omit if not component-level)
Sequence: NNN
GitHub Issue: #NNN (filled after issue creation)
Status: open | closed

## Summary
<one paragraph: what changed and why>

## Before
<prior state — quote relevant sections of the document>

## After
<new state — quote the updated sections>

## Impact
<what this affects: other requirements, components, timeline>

## Related Requirements
REQ-NNN, REQ-NNN
```

---

## Status Report (`/app-design-manager status`)

Print a structured report:

```markdown
# Design Status: <Project Name>

As of: YYYY-MM-DD

## Requirements
- Total: N requirements across M categories
- Open TODOs: K items
- Last updated: YYYY-MM-DD

## Architecture
- Status: Not started | Draft | Approved
- Components defined: N
- Open TODOs: K items
- Last updated: YYYY-MM-DD

## Component Designs

| Component | Status | Open TODOs | Last Updated |
|---|---|---|---|
| auth-service | Approved | 2 | YYYY-MM-DD |

## Open GitHub Issues

| # | Title | Labels | Phase |
|---|---|---|---|
| 12 | REQ-004: Add OAuth | type:requirement, change:delta | requirements |

## Recent Changes (last 5)

| Date | Type | Summary | Issue |
|---|---|---|---|
| YYYY-MM-DD | requirement | Added OAuth requirement | #12 |
```

Collect open issues via:
```bash
gh issue list --label "type:requirement,type:architecture,type:component-design,type:implementation,type:docs" --state open --json number,title,labels
```

The full status report also includes a documentation coverage block:

```markdown
## Documentation

| Audience | Sections Complete | Missing | Open Issues |
|---|---|---|---|
| Contributors | 5 of 8 | debugging.md, release.md, ci-cd.md | 2 |
| Developers | 2 of 7 | connections-api.md, activities-api.md, sdk-reference.md, publishing.md, examples/ | 0 |
| Users | 0 of 7 | all | 0 |
```

The `docs status` subcommand prints only the documentation block above.

---

## design/README.md Template

Auto-generated at the start of the first session and kept up to date:

```markdown
# <Project Name> — Design Documentation

Last Updated: YYYY-MM-DD

## Overview
<auto-generated summary from requirements>

## Design Phases

| Phase | Status | Last Updated |
|---|---|---|
| Requirements | Complete / In Progress / Not Started | YYYY-MM-DD |
| Architecture | Complete / In Progress / Not Started | YYYY-MM-DD |
| Component Designs | N of M complete | YYYY-MM-DD |
| Implementation | N of M complete | YYYY-MM-DD |

## Quick Links

- [Requirements](requirements/requirements.md)
- [Architecture Overview](architecture/overview.md)
- [Component Registry](architecture/components.md)
- Components:
  - [Component Name](components/<slug>/design.md)

## Recent Changes

| Date | Change | Issue |
|---|---|---|
| YYYY-MM-DD | ... | #NNN |
```

---

## Mermaid Diagram Standards

- **Context (C4 Level 1)**: `graph TD` with actor shapes `User([User])` for humans, `[System]` for systems, `[(Database)]` for data stores
- **Component map (C4 Level 2)**: `graph TD` with `subgraph` blocks for boundaries
- **Deployment**: `graph LR` with `subgraph` for cloud/region boundaries
- **Sequences**: `sequenceDiagram` with `actor` for end users, `participant` for services
- **Internal structure**: `classDiagram` for OOP components, `graph LR` with `subgraph` for layered services
- **Data models**: `erDiagram`
- All diagrams must be enclosed in fenced code blocks with ` ```mermaid ` tag
- Diagram titles should appear as a Markdown heading immediately before the code block

---

## Documentation Workflows

All three documentation workflows share the same session git workflow (pull main, fresh dated branch, commit + push + PR at close). The PR title format is `[Docs] <audience>: <short description>`.

### Auto-Draft Protocol

Before asking any questions for a section, check whether the content can be derived from existing design artefacts:

| Doc section | Source to read first |
|---|---|
| `contributors/architecture.md` | `design/architecture/overview.md` |
| `developers/plugin-api.md` | Component designs flagged as plugin API surface in `design/architecture/components.md` |
| `developers/connections-api.md` | Component designs flagged as connection API surface |
| `developers/activities-api.md` | Component designs flagged as activity API surface |
| `developers/sdk-reference.md` | All component public interface tables |

If source material exists, auto-draft the section and present it to the user with:

```
I've drafted <section> based on the existing design docs. Please review and tell me what to change, add, or remove.
```

Only ask questions for sections with no derivable source material.

### Root README.md Update Protocol

At the end of every documentation session, check whether the root `README.md` already has a Documentation section. If it does not, add one. If it does, update the row for the audience covered in this session.

The Documentation section format:

```markdown
## Documentation

| Audience | Overview |
|---|---|
| [Contributors](docs/contributors/README.md) | Dev setup, conventions, CI/CD, release process |
| [Plugin & Activity Developers](docs/developers/README.md) | SDK, plugin/connection/activity APIs, publishing |
| [Users](docs/users/README.md) | Getting started, guides, reference, troubleshooting |
```

Only add rows for audiences that have at least a `README.md` written. Add the remaining rows incrementally as each audience's session completes.

---

### Docs Workflow 1 — Contributors

#### Entry Conditions
- Invoked with `docs contributors` subcommand

#### Step 1 — Read Existing State

Check which files already exist under `docs/contributors/`. For each existing file, note its last-updated date. For missing files, mark them as pending.

#### Step 2 — Initialize Structure

If `docs/contributors/` does not exist, create the full folder structure:

```
docs/contributors/
├── README.md
├── getting-started.md
├── architecture.md
├── code-conventions.md
├── testing.md
├── ci-cd.md
├── contributing.md
├── release.md
└── debugging.md
```

Also ensure `docs/README.md` exists. If not, create it from the Docs Master Index template.

#### Step 3 — Session Scope

Ask the user which sections to work on in this session. Default: all missing sections. The user can narrow to one or several.

#### Step 4 — Per-Section Workflow

For each section in scope, in order:

**`architecture.md`** — auto-draft from `design/architecture/overview.md` and `design/architecture/components.md`. Summarize the architecture for a contributor audience (implementation details, not product framing). Present draft for approval.

**`getting-started.md`** — ask:
- What OS/tools are required (languages, runtimes, package managers)?
- How does a contributor clone and build the project?
- Are there any environment variables or secrets needed to run locally?
- What does "first build success" look like?

**`code-conventions.md`** — ask:
- What languages are used? Any formatters or linters enforced?
- Naming conventions for files, functions, variables?
- File and folder organization rules?
- Any prohibited patterns (e.g., no global state, no raw SQL)?

**`testing.md`** — ask:
- Unit / integration / e2e split? Which frameworks?
- How do you run each test suite locally?
- Minimum coverage expectations?
- Are tests required for every PR?

**`contributing.md`** — ask:
- Branch naming rules?
- PR size preferences and expectations?
- Who reviews PRs? Any required approvers?
- How are merge conflicts handled?
- Is there a code freeze or release branch strategy?

**`ci-cd.md`** — ask:
- What does CI run on every PR (lint, test, build, security scan)?
- How do you interpret and fix CI failures?
- Are there required status checks before merge?
- How does CD work (auto-deploy, manual trigger, environments)?

**`release.md`** — ask:
- Who cuts releases?
- Versioning scheme (semver, calver, custom)?
- How is the changelog produced?
- What does the release checklist look like?

**`debugging.md`** — ask:
- What local debugging tools are available?
- What are the most common failure modes contributors encounter?
- How do you read logs locally vs. in production?
- Any known gotchas for the development environment?

#### Step 5 — Save

Write each approved section to its file. Update `docs/contributors/README.md` with the coverage table. Update root `README.md` Documentation section.

#### Contributors README Template

```markdown
# Contributors Guide: <Project Name>

Last Updated: YYYY-MM-DD

## Welcome

<1-2 sentence welcome statement for new contributors>

## Sections

| Section | Description |
|---|---|
| [Getting Started](getting-started.md) | Dev environment setup, first build |
| [Architecture](architecture.md) | System overview for contributors |
| [Code Conventions](code-conventions.md) | Languages, formatting, naming |
| [Testing](testing.md) | Test strategy and how to run tests |
| [Contributing](contributing.md) | PR process, branch strategy, review |
| [CI/CD](ci-cd.md) | Pipeline overview and how to fix failures |
| [Release Process](release.md) | Versioning, changelog, release steps |
| [Debugging](debugging.md) | Local tooling and common issues |

## Quick Start

1. Read [Getting Started](getting-started.md) to set up your environment
2. Read [Code Conventions](code-conventions.md) before writing any code
3. Read [Contributing](contributing.md) before opening a PR
```

---

### Docs Workflow 2 — Developers (Plugins, Connections, Activities)

#### Entry Conditions
- Invoked with `docs developers` subcommand

#### Step 1 — Read Existing State

Check which files already exist under `docs/developers/`. Note existing files and pending ones.

#### Step 2 — Initialize Structure

If `docs/developers/` does not exist, create the full folder structure:

```
docs/developers/
├── README.md
├── getting-started.md
├── plugin-api.md
├── connections-api.md
├── activities-api.md
├── sdk-reference.md
├── publishing.md
└── examples/
    └── README.md
```

#### Step 3 — Session Scope

Ask the user which sections to work on this session. Default: all missing sections. The user can narrow to one or several.

#### Step 4 — Per-Section Workflow

For each section in scope, in order:

**`plugin-api.md`** — auto-draft from any component designs in `design/components/` whose registry entry lists "plugin" in its responsibility or tech stack. Then ask:
- What is the plugin lifecycle (registration, initialization, teardown)?
- What configuration schema must a plugin provide?
- How does the host application discover plugins (file path, registry, remote)?
- What sandboxing or permission model applies to plugins?

**`connections-api.md`** — auto-draft from connection-related component designs. Then ask:
- What is a connection? What systems can be connected?
- How does a connection authenticate with an external system?
- What data contract must a connection implement (input types, output types)?
- How are connection errors surfaced to the host?
- What retry / timeout behavior is expected?

**`activities-api.md`** — auto-draft from activity-related component designs. Then ask:
- What is an activity? What can it do?
- What is the input/output contract for an activity?
- How does an activity report progress or intermediate results?
- How are errors and timeouts handled?
- Can activities be composed or chained?

**`sdk-reference.md`** — auto-draft by collecting all public interface tables from every component design file. Organize by module/package. Present for review and ask:
- Are there any interfaces missing from the design docs that should be listed here?
- Are there deprecated APIs that should be noted?

**`getting-started.md`** — ask:
- Is there an SDK package developers install? How?
- What is the minimal scaffold for a new plugin / connection / activity?
- Is there a CLI tool or template generator?
- What does "hello world" look like for each extension type?

**`publishing.md`** — ask:
- How does a developer package their plugin/activity/connection for distribution?
- Is there a marketplace or registry to publish to?
- What metadata is required (name, version, description, icon)?
- Are there any review or approval steps before publishing?

**`examples/README.md`** — ask:
- Are there any reference example plugins, connections, or activities already built?
- What scenarios should examples cover?
- Where should example source code live (in this repo, external repos, gists)?

#### Step 5 — Save

Write each approved section. Update `docs/developers/README.md`. Update root `README.md` Documentation section.

#### Developers README Template

```markdown
# Developer Guide: <Project Name> — Plugins, Connections & Activities

Last Updated: YYYY-MM-DD

## Overview

<2-3 sentences: what can developers extend, and what value does the extension system provide>

## Extension Types

| Type | Purpose | API Reference |
|---|---|---|
| Plugin | <description> | [plugin-api.md](plugin-api.md) |
| Connection | <description> | [connections-api.md](connections-api.md) |
| Activity | <description> | [activities-api.md](activities-api.md) |

## Sections

| Section | Description |
|---|---|
| [Getting Started](getting-started.md) | SDK install, scaffold first extension |
| [Plugin API](plugin-api.md) | Plugin lifecycle, registration, config schema |
| [Connections API](connections-api.md) | Connection model, auth, data contracts |
| [Activities API](activities-api.md) | Activity inputs/outputs, errors, chaining |
| [SDK Reference](sdk-reference.md) | Full SDK method reference |
| [Publishing](publishing.md) | Package and distribute your extension |
| [Examples](examples/README.md) | Reference implementations |

## Quick Start

1. Read [Getting Started](getting-started.md) to install the SDK and scaffold your first extension
2. Choose your extension type and read its API reference
3. See [Examples](examples/README.md) for reference implementations
4. Read [Publishing](publishing.md) when ready to distribute
```

---

### Docs Workflow 3 — Users

#### Entry Conditions
- Invoked with `docs users` subcommand

#### Step 1 — Read Existing State

Check which files already exist under `docs/users/`. Note existing files and pending ones. Also read `design/requirements/requirements.md` to understand the user personas and core flows before drafting anything.

#### Step 2 — Initialize Structure

If `docs/users/` does not exist, create the full folder structure:

```
docs/users/
├── README.md
├── getting-started.md
├── guides/
│   └── README.md
├── reference/
│   ├── README.md
│   ├── configuration.md
│   └── commands.md
├── troubleshooting.md
└── faq.md
```

#### Step 3 — Session Scope

Ask the user which sections to work on this session. Default: all missing sections. The user can narrow to one or several.

#### Step 4 — Per-Section Workflow

For each section in scope, in order:

**`getting-started.md`** — ask:
- How does a user install or access the application (download, cloud sign-up, CLI install)?
- What is the first meaningful action a new user should take?
- What does a successful "hello world" look like for a user?
- Are there prerequisites (account, subscription, OS requirement)?

**`guides/`** — ask:
- What are the 3-5 most important tasks a user needs to accomplish with this app?
- For each task: what is the goal, what are the steps, what does success look like?
- Each task becomes one guide file: `docs/users/guides/<task-slug>.md`
- Update `docs/users/guides/README.md` as an index of all guides.

**`reference/configuration.md`** — ask:
- What settings can users configure?
- For each setting: name, valid values, default, effect.
- Auto-draft from any configuration tables found in component design files.

**`reference/commands.md`** — ask (only if the app has a CLI or command palette):
- What are the commands or keyboard shortcuts available?
- For each command: name, syntax, description, example.
- Auto-draft from any interface definition tables in component design files.

**`troubleshooting.md`** — ask:
- What are the top 5 things that go wrong for users?
- For each problem: symptom, likely cause, step-by-step fix.

**`faq.md`** — ask:
- What questions do users ask most frequently?
- List 5-10 Q&A pairs.

#### Step 5 — Save

Write each approved section. Update `docs/users/README.md`. Update root `README.md` Documentation section.

#### Users README Template

```markdown
# User Documentation: <Project Name>

Last Updated: YYYY-MM-DD

## Welcome

<2-3 sentence welcome and value proposition for users>

## Sections

| Section | Description |
|---|---|
| [Getting Started](getting-started.md) | Install, configure, and take your first action |
| [Guides](guides/README.md) | Step-by-step how-to guides for key tasks |
| [Reference](reference/README.md) | Configuration options and command reference |
| [Troubleshooting](troubleshooting.md) | Common problems and how to fix them |
| [FAQ](faq.md) | Frequently asked questions |

## New Here?

Start with [Getting Started](getting-started.md).
```

#### Users Guides Index Template (`guides/README.md`)

```markdown
# How-To Guides: <Project Name>

Last Updated: YYYY-MM-DD

| Guide | Description |
|---|---|
| [Guide Title](guide-slug.md) | One-line description |
```

#### Users Reference Index Template (`reference/README.md`)

```markdown
# Reference: <Project Name>

Last Updated: YYYY-MM-DD

| Reference | Description |
|---|---|
| [Configuration](configuration.md) | All configuration options and their defaults |
| [Commands](commands.md) | CLI commands and keyboard shortcuts |
```

---

### Docs Master Index Template (`docs/README.md`)

```markdown
# Documentation: <Project Name>

Last Updated: YYYY-MM-DD

## Audiences

| Audience | Description | Overview |
|---|---|---|
| Contributors | Building and maintaining the project | [contributors/README.md](contributors/README.md) |
| Plugin & Activity Developers | Extending the platform | [developers/README.md](developers/README.md) |
| Users | Using the application | [users/README.md](users/README.md) |
```

---

## Error Handling

| Situation | Action |
|---|---|
| `git pull` fails | Halt session, show output, ask user to resolve |
| Label creation fails | Show error, ask user to create label manually, then continue |
| `gh issue create` fails | Show error, save issue body to a local `.pending-issues.md` in the design folder so it can be retried |
| Design file already exists (initial session) | Warn the user, ask whether to overwrite or treat as a change session |
| Component slug not found | List available components, ask user to confirm the name |
| Docs section file already exists | Warn the user, ask whether to overwrite or open as an update session |
| No design artefacts to auto-draft from | Skip auto-draft, proceed with questions instead — note the gap to the user |
| Root `README.md` has no Documentation section | Add the Documentation section at the end of the file; never overwrite existing content |
