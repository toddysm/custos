# Change: workflow-platform-extensibility

Date: 2026-05-14
Type: requirement
Sequence: 001
GitHub Issue: #8
Status: open

## Summary
Added three requirements to make explicit what had previously been only partially implied in the baseline: Custos must expose first-class workflow primitives for eventing, orchestration, and control flow; Custos must provide an extensible connector model that starts with OCI registries but generalizes to other external systems; and Custos activities must remain independently pluggable so new activities can be introduced without modifying or upgrading the core platform.

## Before
The requirements document covered specific execution semantics (DAGs, loops, conditions, retries), registry-centric integrations, and custom actions via a stable orchestrator-to-action contract. However, it did not explicitly state:
- that the platform itself must provide a coherent set of common workflow primitives as a first-class capability,
- that connections should be modeled as an extensible platform concept beyond OCI registries, or
- that activities must be addable independently of the platform release cycle.

## After
The following requirements were added:
- REQ-073: first-class workflow primitives for eventing, orchestration, and common control constructs
- REQ-074: extensible connector model starting with OCI registries and expanding to storage accounts, databases, and external systems
- REQ-075: independently pluggable, packaged, versioned, and deployable activities that do not require core platform upgrades

## Impact
These additions sharpen the product boundary of Custos from "workflow engine for registry security workflows" to "extensible workflow platform". They affect the architecture phase directly:
- A connector abstraction now needs explicit architectural treatment, likely as its own component or subsystem.
- The activity contract and packaging model become even more central because REQ-075 makes zero-core-change extensibility a hard requirement.
- The workflow-definition model and execution engine need to express platform-level primitives in a way that is not tied only to today's built-in actions.

## Related Requirements
REQ-007, REQ-008, REQ-009, REQ-011, REQ-022, REQ-023, REQ-024, REQ-055
