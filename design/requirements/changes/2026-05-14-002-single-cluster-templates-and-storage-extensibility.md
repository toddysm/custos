# Change: single-cluster-templates-and-storage-extensibility

Date: 2026-05-14
Type: requirement
Sequence: 002
GitHub Issue: pending
Status: open

## Summary
Added requirements for workflow templates and for a strict single-cluster deployment baseline. Also clarified storage and logging expectations to enforce datastore/backing-service extensibility while using in-cluster defaults in v1.

## Before
The requirements covered workflow orchestration, connectors, and pluggable activities, but did not explicitly require reusable workflow templates. The deployment model leaned on specific stores (for example PostgreSQL and object storage) without explicitly stating that all hard dependencies must run inside one Kubernetes cluster.

## After
The following requirements were added or clarified:
- REQ-076: workflow template support, including creating templates from existing workflows by removing selected configuration
- REQ-077: single-cluster self-contained operation with no mandatory off-cluster dependencies
- REQ-078: Kubernetes-native audit/logging defaults with pluggable external/cloud exporters
- REQ-048 and REQ-050 were updated to require datastore and artifact-store abstractions with in-cluster defaults

## Impact
- Architecture must define abstractions for definition storage, catalog storage, metadata storage, and artifact/log storage rather than binding to one implementation.
- v1 can still start with PostgreSQL and Kubernetes-backed storage, but these are defaults, not hard architectural constraints.
- Logging/audit design should prioritize Kubernetes-native pipelines and collectors while allowing external sinks.
- Template lifecycle becomes a first-class capability in API and UI architecture.

## Related Requirements
REQ-048, REQ-050, REQ-076, REQ-077, REQ-078
