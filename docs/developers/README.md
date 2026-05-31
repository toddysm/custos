# Developer Guide: Custos — Plugins, Connections & Activities

Last Updated: 2026-05-31

## Overview

Custos is a pluggable workflow orchestrator. Developers extend the platform by writing **connectors** (access to external systems), **activities** (units of work executed inside workflows), and **plugins** (broader extension types). Each extension is packaged and distributed as an OCI artifact and described by a versioned manifest contract.

## Extension Types

| Type | Purpose | API Reference |
|---|---|---|
| Connection | Bind workflows and activities to an external system (registry, storage, KMS, identity provider). Provides credentials, capabilities, and event streams. | [connections-api.md](connections-api.md) |
| Activity | Unit of work executed during a workflow step (vuln-scan, SBOM, image-promote, etc.). | _coming soon_ |
| Plugin | Broader extension category covering non-connector, non-activity extensions. | _coming soon_ |

## Sections

| Section | Description |
|---|---|
| [Connections API](connections-api.md) | Connector manifest schema, field reference, and examples |
| [Connector Plugin Author Guide](connector-plugin-author.md) | End-to-end guide for writing, packaging, and publishing a connector plugin (manifest, hook contract, error taxonomy, OCI publication) |
| [Catalog API](catalog-api.md) | REST surface for workflows, templates, activity- and connector-types; validation taxonomy, immutability and deprecation contracts, placeholder reference |
| [Auth API](auth-api.md) | REST and RPC surface for tenants, workspaces, service accounts, tokens, role bindings, and call-context signing; error taxonomy, permission registry, built-in roles |
| [CEL Expressions](cel-expressions.md) | Reference for workflow expressions: bindings, operators, sandbox guarantees, failure modes, worked examples |
| [Workflow Compilation](workflow-compilation.md) | Definition Compiler pipeline, input/output contract, error taxonomy, retry-policy resolution, worked examples |
| [Workflow Run Controller](workflow-run-controller.md) | Run lifecycle state machine, `RunController` public API, `StepHandler` Protocol, Dapr Workflow primitive mapping, replay determinism contract, `run.*` error taxonomy, worked examples |
| [Workflow Step Coordinator](workflow-step-coordinator.md) | Step dispatch by primitive kind, activity step lifecycle, retry policy application, idempotency triple, `step.*` event taxonomy, locked `step.*` error taxonomy, worked examples |
| [Examples](examples/) | Reference connector manifests for the supported target and authentication combinations |

## Quick Start for Connection Developers

1. Read the [Connections API reference](connections-api.md) to understand the manifest contract.
2. Pick a target kind (`oci-registry`, `azure-blob-storage`, `amazon-s3-bucket`) and an authentication type.
3. Copy the matching [example manifest](examples/) and edit `metadata`, `target`, `credentials`, and `events` for your connector.
4. Validate your manifest against the JSON Schema at `design/components/connector-service/schemas/connector-manifest.v1.schema.json`.
5. Publish the manifest as an OCI referrer of your connector image, using `artifactType = application/vnd.custos.connector.manifest.v1+json`.
