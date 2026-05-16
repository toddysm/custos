# Custos — Design Documentation

Last Updated: 2026-05-14

## Overview

Custos is a pluggable workflow orchestrator for supply-chain security operations on OCI artifacts. Users (security analysts, DevOps engineers, PMs, developers) author workflows in YAML or via a visual designer; the orchestrator executes them durably on top of Dapr Workflow, dispatching steps to pluggable actions (OCI containers, HTTP webhooks, WASM modules). Built-in actions cover common supply-chain operations (vulnerability scan, SBOM, signature verification, attestation, policy eval, image promotion), and a stable orchestrator–action contract lets users add their own.

## Design Phases

| Phase | Status | Last Updated |
|---|---|---|
| Requirements | In Progress (revision 2) | 2026-05-14 |
| Architecture | Not Started | — |
| Component Designs | Not Started | — |
| Implementation | Not Started | — |

## Quick Links

- [Requirements](requirements/requirements.md)
- Architecture Overview — _not yet created_
- Component Registry — _not yet created_

## Recent Changes

| Date | Change | Issue |
|---|---|---|
| 2026-05-14 | Added workflow primitives, extensible connectors, and pluggable activity requirements | #8 |
| 2026-05-13 | Initial requirements drafted | #2 |
