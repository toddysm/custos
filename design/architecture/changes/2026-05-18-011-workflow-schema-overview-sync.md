# Change: workflow-schema-overview-sync

Date: 2026-05-18
Type: architecture
Sequence: 011
GitHub Issues: #89, #90
Status: open

## Summary

Bucket C of the design-inconsistency cleanup. The architecture overview's Workflow and Template Schema section showed only the `activity:` + single `connector:` + `if:` form, even though the ARM and Connector Service designs had already locked four additional step constructs (`let`, `forEach` with `where:`, `on_error`) and the multi-connector `connectors:` map form. This change brings the overview into sync. Purely additive — no contract decisions made or revisited.

## Before

The §Workflow and Template Schema example workflow exercised only single-connector activity steps with `if:` conditionals. There was no mention of pure-data `let` steps, `forEach` fan-out, `where:` pre-filter sugar, `on_error` handlers, or the multi-connector `connectors:` map. Reviewers comparing the overview to the ARM and Connector Service designs would correctly conclude the overview was incomplete; architects relying on the overview alone could not reason about fan-out, error handling, or multi-connector binding at the cross-component level.

## After

- The example workflow is rewritten as one realistic supply-chain flow that exercises every step form: `let` to bind the resolved registry host once for reuse downstream, `forEach` + `where:` filtering descriptors by `mediaType` for fan-out, `on_error` for retry/skip policy, an `if:`-gated quarantine branch, and an `image-promote` step that uses the multi-connector `connectors:` map (`source` / `destination` aliases). The `forEach` iterates the original descriptor list (so `where:` can match on `item.mediaType`) and constructs the `ImageRef` inside `with:` — keeping the predicate well-typed against the descriptor schema.
- A new **Step forms** subsection lists every form with a one-line definition and a pointer to the authoritative design (ARM design for `let`, `forEach`/`where:`, `on_error`, and `spec.connectors[]`; Connector Service design for the multi-connector binding example).
- A short cross-component implications paragraph documents the architectural seams the new forms touch:
  - `forEach` fan-out → Workflow Service Sub-Orchestration Manager (COMP-003 sub-module).
  - `connectors:` map → Workflow Service Binder validates at compile time, ARM resolves at execution time, Connector Service issues one lease per alias under the per-step concurrent-lease cap.
  - `on_error` matching → uses ARM's namespaced error-code scheme (`activity.*`, `input.*`, `output.*`, `system.*`, plus connector- and activity-defined namespaces); applied by the Workflow Service Step Coordinator before terminal state reaches Dapr Workflow.
- Header bumped: Version 10 → 11; Change History row added.

## Impact

- The architecture overview is once again the single authoritative reference for the workflow language at the architectural level. Workflow authors, integrators, and cross-component reviewers no longer need to cross-reference ARM and Connector Service designs to discover that fan-out, multi-connector, and error-handling forms exist.
- No component design changes. The reference table points to the existing authoritative locations rather than duplicating contract detail.

## Files changed

- `design/architecture/overview.md`
- `design/architecture/changes/2026-05-18-011-workflow-schema-overview-sync.md` (this file)
