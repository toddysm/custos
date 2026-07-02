# Change: slsa-builder-id-alignment

Date: 2026-07-02
Type: architecture
Sequence: 015
GitHub Issue: #976
Status: open

## Summary

The reusable `slsa-provenance` composite action
(`.github/actions/slsa-provenance/action.yml`) hardcodes the SLSA v0.2
provenance `builder.id` and `invocation.configSource.entryPoint` to
`.github/workflows/build-images.yml`. OOTB-005 (#944) added three additional
publisher workflows that reuse the action
(`publish-activity-copy-image.yml`, `publish-connector-dockerhub.yml`,
`publish-connector-ghcr.yml`). Provenance attached by those workflows therefore
misreports its builder, and the keyless cosign signing identity (the Fulcio
certificate SAN) — which is the *actual* calling workflow — no longer matches
the predicate's `builder.id`, breaking the verification commands in
`docs/operations/supply-chain-verification.md`.

## Decision

The provenance `builder.id` and `entryPoint` are **derived from the calling
workflow** instead of being hardcoded, keeping the predicate consistent with the
Fulcio signing identity that verification pins.

1. By default the action reads `GITHUB_WORKFLOW_REF` — the
   `<owner>/<repo>/.github/workflows/<wf>.yml@<ref>` of the workflow that runs
   the signing job — and sets
   `builder.id = ${GITHUB_SERVER_URL}/${GITHUB_WORKFLOW_REF}` and
   `entryPoint` = the workflow path portion of that ref. For directly-triggered
   workflows (all three publish-* workflows, plus tag/push runs of
   `build-images.yml`) this equals the cosign certificate identity.
2. The action gains an optional `builder-id` input. `build-images.yml` is also
   invoked via `workflow_call` from the release/offline pipelines, where
   `GITHUB_WORKFLOW_REF` points at the *caller* rather than at
   `build-images.yml`. To keep its provenance identity stable across both entry
   paths, `build-images.yml` passes an explicit
   `builder-id = <server>/<repo>/.github/workflows/build-images.yml@<ref>`,
   which the action uses verbatim (and from which it derives `entryPoint`).

### Rationale

- Keyless signing already binds each image's signature/attestations to the
  workflow that produced them (the reusable-workflow `job_workflow_ref` for
  `workflow_call`, the workflow ref for direct triggers). Making `builder.id`
  track that identity is the only way the predicate and the verification policy
  can agree.
- An explicit override for `build-images.yml` avoids a regression: its
  `workflow_call` provenance must continue to name `build-images.yml`, not the
  release/offline caller.

## Consumer impact

`docs/operations/supply-chain-verification.md` broadens its
`--certificate-identity-regexp` to match `build-images.yml` **and** the
`publish-{activity,connector}-*` workflows, and notes that OOTB extension images
(`copy-image`, `dockerhub`, `ghcr`) are signed by their own publisher workflow.
Core service/job images are unchanged (still `build-images.yml`).

## Acceptance

- Provenance from each publisher carries a `builder.id`/`entryPoint` matching
  that workflow's Fulcio signing identity.
- `build-images.yml` provenance identity is unchanged for both direct and
  `workflow_call` runs.
- The verification doc verifies signatures/attestations from all publishers.
