# Change: arm-allow-unpinned-images-config

Date: 2026-06-06
Type: component-design
Component: activity-runtime-manager
Sequence: 008
GitHub Issue: #613
Status: open

## Summary

Records the `ARM_ALLOW_UNPINNED_IMAGES` configuration variable added while
implementing the ARM↔pod I/O bridge (#613, tracker #762). The knob is a
**test/dev-only** escape hatch that makes ARM render the activity image
reference **tag-only** (with `imagePullPolicy: IfNotPresent`) — ignoring the
manifest digest pin — so a locally `kind load`ed image, which has no registry
digest to pin against, can run in the integration suite. **Production remains
strictly digest-pinned**: with the flag at its `false` default, ARM renders the
image `tag@digest` from the manifest, so every production activity runs
content-addressed bits. The design's §Configuration table is updated to stay
authoritative. Version 6 → 7.

## Before

The §Configuration table had no entry for an unpinned-image affordance. ARM
always rendered the activity image `tag@digest` from the manifest, which blocked
the `kind`-based end-to-end output round-trip tests (#613) where the contract
image is loaded locally and has no registry digest to pin against.

## After

Added one row to the §Configuration table:

| Variable | Required | Default | Description |
|---|---|---|---|
| `ARM_ALLOW_UNPINNED_IMAGES` | No | `false` | **Test/dev escape hatch.** When `true`, ARM renders the activity image reference tag-only with `imagePullPolicy: IfNotPresent` — ignoring the manifest digest pin — so a locally `kind load`ed image can run. Production stays strictly digest-pinned (content-addressed) — leave `false`. |

Production posture is unchanged: digest-pinning is the default. The escape hatch
only relaxes pinning when an operator explicitly sets the flag, and provenance
(the resolved digest recorded from the manifest) is unaffected.

## Impact

- No change to any locked cross-component contract. Workflow Service, Connector
  Service, Catalog, and SPL are unaffected.
- New operational surface for the platform/Helm chart: an explicit, default-off
  flag that must stay `false` in production.
- Enables the `kind` integration happy-path and downstream-`ArtifactRef`
  scenarios (#759, #760) without weakening production image pinning.

## Files changed

- `design/components/activity-runtime-manager/design.md`
- `design/components/activity-runtime-manager/changes/2026-06-06-008-arm-allow-unpinned-images-config.md` (this file)

## Related Change Records

- `design/components/activity-runtime-manager/changes/2026-06-02-006-arm-internals-todo-006-007-008.md`
- `design/components/activity-runtime-manager/changes/2026-06-06-007-arm-io-bridge-image-config.md`
