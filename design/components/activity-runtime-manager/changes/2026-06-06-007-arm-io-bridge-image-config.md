# Change: arm-io-bridge-image-config

Date: 2026-06-06
Type: component-design
Component: activity-runtime-manager
Sequence: 007
GitHub Issue: #613
Status: open

## Summary

Records the `ARM_IO_BRIDGE_IMAGE` configuration variable added while
implementing the ARM↔pod I/O bridge (#613, tracker #762). The bridge moves the
activity's typed inputs/outputs and artifacts across the pod boundary with two
short-lived helper containers — an input-injector `initContainer` and an
output-collector **native sidecar** — both built from this image. The design's
§Configuration table is updated so it stays authoritative over the implemented
`ARM_*` env surface. Version 6 → 7.

## Before

The §Configuration table listed `ARM_SIDECAR_IMAGE` but had no entry for the
io-bridge helper image. The bridge mechanism was the deferred follow-up tracked
by #613.

## After

Added one row to the §Configuration table:

| Variable | Required | Default | Description |
|---|---|---|---|
| `ARM_IO_BRIDGE_IMAGE` | No | `busybox:1.37.0@sha256:9532d8c3…96b16028` | Image for the io-bridge input-injector init container and output-collector native sidecar; override to point at an internal mirror. Digest-pinned by default. |

The default is digest-pinned (content-addressed) so the bridge containers run
fixed bits; operators override it only to point at an internal mirror of the
same image.

## Impact

- No change to any locked cross-component contract. Workflow Service, Connector
  Service, Catalog, and SPL are unaffected.
- New operational surface for the platform/Helm chart: the io-bridge image is
  now an explicit, overridable, digest-pinned input.
- The output collector uses the Kubernetes native-sidecar pattern
  (`initContainers` entry with `restartPolicy: Always`), which requires
  Kubernetes >= 1.28.

## Files changed

- `design/components/activity-runtime-manager/design.md`
- `design/components/activity-runtime-manager/changes/2026-06-06-007-arm-io-bridge-image-config.md` (this file)

## Related Change Records

- `design/components/activity-runtime-manager/changes/2026-06-02-006-arm-internals-todo-006-007-008.md`
- `design/components/activity-runtime-manager/changes/2026-06-06-008-arm-allow-unpinned-images-config.md`
