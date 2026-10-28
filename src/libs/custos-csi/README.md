# custos-csi

ArtifactStore adapter for Custos backed by a Kubernetes PVC mounted via CSI driver.

## Configuration

| Variable | Required | Description |
|----------|----------|-------------|
| `CUSTOS_CSI_PVC_MOUNT` | Yes | Absolute path to PVC mount point (e.g., `/mnt/artifacts`) |

## Storage Layout

Artifacts are stored with content-addressing:
```
{CUSTOS_CSI_PVC_MOUNT}/{workspace_id}/{digest-prefix-2}/{digest}
```

Example:
```
/mnt/artifacts/ws-123/ab/abcdef1234567890...sha256
```

New writes persist the optional media type in a neighboring `{digest}.json`
file. Metadata and content are published from unique temporary files using
same-filesystem hard links, so the first complete publication wins without
overwriting another writer. The mount must support POSIX hard links.
Existing blobs without metadata remain readable with an unknown (`None`) media
type. Corrupt metadata produces an explicit backend error. Sweeper deletion
removes both the blob and its metadata; restarting the adapter preserves the
descriptor returned by `put`.

## Features

- Content-addressed storage: identical bytes → identical digest, idempotent `put`
- Streaming SHA256 computation: efficient for large blobs
- Workspace-scoped access: cross-workspace reads blocked
- Sweeper-only delete: enforced via context marker
- Async I/O via `aiofiles`

## Entry Points

This package registers one adapter:
- `csi-artifact` — `ArtifactStoreProvider` backed by PVC

See `design/components/storage-provider-layer/design.md` for the contract surface.
