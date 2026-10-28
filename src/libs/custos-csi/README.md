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
file. Metadata and content are published from unique temporary files under a
process-safe POSIX advisory lock. Each workspace/digest-prefix directory retains
one stable `.lock` file (at most 256 per workspace); do not remove these files
while adapters can access the mount. Readers, publication and deletion use the
same lock, and process exit releases it automatically. If a writer fails after
publishing only metadata, the next put replaces that orphan before publishing
its blob. The mount must support POSIX advisory locks and atomic rename.
Concurrent access must use adapters participating in this locking protocol;
quiesce writes/deletion when upgrading from older, non-locking adapter versions.
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
