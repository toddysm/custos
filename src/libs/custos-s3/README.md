# custos-s3

Custos ArtifactStore adapter for S3-compatible object storage.

## Overview

`custos-s3` provides a high-performance, streaming artifact storage adapter backed by AWS S3 or any S3-compatible object storage service (MinIO, DigitalOcean Spaces, etc).

## Features

- **True streaming digest computation** — SHA256 computed on-the-fly as bytes arrive (O(1) memory)
- **Multipart streaming upload** — chunks streamed directly to S3 (5MB parts), no intermediate buffering
- **Content-addressed storage** — artifacts stored by SHA256 digest, enabling deduplication and idempotent writes
- **Workspace isolation** — all operations enforce workspace scoping; cross-workspace access is blocked
- **Sweeper-only deletion** — delete operations require explicit `is_sweeper=True` flag to prevent accidental data loss
- **Async/await API** — all I/O operations are fully async for high concurrency
- **Multi-region support** — configurable AWS region and custom S3 endpoints for MinIO or local deployments

## Installation

```bash
pip install custos-s3
```

## Configuration

### Environment Variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `CUSTOS_S3_BUCKET` | Yes | — | S3 bucket name |
| `CUSTOS_S3_REGION` | No | (AWS default) | AWS region or endpoint region |
| `CUSTOS_S3_ENDPOINT` | No | (AWS endpoint) | Custom S3 endpoint URL (for MinIO, LocalStack, etc) |

### Example

```bash
export CUSTOS_S3_BUCKET=my-artifacts
export CUSTOS_S3_REGION=us-east-1
# For MinIO:
export CUSTOS_S3_ENDPOINT=http://minio:9000
```

## Usage

### Basic Example

```python
import asyncio
from custos_s3.adapters import make_adapter
from custos_spl.ids import WorkspaceId

async def main():
    adapter = make_adapter()
    workspace_id = WorkspaceId("my-workspace")
    
    # Put (store) an artifact
    async def content_generator():
        yield b"hello "
        yield b"world"
    
    descriptor = await adapter.put(
        workspace_id,
        content_generator(),
        media_type="text/plain"
    )
    print(f"Stored: {descriptor.artifact_id}")
    
    # Get (retrieve) an artifact
    chunks = []
    async for chunk in adapter.get(workspace_id, descriptor.artifact_id):
        chunks.append(chunk)
    content = b"".join(chunks)
    print(f"Retrieved: {content}")
    
    # Check existence
    info = await adapter.head(workspace_id, descriptor.artifact_id)
    if info:
        print(f"Size: {info.size} bytes, Digest: {info.digest}")

asyncio.run(main())
```

## Storage Layout

Artifacts are stored in a content-addressed hierarchy:

```
s3://{bucket}/{workspace_id}/{digest-prefix-2}/{digest}
```

Example:
```
s3://my-artifacts/ws-123/ab/abcdef1234567890...
s3://my-artifacts/ws-456/cd/cdef9876543210...
```

The two-character digest prefix enables efficient list operations and distributes objects across key prefixes.

## Development

### Install dev dependencies

```bash
pip install -e ".[dev]"
```

### Run integration tests

```bash
export CUSTOS_S3_BUCKET=test-bucket
python -m pytest tests/
```

Tests use live S3 operations. For local development, use MinIO or AWS LocalStack.

## License

Apache 2.0 — see LICENSE in the repository root.
