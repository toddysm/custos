# Custos S3 ArtifactStore Adapter — Integration Tests

Unit tests for the S3 adapter require moto + aioboto3 compatibility fixes due to async response handling.

For now, functional validation can be done via manual testing with a live S3 bucket:

```bash
export CUSTOS_S3_BUCKET=my-test-bucket
export CUSTOS_S3_REGION=us-east-1
python -c "
import asyncio
from custos_s3.adapters import make_adapter
from custos_spl.ids import WorkspaceId, ArtifactId

async def test():
    adapter = make_adapter()
    ws = WorkspaceId('test-ws')

    async def content():
        yield b'test data'

    desc = await adapter.put(ws, content(), media_type='text/plain')
    print(f'Put: {desc.artifact_id}')

    chunks = []
    async for chunk in adapter.get(ws, desc.artifact_id):
        chunks.append(chunk)
    print(f'Got: {b\"\".join(chunks)}')

    head = await adapter.head(ws, desc.artifact_id)
    print(f'Head: size={head.size}, digest={head.digest}')

    await adapter.delete(ws, desc.artifact_id, is_sweeper=True)
    print('Deleted')

asyncio.run(test())
"
```

Comprehensive test suite TBD in follow-up work.
