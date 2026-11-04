"""Verify runtime uncertainty against the actual Connector authority and private-proof guards."""

import asyncio
import json
import threading
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import cast
from unittest.mock import AsyncMock, Mock

import httpx
import pytest
from custos_arm.config import load_settings
from custos_arm.contract import StepRef
from custos_arm.credential_proxy import CredentialProxy, ProxyRequest
from custos_arm.io import IOBroker
from custos_arm.limit import ResourceLimiter
from custos_arm.manifest import parse_manifest
from custos_arm.resolve import ActivityResolver, ActivityTypeVersion
from custos_arm.result import ResultMapper
from custos_arm.runtime import RuntimeDriver, RuntimeDriverDispatcher, SandboxHandle
from custos_arm.runtime.oci import SandboxFailureError
from custos_arm.scheduler import ActivityScheduler, ScheduleRequest
from custos_arm.secrets import ConnectorContext, SecretInjector, SidecarTokenMinter
from custos_arm.store import ExecutionRepository
from custos_arm.store.artifact import ArtifactStoreClient
from custos_callctx.execution import ExecutionIdentity
from custos_callctx.execution_client import ExecutionContextClient
from custos_spl.interfaces.activity_execution import ActivityExecutionRecord
from custos_spl.interfaces.lease_store import LeaseStoreProvider
from custos_spl.interfaces.metadata_store import MetadataStoreProvider, StepAttempt
from fastapi import HTTPException

from custos_connector.credentials import CredentialDenied, CredentialIssuer, CredentialRequest
from custos_connector.middleware import CallContext
from tests.test_activity_credentials import AtomicLeases, Attempts
from tests.test_binding_router import _build_providers


class RuntimeAttempts(Attempts):
    """An external store seam shared by the real runtime repository and Connector issuer."""

    async def get_step_attempts(self, *args: object) -> tuple[StepAttempt, ...]:
        """This isolated workspace has no legacy execution records to migrate."""
        return ()

    async def claim_activity_execution(self, *args: object) -> ActivityExecutionRecord:
        """Accept exactly one runtime-owned snapshot; no caller-crafted row is used."""
        assert self.row is None
        workspace, run, step, attempt, request_hash, owner, snapshot = args
        assert all(
            isinstance(value, str)
            for value in (workspace, run, step, request_hash, owner, snapshot)
        )
        assert isinstance(attempt, int)
        self.row = ActivityExecutionRecord(
            str(workspace),
            str(run),
            str(step),
            attempt,
            str(request_hash),
            str(owner),
            str(snapshot),
            False,
        )
        return self.row

    async def write_activity_execution(
        self,
        *args: object,
        **kwargs: object,
    ) -> ActivityExecutionRecord:
        """Require the actual owned prior row before advancing its snapshot."""
        record, snapshot = args
        assert record == self.row and self.row is not None
        assert isinstance(snapshot, str) and isinstance(kwargs["completed"], bool)
        self.row = replace(self.row, snapshot_json=snapshot, completed=kwargs["completed"])
        return self.row

    async def revoke_activity_authority(
        self,
        record: ActivityExecutionRecord,
    ) -> ActivityExecutionRecord:
        """Preserve the accepted snapshot while irreversibly closing credential authority."""
        assert self.row is not None and record.owner == self.row.owner
        self.row = replace(self.row, authority_revoked=True)
        return self.row


@pytest.mark.parametrize("phase", ["start", "monitor"])
async def test_runtime_uncertainty_denies_issuer_and_old_proof(
    tmp_path: Path,
    phase: str,
) -> None:
    """After a genuine scheduling failure, both admission guards reject the retained live scope."""
    store = RuntimeAttempts()
    providers = replace(
        _build_providers(),
        metadata_store=cast(MetadataStoreProvider, store),
        lease_store=cast(LeaseStoreProvider, AtomicLeases()),
    )
    issuer = CredentialIssuer(providers)
    settings = load_settings(
        {
            "ARM_ARTIFACT_STORE": "artifacts",
            "ARM_METADATA_STORE": "metadata",
            "ARM_CATALOG_ENDPOINT": "http://catalog",
            "ARM_CONNECTOR_ENDPOINT": "http://connector",
            "ARM_SANDBOX_NAMESPACE": "sandboxes",
            "ARM_SIDECAR_IMAGE": "registry.test/sidecar:1",
            "ARM_CREDENTIAL_PROXY_URL": "https://runtime.internal",
            "ENVIRONMENT": "development",
        }
    )
    manifest = parse_manifest(
        {
            "apiVersion": "custos.dev/v1",
            "kind": "ActivityManifest",
            "metadata": {
                "namespace": "test",
                "type": "echo",
                "version": "1.0.0",
                "description": "test",
                "owner": "test",
            },
            "spec": {
                "contractVersion": "1",
                "runtime": {
                    "kind": "oci-container",
                    "image": "registry.test/activity:1",
                    "digest": "sha256:" + "a" * 64,
                },
                "inputs": {"schema": {"type": "object"}},
                "outputs": {"schema": {"type": "object"}},
                "resources": {"timeout": "PT5M"},
                "connectors": [
                    {
                        "name": "source",
                        "type": "oci-registry",
                        "required": True,
                        "capabilities": ["oci.pull"],
                    }
                ],
            },
        }
    )
    resolver = Mock(spec=ActivityResolver)
    resolver.resolve = AsyncMock(
        return_value=ActivityTypeVersion(
            namespace="test",
            type="echo",
            version="1.0.0",
            digest="sha256:" + "a" * 64,
            manifest=manifest,
        )
    )
    driver = Mock(spec=RuntimeDriver)
    driver.kind = "oci-container"
    handle = SandboxHandle(
        kind=driver.kind,
        reference="namespace/job",
        input_root=tmp_path / "in",
        output_root=tmp_path / "out",
    )
    driver.prepare.return_value = handle
    entered, fail = asyncio.Event(), threading.Event()
    loop = asyncio.get_running_loop()

    def uncertain(*args: object) -> None:
        """Hold the external operation until the live authority control has been exercised."""
        loop.call_soon_threadsafe(entered.set)
        assert fail.wait(5)
        raise SandboxFailureError("private-transport-diagnostic")

    if phase == "start":
        driver.start.side_effect = uncertain
    else:
        driver.await_terminal.side_effect = uncertain
    scheduler = ActivityScheduler(
        resolver=resolver,
        limiter=ResourceLimiter(settings),
        broker=IOBroker(Mock(spec=ArtifactStoreClient), output_max_bytes=1024),
        injector=SecretInjector(token_minter=SidecarTokenMinter()),
        mapper=ResultMapper(),
        dispatcher=RuntimeDriverDispatcher((driver,)),
        repository=ExecutionRepository(
            providers.metadata_store, idempotency_ttl=timedelta(days=1), durable_store=store
        ),
        settings=settings,
        now=lambda: datetime.now(UTC),
    )
    identity = ExecutionIdentity("ws", "executor", "stable")
    request = ScheduleRequest(
        workspace_id="ws",
        principal_id="executor",
        execution_identity=identity,
        step=StepRef(runId="run", stepId="step", attempt=1),
        activity_ref="test/echo@1.0.0",
        inputs={},
        connector_contexts=(
            ConnectorContext(
                slot_name="source",
                connector_type="oci-registry",
                connector_instance_id="instance",
                capabilities=("oci.pull",),
                binding={
                    "endpoint": "https://registry.test",
                    "handle": {},
                    "extras": {},
                    "tokenTypeHint": None,
                },
            ),
        ),
    )
    context = CallContext(workspace_id="ws", principal_id="executor")
    credential = CredentialRequest(
        operation="issue",
        runId="run",
        stepId="step",
        attempt=1,
        slot="source",
        capability="oci.pull",
    )
    caller = asyncio.create_task(scheduler.schedule(request))
    try:
        await asyncio.wait_for(entered.wait(), 2)
        assert (await issuer.authority(context, credential))["state"] == "running"
    finally:
        fail.set()
        result = await asyncio.wait_for(caller, 2)
    assert result.error is not None and result.error.code == "activity.execution_unconfirmed"
    bundle = json.loads((handle.sidecar_root / "runtime.json").read_text())
    execution = Mock(spec=ExecutionContextClient)
    async with httpx.AsyncClient() as http:
        proxy = CredentialProxy(store, execution, http, "http://connector")
        for drained in (False, True):
            if drained:
                await scheduler.drain()
            with pytest.raises(CredentialDenied):
                await issuer.operate(context, credential)
            with pytest.raises(HTTPException) as denied:
                await proxy.forward(
                    ProxyRequest(workspaceId="ws", **credential.model_dump(by_alias=True)),
                    "Bearer " + bundle["proof"],
                )
            assert denied.value.status_code == 403
            execution.headers.assert_not_called()
    assert store.row is not None and not store.row.completed and store.row.authority_revoked
    driver.prepare.assert_called_once()
    driver.cancel.assert_called_once()
