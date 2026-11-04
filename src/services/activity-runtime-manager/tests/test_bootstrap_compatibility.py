"""Actual ARM issuer -> actual sidecar verifier, without a test-only bootstrap implementation."""

import io
import stat
import tarfile
from datetime import UTC, datetime
from pathlib import Path

import pytest
from custos_sidecar.auth import BootstrapTokenVerifier, BoundTriple
from custos_sidecar.errors import SidecarError

from custos_arm.contract import StepRef
from custos_arm.runtime import SandboxHandle
from custos_arm.runtime.oci.lifecycle import _tar_directory
from custos_arm.scheduler.fsio import FilesystemSecretSink
from custos_arm.secrets import SecretInjector, SidecarTokenMinter

STEP = StepRef(runId="opaque/run", stepId="step", attempt=1)
TRIPLE = BoundTriple("opaque/run", "step", 1)


def test_actual_issuer_verifies_and_keeps_private_material_out_of_repr() -> None:
    """The former opaque-token failure is fixed using the same codec as the live sidecar."""
    minter = SidecarTokenMinter(clock=lambda: 1000)
    token = minter.mint(step=STEP, workspace_id="ws")
    BootstrapTokenVerifier(key=token.verification_key, triple=TRIPLE, clock=lambda: 1001).verify(
        token.value
    )
    assert token is minter.mint(step=STEP, workspace_id="ws")
    assert len(token.verification_key) >= 32
    assert token.value not in repr(token)
    assert repr(token.verification_key) not in repr(token)


def test_workspace_and_attempt_cannot_share_bootstrap_authority() -> None:
    """Even identical opaque execution coordinates in two workspaces get independent keys."""
    minter = SidecarTokenMinter(clock=lambda: 1000)
    first = minter.mint(step=STEP, workspace_id="ws-1")
    other = minter.mint(step=STEP, workspace_id="ws-2")
    assert first.value != other.value and first.verification_key != other.verification_key
    with pytest.raises(SidecarError):
        BootstrapTokenVerifier(
            key=other.verification_key, triple=TRIPLE, clock=lambda: 1001
        ).verify(first.value)
    with pytest.raises(SidecarError):
        BootstrapTokenVerifier(
            key=first.verification_key,
            triple=BoundTriple("opaque/run", "step", 2),
            clock=lambda: 1001,
        ).verify(first.value)


def test_expiry_is_clamped_and_replay_cannot_extend_it() -> None:
    """At the exact deadline both the real verifier and local issuer reject old authority."""
    clock = [1000.25]
    minter = SidecarTokenMinter(clock=lambda: clock[0])
    deadline = datetime.fromtimestamp(1010, tz=UTC)
    token = minter.mint(step=STEP, workspace_id="ws", deadline=deadline)
    assert token.expires_at == 1010
    clock[0] = 1005
    assert minter.mint(step=STEP, workspace_id="ws", deadline=deadline) is token
    with pytest.raises(ValueError, match="exceeds"):
        minter.mint(step=STEP, workspace_id="ws", deadline=datetime.fromtimestamp(1009, tz=UTC))
    clock[0] = 1010
    assert not minter.is_active(token)
    with pytest.raises(SidecarError):
        BootstrapTokenVerifier(
            key=token.verification_key, triple=TRIPLE, clock=lambda: clock[0]
        ).verify(token.value)
    with pytest.raises(ValueError, match="expired"):
        minter.mint(step=STEP, workspace_id="ws")


def test_stale_revoke_does_not_remove_replacement() -> None:
    """A stale local revocation cannot evict a separately minted active generation."""
    minter = SidecarTokenMinter(clock=lambda: 1000)
    old = minter.mint(step=STEP, workspace_id="ws")
    minter.revoke(old)
    new = minter.mint(step=STEP, workspace_id="ws")
    minter.revoke(old)
    assert minter.is_active(new) and not minter.is_active(old)


@pytest.mark.parametrize("bits", [0, 31, True])
def test_issuer_rejects_weak_key_configuration(bits: int) -> None:
    """A private bootstrap key must retain at least the established 256-bit entropy."""
    with pytest.raises(ValueError):
        SidecarTokenMinter(token_bytes=bits)


async def test_real_private_files_verify_without_exposing_key_to_input_archive(
    tmp_path: Path,
) -> None:
    """Run the actual injector/filesystem/codec chain and inspect the activity-visible archive."""
    handle = SandboxHandle("oci-container", "ns/job", tmp_path / "in", tmp_path / "out")
    sink = FilesystemSecretSink(handle.input_root, private_root=handle.sidecar_root)
    injector = SecretInjector(token_minter=SidecarTokenMinter(clock=lambda: 1000))
    result = await injector.inject(
        sink=sink,
        step=STEP,
        workspace_id="ws",
        connectors=[],
        contexts=[],
        deadline=datetime.fromtimestamp(1060, tz=UTC),
    )
    key_path = handle.sidecar_root / "sidecar-key"
    token_path = handle.input_root / "sidecar-token"
    assert stat.S_IMODE(key_path.stat().st_mode) == 0o400
    assert stat.S_IMODE(token_path.stat().st_mode) == 0o400
    assert not (handle.input_root / "sidecar-key").exists()
    key = key_path.read_bytes()
    assert key not in _tar_directory(handle.input_root)
    BootstrapTokenVerifier(key=key, triple=TRIPLE, clock=lambda: 1001).verify(
        token_path.read_text()
    )
    assert result.token.workspace_id == "ws"
    assert result.token.expires_at == 1060
    public = _tar_directory(handle.input_root, group_readable=True)
    private = _tar_directory(handle.sidecar_root)
    with tarfile.open(fileobj=io.BytesIO(public)) as archive:
        assert archive.getmember("sidecar-token").mode == 0o440
    with tarfile.open(fileobj=io.BytesIO(private)) as archive:
        assert archive.getmember("sidecar-key").mode == 0o400


async def test_private_sink_rejects_missing_or_overlapping_boundary(tmp_path: Path) -> None:
    """A missing private volume is an explicit error, never a write into ordinary inputs."""
    with pytest.raises(ValueError, match="not configured"):
        await FilesystemSecretSink(tmp_path).write_private_secret(
            relative_path="sidecar-key", content=b"unit", mode=0o400
        )
    with pytest.raises(ValueError, match="separate"):
        FilesystemSecretSink(tmp_path / "in", private_root=tmp_path / "in" / "private")
    with pytest.raises(ValueError, match="separate"):
        FilesystemSecretSink(tmp_path / "in", private_root=tmp_path)
    sink = FilesystemSecretSink(tmp_path / "in", private_root=tmp_path / "private")
    with pytest.raises(ValueError, match="single path"):
        await sink.write_private_secret(relative_path="../key", content=b"unit", mode=0o400)


async def test_failed_private_delivery_drops_local_bootstrap_authority(tmp_path: Path) -> None:
    """A failed bootstrap write cannot retain a newly minted private key in the active registry."""
    minter = SidecarTokenMinter(clock=lambda: 1000)
    injector = SecretInjector(token_minter=minter)
    token = minter.mint(step=STEP, workspace_id="ws")
    with pytest.raises(ValueError, match="not configured"):
        await injector.inject(
            sink=FilesystemSecretSink(tmp_path),
            step=STEP,
            workspace_id="ws",
            connectors=[],
            contexts=[],
        )
    assert not minter.is_active(token)
    assert not (tmp_path / "sidecar-token").exists()
