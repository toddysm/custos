"""Operator-provided public CA trust stays bounded and private to the credential Sidecar."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID

from custos_arm.result import ResultClass
from custos_arm.runtime.oci.job import build_activity_job
from tests.test_durable_execution import SharedAttempts, repository
from tests.test_scheduler import _FakeDriver, _request, _scheduler, _settings, _success_outputs


def public_ca() -> bytes:
    """Generate an ephemeral test CA; private key material never leaves this function."""
    key = ec.generate_private_key(ec.SECP256R1())
    name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "unit-test-ca")])
    now = datetime.now(UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - timedelta(minutes=1))
        .not_valid_after(now + timedelta(days=1))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    return certificate.public_bytes(serialization.Encoding.PEM)


@pytest.mark.parametrize("kind", ["valid", "invalid", "oversized"])
async def test_ca_projection_is_private_and_fail_closed(tmp_path: Path, kind: str) -> None:
    """Reject invalid trust before starting a sandbox; project valid trust only to the Sidecar."""
    ca = public_ca() if kind == "valid" else b"invalid" if kind == "invalid" else b"x" * 8193
    path = tmp_path / "ca.pem"
    path.write_bytes(ca)
    driver = _FakeDriver(tmp_path / "sandbox", outputs=_success_outputs())
    scheduler = _scheduler(
        driver,
        repo=repository(SharedAttempts()),
        settings=replace(_settings(), sidecar_ca_file=str(path)),
    )
    result = await scheduler.schedule(_request())
    assert driver.handle is not None
    if kind != "valid":
        assert result.class_ is not ResultClass.SUCCESS and driver.started == 0
        return
    assert result.class_ is ResultClass.SUCCESS
    assert (driver.handle.sidecar_root / "ca.pem").read_bytes() == ca
    assert (driver.handle.sidecar_root / "ca.pem").stat().st_mode & 0o777 == 0o400
    assert not (driver.handle.input_root / "ca.pem").exists()
    assert driver.last_plan is not None and driver.last_plan.sidecar.custom_ca
    plan = replace(
        driver.last_plan,
        sidecar=replace(driver.last_plan.sidecar, credential_broker=True),
    )
    pod = build_activity_job(plan)["spec"]["template"]["spec"]
    sidecar = next(
        item
        for item in pod["initContainers"]
        if item.get("restartPolicy") == "Always" and item["name"] == "connector-sidecar"
    )
    assert {"name": "CUSTOS_SIDECAR_CA_BUNDLE", "value": "/custos/sidecar/ca.pem"} in sidecar["env"]
