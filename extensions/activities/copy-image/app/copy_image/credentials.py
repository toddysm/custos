"""Per-slot registry credential materialization for skopeo (COPY-IMPL-003).

Production material comes from the authenticated local Sidecar broker.
Explicit legacy credential files remain supported without mixing partial
files and broker responses. Material is written to a private Docker-style
``auth.json`` that ``skopeo`` consumes via ``--authfile``.

Deliberately thin: there is **no** proactive token Authenticator here.
Credential minting, leasing, and refresh are the connector + sidecar's
responsibility; ``skopeo`` performs the registry token exchange (and any
mid-copy refresh) against the spec-compliant Docker Hub / GHCR endpoints
itself. See ``design/architecture/registry-credential-refresh.md``.
"""

from __future__ import annotations

import base64
import json
import logging
import os
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urlencode

from copy_image import broker
from copy_image.contract import ActivityError, Sandbox

#: Default secret-file keys ARM injects for an x-dapr-secret connector
#: (the DaprSecretResolver resolves the durable PAT to ``{username, token}``).
DEFAULT_USERNAME_KEY = "username"
DEFAULT_TOKEN_KEY = "token"


@dataclass(frozen=True, repr=False)
class SlotCredentials:
    """A resolved ``username`` + opaque ``secret`` for one connector slot.

    ``secret`` is sensitive: :meth:`__repr__` / :meth:`__str__` redact it so
    it never lands in logs, tracebacks, or the copy-report.
    Broker leases must be released by the caller. ``provider_expires_at=None``
    means unknown upstream lifetime, including for legacy file credentials.
    """

    username: str
    secret: str
    lease_id: str | None = None
    provider_expires_at: datetime | None = None

    def __repr__(self) -> str:
        return f"SlotCredentials(username={self.username!r}, secret=<redacted>)"

    __str__ = __repr__

    def docker_auth(self) -> str:
        """The base64 ``user:secret`` value for a Docker ``auths`` entry."""
        raw = f"{self.username}:{self.secret}".encode()
        return base64.b64encode(raw).decode("ascii")


def read_slot_credentials(
    sandbox: Sandbox,
    slot: str,
    *,
    username_key: str = DEFAULT_USERNAME_KEY,
    token_key: str = DEFAULT_TOKEN_KEY,
) -> SlotCredentials:
    """Read explicit legacy files or obtain validated registry material from the Sidecar.

    Partial file injection fails rather than mixing credential sources. Broker
    responses must match the attempt and declared slot purpose. Unknown provider
    expiry stays unknown; it is never replaced with the platform lease deadline.
    """
    if (
        sandbox.has_secret(slot, username_key)
        or sandbox.has_secret(slot, token_key)
        or sandbox.sidecar_token() is None
    ):
        return SlotCredentials(
            username=sandbox.read_secret(slot, username_key),
            secret=sandbox.read_secret(slot, token_key),
        )
    purposes = {"source": "oci.pull", "dest": "oci.push"}
    if slot not in purposes:
        raise ActivityError("activity.contract_violation", "permanent", "unsupported registry slot")
    purpose = purposes[slot]
    response = broker.request(
        sandbox,
        slot,
        "/v1/token?" + urlencode({"slot": slot, "purpose": purpose}),
    )
    ctx = sandbox.read_context()
    lease, credential = response.get("lease"), response.get("credential")
    acquired_lease: str | None = None
    try:
        if (
            response.get("schemaVersion") != "2"
            or not isinstance(lease, dict)
            or not isinstance(credential, dict)
            or not ctx.workspace_id
            or type(ctx.attempt) is not int
            or type(lease.get("attempt")) is not int
            or (
                lease.get("workspaceId"),
                lease.get("runId"),
                lease.get("stepId"),
                lease.get("attempt"),
                lease.get("slot"),
                lease.get("capability"),
            )
            != (ctx.workspace_id, ctx.run_id, ctx.step_id, ctx.attempt, slot, purpose)
        ):
            raise ValueError
        lease_id = lease.get("leaseId")
        material = credential.get("material")
        if not isinstance(lease_id, str) or not 1 <= len(lease_id) <= 120:
            raise ValueError
        acquired_lease = lease_id
        if (
            not isinstance(material, dict)
            or not isinstance(credential.get("authenticationType"), str)
            or not credential["authenticationType"]
        ):
            raise ValueError
        username, secret = material.get("username"), material.get("token")
        if (
            not isinstance(username, str)
            or not username
            or not isinstance(secret, str)
            or not secret
        ):
            raise ValueError
        _timestamp(lease.get("expiresAt"), live=True)
        _timestamp(credential.get("issuedAt"), live=False)
        if "expiresAt" not in credential:
            raise ValueError
        expires = credential["expiresAt"]
        provider_expiry = None if expires is None else _timestamp(expires, live=True)
        return SlotCredentials(username, secret, lease_id, provider_expiry)
    except ValueError:
        if acquired_lease is not None:
            try:
                _release_lease(sandbox, slot, acquired_lease)
            except ActivityError:
                logging.getLogger(__name__).warning("Credential lease cleanup failed for %s", slot)
        raise ActivityError(
            "activity.contract_violation",
            "permanent",
            f"invalid registry credential envelope for {slot}",
        ) from None


def _timestamp(value: object, *, live: bool) -> datetime:
    """Require timezone-aware provider/lease metadata; live deadlines must be future values."""
    if not isinstance(value, str):
        raise ValueError
    parsed = datetime.fromisoformat(value)
    if parsed.utcoffset() is None or (live and parsed <= datetime.now(UTC)):
        raise ValueError
    return parsed


def release_slot_credentials(sandbox: Sandbox, slot: str, credentials: SlotCredentials) -> None:
    """Release an acquired broker lease; legacy file credentials have no platform lease."""
    if credentials.lease_id is None:
        return
    _release_lease(sandbox, slot, credentials.lease_id)


def _release_lease(sandbox: Sandbox, slot: str, lease_id: str) -> None:
    """Release a verified scoped lease, including one whose provider material was unusable."""
    result = broker.request(
        sandbox,
        slot,
        "/v1/token/release",
        payload={"leaseId": lease_id},
    )
    if result != {"released": True}:
        raise ActivityError(
            "activity.contract_violation", "permanent", "invalid lease release reply"
        )


def build_auths(
    host_to_creds: Mapping[str, SlotCredentials],
) -> dict[str, dict[str, dict[str, str]]]:
    """Build a Docker ``auth.json`` document keyed by registry host."""
    auths = {host: {"auth": creds.docker_auth()} for host, creds in host_to_creds.items()}
    return {"auths": auths}


def write_authfile(directory: Path, host_to_creds: Mapping[str, SlotCredentials]) -> Path:
    """Write a private (``0600``) ``auth.json`` and return its path.

    ``skopeo`` is invoked with ``--authfile <path>``; keeping creds in a file
    (rather than on the command line) avoids leaking them through the process
    table. The file is created with ``0600`` up front and ``fchmod``-ed before
    any secret material is written, so there is no window where the auth
    document is readable with a broader mode (even if it pre-existed).
    """
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "auth.json"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w", encoding="utf-8") as handle:
        os.fchmod(handle.fileno(), 0o600)
        handle.write(json.dumps(build_auths(host_to_creds)))
    return path


def redact(text: str, secrets: Iterable[str]) -> str:
    """Replace any occurrence of a secret value in ``text`` with ``***``.

    Used to scrub skopeo stderr / error detail before it reaches the
    failure envelope, the copy-report, or logs.
    """
    scrubbed = text
    for secret in secrets:
        if secret:
            scrubbed = scrubbed.replace(secret, "***")
    return scrubbed
