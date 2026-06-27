"""Per-slot registry credential materialization for skopeo (COPY-IMPL-003).

ARM injects each bound connector's resolved credential as plaintext files
under ``/custos/in/secrets/<slot>/<key>``. This module reads those, redacts
them everywhere they could leak, and materializes a Docker-style
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
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path

from copy_image.contract import Sandbox

#: Default secret-file keys ARM injects for an x-dapr-secret connector
#: (the DaprSecretResolver resolves the durable PAT to ``{username, token}``).
DEFAULT_USERNAME_KEY = "username"
DEFAULT_TOKEN_KEY = "token"


@dataclass(frozen=True, repr=False)
class SlotCredentials:
    """A resolved ``username`` + opaque ``secret`` for one connector slot.

    ``secret`` is sensitive: :meth:`__repr__` / :meth:`__str__` redact it so
    it never lands in logs, tracebacks, or the copy-report.
    """

    username: str
    secret: str

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
    """Read the injected ``username``/``token`` secrets for ``slot``.

    A missing secret raises :class:`ActivityError` with code
    ``<slot>.unauthorized`` (permanent), matching the manifest's declared
    errors.
    """
    username = sandbox.read_secret(slot, username_key)
    secret = sandbox.read_secret(slot, token_key)
    return SlotCredentials(username=username, secret=secret)


def build_auths(
    host_to_creds: Mapping[str, SlotCredentials],
) -> dict[str, dict[str, dict[str, str]]]:
    """Build a Docker ``auth.json`` document keyed by registry host."""
    return {"auths": {host: {"auth": creds.docker_auth()} for host, creds in host_to_creds.items()}}


def write_authfile(directory: Path, host_to_creds: Mapping[str, SlotCredentials]) -> Path:
    """Write a private (``0600``) ``auth.json`` and return its path.

    ``skopeo`` is invoked with ``--authfile <path>``; keeping creds in a file
    (rather than on the command line) avoids leaking them through the process
    table.
    """
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "auth.json"
    path.write_text(json.dumps(build_auths(host_to_creds)), encoding="utf-8")
    path.chmod(0o600)
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
