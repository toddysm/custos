"""Sidecar settings (CONN-IMPL-019).

The sidecar is configured entirely at pod start \u2014 it never reads
config after that. Inputs arrive via environment variables seeded by
ARM. All paths and string values are validated eagerly so a misconfig
crashes the pod at start (visible to the operator) rather than at
first request.

Settings are deliberately a single frozen dataclass with explicit
fields; tests construct it inline, the ``__main__`` entry point calls
:func:`load_settings` to parse env vars.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Final

#: Default socket path the design declares for the sidecar UDS.
DEFAULT_SOCKET_PATH: Final[str] = "/custos/run/connector.sock"
#: Default path to the bootstrap token tmpfs file ARM seeds at start.
DEFAULT_BOOTSTRAP_TOKEN_PATH: Final[str] = "/custos/in/sidecar-token"
#: Default path to the shared HMAC verification key.
DEFAULT_BOOTSTRAP_KEY_PATH: Final[str] = "/custos/in/sidecar-key"

_ENV_PREFIX: Final[str] = "CUSTOS_SIDECAR_"


@dataclass(frozen=True, slots=True)
class Settings:
    """Sidecar runtime configuration.

    All fields are required; :func:`load_settings` validates env vars
    and raises :class:`ValueError` for any missing input so the pod
    crashes at start with a clear message.

    Attributes:
        socket_path: Absolute path the UDS HTTP server binds to.
        bootstrap_token_path: Path to the tmpfs file the activity
            container reads to authenticate to the sidecar. The
            sidecar does not read this itself \u2014 it is exposed here
            so the integration harness can mint a matching token.
        bootstrap_key_path: Path to the shared HMAC verification key
            ARM seeds the sidecar with. Read once at start by
            :func:`load_settings`.
        run_id / step_id / attempt: The sidecar's bound triple. Every
            bootstrap token presented over the UDS must encode this
            exact triple.
        workspace_id: The workspace the sidecar serves. Carried in the
            call-context the Lease Gateway sends to Connector Service.
            Informational on the sidecar's own surface.
        connector_service_url: Base URL of Connector Service the Lease
            Gateway POSTs to.
        call_context: Pre-serialized JSON call-context blob the Lease
            Gateway puts in ``X-Call-Context``. Carries the
            ``connector:lease-mint`` permission. Opaque to the sidecar.
        contexts_wire: List of slot-context JSON envelopes ARM seeds
            at start; decoded into a :class:`ContextRegistry` by the
            app factory.
        activity_gid: Optional numeric GID of the activity container.
            When set, ``__main__`` ``chown``s the UDS file to this
            group after uvicorn binds it (with mode ``0o660``) so the
            activity UID can ``connect(2)`` to the socket. When
            ``None``, only the chmod is performed and the socket
            keeps the sidecar UID's primary group.
    """

    socket_path: str
    bootstrap_token_path: str
    bootstrap_key_path: str
    run_id: str
    step_id: str
    attempt: int
    workspace_id: str
    connector_service_url: str
    call_context: str
    contexts_wire: list[dict[str, object]] = field(default_factory=list)
    activity_gid: int | None = None


def _require(env: Mapping[str, str], name: str) -> str:
    key = _ENV_PREFIX + name
    value = env.get(key)
    if value is None or value == "":
        raise ValueError(f"{key} environment variable is required")
    return value


def load_settings(env: Mapping[str, str] | None = None) -> Settings:
    """Parse :class:`Settings` from environment variables.

    ``env`` defaults to :data:`os.environ` for production; tests
    pass an explicit mapping. The variable namespace is
    ``CUSTOS_SIDECAR_*``:

    * ``RUN_ID`` / ``STEP_ID`` / ``ATTEMPT`` \u2014 sidecar's bound triple.
    * ``WORKSPACE_ID`` \u2014 the workspace the sidecar serves.
    * ``CONNECTOR_SERVICE_URL`` \u2014 base URL for the CS internal RPC.
    * ``CALL_CONTEXT`` \u2014 pre-serialized X-Call-Context JSON blob.
    * ``CONTEXTS_JSON`` \u2014 JSON-encoded list of slot-context envelopes.
    * ``SOCKET_PATH`` (opt, default ``/custos/run/connector.sock``)
    * ``BOOTSTRAP_TOKEN_PATH`` (opt)
    * ``BOOTSTRAP_KEY_PATH`` (opt)
    * ``ACTIVITY_GID`` (opt) — numeric GID of the activity container.
      When set, the UDS file is ``chown``ed to this group so the
      activity UID can connect.
    """
    import json

    env = env if env is not None else dict(os.environ)
    attempt_raw = _require(env, "ATTEMPT")
    try:
        attempt = int(attempt_raw)
    except ValueError as exc:
        raise ValueError(f"{_ENV_PREFIX}ATTEMPT must be an integer; got {attempt_raw!r}") from exc
    if attempt <= 0:
        raise ValueError(f"{_ENV_PREFIX}ATTEMPT must be positive; got {attempt}")
    contexts_raw = env.get(_ENV_PREFIX + "CONTEXTS_JSON", "[]")
    try:
        contexts_wire = json.loads(contexts_raw)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{_ENV_PREFIX}CONTEXTS_JSON is not valid JSON: {exc!s}") from exc
    if not isinstance(contexts_wire, list):
        raise ValueError(
            f"{_ENV_PREFIX}CONTEXTS_JSON must be a JSON array; got {type(contexts_wire).__name__}"
        )
    activity_gid_raw = env.get(_ENV_PREFIX + "ACTIVITY_GID")
    activity_gid: int | None
    if activity_gid_raw is None or activity_gid_raw == "":
        activity_gid = None
    else:
        try:
            activity_gid = int(activity_gid_raw)
        except ValueError as exc:
            raise ValueError(
                f"{_ENV_PREFIX}ACTIVITY_GID must be an integer; got {activity_gid_raw!r}"
            ) from exc
        if activity_gid < 0:
            raise ValueError(f"{_ENV_PREFIX}ACTIVITY_GID must be non-negative; got {activity_gid}")
    return Settings(
        socket_path=env.get(_ENV_PREFIX + "SOCKET_PATH", DEFAULT_SOCKET_PATH),
        bootstrap_token_path=env.get(
            _ENV_PREFIX + "BOOTSTRAP_TOKEN_PATH", DEFAULT_BOOTSTRAP_TOKEN_PATH
        ),
        bootstrap_key_path=env.get(_ENV_PREFIX + "BOOTSTRAP_KEY_PATH", DEFAULT_BOOTSTRAP_KEY_PATH),
        run_id=_require(env, "RUN_ID"),
        step_id=_require(env, "STEP_ID"),
        attempt=attempt,
        workspace_id=_require(env, "WORKSPACE_ID"),
        connector_service_url=_require(env, "CONNECTOR_SERVICE_URL"),
        call_context=_require(env, "CALL_CONTEXT"),
        contexts_wire=contexts_wire,
        activity_gid=activity_gid,
    )


__all__ = [
    "DEFAULT_BOOTSTRAP_KEY_PATH",
    "DEFAULT_BOOTSTRAP_TOKEN_PATH",
    "DEFAULT_SOCKET_PATH",
    "Settings",
    "load_settings",
]
