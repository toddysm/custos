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
#: Default TCP port the control-channel HTTPS server binds (CONN-IMPL-020).
DEFAULT_CONTROL_PORT: Final[int] = 9443
#: Default bind host for the control channel. ``0.0.0.0`` so an
#: in-cluster operator (ARM or CS) can reach the pod IP.
DEFAULT_CONTROL_HOST: Final[str] = "0.0.0.0"

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
        control_enabled: Whether to start the control-channel HTTPS
            server (CONN-IMPL-020). When ``True``, the three mTLS path
            fields (:attr:`control_tls_cert_path`,
            :attr:`control_tls_key_path`, :attr:`control_tls_ca_path`)
            must all be set; ``control_host`` and ``control_port`` keep
            their defaults if unset. When ``False`` (typical for unit
            tests / dev), the sidecar only serves the UDS surface and
            revoke is unavailable.
        control_host: Bind host for the control channel; defaults to
            ``0.0.0.0`` so a peer reaching the pod IP can connect.
        control_port: TCP port for the control channel. Defaults to
            9443 per the design's locked port allocation.
        control_tls_cert_path: PEM-encoded server certificate the
            control server presents during the TLS handshake. The
            sidecar reads it once at start (via uvicorn / ssl).
        control_tls_key_path: PEM-encoded private key matching
            :attr:`control_tls_cert_path`.
        control_tls_ca_path: PEM-encoded CA bundle the control server
            uses to verify client certificates. mTLS is mandatory:
            uvicorn is configured with ``ssl_cert_reqs=CERT_REQUIRED``
            so any peer presenting an unsigned client cert is
            rejected at the TLS layer (before any handler runs).
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
    control_enabled: bool = False
    control_host: str = DEFAULT_CONTROL_HOST
    control_port: int = DEFAULT_CONTROL_PORT
    control_tls_cert_path: str | None = None
    control_tls_key_path: str | None = None
    control_tls_ca_path: str | None = None

    def __post_init__(self) -> None:
        """Validate cross-field invariants the env loader cannot express.

        When :attr:`control_enabled` is true the three mTLS path
        fields must all be set; otherwise the sidecar would start the
        TLS listener with a half-configured SSL context and silently
        accept unauthenticated peers.
        """
        if self.control_enabled:
            missing = [
                name
                for name, value in (
                    ("control_tls_cert_path", self.control_tls_cert_path),
                    ("control_tls_key_path", self.control_tls_key_path),
                    ("control_tls_ca_path", self.control_tls_ca_path),
                )
                if value is None or value == ""
            ]
            if missing:
                raise ValueError(
                    "control_enabled is true but mTLS paths are missing: " + ", ".join(missing)
                )


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
    * ``CONTROL_ENABLED`` (opt, default ``false``) — when ``true``, the
      sidecar starts the control-channel HTTPS server (CONN-IMPL-020)
      on :data:`DEFAULT_CONTROL_PORT` (overridable via
      ``CONTROL_PORT``). When ``true``, ``CONTROL_TLS_CERT_PATH``,
      ``CONTROL_TLS_KEY_PATH`` and ``CONTROL_TLS_CA_PATH`` are all
      required.
    * ``CONTROL_HOST`` (opt, default ``0.0.0.0``) — bind host.
    * ``CONTROL_PORT`` (opt, default ``9443``) — bind port.
    * ``CONTROL_TLS_CERT_PATH`` (req when control enabled) — path to
      PEM server certificate.
    * ``CONTROL_TLS_KEY_PATH`` (req when control enabled) — path to
      PEM server private key.
    * ``CONTROL_TLS_CA_PATH`` (req when control enabled) — path to
      PEM CA bundle for client-cert verification (mTLS).
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
    control_enabled = _parse_bool(env.get(_ENV_PREFIX + "CONTROL_ENABLED"), default=False)
    control_port_raw = env.get(_ENV_PREFIX + "CONTROL_PORT")
    if control_port_raw is None or control_port_raw == "":
        control_port = DEFAULT_CONTROL_PORT
    else:
        try:
            control_port = int(control_port_raw)
        except ValueError as exc:
            raise ValueError(
                f"{_ENV_PREFIX}CONTROL_PORT must be an integer; got {control_port_raw!r}"
            ) from exc
        if not (1 <= control_port <= 65535):
            raise ValueError(f"{_ENV_PREFIX}CONTROL_PORT must be in 1..65535; got {control_port}")
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
        control_enabled=control_enabled,
        control_host=env.get(_ENV_PREFIX + "CONTROL_HOST", DEFAULT_CONTROL_HOST),
        control_port=control_port,
        control_tls_cert_path=env.get(_ENV_PREFIX + "CONTROL_TLS_CERT_PATH") or None,
        control_tls_key_path=env.get(_ENV_PREFIX + "CONTROL_TLS_KEY_PATH") or None,
        control_tls_ca_path=env.get(_ENV_PREFIX + "CONTROL_TLS_CA_PATH") or None,
    )


def _parse_bool(raw: str | None, *, default: bool) -> bool:
    """Parse a permissive truthy/falsy env var value.

    Recognises ``1/0``, ``true/false``, ``yes/no``, ``on/off`` in any
    case. Empty / missing falls back to ``default``. Anything else
    raises :class:`ValueError` so a typo in the operator's manifest
    crashes the pod at start instead of silently defaulting.
    """
    if raw is None or raw == "":
        return default
    lowered = raw.strip().lower()
    if lowered in {"1", "true", "yes", "on"}:
        return True
    if lowered in {"0", "false", "no", "off"}:
        return False
    raise ValueError(f"unrecognised boolean: {raw!r}")


__all__ = [
    "DEFAULT_BOOTSTRAP_KEY_PATH",
    "DEFAULT_BOOTSTRAP_TOKEN_PATH",
    "DEFAULT_CONTROL_HOST",
    "DEFAULT_CONTROL_PORT",
    "DEFAULT_SOCKET_PATH",
    "Settings",
    "load_settings",
]
