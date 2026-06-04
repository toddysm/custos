"""Secret Injector domain models and the sandbox write-target protocol.

The Secret Injector materializes connector credentials and the sidecar
bootstrap token into the activity's input tree. It writes through a
:class:`SecretSink` so it stays decoupled from the concrete sandbox runtime:
the OCI Container Driver (ARM-IMPL-013) supplies a sink rooted at the pod's
tmpfs ``/custos/in`` mount, while tests supply an in-memory double.

The on-disk layout is the locked Activity Contract (design § Sandbox
filesystem layout):

* ``secrets/<connector-slot>/<key>`` — one file per materialized credential,
  namespaced under the manifest ``spec.connectors[].name`` slot. ``0400``.
* ``sidecar-token`` — the ARM-minted bootstrap token scoped to
  ``(runId, stepId, attempt)``. ``0400``, its own file (never a ``ctx.json``
  field) so the activity authenticates without parsing JSON.

Plaintext credentials live ONLY under ``secrets/``; they never appear in
``inputs.json``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Final, Protocol, runtime_checkable

#: Sub-directory of ``/custos/in`` that holds materialized connector
#: credentials. Mirrors the locked Activity Contract path
#: ``/custos/in/secrets/<connector-slot>/<key>``.
SECRETS_SUBDIR: Final[str] = "secrets"

#: Filename (under ``/custos/in``) of the sidecar bootstrap token.
SIDECAR_TOKEN_FILENAME: Final[str] = "sidecar-token"

#: POSIX mode for every secret file and the bootstrap token: read-only for the
#: owning activity process, nothing for group/other.
SECRET_FILE_MODE: Final[int] = 0o400


@dataclass(frozen=True, slots=True)
class ConnectorContext:
    """A pre-resolved connector slot ARM materializes inside the sandbox.

    Produced by the Workflow Service's ``BindForStep`` and delivered to ARM in
    the ``ScheduleActivity`` request. ARM exposes only the credential-free
    :class:`~custos_arm.contract.types.ConnectorRef` to expressions; the
    credential material in :attr:`secrets` is written to tmpfs and is never
    reachable from expressions or ``inputs.json``.

    :param slot_name: Logical slot name; matches ``spec.connectors[].name``.
    :param connector_type: Connector type, e.g. ``"oci-registry"``.
    :param connector_instance_id: Bound instance the credentials belong to.
    :param secrets: Materialized ``key -> value`` credentials. Empty for
        token-only connector types whose activities mint short-lived tokens
        from the sidecar instead of reading static files.
    :param lease_id: Connector Service lease id ARM may ``RefreshLease`` for a
        long-running step; ``None`` when ARM holds no refreshable lease.
    :raises ValueError: If any identity field is empty or a secret key is empty.
    """

    slot_name: str
    connector_type: str
    connector_instance_id: str
    secrets: Mapping[str, str] = field(default_factory=dict)
    lease_id: str | None = None

    def __post_init__(self) -> None:
        if not self.slot_name:
            raise ValueError("ConnectorContext.slot_name must be a non-empty string")
        if not self.connector_type:
            raise ValueError("ConnectorContext.connector_type must be a non-empty string")
        if not self.connector_instance_id:
            raise ValueError("ConnectorContext.connector_instance_id must be a non-empty string")
        for key in self.secrets:
            if not key:
                raise ValueError(
                    f"ConnectorContext.secrets for slot {self.slot_name!r} has an empty key"
                )
        if self.lease_id is not None and not self.lease_id:
            raise ValueError("ConnectorContext.lease_id, when set, must be a non-empty string")


@dataclass(frozen=True, slots=True)
class SidecarToken:
    """An ARM-minted sidecar bootstrap token bound to one step attempt.

    The activity reads the token value once from ``/custos/in/sidecar-token``
    and sends it in the ``Custos-Sidecar-Token`` header on every sidecar API
    request. The scope triple lets the sidecar reject a token replayed across
    a different attempt.

    :raises ValueError: If the token value is empty or the scope is malformed.
    """

    value: str
    run_id: str
    step_id: str
    attempt: int

    def __post_init__(self) -> None:
        if not self.value:
            raise ValueError("SidecarToken.value must be a non-empty string")
        if not self.run_id:
            raise ValueError("SidecarToken.run_id must be a non-empty string")
        if not self.step_id:
            raise ValueError("SidecarToken.step_id must be a non-empty string")
        if isinstance(self.attempt, bool) or not isinstance(self.attempt, int):
            raise ValueError("SidecarToken.attempt must be an int")
        if self.attempt < 1:
            raise ValueError("SidecarToken.attempt must be >= 1")

    @property
    def scope(self) -> tuple[str, str, int]:
        """The ``(runId, stepId, attempt)`` triple the token is bound to."""
        return (self.run_id, self.step_id, self.attempt)


@dataclass(frozen=True, slots=True)
class InjectionResult:
    """Outcome of one :meth:`SecretInjector.inject` call.

    :param token: The bootstrap token written to ``sidecar-token``.
    :param secret_files: Relative paths (under ``/custos/in``) of every
        materialized credential file, in deterministic written order.
    """

    token: SidecarToken
    secret_files: tuple[str, ...]


@runtime_checkable
class SecretSink(Protocol):
    """Write-target for the sandbox input tree (rooted at ``/custos/in``).

    The runtime driver implements this over the pod's tmpfs mount; the broker
    and tests implement it in memory. Paths are POSIX-relative to the input
    root and never contain ``..`` segments.
    """

    async def write_secret(self, *, relative_path: str, content: bytes, mode: int) -> None:
        """Atomically create ``relative_path`` with ``content`` and ``mode``.

        Intermediate directories are created as needed. Implementations MUST
        place the file on a tmpfs (memory-backed) mount so plaintext
        credentials never touch durable storage.
        """
        ...


__all__ = [
    "SECRETS_SUBDIR",
    "SECRET_FILE_MODE",
    "SIDECAR_TOKEN_FILENAME",
    "ConnectorContext",
    "InjectionResult",
    "SecretSink",
    "SidecarToken",
]
