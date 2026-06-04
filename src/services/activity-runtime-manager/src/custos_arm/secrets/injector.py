"""Secret Injector — materializes connector credentials + the sidecar token.

The Scheduler calls :meth:`SecretInjector.inject` after the I/O Broker has
populated ``inputs.json`` and before the runtime driver starts the activity.
The injector:

#. fails fast if a manifest-required connector slot was never bound
   (``input.missing_connector``);
#. writes each materialized credential to ``secrets/<slot>/<key>`` (``0400``,
   tmpfs) — never into ``inputs.json``;
#. mints the ``(runId, stepId, attempt)``-scoped bootstrap token and writes it
   to ``sidecar-token`` (``0400``).

For long-running steps the Scheduler additionally calls
:meth:`SecretInjector.refresh_leases` to extend the connector leases ARM holds,
and :meth:`SecretInjector.revoke` once the attempt reaches a terminal state.
"""

from __future__ import annotations

import posixpath
from collections.abc import Sequence
from datetime import datetime

from custos_arm.contract import StepRef
from custos_arm.manifest import ConnectorSpec
from custos_arm.secrets.errors import MissingConnectorError, MissingSecretError
from custos_arm.secrets.lease import ConnectorLeaseClient, Lease
from custos_arm.secrets.models import (
    SECRET_FILE_MODE,
    SECRETS_SUBDIR,
    SIDECAR_TOKEN_FILENAME,
    ConnectorContext,
    InjectionResult,
    SecretSink,
    SidecarToken,
)
from custos_arm.secrets.token import SidecarTokenMinter


class SecretInjector:
    """Materializes connector secrets and the sidecar bootstrap token.

    :param token_minter: Mints/revokes scope-bound bootstrap tokens.
    :param lease_client: Connector Service ``RefreshLease`` adapter; required
        only when :meth:`refresh_leases` is used for long-running steps.
    """

    def __init__(
        self,
        *,
        token_minter: SidecarTokenMinter,
        lease_client: ConnectorLeaseClient | None = None,
    ) -> None:
        self._minter = token_minter
        self._lease_client = lease_client

    async def inject(
        self,
        *,
        sink: SecretSink,
        step: StepRef,
        connectors: Sequence[ConnectorSpec],
        contexts: Sequence[ConnectorContext],
    ) -> InjectionResult:
        """Materialize secrets + the bootstrap token into the input tree.

        :raises MissingConnectorError: a ``required`` connector slot has no
            bound context (permanent).
        :raises MissingSecretError: a bound context carries an empty credential
            value (permanent).
        :raises ValueError: two contexts claim the same slot (malformed bind).
        """
        by_slot = self._index_contexts(contexts)
        self._reject_undeclared_contexts(connectors, by_slot)
        self._require_bound_connectors(connectors, by_slot)
        self._reject_empty_secrets(contexts)

        secret_files: list[str] = []
        for context in sorted(contexts, key=lambda c: c.slot_name):
            for key in sorted(context.secrets):
                relative_path = posixpath.join(SECRETS_SUBDIR, context.slot_name, key)
                await sink.write_secret(
                    relative_path=relative_path,
                    content=context.secrets[key].encode("utf-8"),
                    mode=SECRET_FILE_MODE,
                )
                secret_files.append(relative_path)

        token = self._minter.mint(step=step)
        await sink.write_secret(
            relative_path=SIDECAR_TOKEN_FILENAME,
            content=token.value.encode("utf-8"),
            mode=SECRET_FILE_MODE,
        )

        return InjectionResult(token=token, secret_files=tuple(secret_files))

    async def refresh_leases(
        self,
        *,
        contexts: Sequence[ConnectorContext],
        requested_ttl_sec: int | None = None,
        step_deadline: datetime | None = None,
    ) -> list[Lease]:
        """Extend every refreshable connector lease for a long-running step.

        Contexts without a ``lease_id`` are skipped. Returns the refreshed
        leases in slot order.

        :raises ValueError: a context carries a ``lease_id`` but no
            ``lease_client`` was configured.
        :raises LeaseRefreshRejectedError: a lease is gone (permanent).
        :raises ConnectorUnavailableError: the Connector Service is unreachable
            (transient).
        """
        refreshed: list[Lease] = []
        for context in sorted(contexts, key=lambda c: c.slot_name):
            if context.lease_id is None:
                continue
            if self._lease_client is None:
                raise ValueError(
                    f"slot {context.slot_name!r} carries a lease but no lease_client is configured"
                )
            refreshed.append(
                await self._lease_client.refresh_lease(
                    lease_id=context.lease_id,
                    requested_ttl_sec=requested_ttl_sec,
                    step_deadline=step_deadline,
                )
            )
        return refreshed

    def revoke(self, *, token: SidecarToken) -> None:
        """Revoke the bootstrap token at step terminal. Idempotent."""
        self._minter.revoke(token)

    @staticmethod
    def _index_contexts(
        contexts: Sequence[ConnectorContext],
    ) -> dict[str, ConnectorContext]:
        by_slot: dict[str, ConnectorContext] = {}
        for context in contexts:
            if context.slot_name in by_slot:
                raise ValueError(f"duplicate connector context for slot {context.slot_name!r}")
            by_slot[context.slot_name] = context
        return by_slot

    @staticmethod
    def _reject_undeclared_contexts(
        connectors: Sequence[ConnectorSpec],
        by_slot: dict[str, ConnectorContext],
    ) -> None:
        declared = {spec.name for spec in connectors}
        undeclared = sorted(slot for slot in by_slot if slot not in declared)
        if undeclared:
            raise ValueError(
                f"connector context(s) for slot(s) not declared in the manifest: "
                f"{', '.join(undeclared)}"
            )

    @staticmethod
    def _require_bound_connectors(
        connectors: Sequence[ConnectorSpec],
        by_slot: dict[str, ConnectorContext],
    ) -> None:
        missing = sorted(
            spec.name for spec in connectors if spec.required and spec.name not in by_slot
        )
        if missing:
            raise MissingConnectorError(
                f"required connector slot(s) not bound: {', '.join(missing)}",
                issues=missing,
            )

    @staticmethod
    def _reject_empty_secrets(contexts: Sequence[ConnectorContext]) -> None:
        empty = sorted(
            f"{context.slot_name}/{key}"
            for context in contexts
            for key, value in context.secrets.items()
            if not value
        )
        if empty:
            raise MissingSecretError(
                f"connector credential(s) have an empty value: {', '.join(empty)}",
                issues=empty,
            )


__all__ = [
    "SecretInjector",
]
