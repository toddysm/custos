"""``CursorService`` — the per-instance pull cursor lifecycle.

See ``custos_connector.cursor`` (package docstring) for the design
contract this module implements.

Value encoding boundary
-----------------------

The SPL ``ConnectorCursor`` row persists ``value`` as a ``str``. The
plugin's ``CursorEnvelope.value`` is typed ``Any`` — typically a
string but the wire contract allows any JSON-compatible value. We
bridge the two boundaries with a single deterministic convention:

* ``value == ""`` in the SPL row is the **uninitialized sentinel** —
  what :meth:`MetadataStoreProvider.acquire_cursor_lease` writes on
  first touch. It maps to ``CursorEnvelope.value = None`` so the
  plugin can branch on ``cursor.value is None`` and apply its
  declared ``initialCursorBehavior``.
* Any committed ``value`` is the JSON serialization of the plugin's
  next-cursor value. The literal JSON string ``"null"`` is therefore
  distinguishable from the uninitialized ``""`` sentinel: a plugin
  that deliberately commits ``value=null`` lands as ``"null"`` and
  round-trips back as ``None`` (with no risk of being confused with
  "never written").

Halting
-------

When the plugin raises ``CursorEncodingMismatch`` or ``CursorExpired``
the service emits the matching audit event, flips the instance's
``status`` to a halt sentinel (``cursor_migration_required`` or
``cursor_expired``), releases the lease, and re-raises a typed
:class:`CursorHalted` subclass. The Pull-loop scheduler
(CONN-IMPL-023, #306) is expected to consult ``instance.status``
before scheduling the next tick — a halted instance never ticks
again until an operator clears the halt via the admin rewind
(CONN-IMPL-024, #307).
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import socket
from collections.abc import Awaitable, Callable, Mapping, Sequence
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any, ClassVar, Final, TypeAlias

from custos_spl import LeaseBusy, LeaseExpired, LeaseHandle
from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.metadata_store import ConnectorCursor

from custos_connector._telemetry import CURSOR_LAG_REGISTRY, observe_pull_tick
from custos_connector.audit import (
    audit_cursor_advanced,
    audit_cursor_encoding_mismatch,
    audit_cursor_expired,
)
from custos_connector.runtime import (
    CursorEncodingMismatch,
    CursorEnvelope,
    CursorExpired,
    ListenMode,
)

if TYPE_CHECKING:
    from custos_spl.interfaces.catalog_store import (
        CatalogStoreProvider,
        ConnectorTypeVersion,
    )
    from custos_spl.interfaces.connector_instance_store import (
        ConnectorInstance,
        ConnectorInstanceStoreProvider,
    )
    from custos_spl.interfaces.metadata_store import MetadataStoreProvider

    from custos_connector.runtime import PluginInvoker


#: Default lease TTL per tick (60 s) per design § Pull Cursor Model.
#: A tick that runs longer than this will lose its lease on
#: :meth:`commit_cursor`; the scheduler should size the per-instance
#: tick interval well above any expected plugin ``listen()`` latency.
DEFAULT_CURSOR_LEASE_TTL_SECONDS: Final[int] = 60

#: Instance ``status`` sentinel set when a plugin returns
#: :class:`CursorEncodingMismatch`. The scheduler MUST treat this
#: status as a halt: no further ticks until an operator clears it
#: via the admin rewind (CONN-IMPL-024).
HALT_STATUS_ENCODING_MISMATCH: Final[str] = "cursor_migration_required"

#: Instance ``status`` sentinel set when a plugin returns
#: :class:`CursorExpired`. Same halt semantics as
#: :data:`HALT_STATUS_ENCODING_MISMATCH`.
HALT_STATUS_EXPIRED: Final[str] = "cursor_expired"


_DEFAULT_ACTOR: Final[str] = "connector-service"

_LOGGER = logging.getLogger("custos_connector.cursor")


# ---------------------------------------------------------------------------
# Errors
# ---------------------------------------------------------------------------


class CursorHalted(Exception):
    """Raised by :meth:`CursorService.tick` when the instance is halted.

    The instance ``status`` has already been flipped to a halt
    sentinel by the time this is raised. The scheduler should
    consult ``status`` (rather than catching this) when deciding
    whether to fire the next tick — the exception is the
    immediate-call signal, the status flip is the durable one.
    """


class CursorEncodingMismatchHalt(CursorHalted):
    """Plugin returned :class:`CursorEncodingMismatch`.

    Instance ``status`` is now :data:`HALT_STATUS_ENCODING_MISMATCH`.
    Operator must rewind via the admin REST surface.
    """

    def __init__(
        self,
        detail: str,
        *,
        persisted_encoding: str | None,
        plugin_encoding: str | None,
    ) -> None:
        super().__init__(detail)
        self.persisted_encoding = persisted_encoding
        self.plugin_encoding = plugin_encoding


class CursorExpiredHalt(CursorHalted):
    """Plugin returned :class:`CursorExpired`.

    Instance ``status`` is now :data:`HALT_STATUS_EXPIRED`. Operator
    must rewind via the admin REST surface.
    """


class CursorInstanceUnavailable(Exception):
    """Raised when a tick is attempted on an absent / ineligible instance.

    Distinct from :class:`CursorHalted` because halt is durable state
    on the row; unavailability covers transient or configuration
    issues (deleted instance, disabled instance, missing
    connector-type row in the catalog). The scheduler is expected
    to handle this by dropping the instance from its rotation, not
    by retrying immediately.
    """

    def __init__(self, instance_id: str, reason: str) -> None:
        super().__init__(f"instance {instance_id!r} is unavailable for tick: {reason}")
        self.instance_id = instance_id
        self.reason = reason


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CursorEnvelopeRecord:
    """Admin-facing view of a cursor.

    The persisted SPL row carries only ``value`` (opaque string) and
    ``advanced_at``; ``encoding`` is recombined here from the current
    connector-type manifest (the source of truth for which encoding
    the plugin currently emits).

    ``value`` is the plugin-side JSON value (``None`` for the
    uninitialized sentinel). Use :func:`fingerprint_envelope` if you
    need an audit-safe fingerprint of the persisted form.
    """

    workspace_id: str
    instance_id: str
    encoding: str
    value: Any
    advanced_at: datetime
    lease_holder: str | None
    lease_expires_at: datetime | None


@dataclass(frozen=True, slots=True)
class TickResult:
    """Outcome of a single successful :meth:`CursorService.tick`."""

    instance_id: str
    event_count: int
    cursor_before: CursorEnvelopeRecord
    cursor_after: CursorEnvelopeRecord
    #: Receiver endpoint returned by the plugin's ``listen`` hook for
    #: push-mode connectors. Always ``None`` in pull-mode ticks; kept
    #: on the result type for forward-compat with the Listen Manager
    #: (CONN-IMPL-025).
    receiver_endpoint: str | None


#: Publisher delegate the tick calls before committing the cursor.
#:
#: Implementations MUST raise on partial publish so the cursor does
#: not advance past unacknowledged events. The Trigger Service-backed
#: implementation (#308 / TS-side work) is responsible for the
#: actual fan-out to subscriptions; for #305 we ship the contract
#: and the in-tick at-least-once ordering.
EventPublisher: TypeAlias = Callable[
    [WorkspaceId, "ConnectorInstance", Sequence[Mapping[str, Any]]],
    Awaitable[None],
]


# ---------------------------------------------------------------------------
# Encoding / fingerprint helpers
# ---------------------------------------------------------------------------


_UNINITIALIZED_PERSISTED: Final[str] = ""


def _persisted_to_envelope_value(persisted: str) -> Any:
    """Decode the SPL row's ``value`` into a plugin-side JSON value.

    The empty string sentinel maps to ``None`` (= uninitialized). Any
    other persisted string is decoded as JSON; if decoding fails (which
    should never happen for values we wrote ourselves) we surface the
    raw string so the audit trail at least carries something
    investigatable.
    """
    if persisted == _UNINITIALIZED_PERSISTED:
        return None
    try:
        return json.loads(persisted)
    except json.JSONDecodeError:
        return persisted


def _envelope_value_to_persisted(envelope_value: Any) -> str:
    """Serialize the plugin's ``next_cursor.value`` for SPL persistence.

    Always JSON-encodes so the literal JSON token ``"null"`` (from
    plugin-emitted ``None``) is distinguishable from the uninitialized
    empty-string sentinel. Sorting keys keeps the persisted form
    canonical so the audit fingerprint is stable under accidental
    re-ordering inside the plugin.
    """
    return json.dumps(envelope_value, sort_keys=True, separators=(",", ":"))


def _fingerprint_persisted(persisted: str) -> str | None:
    """SHA-256 fingerprint of the persisted value string.

    ``None`` for the uninitialized sentinel so audit consumers can
    distinguish "never written" from "written and emptied".
    """
    if persisted == _UNINITIALIZED_PERSISTED:
        return None
    return "sha256:" + hashlib.sha256(persisted.encode("utf-8")).hexdigest()


def _length_persisted(persisted: str) -> int | None:
    """Byte-length of the persisted value string.

    ``None`` for the uninitialized sentinel. Byte length (not
    character length) so audit consumers can size operational
    dashboards against the storage cost.
    """
    if persisted == _UNINITIALIZED_PERSISTED:
        return None
    return len(persisted.encode("utf-8"))


def _default_holder_id() -> str:
    """Build the default lease holder identifier.

    ``<hostname>:<pid>`` is unique enough across the connector-service
    fleet to root-cause "who holds the lease" from the audit trail
    without needing a separate per-replica config knob. Callers that
    want a stronger holder (e.g. pod-uid + container-id) can pass
    ``holder_id`` explicitly to :class:`CursorService`.
    """
    return f"{socket.gethostname()}:{os.getpid()}"


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------


class CursorService:
    """Per-instance pull cursor lifecycle.

    The service is workspace-aware (every method takes
    ``workspace_id`` first) but otherwise stateless across ticks —
    one instance of :class:`CursorService` can drive ticks for any
    number of connector instances. Per-instance state lives entirely
    on the SPL row + the in-memory cursor lease the tick holds.
    """

    DEFAULT_LEASE_TTL_SECONDS: ClassVar[int] = DEFAULT_CURSOR_LEASE_TTL_SECONDS

    def __init__(
        self,
        *,
        metadata_store: MetadataStoreProvider,
        catalog_store: CatalogStoreProvider,
        instance_store: ConnectorInstanceStoreProvider,
        plugin_invoker: PluginInvoker,
        publisher: EventPublisher,
        holder_id: str | None = None,
        lease_ttl_seconds: int = DEFAULT_CURSOR_LEASE_TTL_SECONDS,
        actor: str = _DEFAULT_ACTOR,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        if lease_ttl_seconds <= 0:
            raise ValueError(f"lease_ttl_seconds must be positive (got {lease_ttl_seconds!r})")
        self._metadata = metadata_store
        self._catalog = catalog_store
        self._instances = instance_store
        self._invoker = plugin_invoker
        self._publisher = publisher
        self._holder_id = holder_id or _default_holder_id()
        self._lease_ttl_seconds = lease_ttl_seconds
        self._actor = actor
        self._clock = clock if clock is not None else lambda: datetime.now(UTC)

    @property
    def holder_id(self) -> str:
        return self._holder_id

    # ------------------------------------------------------------------
    # Admin read
    # ------------------------------------------------------------------

    async def read_envelope(
        self,
        workspace_id: str,
        instance_id: str,
    ) -> CursorEnvelopeRecord | None:
        """Read the current cursor envelope without acquiring a lease.

        Returns ``None`` if no cursor row exists yet (the instance has
        never ticked). The ``encoding`` field is sourced from the
        current connector-type manifest, NOT from the persisted row
        (the row carries only ``value`` + ``advanced_at``). A
        connector-type that has bumped its ``cursorEncoding`` will
        therefore report the new encoding even though the persisted
        ``value`` is still in the old format; the mismatch will only
        surface on the next tick, when the plugin returns
        :class:`CursorEncodingMismatch`.
        """
        instance = await self._instances.get_connector_instance(
            WorkspaceId(workspace_id), ConnectorInstanceId(instance_id)
        )
        if instance is None:
            return None
        type_version = await self._catalog.get_connector_type_version(
            instance.type, instance.version
        )
        if type_version is None:
            raise CursorInstanceUnavailable(
                instance_id,
                f"connector-type ({instance.type!r}, {instance.version!r}) "
                "is not registered in the catalog",
            )
        row = await self._metadata.read_cursor(
            WorkspaceId(workspace_id), ConnectorInstanceId(instance_id)
        )
        if row is None:
            return None
        encoding = _extract_cursor_encoding(type_version)
        return _row_to_envelope_record(row, encoding)

    # ------------------------------------------------------------------
    # Tick
    # ------------------------------------------------------------------

    async def tick(
        self,
        workspace_id: str,
        instance_id: str,
    ) -> TickResult:
        """Execute one pull tick for a connector instance.

        Steps, in order:

        1. Load the instance + connector-type rows; refuse if either
           is missing or the instance is disabled / already halted.
        2. Acquire the cursor lease (first-touch insert is implicit
           in the SPL adapter).
        3. Invoke ``listen(mode=pull, cursor=envelope)`` on the plugin.
        4. Publish-ack the batch via the injected publisher.
        5. Commit the new cursor under the still-held lease.
        6. Emit ``cursor.advanced``.

        Halt paths (steps 3 / 4 raising):

        * :class:`CursorEncodingMismatch` → emit
          ``cursor.encoding_mismatch``, flip instance status to
          :data:`HALT_STATUS_ENCODING_MISMATCH`, release lease, raise
          :class:`CursorEncodingMismatchHalt`.
        * :class:`CursorExpired` → emit ``cursor.expired``, flip
          instance status to :data:`HALT_STATUS_EXPIRED`, release
          lease, raise :class:`CursorExpiredHalt`.
        * Any other exception (including the publisher's) → release
          the lease and re-raise. The cursor stays at its previous
          value and the batch will be re-emitted on the next tick.
        """
        ws = WorkspaceId(workspace_id)
        inst_id = ConnectorInstanceId(instance_id)
        with observe_pull_tick() as _span:
            _span.set_attribute("workspaceId", workspace_id)
            _span.set_attribute("connectorInstanceId", instance_id)
            instance = await self._load_eligible_instance(ws, inst_id)
            type_version = await self._catalog.get_connector_type_version(
                instance.type, instance.version
            )
            if type_version is None:
                raise CursorInstanceUnavailable(
                    instance_id,
                    f"connector-type ({instance.type!r}, {instance.version!r}) "
                    "is not registered in the catalog",
                )
            encoding = _extract_cursor_encoding(type_version)

            row_before, lease = await self._metadata.acquire_cursor_lease(
                ws, inst_id, self._holder_id, self._lease_ttl_seconds
            )
            envelope_before = _row_to_envelope_record(row_before, encoding)

            try:
                listen_result = await self._call_plugin(
                    instance=instance,
                    type_version=type_version,
                    encoding=encoding,
                    envelope_before=envelope_before,
                    row_before=row_before,
                    lease=lease,
                    workspace_id=workspace_id,
                )

                # ---- at-least-once: publish-ack EVERYTHING before commit
                await self._publisher(ws, instance, listen_result.events)

                new_persisted = _resolve_committed_value(
                    listen_result.next_cursor, row_before.value
                )
                commit_at = self._clock()
                row_after = await self._metadata.commit_cursor(ws, lease, new_persisted, commit_at)
                envelope_after = _row_to_envelope_record(row_after, encoding)
            except (LeaseBusy, LeaseExpired):
                await self._release_lease_quietly(ws, lease)
                raise
            except CursorHalted:
                # Already handled (audit + halt + release) inside ``_call_plugin``.
                raise
            except BaseException:
                await self._release_lease_quietly(ws, lease)
                raise

            await audit_cursor_advanced(
                self._metadata,
                workspace_id=workspace_id,
                actor=self._actor,
                instance_id=instance_id,
                from_encoding=envelope_before.encoding,
                from_value_fingerprint=_fingerprint_persisted(row_before.value),
                from_value_length=_length_persisted(row_before.value),
                to_encoding=envelope_after.encoding,
                to_value_fingerprint=_fingerprint_persisted(row_after.value),
                to_value_length=_length_persisted(row_after.value),
                event_count=len(listen_result.events),
                reason="tick",
            )

            # CONN-IMPL-029: record the advancedAt timestamp under the
            # registry so the cursor-lag observable gauge reports a
            # fresh (now - advancedAt) age on every Prometheus scrape.
            CURSOR_LAG_REGISTRY.record_advanced(
                workspace_id=workspace_id,
                instance_id=instance_id,
                advanced_at=commit_at,
            )
            _span.set_attribute("eventCount", len(listen_result.events))

            return TickResult(
                instance_id=instance_id,
                event_count=len(listen_result.events),
                cursor_before=envelope_before,
                cursor_after=envelope_after,
                receiver_endpoint=listen_result.receiver_endpoint,
            )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _load_eligible_instance(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
    ) -> ConnectorInstance:
        instance = await self._instances.get_connector_instance(workspace_id, instance_id)
        if instance is None:
            raise CursorInstanceUnavailable(
                str(instance_id),
                "connector instance does not exist",
            )
        if not instance.enabled:
            raise CursorInstanceUnavailable(
                str(instance_id),
                "connector instance is disabled",
            )
        if instance.status in (HALT_STATUS_EXPIRED, HALT_STATUS_ENCODING_MISMATCH):
            raise CursorInstanceUnavailable(
                str(instance_id),
                f"connector instance is halted (status={instance.status!r})",
            )
        return instance

    async def _call_plugin(
        self,
        *,
        instance: ConnectorInstance,
        type_version: ConnectorTypeVersion,
        encoding: str,
        envelope_before: CursorEnvelopeRecord,
        row_before: ConnectorCursor,
        lease: LeaseHandle,
        workspace_id: str,
    ) -> Any:
        """Invoke ``listen(mode=pull, ...)`` and translate halt errors.

        Returns the plugin's :class:`ListenResult` on success. On the
        two halt errors emits the audit event, flips instance status,
        releases the lease, and raises the matching
        :class:`CursorHalted` subclass.
        """
        plugin_envelope = CursorEnvelope(
            encoding=envelope_before.encoding,
            value=envelope_before.value,
            advanced_at=envelope_before.advanced_at,
        )
        try:
            return await self._invoker.listen(
                connector=type_version,
                instance=instance,
                mode=ListenMode.PULL,
                cursor=plugin_envelope,
            )
        except CursorEncodingMismatch as exc:
            await audit_cursor_encoding_mismatch(
                self._metadata,
                workspace_id=workspace_id,
                actor=self._actor,
                instance_id=str(instance.instance_id),
                persisted_encoding=exc.persisted_encoding or encoding,
                plugin_encoding=exc.plugin_encoding,
                error_detail=exc.detail,
            )
            await self._flip_instance_status(
                WorkspaceId(workspace_id),
                instance.instance_id,
                HALT_STATUS_ENCODING_MISMATCH,
            )
            await self._release_lease_quietly(WorkspaceId(workspace_id), lease)
            # CONN-IMPL-029: forget the cursor-lag entry on halt so
            # the gauge stops reporting stale lag for an instance
            # the scheduler is no longer ticking.
            CURSOR_LAG_REGISTRY.forget(
                workspace_id=workspace_id,
                instance_id=str(instance.instance_id),
            )
            raise CursorEncodingMismatchHalt(
                exc.detail,
                persisted_encoding=exc.persisted_encoding or encoding,
                plugin_encoding=exc.plugin_encoding,
            ) from exc
        except CursorExpired as exc:
            await audit_cursor_expired(
                self._metadata,
                workspace_id=workspace_id,
                actor=self._actor,
                instance_id=str(instance.instance_id),
                encoding=envelope_before.encoding,
                value_fingerprint=_fingerprint_persisted(row_before.value),
                value_length=_length_persisted(row_before.value),
                error_detail=exc.detail,
            )
            await self._flip_instance_status(
                WorkspaceId(workspace_id),
                instance.instance_id,
                HALT_STATUS_EXPIRED,
            )
            await self._release_lease_quietly(WorkspaceId(workspace_id), lease)
            # CONN-IMPL-029: forget the cursor-lag entry on expired
            # halt for the same reason as the encoding-mismatch path.
            CURSOR_LAG_REGISTRY.forget(
                workspace_id=workspace_id,
                instance_id=str(instance.instance_id),
            )
            raise CursorExpiredHalt(exc.detail) from exc

    async def _flip_instance_status(
        self,
        workspace_id: WorkspaceId,
        instance_id: ConnectorInstanceId,
        status: str,
    ) -> None:
        """Patch the instance ``status`` to a halt sentinel (best-effort).

        ``patch_connector_instance`` can raise on classified DB errors
        or unknown-field ``ValueError``. We deliberately swallow those
        here and log a warning so the typed :class:`CursorHalted`
        subclass is what propagates out of :meth:`tick` rather than
        the patch error — the halt audit event has already landed,
        and the next tick will replay the halt path (plugin returns
        the same error, audit fires again, scheduler keeps the
        instance out of the rotation through whatever fallback it
        uses).
        """
        try:
            await self._instances.patch_connector_instance(
                workspace_id, instance_id, {"status": status}
            )
        except Exception:
            _LOGGER.warning(
                "failed to flip connector instance %s/%s to halt status %r; "
                "halt audit was emitted but the durable status flip did not land, "
                "next tick will retry the halt path",
                str(workspace_id),
                str(instance_id),
                status,
                exc_info=True,
            )

    async def _release_lease_quietly(
        self,
        workspace_id: WorkspaceId,
        lease: LeaseHandle,
    ) -> None:
        """Release a lease, swallowing release-time failures.

        ``release_cursor_lease`` is idempotent per the SPL contract,
        so calling it after a successful ``commit_cursor`` (which
        already cleared the lease) or after a failed acquire is a
        no-op. We additionally swallow exceptions here so the lease
        release never masks the original tick failure that triggered
        the cleanup.
        """
        with suppress(Exception):
            await self._metadata.release_cursor_lease(workspace_id, lease)


# ---------------------------------------------------------------------------
# Module-private helpers
# ---------------------------------------------------------------------------


def _extract_cursor_encoding(type_version: ConnectorTypeVersion) -> str:
    """Pull ``spec.events.pull.cursorEncoding`` out of a normalized manifest.

    Raises :class:`CursorInstanceUnavailable` (treated by the
    scheduler as "drop this instance from the rotation") if the
    manifest does not declare a pull-cursor encoding. This is a
    configuration error — the manifest validator rejects pull-mode
    manifests that omit ``events.pull.cursorEncoding`` — but we
    still defensively check here because the catalog row could
    in principle have been written by an older code path.
    """
    spec = type_version.normalized_manifest.get("spec")
    if not isinstance(spec, Mapping):
        raise CursorInstanceUnavailable(
            "<unknown>",
            f"connector-type ({type_version.type!r}, {type_version.version!r}) "
            "manifest has no 'spec' object",
        )
    events = spec.get("events")
    if not isinstance(events, Mapping):
        raise CursorInstanceUnavailable(
            "<unknown>",
            f"connector-type ({type_version.type!r}, {type_version.version!r}) "
            "manifest declares no 'events' block",
        )
    pull = events.get("pull")
    if not isinstance(pull, Mapping):
        raise CursorInstanceUnavailable(
            "<unknown>",
            f"connector-type ({type_version.type!r}, {type_version.version!r}) "
            "manifest declares no 'events.pull' block (not a pull-mode connector)",
        )
    encoding = pull.get("cursorEncoding")
    if not isinstance(encoding, str) or not encoding:
        raise CursorInstanceUnavailable(
            "<unknown>",
            f"connector-type ({type_version.type!r}, {type_version.version!r}) "
            "manifest declares no 'events.pull.cursorEncoding'",
        )
    return encoding


def _row_to_envelope_record(row: ConnectorCursor, encoding: str) -> CursorEnvelopeRecord:
    """Recombine a SPL ``ConnectorCursor`` row with the manifest encoding."""
    return CursorEnvelopeRecord(
        workspace_id=str(row.workspace_id),
        instance_id=str(row.instance_id),
        encoding=encoding,
        value=_persisted_to_envelope_value(row.value),
        advanced_at=row.advanced_at,
        lease_holder=row.lease_holder,
        lease_expires_at=row.lease_expires_at,
    )


def _resolve_committed_value(
    next_cursor: CursorEnvelope | None,
    previous_persisted: str,
) -> str:
    """Decide what string to commit for ``value``.

    * If the plugin returned a ``nextCursor`` envelope, encode its
      ``value`` deterministically.
    * If the plugin returned no ``nextCursor`` (an empty tick — the
      plugin saw no new events), keep the previous persisted value
      so ``advanced_at`` advances on commit even though ``value``
      does not. This matches design § Pull Cursor Model where
      ``advancedAt`` records the "last cursor write" — initialization,
      advance, or operator rewind — and so a tick that committed
      ``value`` unchanged still bumps ``advancedAt``.
    """
    if next_cursor is None:
        return previous_persisted
    return _envelope_value_to_persisted(next_cursor.value)


# ---------------------------------------------------------------------------
# Package-internal public re-exports (CONN-IMPL-024)
# ---------------------------------------------------------------------------

#: Public alias of :func:`_envelope_value_to_persisted` for the cursor
#: admin router (:mod:`custos_connector.cursor.router`). The underscore
#: helper remains the source of truth so the tick path stays untouched;
#: the alias gives the router a stable name to import without reaching
#: across the module-private boundary.
encode_envelope_value = _envelope_value_to_persisted

#: Public alias of :func:`_fingerprint_persisted` for the cursor admin
#: router. ``None`` for the uninitialized sentinel.
fingerprint_persisted_value = _fingerprint_persisted

#: Public alias of :func:`_length_persisted` for the cursor admin
#: router. Byte-length; ``None`` for the uninitialized sentinel.
persisted_value_byte_length = _length_persisted

#: Public alias of the persisted "never written" sentinel used by the
#: cursor admin router when an operator rewinds ``to: "beginning"``.
UNINITIALIZED_PERSISTED_CURSOR = _UNINITIALIZED_PERSISTED
