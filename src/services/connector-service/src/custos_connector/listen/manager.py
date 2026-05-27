"""Listen Manager (CONN-IMPL-025, issue #308).

Orchestrates the lifecycle of every connector instance's event-listen
path:

* **Pull mode** — registers the instance with
  :class:`custos_connector.scheduler.PullLoopScheduler` so the
  pull-tick driver picks it up. The scheduler already calls
  :class:`CursorService.tick` per registered instance; the
  ``EventPublisher`` callable plumbed into :class:`CursorService` is
  :class:`custos_connector.listen.publisher.PublisherBridge`, so the
  pull-tick path implicitly flows through normalize → audit → publish
  without the scheduler knowing about the listen module.

* **Push mode** — records an in-memory registration entry
  (:class:`PushRegistration`) so the push receiver
  (:func:`custos_connector.listen.router.post_events`) can:
  (a) confirm an inbound webhook is targeting an active instance,
  (b) look up the per-instance :class:`SignatureVerifier`,
  (c) refuse the request when the instance is paused / deactivated.

* **Sink connectors** (no ``events`` block on the manifest) — return
  :class:`ActivationResult` with ``skipped=True, reason="sink-connector"``.
  Sinks consume bind-time work but never emit events; activation is a
  no-op so the lifecycle stays uniform across both directions.

The Manager reads the connector type's manifest from the
:class:`CatalogStoreProvider` and inspects ``spec.events.delivery`` to
decide which modes to wire. ``events.produced`` is forwarded into the
per-instance :class:`EventNormalizer` so unknown event-type emissions
get caught at the normalize step.

Activation is idempotent: calling :meth:`activate` twice for the same
``(workspace_id, instance_id)`` returns the cached
:class:`ActivationResult` without re-touching the scheduler or push
registry. Deactivation is similarly idempotent — unknown instances
return cleanly without raising.

State storage
-------------

Push registry + activation cache live in process memory. M1 ships
single-replica so this is sufficient; a multi-replica M2+
deployment will need to either persist the registry through SPL or
have every replica re-activate from instance-store rows on startup.
That re-hydration is tracked separately (TODO(#316)).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, Final

from custos_connector.listen.normalizer import (
    DELIVERY_MODE_PULL,
    DELIVERY_MODE_PUSH,
    EventNormalizer,
)
from custos_connector.listen.signature import (
    RejectAllSignatureVerifier,
    SignatureVerifier,
)

if TYPE_CHECKING:
    from custos_spl.interfaces.catalog_store import CatalogStoreProvider
    from custos_spl.interfaces.connector_instance_store import (
        ConnectorInstance,
    )

    from custos_connector.scheduler import PullLoopScheduler

_LOGGER: Final[logging.Logger] = logging.getLogger("custos_connector.listen.manager")

__all__ = [
    "REASON_SINK_CONNECTOR",
    "ActivationResult",
    "ListenManager",
    "ListenManagerError",
    "PushRegistration",
    "UnknownConnectorTypeError",
]

#: Reason code returned on :attr:`ActivationResult.reason` when the
#: connector type has no ``spec.events`` block (i.e. it is a sink).
#: Operators and tests assert on this string verbatim.
REASON_SINK_CONNECTOR: Final[str] = "sink-connector"


class ListenManagerError(Exception):
    """Base class for listen-manager activation failures."""


class UnknownConnectorTypeError(ListenManagerError):
    """The catalog has no row for ``(type, version)`` referenced by the instance.

    Indicates an instance row referencing a type version that was
    later purged or never existed. The caller (typically the
    instance-create / instance-enable handler) surfaces this as a
    400-class error; the manager itself does not 404-ify it because
    the listen path can be activated from contexts where the right
    error code differs (admin replay, startup re-activation, etc.).
    """


@dataclass(frozen=True, slots=True)
class ActivationResult:
    """Outcome of a single :meth:`ListenManager.activate` call.

    Returned even when activation was a no-op (sink connectors) so
    callers have a uniform return shape and can audit / log the
    decision.
    """

    #: ``True`` when the activation did not register any delivery
    #: mode (sink connector). ``False`` for both pull and push
    #: registrations.
    skipped: bool
    #: Free-form reason carried alongside ``skipped=True``. The only
    #: reason emitted today is :data:`REASON_SINK_CONNECTOR`.
    reason: str | None
    #: Tuple of delivery modes registered for this instance, in
    #: manifest-declared order. Empty when ``skipped=True``.
    modes: tuple[str, ...]


@dataclass(slots=True)
class PushRegistration:
    """In-memory registration entry for a push-mode connector instance.

    Held in :attr:`ListenManager._push_registry` keyed by
    ``(workspace_id, instance_id)``. The router consults this entry
    to find the right :class:`SignatureVerifier` and the
    :class:`EventNormalizer` instance whose ``produced_event_types``
    catalog matches the version of the connector type the instance
    was bound to (so a type-version upgrade does not silently
    re-validate against the new catalog).
    """

    workspace_id: str
    instance_id: str
    normalizer: EventNormalizer
    signature_verifier: SignatureVerifier
    produced_event_types: tuple[str, ...]
    #: ``False`` after :meth:`ListenManager.deactivate` — the router
    #: refuses requests for the instance while still holding the
    #: entry so audit can attribute the rejection to a known instance.
    active: bool = True


class ListenManager:
    """Lifecycle manager for connector-instance listen paths.

    Responsibilities:

    1. On :meth:`activate`: read the connector type's manifest,
       branch on ``spec.events.delivery`` to register pull / push,
       and stash the :class:`EventNormalizer` (with the right
       ``events.produced`` catalog) so both paths share validation.
    2. On :meth:`deactivate`: tear down both registrations.
    3. Expose :meth:`get_push_registration` so the push router can
       resolve the per-instance verifier + normalizer on every
       inbound POST.

    A single :class:`SignatureVerifier` (the default one supplied to
    the constructor) is used for every push registration today.
    Per-instance verifier injection lands in the follow-up that
    plumbs HMAC secrets out of instance config.
    """

    __slots__ = (
        "_activations",
        "_catalog_store",
        "_default_signature_verifier",
        "_lock",
        "_normalizers",
        "_push_registry",
        "_scheduler",
    )

    def __init__(
        self,
        *,
        catalog_store: CatalogStoreProvider,
        scheduler: PullLoopScheduler,
        default_signature_verifier: SignatureVerifier | None = None,
    ) -> None:
        self._catalog_store = catalog_store
        self._scheduler = scheduler
        self._default_signature_verifier: SignatureVerifier = (
            default_signature_verifier
            if default_signature_verifier is not None
            else RejectAllSignatureVerifier()
        )
        self._push_registry: dict[tuple[str, str], PushRegistration] = {}
        self._activations: dict[tuple[str, str], ActivationResult] = {}
        # Per-(type, version) normalizer cache so the pull-tick path
        # can resolve the right ``produced`` catalog for every
        # connector instance regardless of which connector type it
        # was bound to. Keyed by the manifest coordinates because the
        # normalizer is fully determined by the manifest's
        # ``spec.events.produced`` tuple — two instances of the same
        # connector type version share one normalizer.
        self._normalizers: dict[tuple[str, str], EventNormalizer] = {}
        # Activations mutate three structures (push registry,
        # scheduler, activation cache); serialize concurrent
        # activate / deactivate calls per Manager instance so
        # concurrent webhook bind-then-activate races cannot leave
        # half-registered state behind.
        self._lock = asyncio.Lock()

    async def activate(self, instance: ConnectorInstance) -> ActivationResult:
        """Wire up the listen path for ``instance``.

        Idempotent: re-activating an instance returns the cached
        :class:`ActivationResult` without re-touching the scheduler.

        Steps:

        1. Look up the connector type version in the catalog.
        2. Read ``spec.events``: if absent → sink connector → return
           ``ActivationResult(skipped=True, reason="sink-connector")``.
        3. For every mode in ``spec.events.delivery``:

           - ``pull`` → :meth:`PullLoopScheduler.register`
           - ``push`` → record a :class:`PushRegistration` keyed by
             ``(workspace_id, instance_id)``

        Both paths get the same :class:`EventNormalizer`
        (constructed from ``spec.events.produced``) so a single
        instance bound for both delivery modes still validates
        through one catalog.

        Returns the :class:`ActivationResult`. Raises
        :class:`UnknownConnectorTypeError` if the catalog has no row
        for ``(instance.type, instance.version)``.
        """
        workspace_id = str(instance.workspace_id)
        instance_id = str(instance.instance_id)
        key = (workspace_id, instance_id)

        async with self._lock:
            cached = self._activations.get(key)
            if cached is not None:
                return cached

            type_version = await self._catalog_store.get_connector_type_version(
                instance.type, instance.version
            )
            if type_version is None:
                raise UnknownConnectorTypeError(
                    f"connector type version not found: {instance.type}@{instance.version}"
                )

            events_block = _extract_events_block(type_version.normalized_manifest)
            if events_block is None:
                result = ActivationResult(skipped=True, reason=REASON_SINK_CONNECTOR, modes=())
                self._activations[key] = result
                _LOGGER.info(
                    "listen activate: sink connector (no events block) ws=%s inst=%s",
                    workspace_id,
                    instance_id,
                )
                return result

            delivery_modes = _read_delivery_modes(events_block)
            produced = _read_produced_event_types(events_block)
            normalizer = self._normalizers.setdefault(
                (instance.type, instance.version),
                EventNormalizer(produced_event_types=produced),
            )

            registered_modes: list[str] = []
            if DELIVERY_MODE_PULL in delivery_modes:
                self._scheduler.register(workspace_id, instance_id)
                registered_modes.append(DELIVERY_MODE_PULL)
            if DELIVERY_MODE_PUSH in delivery_modes:
                self._push_registry[key] = PushRegistration(
                    workspace_id=workspace_id,
                    instance_id=instance_id,
                    normalizer=normalizer,
                    signature_verifier=self._default_signature_verifier,
                    produced_event_types=produced,
                    active=True,
                )
                registered_modes.append(DELIVERY_MODE_PUSH)

            result = ActivationResult(
                skipped=False,
                reason=None,
                modes=tuple(registered_modes),
            )
            self._activations[key] = result
            _LOGGER.info(
                "listen activate ws=%s inst=%s modes=%s",
                workspace_id,
                instance_id,
                registered_modes,
            )
            return result

    async def deactivate(self, workspace_id: str, instance_id: str) -> bool:
        """Tear down the listen path for an instance.

        Returns ``True`` when an activation was previously registered
        and was unwound; ``False`` when the instance was unknown
        (idempotent — callers do not need to track activation state).

        Tears down both the scheduler registration (if any) and the
        push registry entry (if any) regardless of which modes the
        instance was registered under, so a manifest change that
        flips a delivery mode does not leak the old registration.
        """
        key = (workspace_id, instance_id)
        async with self._lock:
            previous = self._activations.pop(key, None)
            push_entry = self._push_registry.pop(key, None)
            scheduler_dropped = self._scheduler.unregister(workspace_id, instance_id)
            if push_entry is not None:
                push_entry.active = False
            unwound = previous is not None or push_entry is not None or scheduler_dropped
            if unwound:
                _LOGGER.info(
                    "listen deactivate ws=%s inst=%s scheduler_dropped=%s push_dropped=%s",
                    workspace_id,
                    instance_id,
                    scheduler_dropped,
                    push_entry is not None,
                )
            return unwound

    def get_push_registration(self, workspace_id: str, instance_id: str) -> PushRegistration | None:
        """Return the live push registration, or ``None`` if not registered.

        Called by the push router on every inbound POST. Returns
        ``None`` for unknown instances and for instances that were
        deactivated (the entry is removed from the registry on
        deactivate, so a stale lookup is indistinguishable from an
        unknown instance — both surface as 404 on the wire).
        """
        return self._push_registry.get((workspace_id, instance_id))

    async def get_normalizer_for_instance(self, instance: ConnectorInstance) -> EventNormalizer:
        """Resolve the per-(type, version) :class:`EventNormalizer`.

        The pull-tick path needs to validate every connector
        instance's events against the ``produced`` catalog declared
        on the *instance's* connector type — a single global
        normalizer would either over-accept (no validation) or
        spuriously reject (when its catalog does not match the
        instance's type version). This method is the seam the
        pull-side bridge calls on every tick to fetch the right
        normalizer for the instance at hand.

        Cached by ``(type, version)`` because the normalizer is
        fully determined by the manifest's ``spec.events.produced``
        tuple; the cache hit on every subsequent tick keeps the
        catalog read off the hot path. The cache is populated
        either by :meth:`activate` (first activation of an instance
        of the same type version) or lazily here on first lookup
        when the bridge is called for an instance that the manager
        has not yet activated through its own path (e.g. startup
        re-hydration).

        Raises :class:`UnknownConnectorTypeError` when the catalog
        has no row for ``(type, version)`` — same contract as
        :meth:`activate`.
        """
        key = (instance.type, instance.version)
        cached = self._normalizers.get(key)
        if cached is not None:
            return cached
        type_version = await self._catalog_store.get_connector_type_version(
            instance.type, instance.version
        )
        if type_version is None:
            raise UnknownConnectorTypeError(
                f"connector type version not found: {instance.type}@{instance.version}"
            )
        events_block = _extract_events_block(type_version.normalized_manifest)
        produced = _read_produced_event_types(events_block) if events_block else ()
        normalizer = EventNormalizer(produced_event_types=produced)
        self._normalizers[key] = normalizer
        return normalizer

    def activations(self) -> Mapping[tuple[str, str], ActivationResult]:
        """Snapshot of the activation cache. Intended for tests / metrics."""
        return dict(self._activations)


# ---------------------------------------------------------------------------
# Manifest readers
# ---------------------------------------------------------------------------


def _extract_events_block(manifest: Mapping[str, Any]) -> Mapping[str, Any] | None:
    """Return ``manifest["spec"]["events"]`` if it exists, else ``None``.

    Tolerates either the top-level ``spec`` envelope (the published
    normalized manifest shape) or a flat manifest with ``events`` at
    the top (a few legacy test fixtures use the flat form). The two
    forms are checked in that order so a normalized manifest with an
    accidentally-top-level ``events`` field still routes through the
    ``spec`` path first.
    """
    spec = manifest.get("spec")
    if isinstance(spec, Mapping):
        events = spec.get("events")
        if isinstance(events, Mapping):
            return events
    events = manifest.get("events")
    if isinstance(events, Mapping):
        return events
    return None


def _read_delivery_modes(events_block: Mapping[str, Any]) -> tuple[str, ...]:
    """Return the validated tuple of delivery modes.

    Manifest validation already enforces ``delivery ∈ {push, pull}``
    and non-empty, so this is mostly a typed read. Anything weirder
    than a list-of-strings produces an empty tuple → the activation
    silently degenerates to "sink-like" behavior. That is intentional:
    the manifest validator is the right error site, not the listen
    manager. The audit log already records the actual modes registered.
    """
    delivery = events_block.get("delivery")
    if not isinstance(delivery, list):
        return ()
    modes: list[str] = []
    for value in delivery:
        if isinstance(value, str) and value:
            modes.append(value)
    return tuple(modes)


def _read_produced_event_types(events_block: Mapping[str, Any]) -> tuple[str, ...]:
    """Return the validated tuple of event-type catalog tokens."""
    produced = events_block.get("produced")
    if not isinstance(produced, list):
        return ()
    types: list[str] = []
    for value in produced:
        if isinstance(value, str) and value:
            types.append(value)
    return tuple(types)
