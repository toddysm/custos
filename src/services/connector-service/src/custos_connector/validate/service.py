"""Domain service for the ValidateConnector internal RPC (CONN-IMPL-027).

This service has no state of its own — it composes the
:class:`CatalogStoreProvider` (for manifest lookup) and the
:class:`ConnectorInstanceStoreProvider` (for instance lookup) with
:func:`validate_instance_config` so the wire layer in
:mod:`custos_connector.api.validate` stays a thin adapter. Putting
the dispatch logic here keeps the two preflight modes' branching
testable in isolation from FastAPI.

Two errors flow out of :meth:`ValidateConnectorService.validate`:

* :class:`ConnectorInstanceNotFound` — instance mode, no matching
  ``(workspace_id, instance_id)`` row. The wire layer renders 404.
* :class:`ConnectorTypeNotRegistered` — either mode, the
  ``(type, version)`` pinned on the instance / supplied by the
  caller does not exist in the catalog. The wire layer renders 404
  to keep the response shape consistent with the existing
  :mod:`custos_connector.api.instances` surface.

The validator's :class:`InstanceConfigValidationError` is *not*
caught here — it propagates so the wire layer can render it through
the same ``connector.instance_config_invalid`` envelope every other
validate-on-the-way-in route uses.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Literal

from custos_spl.ids import ConnectorInstanceId, WorkspaceId
from custos_spl.interfaces.catalog_store import CatalogStoreProvider
from custos_spl.interfaces.connector_instance_store import (
    ConnectorInstanceStoreProvider,
)

from custos_connector.instances.service import (
    ConnectorInstanceNotFound,
    ConnectorTypeNotRegistered,
)
from custos_connector.instances.validator import validate_instance_config


@dataclass(frozen=True, slots=True)
class ValidateInstanceRequest:
    """Instance-mode preflight: re-validate an existing instance.

    Used by Workflow Service ahead of ``BindForStep`` so a manifest
    that was edited (catalog re-put with a different ``digest``) or
    deprecated since the instance was activated surfaces as a
    pre-bind 400 instead of a runtime bind failure.

    Attributes:
        instance_id: The connector-instance ID inside the
            call-context's workspace. The service rejects with
            :class:`ConnectorInstanceNotFound` when the row is
            absent.
        required_capabilities: Capability tokens the caller intends
            to use at bind time. ``None`` skips the capability
            availability check and only re-runs the config / auth
            validation against the (current) manifest.
    """

    mode: Literal["instance"]
    instance_id: str
    required_capabilities: tuple[str, ...] | None = None


@dataclass(frozen=True, slots=True)
class ValidateManifestRequest:
    """Manifest-mode preflight: "test before save" validation.

    Used by Catalog Service on the operator-facing UI so the
    operator gets the full per-issue diff before persisting the
    instance row. No persistence write happens here; the call only
    re-runs :func:`validate_instance_config` against a transient
    payload.

    Attributes:
        type: ``ConnectorType`` name registered in the catalog.
            Rejected with :class:`ConnectorTypeNotRegistered` when
            absent.
        version: ``ConnectorTypeVersion`` version string.
        target_config: Operator-supplied target config overrides
            (the same bag instance ``POST /v1/.../instances`` would
            persist). Merged on top of ``spec.target.config`` in
            the validator.
        credentials_authentication: Operator-supplied auth field
            overrides. Merged on top of
            ``spec.credentials.authentication`` in the validator.
        used_capabilities: Operator-pinned capability subset.
            ``None`` skips the availability check.
    """

    mode: Literal["manifest"]
    type: str
    version: str
    target_config: Mapping[str, Any]
    credentials_authentication: Mapping[str, Any]
    used_capabilities: tuple[str, ...] | None = None


#: Tagged-union of the two preflight modes. The dispatch
#: in :meth:`ValidateConnectorService.validate` keys off the
#: ``mode`` literal so callers never see a runtime ``isinstance``
#: chain in the service layer.
ValidateRequest = ValidateInstanceRequest | ValidateManifestRequest


@dataclass(frozen=True, slots=True)
class ValidateResult:
    """Successful validate response.

    The service only constructs this on the success path; failure
    surfaces as a typed exception so the wire layer can render the
    canonical error envelope. Carrying the resolved
    ``(type, version)`` lets callers log the manifest version they
    actually validated against without a second catalog round-trip.
    """

    type: str
    version: str


class ValidateConnectorService:
    """Run the manifest validator on behalf of internal callers.

    Stateless; safe to share one instance across the whole FastAPI
    app. The constructor takes the same provider seams as the
    upstream :class:`InstanceService` so swapping in test fakes
    follows the same pattern.
    """

    __slots__ = ("_catalog_store", "_instance_store")

    def __init__(
        self,
        *,
        catalog_store: CatalogStoreProvider,
        instance_store: ConnectorInstanceStoreProvider,
    ) -> None:
        self._catalog_store = catalog_store
        self._instance_store = instance_store

    async def validate(
        self,
        *,
        workspace_id: str,
        request: ValidateRequest,
    ) -> ValidateResult:
        """Dispatch on ``request.mode`` and run the validator.

        Re-raises :class:`InstanceConfigValidationError` on
        validation failure. Raises
        :class:`ConnectorInstanceNotFound` (instance mode) or
        :class:`ConnectorTypeNotRegistered` (either mode) on
        lookup failure.
        """
        if request.mode == "instance":
            return await self._validate_instance(workspace_id=workspace_id, request=request)
        return await self._validate_manifest(request=request)

    async def _validate_instance(
        self,
        *,
        workspace_id: str,
        request: ValidateInstanceRequest,
    ) -> ValidateResult:
        instance = await self._instance_store.get_connector_instance(
            WorkspaceId(workspace_id),
            ConnectorInstanceId(request.instance_id),
        )
        if instance is None:
            raise ConnectorInstanceNotFound(
                workspace_id=workspace_id, instance_id=request.instance_id
            )
        catalog_row = await self._catalog_store.get_connector_type_version(
            instance.type, instance.version
        )
        if catalog_row is None:
            raise ConnectorTypeNotRegistered(type=instance.type, version=instance.version)
        # Re-validate the instance against the *current* manifest.
        # The persisted target_config / credentials_authentication
        # are the authoritative operator-supplied bags (the catalog
        # manifest contributes the required-field shape; the
        # instance carries the values).
        validate_instance_config(
            manifest=catalog_row.normalized_manifest,
            target_config=instance.target_config,
            credentials_authentication=instance.credentials_authentication,
            used_capabilities=_pick_capabilities(
                instance_capabilities=instance.used_capabilities,
                requested_capabilities=request.required_capabilities,
            ),
        )
        return ValidateResult(type=instance.type, version=instance.version)

    async def _validate_manifest(
        self,
        *,
        request: ValidateManifestRequest,
    ) -> ValidateResult:
        catalog_row = await self._catalog_store.get_connector_type_version(
            request.type, request.version
        )
        if catalog_row is None:
            raise ConnectorTypeNotRegistered(type=request.type, version=request.version)
        validate_instance_config(
            manifest=catalog_row.normalized_manifest,
            target_config=request.target_config,
            credentials_authentication=request.credentials_authentication,
            used_capabilities=request.used_capabilities,
        )
        return ValidateResult(type=request.type, version=request.version)


def _pick_capabilities(
    *,
    instance_capabilities: Sequence[str] | None,
    requested_capabilities: tuple[str, ...] | None,
) -> tuple[str, ...] | None:
    """Pick the capability set to validate for an instance-mode call.

    Caller-supplied ``required_capabilities`` win when present
    (Workflow Service knows which capabilities the step will
    actually exercise). When absent, re-validate the instance's
    own pinned subset so a manifest that dropped a capability the
    instance still claims surfaces.
    ``None`` on both sides skips the capability check entirely.
    """
    if requested_capabilities is not None:
        return requested_capabilities
    if instance_capabilities is None:
        return None
    return tuple(instance_capabilities)


__all__ = [
    "ValidateConnectorService",
    "ValidateInstanceRequest",
    "ValidateManifestRequest",
    "ValidateRequest",
    "ValidateResult",
]
