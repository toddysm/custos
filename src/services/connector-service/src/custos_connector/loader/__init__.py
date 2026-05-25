"""Plugin Loader (CONN-IMPL-008) — connector-type registration glue.

This package wires the manifest pipeline (validator → normalizer →
digest, all from :mod:`custos_connector.manifest`) together with the
:class:`~custos_spl.CatalogStoreProvider` write path, derives the
identity category implied by ``credentials.authenticationType`` (see
design § Identity and Credential Model), and emits the
``connector.registration.*`` audit envelopes.

The intended consumer is the public REST surface (CONN-IMPL-026): a
single ``POST /connector-types`` endpoint that hands an OCI image
reference to :meth:`Loader.register` and surfaces the resulting
:class:`LoadedConnectorType` to the caller. Internal RPCs that need to
look up an already-registered version use :meth:`Loader.get` /
:meth:`Loader.list_versions`.
"""

from __future__ import annotations

from custos_connector.loader.errors import LoaderError, LoaderErrorCode
from custos_connector.loader.identity import (
    BUILTIN_IDENTITY_CATEGORIES,
    IdentityCategory,
    derive_identity_category,
)
from custos_connector.loader.registry import (
    AUDIT_EVENT_DEPRECATION_TOGGLED,
    AUDIT_EVENT_REGISTRATION_ACCEPTED,
    AUDIT_EVENT_REGISTRATION_REJECTED,
    LoadedConnectorType,
    Loader,
)

__all__ = [
    "AUDIT_EVENT_DEPRECATION_TOGGLED",
    "AUDIT_EVENT_REGISTRATION_ACCEPTED",
    "AUDIT_EVENT_REGISTRATION_REJECTED",
    "BUILTIN_IDENTITY_CATEGORIES",
    "IdentityCategory",
    "LoadedConnectorType",
    "Loader",
    "LoaderError",
    "LoaderErrorCode",
    "derive_identity_category",
]
