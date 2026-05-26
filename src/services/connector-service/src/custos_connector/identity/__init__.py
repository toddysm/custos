"""Identity resolvers (CONN-IMPL-015, Phase F).

Per design § Identity and Credential Model, Connector Service derives an
:class:`~custos_connector.loader.IdentityCategory` from the manifest's
``credentials.authenticationType`` at registration time
(CONN-IMPL-014). The matching *resolution* — turning the per-instance
``credentials.authentication`` payload plus the workload identity into
the opaque credential material the plugin needs at bind time — lives
here.

The public surface is:

* :class:`IdentityResolver` — Protocol implemented by each per-auth-type
  resolver.
* :class:`ResolvedIdentity` — the opaque-to-Connector-Service result
  that flows into :meth:`PluginInvoker.bind` at the CONN-IMPL-016 call
  site (Phase G).
* :class:`IdentityResolverRegistry` — composes the built-in resolvers
  with an optional set of vendor (``x-<vendor>``) overrides, owns the
  per-instance TTL cache, and emits the rate-limited
  ``connector.identity.resolved`` / ``connector.identity.failed`` audit
  events.

The four built-in resolvers shipped here are:

* :class:`AzureKeyVaultResolver` — KMS-backed via the Key Vault REST API.
* :class:`AmazonKmsResolver` — KMS-backed via AWS Secrets Manager.
* :class:`AzureManagedIdentityResolver` — workload identity via Dapr
  Secrets API (or any compatible secret-store seam).
* :class:`OidcFederatedResolver` — federated via OIDC token exchange
  with a JWKS-validated issuer.

All four are wired through a single
:class:`~custos_connector.identity.transport.AsyncHttpClient` Protocol
seam so unit tests use canned responses and production deployments use
the bundled httpx adapter; no cloud-vendor SDKs are pulled in.
"""

from __future__ import annotations

from custos_connector.identity.errors import (
    IdentityResolverError,
    IdentityResolverErrorCode,
)
from custos_connector.identity.models import ResolvedIdentity
from custos_connector.identity.protocols import IdentityResolver
from custos_connector.identity.registry import IdentityResolverRegistry
from custos_connector.identity.resolvers import (
    AmazonKmsResolver,
    AzureKeyVaultResolver,
    AzureManagedIdentityResolver,
    OidcFederatedResolver,
)
from custos_connector.identity.transport import (
    AsyncHttpClient,
    HttpRequest,
    HttpResponse,
    HttpxAsyncHttpClient,
)

__all__ = [
    "AmazonKmsResolver",
    "AsyncHttpClient",
    "AzureKeyVaultResolver",
    "AzureManagedIdentityResolver",
    "HttpRequest",
    "HttpResponse",
    "HttpxAsyncHttpClient",
    "IdentityResolver",
    "IdentityResolverError",
    "IdentityResolverErrorCode",
    "IdentityResolverRegistry",
    "OidcFederatedResolver",
    "ResolvedIdentity",
]
