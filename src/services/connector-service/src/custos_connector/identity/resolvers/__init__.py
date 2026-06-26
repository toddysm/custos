"""Built-in identity resolvers for the four manifest tokens v1 ships.

Each module is intentionally self-contained: it knows the upstream
provider's URL shape, the expected response payload, and the redacted
descriptor to use as a cache key. None of them import an
upstream-vendor SDK — they all speak HTTP via the
:class:`~custos_connector.identity.transport.AsyncHttpClient` Protocol
seam so unit tests can swap a stub implementation in and production
wires in :class:`HttpxAsyncHttpClient` against a configured
``httpx.AsyncClient``.
"""

from __future__ import annotations

from custos_connector.identity.resolvers.amazon_kms import AmazonKmsResolver
from custos_connector.identity.resolvers.azure_key_vault import (
    AzureKeyVaultResolver,
)
from custos_connector.identity.resolvers.azure_managed_identity import (
    AzureManagedIdentityResolver,
)
from custos_connector.identity.resolvers.dapr_secret import DaprSecretResolver
from custos_connector.identity.resolvers.oidc import OidcFederatedResolver

__all__ = [
    "AmazonKmsResolver",
    "AzureKeyVaultResolver",
    "AzureManagedIdentityResolver",
    "DaprSecretResolver",
    "OidcFederatedResolver",
]
