"""OIDC verifier subpackage (Phase H, AS-IMPL-020..023).

Auth Service consumes external OIDC providers to authenticate human
users and workload identities (GitHub Actions, Azure Entra ID). The
subpackage is split into:

* :mod:`custos_auth.oidc.config` — issuer-config schema dataclasses
  and the ``CUSTOS_AUTH_OIDC_ISSUERS`` JSON parser.
* :mod:`custos_auth.oidc.jwks_cache` — async JWKS HTTP fetcher with
  ``Cache-Control`` honour and on-miss refresh.
* :mod:`custos_auth.oidc.verifier` — RFC-compliant ID-token verifier
  (issuer/audience/exp/nbf/algorithm enforcement) and the
  ``authn.success`` / ``authn.failure`` audit emission.
* :mod:`custos_auth.oidc.presets` — GitHub (AS-IMPL-021) and
  Azure Entra ID (AS-IMPL-022) preset adapters.
* :mod:`custos_auth.oidc.provisioning` — zero-binding User
  provisioning (AS-IMPL-023).

Phase H is the M3 milestone deliverable; the M1 reference deployment
keeps ``CUSTOS_AUTH_OIDC_ENABLED=false`` so the verifier code paths
are present but the public callback returns ``503 oidc_not_enabled``.
"""

from custos_auth.oidc.config import (
    DEFAULT_ALGORITHMS,
    KNOWN_PRESETS,
    GroupBinding,
    IssuersConfig,
    OidcConfigError,
    OidcIssuerConfig,
    parse_issuers_config,
)
from custos_auth.oidc.jwks_cache import JwksCache, JwksCacheError
from custos_auth.oidc.provisioning import OidcProvisioner
from custos_auth.oidc.verifier import (
    OidcVerificationError,
    OidcVerifier,
    VerifiedOidcIdentity,
)

__all__ = [
    "DEFAULT_ALGORITHMS",
    "KNOWN_PRESETS",
    "GroupBinding",
    "IssuersConfig",
    "JwksCache",
    "JwksCacheError",
    "OidcConfigError",
    "OidcIssuerConfig",
    "OidcProvisioner",
    "OidcVerificationError",
    "OidcVerifier",
    "VerifiedOidcIdentity",
    "parse_issuers_config",
]
