"""Live, unauthenticated reachability probe for Docker Hub.

The ``health`` hook cannot authenticate. The plugin only ever receives a
*reference* to the Docker Hub Personal Access Token (the
``x-dapr-secret`` ``authentication`` block), never the resolved secret,
and ``bind`` hands back a binding handle the sidecar maps onto the leased
credential at the data plane. So the probe verifies what it *can* without
credentials: that the registry is reachable, speaks the OCI distribution
protocol, and advertises the expected Docker Hub token endpoint.

Concretely it performs ``GET <endpoint>/v2/`` and asserts the response is
an HTTP ``401`` carrying a ``WWW-Authenticate: Bearer`` challenge whose
``realm`` points at ``https://auth.docker.io/token`` and whose ``service``
is ``registry.docker.io``. Anonymous Docker Hub returns exactly that, so a
matching challenge is a strong liveness + protocol signal. The actual
Layer-2 token exchange (PAT -> per-repository bearer) is the consuming
activity's job, not the plugin's — see
``design/architecture/registry-credential-refresh.md``.
"""

from __future__ import annotations

from typing import Any

import httpx

_DEFAULT_TIMEOUT: float = 5.0
_EXPECTED_REALM: str = "https://auth.docker.io/token"
_EXPECTED_SERVICE: str = "registry.docker.io"


def _parse_challenge(header: str) -> dict[str, str]:
    """Parse the ``key="value"`` params of a ``WWW-Authenticate`` header.

    Tolerates an optional leading ``Bearer`` scheme token, quoted or
    unquoted values, and surrounding whitespace. Returns a lowercase-keyed
    mapping; malformed segments are skipped rather than raising.
    """
    header = header.strip()
    if not header:
        return {}
    scheme, sep, rest = header.partition(" ")
    if sep and scheme.lower() == "bearer":
        header = rest
    params: dict[str, str] = {}
    for segment in header.split(","):
        segment = segment.strip()
        if "=" not in segment:
            continue
        key, _, value = segment.partition("=")
        params[key.strip().lower()] = value.strip().strip('"')
    return params


def check_reachability(
    endpoint: str,
    *,
    verify_tls: bool = True,
    timeout: float = _DEFAULT_TIMEOUT,
    client: httpx.Client | None = None,
) -> dict[str, Any]:
    """Probe ``GET <endpoint>/v2/`` for the Docker Hub Bearer challenge.

    Returns a result dict with ``healthy`` (bool), ``detail`` (str) and
    the discovered ``registryEndpoint`` / ``tokenEndpoint`` / ``service``
    so the ``health`` hook can surface a useful diagnostic. Network
    failures and unexpected responses resolve to ``healthy=False`` rather
    than raising, because a health probe must always return a verdict.
    """
    base = endpoint.rstrip("/")
    url = f"{base}/v2/"
    owns_client = client is None
    if client is None:
        client = httpx.Client(verify=verify_tls, timeout=timeout, follow_redirects=False)
    try:
        try:
            response = client.get(url)
        except httpx.HTTPError as exc:
            return {
                "healthy": False,
                "detail": f"registry unreachable: {type(exc).__name__}",
                "registryEndpoint": url,
            }
        if response.status_code != httpx.codes.UNAUTHORIZED:
            return {
                "healthy": False,
                "detail": (
                    f"unexpected status {response.status_code} from GET /v2/ "
                    "(expected 401 Bearer challenge)"
                ),
                "registryEndpoint": url,
            }
        params = _parse_challenge(response.headers.get("WWW-Authenticate", ""))
        realm = params.get("realm", "")
        service = params.get("service", "")
        if not realm:
            return {
                "healthy": False,
                "detail": "401 from GET /v2/ but no Bearer challenge advertised",
                "registryEndpoint": url,
            }
        realm_ok = realm.rstrip("/") == _EXPECTED_REALM.rstrip("/")
        service_ok = service == _EXPECTED_SERVICE
        healthy = realm_ok and service_ok
        if healthy:
            detail = "registry reachable; Bearer challenge advertises the Docker Hub token endpoint"
        else:
            detail = (
                "registry reachable but challenge does not match Docker Hub "
                f"(realm={realm!r}, service={service!r})"
            )
        return {
            "healthy": healthy,
            "detail": detail,
            "registryEndpoint": url,
            "tokenEndpoint": realm,
            "service": service,
        }
    finally:
        if owns_client:
            client.close()
