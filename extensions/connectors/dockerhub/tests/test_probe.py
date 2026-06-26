"""Unit tests for the live Docker Hub reachability probe.

Uses ``respx`` to intercept the ``GET /v2/`` request so the probe's HTTP
handling, challenge parsing and verdict logic are tested deterministically
without touching the network.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from dockerhub_plugin.probe import _parse_challenge, check_reachability

_V2_URL = "https://registry-1.docker.io/v2/"
_DOCKERHUB_CHALLENGE = 'Bearer realm="https://auth.docker.io/token",service="registry.docker.io"'


@respx.mock
def test_healthy_on_expected_bearer_challenge() -> None:
    respx.get(_V2_URL).mock(
        return_value=httpx.Response(401, headers={"WWW-Authenticate": _DOCKERHUB_CHALLENGE})
    )
    result = check_reachability("https://registry-1.docker.io")
    assert result["healthy"] is True
    assert result["tokenEndpoint"] == "https://auth.docker.io/token"
    assert result["service"] == "registry.docker.io"
    assert result["registryEndpoint"] == _V2_URL


@respx.mock
def test_trailing_slash_endpoint_is_normalized() -> None:
    respx.get(_V2_URL).mock(
        return_value=httpx.Response(401, headers={"WWW-Authenticate": _DOCKERHUB_CHALLENGE})
    )
    result = check_reachability("https://registry-1.docker.io/")
    assert result["healthy"] is True


@respx.mock
def test_unhealthy_on_unexpected_realm() -> None:
    respx.get(_V2_URL).mock(
        return_value=httpx.Response(
            401,
            headers={
                "WWW-Authenticate": 'Bearer realm="https://evil.example/token",'
                'service="registry.docker.io"'
            },
        )
    )
    result = check_reachability("https://registry-1.docker.io")
    assert result["healthy"] is False
    assert "does not match Docker Hub" in result["detail"]


@respx.mock
def test_unhealthy_when_no_challenge_header() -> None:
    respx.get(_V2_URL).mock(return_value=httpx.Response(401))
    result = check_reachability("https://registry-1.docker.io")
    assert result["healthy"] is False
    assert "no Bearer challenge" in result["detail"]


@respx.mock
def test_unhealthy_on_unexpected_status() -> None:
    respx.get(_V2_URL).mock(return_value=httpx.Response(200))
    result = check_reachability("https://registry-1.docker.io")
    assert result["healthy"] is False
    assert "unexpected status 200" in result["detail"]


@respx.mock
def test_unhealthy_on_connect_error() -> None:
    respx.get(_V2_URL).mock(side_effect=httpx.ConnectError("boom"))
    result = check_reachability("https://registry-1.docker.io")
    assert result["healthy"] is False
    assert "unreachable" in result["detail"]
    assert "ConnectError" in result["detail"]


@respx.mock
def test_injected_client_is_not_closed_by_probe() -> None:
    respx.get(_V2_URL).mock(
        return_value=httpx.Response(401, headers={"WWW-Authenticate": _DOCKERHUB_CHALLENGE})
    )
    client = httpx.Client()
    check_reachability("https://registry-1.docker.io", client=client)
    assert client.is_closed is False
    client.close()


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        (
            'Bearer realm="https://auth.docker.io/token",service="registry.docker.io"',
            {"realm": "https://auth.docker.io/token", "service": "registry.docker.io"},
        ),
        ("realm=foo, service=bar", {"realm": "foo", "service": "bar"}),
        ("", {}),
        ("Bearer", {}),
    ],
)
def test_parse_challenge(header: str, expected: dict[str, str]) -> None:
    assert _parse_challenge(header) == expected
