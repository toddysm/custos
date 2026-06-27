"""Unit tests for the live GHCR reachability probe.

Uses ``respx`` to intercept the ``GET /v2/`` request so the probe's HTTP
handling, challenge parsing and verdict logic are tested deterministically
without touching the network.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from ghcr_plugin.probe import _parse_challenge, check_reachability

_V2_URL = "https://ghcr.io/v2/"
_GHCR_CHALLENGE = 'Bearer realm="https://ghcr.io/token",service="ghcr.io"'


@respx.mock
def test_healthy_on_expected_bearer_challenge() -> None:
    respx.get(_V2_URL).mock(
        return_value=httpx.Response(401, headers={"WWW-Authenticate": _GHCR_CHALLENGE})
    )
    result = check_reachability("https://ghcr.io")
    assert result["healthy"] is True
    assert result["tokenEndpoint"] == "https://ghcr.io/token"
    assert result["service"] == "ghcr.io"
    assert result["registryEndpoint"] == _V2_URL


@respx.mock
def test_trailing_slash_endpoint_is_normalized() -> None:
    respx.get(_V2_URL).mock(
        return_value=httpx.Response(401, headers={"WWW-Authenticate": _GHCR_CHALLENGE})
    )
    result = check_reachability("https://ghcr.io/")
    assert result["healthy"] is True


@respx.mock
def test_unhealthy_on_unexpected_realm() -> None:
    respx.get(_V2_URL).mock(
        return_value=httpx.Response(
            401,
            headers={
                "WWW-Authenticate": 'Bearer realm="https://evil.example/token",service="ghcr.io"'
            },
        )
    )
    result = check_reachability("https://ghcr.io")
    assert result["healthy"] is False
    assert "does not match GHCR" in result["detail"]


@respx.mock
def test_unhealthy_when_no_challenge_header() -> None:
    respx.get(_V2_URL).mock(return_value=httpx.Response(401))
    result = check_reachability("https://ghcr.io")
    assert result["healthy"] is False
    assert "no Bearer challenge" in result["detail"]


@respx.mock
def test_unhealthy_on_unexpected_status() -> None:
    respx.get(_V2_URL).mock(return_value=httpx.Response(200))
    result = check_reachability("https://ghcr.io")
    assert result["healthy"] is False
    assert "unexpected status 200" in result["detail"]


@respx.mock
def test_unhealthy_on_connect_error() -> None:
    respx.get(_V2_URL).mock(side_effect=httpx.ConnectError("boom"))
    result = check_reachability("https://ghcr.io")
    assert result["healthy"] is False
    assert "unreachable" in result["detail"]
    assert "ConnectError" in result["detail"]


@respx.mock
def test_injected_client_is_not_closed_by_probe() -> None:
    respx.get(_V2_URL).mock(
        return_value=httpx.Response(401, headers={"WWW-Authenticate": _GHCR_CHALLENGE})
    )
    client = httpx.Client()
    check_reachability("https://ghcr.io", client=client)
    assert client.is_closed is False
    client.close()


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        (
            'Bearer realm="https://ghcr.io/token",service="ghcr.io"',
            {"realm": "https://ghcr.io/token", "service": "ghcr.io"},
        ),
        ("realm=foo, service=bar", {"realm": "foo", "service": "bar"}),
        ("", {}),
        ("Bearer", {}),
    ],
)
def test_parse_challenge(header: str, expected: dict[str, str]) -> None:
    assert _parse_challenge(header) == expected
