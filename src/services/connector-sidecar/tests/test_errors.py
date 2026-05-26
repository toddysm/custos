"""Unit tests for :mod:`custos_sidecar.errors`."""

from __future__ import annotations

from custos_sidecar.errors import (
    SidecarError,
    SidecarErrorCode,
    http_status_for,
    problem_response,
)


def test_http_status_for_every_code():
    expected = {
        SidecarErrorCode.INVALID_REQUEST: 400,
        SidecarErrorCode.BOOTSTRAP_INVALID: 401,
        SidecarErrorCode.CAPABILITY_FORBIDDEN: 403,
        SidecarErrorCode.SLOT_NOT_FOUND: 404,
        SidecarErrorCode.LEASE_NOT_FOUND: 404,
        SidecarErrorCode.LEASE_REVOKED: 410,
        SidecarErrorCode.CAPACITY_EXCEEDED: 429,
        SidecarErrorCode.UPSTREAM_FAILED: 502,
        SidecarErrorCode.CONNECTOR_UNAVAILABLE: 503,
    }
    assert {c: http_status_for(c) for c in SidecarErrorCode} == expected


def test_problem_response_shape():
    exc = SidecarError(SidecarErrorCode.SLOT_NOT_FOUND, "no slot 'x'")
    resp = problem_response(exc, instance="/v1/token")
    assert resp.status_code == 404
    assert resp.media_type == "application/problem+json"
    body = bytes(resp.body).decode("utf-8")
    assert '"type":"urn:custos:sidecar:error:slot-not-found"' in body
    assert '"title":"slot-not-found"' in body
    assert '"status":404' in body
    assert '"detail":"no slot \'x\'"' in body
    assert '"instance":"/v1/token"' in body
    assert "retry-after" not in {k.lower() for k in resp.headers}


def test_problem_response_capacity_carries_retry_after():
    exc = SidecarError(SidecarErrorCode.CAPACITY_EXCEEDED, "cap", retry_after_sec=7)
    resp = problem_response(exc)
    assert resp.status_code == 429
    assert resp.headers["retry-after"] == "7"


def test_problem_response_omits_instance_when_none():
    exc = SidecarError(SidecarErrorCode.INVALID_REQUEST, "bad")
    body = bytes(problem_response(exc).body).decode("utf-8")
    assert "instance" not in body
