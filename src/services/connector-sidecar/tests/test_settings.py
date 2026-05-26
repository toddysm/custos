"""Unit tests for :mod:`custos_sidecar.settings`."""

from __future__ import annotations

import json

import pytest

from custos_sidecar.settings import (
    DEFAULT_BOOTSTRAP_KEY_PATH,
    DEFAULT_BOOTSTRAP_TOKEN_PATH,
    DEFAULT_SOCKET_PATH,
    load_settings,
)


def _required_env() -> dict[str, str]:
    return {
        "CUSTOS_SIDECAR_RUN_ID": "r1",
        "CUSTOS_SIDECAR_STEP_ID": "s1",
        "CUSTOS_SIDECAR_ATTEMPT": "2",
        "CUSTOS_SIDECAR_WORKSPACE_ID": "ws_test",
        "CUSTOS_SIDECAR_CONNECTOR_SERVICE_URL": "http://connector:8080",
        "CUSTOS_SIDECAR_CALL_CONTEXT": '{"workspace_id":"ws_test"}',
    }


def test_load_with_defaults():
    settings = load_settings(_required_env())
    assert settings.socket_path == DEFAULT_SOCKET_PATH
    assert settings.bootstrap_token_path == DEFAULT_BOOTSTRAP_TOKEN_PATH
    assert settings.bootstrap_key_path == DEFAULT_BOOTSTRAP_KEY_PATH
    assert settings.run_id == "r1"
    assert settings.step_id == "s1"
    assert settings.attempt == 2
    assert settings.workspace_id == "ws_test"
    assert settings.connector_service_url == "http://connector:8080"
    assert settings.call_context == '{"workspace_id":"ws_test"}'
    assert settings.contexts_wire == []


def test_load_with_contexts_json():
    env = _required_env()
    env["CUSTOS_SIDECAR_CONTEXTS_JSON"] = json.dumps(
        [{"slot": "primary", "connectorInstanceId": "ci_p"}]
    )
    settings = load_settings(env)
    assert settings.contexts_wire == [{"slot": "primary", "connectorInstanceId": "ci_p"}]


def test_missing_required_raises():
    env = _required_env()
    del env["CUSTOS_SIDECAR_RUN_ID"]
    with pytest.raises(ValueError, match="CUSTOS_SIDECAR_RUN_ID"):
        load_settings(env)


def test_non_int_attempt_raises():
    env = _required_env()
    env["CUSTOS_SIDECAR_ATTEMPT"] = "first"
    with pytest.raises(ValueError, match="ATTEMPT must be an integer"):
        load_settings(env)


def test_non_positive_attempt_raises():
    env = _required_env()
    env["CUSTOS_SIDECAR_ATTEMPT"] = "0"
    with pytest.raises(ValueError, match="ATTEMPT must be positive"):
        load_settings(env)


def test_malformed_contexts_json_raises():
    env = _required_env()
    env["CUSTOS_SIDECAR_CONTEXTS_JSON"] = "{not json"
    with pytest.raises(ValueError, match="CONTEXTS_JSON is not valid JSON"):
        load_settings(env)


def test_non_array_contexts_json_raises():
    env = _required_env()
    env["CUSTOS_SIDECAR_CONTEXTS_JSON"] = '{"foo": "bar"}'
    with pytest.raises(ValueError, match="must be a JSON array"):
        load_settings(env)


def test_socket_path_override():
    env = _required_env()
    env["CUSTOS_SIDECAR_SOCKET_PATH"] = "/tmp/sock"
    settings = load_settings(env)
    assert settings.socket_path == "/tmp/sock"


def test_activity_gid_defaults_to_none():
    settings = load_settings(_required_env())
    assert settings.activity_gid is None


def test_activity_gid_blank_treated_as_none():
    env = _required_env()
    env["CUSTOS_SIDECAR_ACTIVITY_GID"] = ""
    settings = load_settings(env)
    assert settings.activity_gid is None


def test_activity_gid_parsed_when_set():
    env = _required_env()
    env["CUSTOS_SIDECAR_ACTIVITY_GID"] = "65532"
    settings = load_settings(env)
    assert settings.activity_gid == 65532


def test_non_int_activity_gid_raises():
    env = _required_env()
    env["CUSTOS_SIDECAR_ACTIVITY_GID"] = "activity"
    with pytest.raises(ValueError, match="ACTIVITY_GID must be an integer"):
        load_settings(env)


def test_negative_activity_gid_raises():
    env = _required_env()
    env["CUSTOS_SIDECAR_ACTIVITY_GID"] = "-1"
    with pytest.raises(ValueError, match="ACTIVITY_GID must be non-negative"):
        load_settings(env)
