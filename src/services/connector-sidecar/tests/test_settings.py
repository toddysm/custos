"""Unit tests for :mod:`custos_sidecar.settings`."""

from __future__ import annotations

import json

import pytest

from custos_sidecar.settings import (
    DEFAULT_BOOTSTRAP_KEY_PATH,
    DEFAULT_BOOTSTRAP_TOKEN_PATH,
    DEFAULT_CONTROL_HOST,
    DEFAULT_CONTROL_PORT,
    DEFAULT_SOCKET_PATH,
    Settings,
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


# --------------------------------------------------------------------------- #
# Control-channel mTLS settings (CONN-IMPL-020)
# --------------------------------------------------------------------------- #


def test_control_disabled_by_default():
    settings = load_settings(_required_env())
    assert settings.control_enabled is False
    assert settings.control_host == DEFAULT_CONTROL_HOST
    assert settings.control_port == DEFAULT_CONTROL_PORT
    assert settings.control_tls_cert_path is None
    assert settings.control_tls_key_path is None
    assert settings.control_tls_ca_path is None


def test_control_enabled_with_mtls_paths():
    env = _required_env()
    env["CUSTOS_SIDECAR_CONTROL_ENABLED"] = "true"
    env["CUSTOS_SIDECAR_CONTROL_TLS_CERT_PATH"] = "/certs/server.crt"
    env["CUSTOS_SIDECAR_CONTROL_TLS_KEY_PATH"] = "/certs/server.key"
    env["CUSTOS_SIDECAR_CONTROL_TLS_CA_PATH"] = "/certs/ca.crt"
    settings = load_settings(env)
    assert settings.control_enabled is True
    assert settings.control_tls_cert_path == "/certs/server.crt"
    assert settings.control_tls_key_path == "/certs/server.key"
    assert settings.control_tls_ca_path == "/certs/ca.crt"


def test_control_port_override():
    env = _required_env()
    env["CUSTOS_SIDECAR_CONTROL_PORT"] = "8443"
    settings = load_settings(env)
    assert settings.control_port == 8443


def test_control_host_override():
    env = _required_env()
    env["CUSTOS_SIDECAR_CONTROL_HOST"] = "127.0.0.1"
    settings = load_settings(env)
    assert settings.control_host == "127.0.0.1"


def test_control_port_non_int_raises():
    env = _required_env()
    env["CUSTOS_SIDECAR_CONTROL_PORT"] = "nope"
    with pytest.raises(ValueError, match="CONTROL_PORT must be an integer"):
        load_settings(env)


@pytest.mark.parametrize("bad", ["0", "65536", "-1"])
def test_control_port_out_of_range_raises(bad: str) -> None:
    env = _required_env()
    env["CUSTOS_SIDECAR_CONTROL_PORT"] = bad
    with pytest.raises(ValueError, match=r"CONTROL_PORT must be in 1\.\.65535"):
        load_settings(env)


@pytest.mark.parametrize(
    "truthy,expected",
    [
        ("1", True),
        ("true", True),
        ("TRUE", True),
        ("yes", True),
        ("on", True),
        ("0", False),
        ("false", False),
        ("no", False),
        ("off", False),
    ],
)
def test_control_enabled_parses_truthy_falsy(truthy: str, expected: bool) -> None:
    env = _required_env()
    env["CUSTOS_SIDECAR_CONTROL_ENABLED"] = truthy
    if expected:
        # When enabled, mTLS paths must be supplied.
        env["CUSTOS_SIDECAR_CONTROL_TLS_CERT_PATH"] = "/c"
        env["CUSTOS_SIDECAR_CONTROL_TLS_KEY_PATH"] = "/k"
        env["CUSTOS_SIDECAR_CONTROL_TLS_CA_PATH"] = "/a"
    settings = load_settings(env)
    assert settings.control_enabled is expected


def test_control_enabled_garbage_raises():
    env = _required_env()
    env["CUSTOS_SIDECAR_CONTROL_ENABLED"] = "maybe"
    with pytest.raises(ValueError, match="unrecognised boolean"):
        load_settings(env)


@pytest.mark.parametrize(
    "drop_env",
    [
        "CUSTOS_SIDECAR_CONTROL_TLS_CERT_PATH",
        "CUSTOS_SIDECAR_CONTROL_TLS_KEY_PATH",
        "CUSTOS_SIDECAR_CONTROL_TLS_CA_PATH",
    ],
)
def test_control_enabled_missing_mtls_path_raises(drop_env: str) -> None:
    env = _required_env()
    env["CUSTOS_SIDECAR_CONTROL_ENABLED"] = "true"
    env["CUSTOS_SIDECAR_CONTROL_TLS_CERT_PATH"] = "/c"
    env["CUSTOS_SIDECAR_CONTROL_TLS_KEY_PATH"] = "/k"
    env["CUSTOS_SIDECAR_CONTROL_TLS_CA_PATH"] = "/a"
    del env[drop_env]
    with pytest.raises(ValueError, match="control_enabled is true but mTLS paths are missing"):
        load_settings(env)


def test_settings_post_init_rejects_partial_mtls():
    """Direct construction also enforces the cross-field invariant."""
    with pytest.raises(ValueError, match="mTLS paths are missing"):
        Settings(
            socket_path="/s",
            bootstrap_token_path="/t",
            bootstrap_key_path="/k",
            run_id="r",
            step_id="s",
            attempt=1,
            workspace_id="ws",
            connector_service_url="http://x",
            call_context="{}",
            control_enabled=True,
            # cert+key set, ca missing.
            control_tls_cert_path="/c",
            control_tls_key_path="/k",
        )
