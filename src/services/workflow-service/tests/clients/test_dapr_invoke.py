"""Tests for the Dapr Service-Invocation HTTP transport primitives (WF-IMPL-073)."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from custos_workflow.clients._dapr_invoke import (
    DEFAULT_DAPR_HOST,
    DEFAULT_DAPR_HTTP_PORT,
    DEFAULT_OUTBOUND_RPC_TIMEOUT_SECONDS,
    ENV_DAPR_HTTP_HOST,
    ENV_DAPR_HTTP_PORT,
    DaprInvokeEndpoint,
    build_invoke_url,
    read_dapr_env,
)

# ---------------------------------------------------------------------------
# Module-level constants
# ---------------------------------------------------------------------------


class TestConstants:
    """Defaults are pinned to the Dapr-documented values."""

    def test_default_host_is_localhost(self) -> None:
        assert DEFAULT_DAPR_HOST == "127.0.0.1"

    def test_default_http_port_is_3500(self) -> None:
        assert DEFAULT_DAPR_HTTP_PORT == 3500

    def test_default_outbound_rpc_timeout_seconds_is_10(self) -> None:
        # Mirrors DEFAULT_DAPR_PUBLISH_TIMEOUT_SECONDS so a single
        # operator-tunable governs both Pub/Sub publish and outbound
        # RPC at the sidecar latency boundary.
        assert pytest.approx(10.0) == DEFAULT_OUTBOUND_RPC_TIMEOUT_SECONDS

    def test_env_var_names_match_dapr_conventions(self) -> None:
        # ``DAPR_HTTP_HOST`` / ``DAPR_HTTP_PORT`` are the Dapr-side
        # documented names — they intentionally do NOT carry the
        # ``WF_`` prefix used for workflow-service-specific knobs.
        assert ENV_DAPR_HTTP_HOST == "DAPR_HTTP_HOST"
        assert ENV_DAPR_HTTP_PORT == "DAPR_HTTP_PORT"


# ---------------------------------------------------------------------------
# DaprInvokeEndpoint
# ---------------------------------------------------------------------------


class TestDaprInvokeEndpoint:
    """The endpoint triple is frozen, hashable, and validated."""

    def test_happy_path(self) -> None:
        endpoint = DaprInvokeEndpoint(host="127.0.0.1", http_port=3500, app_id="arm")
        assert endpoint.host == "127.0.0.1"
        assert endpoint.http_port == 3500
        assert endpoint.app_id == "arm"

    def test_endpoint_is_frozen(self) -> None:
        endpoint = DaprInvokeEndpoint(host="127.0.0.1", http_port=3500, app_id="arm")
        with pytest.raises(FrozenInstanceError):
            endpoint.host = "10.0.0.1"  # type: ignore[misc]

    def test_endpoint_is_hashable(self) -> None:
        a = DaprInvokeEndpoint(host="127.0.0.1", http_port=3500, app_id="arm")
        b = DaprInvokeEndpoint(host="127.0.0.1", http_port=3500, app_id="arm")
        c = DaprInvokeEndpoint(host="127.0.0.1", http_port=3500, app_id="connector")
        # A single-element set should collapse equal instances and
        # keep distinct ones — proves the dataclass participates in
        # hashing the way the docstring promises.
        assert {a, b, c} == {a, c}

    def test_empty_host_rejected(self) -> None:
        with pytest.raises(ValueError, match="host must be a non-empty string"):
            DaprInvokeEndpoint(host="", http_port=3500, app_id="arm")

    def test_empty_app_id_rejected(self) -> None:
        with pytest.raises(ValueError, match="app_id must be a non-empty string"):
            DaprInvokeEndpoint(host="127.0.0.1", http_port=3500, app_id="")

    @pytest.mark.parametrize("bad_port", [0, -1, -3500])
    def test_non_positive_port_rejected(self, bad_port: int) -> None:
        with pytest.raises(ValueError, match="http_port must be positive"):
            DaprInvokeEndpoint(host="127.0.0.1", http_port=bad_port, app_id="arm")

    def test_bool_port_rejected_explicitly(self) -> None:
        # ``isinstance(True, int) is True`` would otherwise sneak
        # past the positivity check. ``bool`` is a subclass of
        # ``int`` so mypy considers this assignment well-typed —
        # no ``type: ignore`` needed.
        with pytest.raises(ValueError, match="http_port must be an int"):
            DaprInvokeEndpoint(host="127.0.0.1", http_port=True, app_id="arm")

    def test_non_int_port_rejected(self) -> None:
        with pytest.raises(ValueError, match="http_port must be an int"):
            DaprInvokeEndpoint(host="127.0.0.1", http_port="3500", app_id="arm")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# build_invoke_url
# ---------------------------------------------------------------------------


class TestBuildInvokeUrl:
    """Canonical Dapr Service-Invocation HTTP URL shape."""

    def test_canonical_shape(self) -> None:
        endpoint = DaprInvokeEndpoint(host="127.0.0.1", http_port=3500, app_id="arm")
        url = build_invoke_url(endpoint, "ScheduleActivity")
        assert url == "http://127.0.0.1:3500/v1.0/invoke/arm/method/ScheduleActivity"

    def test_with_different_app_id_and_method(self) -> None:
        endpoint = DaprInvokeEndpoint(host="dapr.local", http_port=8500, app_id="connector-service")
        url = build_invoke_url(endpoint, "BindForStep")
        assert url == "http://dapr.local:8500/v1.0/invoke/connector-service/method/BindForStep"

    def test_leading_slash_in_method_is_normalised(self) -> None:
        endpoint = DaprInvokeEndpoint(host="127.0.0.1", http_port=3500, app_id="arm")
        # Two callers ending up with ``/method`` vs ``method`` is a
        # classic source of double-slash bugs; the builder absorbs it.
        url = build_invoke_url(endpoint, "/ScheduleActivity")
        assert url == "http://127.0.0.1:3500/v1.0/invoke/arm/method/ScheduleActivity"

    def test_empty_method_rejected(self) -> None:
        endpoint = DaprInvokeEndpoint(host="127.0.0.1", http_port=3500, app_id="arm")
        with pytest.raises(ValueError, match="non-empty method name"):
            build_invoke_url(endpoint, "")

    def test_slash_only_method_rejected(self) -> None:
        endpoint = DaprInvokeEndpoint(host="127.0.0.1", http_port=3500, app_id="arm")
        with pytest.raises(ValueError, match="non-slash characters"):
            build_invoke_url(endpoint, "/")


# ---------------------------------------------------------------------------
# read_dapr_env
# ---------------------------------------------------------------------------


class TestReadDaprEnv:
    """Env parser: app-id required, host + port optional with defaults."""

    def test_happy_path_with_only_app_id(self) -> None:
        env = {"WF_ARM_ENDPOINT": "activity-runtime-manager"}
        endpoint = read_dapr_env(env, "WF_ARM_ENDPOINT")
        assert endpoint == DaprInvokeEndpoint(
            host=DEFAULT_DAPR_HOST,
            http_port=DEFAULT_DAPR_HTTP_PORT,
            app_id="activity-runtime-manager",
        )

    def test_host_override(self) -> None:
        env = {
            "WF_CONNECTOR_ENDPOINT": "connector-service",
            "DAPR_HTTP_HOST": "dapr-sidecar",
        }
        endpoint = read_dapr_env(env, "WF_CONNECTOR_ENDPOINT")
        assert endpoint.host == "dapr-sidecar"
        assert endpoint.http_port == DEFAULT_DAPR_HTTP_PORT
        assert endpoint.app_id == "connector-service"

    def test_port_override(self) -> None:
        env = {
            "WF_ARM_ENDPOINT": "activity-runtime-manager",
            "DAPR_HTTP_PORT": "9001",
        }
        endpoint = read_dapr_env(env, "WF_ARM_ENDPOINT")
        assert endpoint.http_port == 9001

    def test_both_host_and_port_override(self) -> None:
        env = {
            "WF_ARM_ENDPOINT": "arm",
            "DAPR_HTTP_HOST": "10.0.0.5",
            "DAPR_HTTP_PORT": "13500",
        }
        endpoint = read_dapr_env(env, "WF_ARM_ENDPOINT")
        assert endpoint == DaprInvokeEndpoint(host="10.0.0.5", http_port=13500, app_id="arm")

    def test_missing_app_id_var_raises_runtime_error_naming_var(self) -> None:
        env: dict[str, str] = {}
        with pytest.raises(RuntimeError, match="WF_ARM_ENDPOINT must be set"):
            read_dapr_env(env, "WF_ARM_ENDPOINT")

    def test_empty_app_id_var_raises_runtime_error(self) -> None:
        env = {"WF_ARM_ENDPOINT": "   "}
        with pytest.raises(RuntimeError, match="WF_ARM_ENDPOINT must be set"):
            read_dapr_env(env, "WF_ARM_ENDPOINT")

    def test_whitespace_around_app_id_is_stripped(self) -> None:
        env = {"WF_ARM_ENDPOINT": "  activity-runtime-manager  "}
        endpoint = read_dapr_env(env, "WF_ARM_ENDPOINT")
        assert endpoint.app_id == "activity-runtime-manager"

    def test_empty_host_falls_back_to_default(self) -> None:
        env = {
            "WF_ARM_ENDPOINT": "arm",
            "DAPR_HTTP_HOST": "   ",
        }
        endpoint = read_dapr_env(env, "WF_ARM_ENDPOINT")
        assert endpoint.host == DEFAULT_DAPR_HOST

    def test_empty_port_falls_back_to_default(self) -> None:
        env = {
            "WF_ARM_ENDPOINT": "arm",
            "DAPR_HTTP_PORT": "",
        }
        endpoint = read_dapr_env(env, "WF_ARM_ENDPOINT")
        assert endpoint.http_port == DEFAULT_DAPR_HTTP_PORT

    def test_non_integer_port_raises_value_error_naming_var(self) -> None:
        env = {
            "WF_ARM_ENDPOINT": "arm",
            "DAPR_HTTP_PORT": "not-a-number",
        }
        with pytest.raises(ValueError, match="DAPR_HTTP_PORT must be a positive integer"):
            read_dapr_env(env, "WF_ARM_ENDPOINT")

    def test_non_positive_port_string_rejected_by_endpoint_validation(self) -> None:
        # Parses fine as int, but DaprInvokeEndpoint's own guard
        # rejects 0 / negative — this proves the parser delegates
        # to the dataclass for positivity checks.
        env = {
            "WF_ARM_ENDPOINT": "arm",
            "DAPR_HTTP_PORT": "0",
        }
        with pytest.raises(ValueError, match="http_port must be positive"):
            read_dapr_env(env, "WF_ARM_ENDPOINT")
