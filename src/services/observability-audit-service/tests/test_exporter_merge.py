"""Tests for the Collector-config merge + validation algorithm (OBS-IMPL-010).

Cover three guarantees: valid customer exporter blocks merge into a well-formed
Collector config; invalid blocks are rejected with a captured reason while the
last-good config is retained; and the merge is deterministic + idempotent.
"""

from __future__ import annotations

from typing import Any

import pytest
import yaml

from custos_obs.exporters.merge import (
    CollectorConfigError,
    CollectorConfigMerger,
    MergeOutcome,
    merge_collector_config,
)

BASE_CONFIG = """
receivers:
  otlp:
    protocols:
      grpc: {}
      http: {}
processors:
  batch: {}
exporters:
  logging: {}
service:
  pipelines:
    logs:
      receivers: [otlp]
      processors: [batch]
      exporters: [logging]
    metrics:
      receivers: [otlp]
      processors: [batch]
      exporters: [logging]
    traces:
      receivers: [otlp]
      processors: [batch]
      exporters: [logging]
"""

CUSTOMER_BLOCK = """
exporters:
  loki/customer:
    endpoint: https://loki.example/loki/api/v1/push
  datadog:
    api:
      key: secret
pipelines:
  logs: [loki/customer]
  traces: [datadog]
"""


def _parse(config: str) -> dict[str, Any]:
    loaded = yaml.safe_load(config)
    assert isinstance(loaded, dict)
    return loaded


# --------------------------------------------------------------------------- #
# Happy path                                                                  #
# --------------------------------------------------------------------------- #


def test_valid_block_merges_exporters_and_pipelines() -> None:
    result = merge_collector_config(BASE_CONFIG, CUSTOMER_BLOCK)
    merged = _parse(result.effective_config)

    assert set(merged["exporters"]) == {"logging", "loki/customer", "datadog"}
    assert merged["service"]["pipelines"]["logs"]["exporters"] == ["logging", "loki/customer"]
    assert merged["service"]["pipelines"]["traces"]["exporters"] == ["datadog", "logging"]
    # Untouched pipeline keeps its base exporters.
    assert merged["service"]["pipelines"]["metrics"]["exporters"] == ["logging"]
    assert result.exporter_names == ("datadog", "loki/customer")


def test_merge_accepts_mapping_inputs() -> None:
    result = merge_collector_config(_parse(BASE_CONFIG), _parse(CUSTOMER_BLOCK))
    merged = _parse(result.effective_config)
    assert "loki/customer" in merged["exporters"]


def test_empty_customer_block_returns_base() -> None:
    result = merge_collector_config(BASE_CONFIG, None)
    merged = _parse(result.effective_config)
    assert set(merged["exporters"]) == {"logging"}
    assert result.exporter_names == ()


def test_customer_block_without_pipelines_only_defines_exporters() -> None:
    block = "exporters:\n  loki/customer:\n    endpoint: https://loki.example\n"
    result = merge_collector_config(BASE_CONFIG, block)
    merged = _parse(result.effective_config)
    assert "loki/customer" in merged["exporters"]
    # Not attached to any pipeline.
    assert merged["service"]["pipelines"]["logs"]["exporters"] == ["logging"]


# --------------------------------------------------------------------------- #
# Determinism + idempotency                                                   #
# --------------------------------------------------------------------------- #


def test_merge_is_deterministic() -> None:
    first = merge_collector_config(BASE_CONFIG, CUSTOMER_BLOCK)
    second = merge_collector_config(BASE_CONFIG, CUSTOMER_BLOCK)
    assert first.effective_config == second.effective_config


def test_merge_is_idempotent_through_merger() -> None:
    merger = CollectorConfigMerger(base_config=BASE_CONFIG)
    once = merger.apply(CUSTOMER_BLOCK)
    twice = merger.apply(CUSTOMER_BLOCK)
    assert once.effective_config == twice.effective_config
    assert once.exporter_names == twice.exporter_names == ("datadog", "loki/customer")


def test_pipeline_exporters_are_sorted() -> None:
    block = "exporters:\n  aaa: {}\n  zzz: {}\npipelines:\n  logs: [zzz, aaa]\n"
    result = merge_collector_config(BASE_CONFIG, block)
    merged = _parse(result.effective_config)
    assert merged["service"]["pipelines"]["logs"]["exporters"] == ["aaa", "logging", "zzz"]


def test_repeated_attachment_does_not_duplicate() -> None:
    block = "exporters:\n  loki/customer: {}\npipelines:\n  logs: [loki/customer, loki/customer]\n"
    result = merge_collector_config(BASE_CONFIG, block)
    merged = _parse(result.effective_config)
    assert merged["service"]["pipelines"]["logs"]["exporters"] == ["logging", "loki/customer"]


# --------------------------------------------------------------------------- #
# Rejection / validation                                                      #
# --------------------------------------------------------------------------- #


def test_invalid_yaml_is_rejected() -> None:
    with pytest.raises(CollectorConfigError, match="not valid YAML"):
        merge_collector_config(BASE_CONFIG, "exporters: [oops\n")


def test_non_mapping_customer_block_is_rejected() -> None:
    with pytest.raises(CollectorConfigError, match="must be a mapping"):
        merge_collector_config(BASE_CONFIG, "- just\n- a\n- list\n")


def test_unsupported_top_level_key_is_rejected() -> None:
    block = "exporters:\n  loki/customer: {}\nreceivers:\n  evil: {}\n"
    with pytest.raises(CollectorConfigError, match="unsupported keys"):
        merge_collector_config(BASE_CONFIG, block)


def test_exporter_name_collision_is_rejected() -> None:
    block = "exporters:\n  logging:\n    verbosity: detailed\n"
    with pytest.raises(CollectorConfigError, match="may not redefine base exporters"):
        merge_collector_config(BASE_CONFIG, block)


def test_invalid_exporter_name_is_rejected() -> None:
    block = "exporters:\n  a/b/c: {}\n"
    with pytest.raises(CollectorConfigError, match="must be 'type' or 'type/name'"):
        merge_collector_config(BASE_CONFIG, block)


def test_unknown_pipeline_signal_is_rejected() -> None:
    block = "exporters:\n  loki/customer: {}\npipelines:\n  profiles: [loki/customer]\n"
    with pytest.raises(CollectorConfigError, match="is not a valid signal"):
        merge_collector_config(BASE_CONFIG, block)


def test_pipeline_absent_from_base_is_rejected() -> None:
    base = (
        "exporters:\n  logging: {}\nservice:\n  pipelines:\n    logs:\n      exporters: [logging]\n"
    )
    block = "exporters:\n  datadog: {}\npipelines:\n  traces: [datadog]\n"
    with pytest.raises(CollectorConfigError, match="not present in the base"):
        merge_collector_config(base, block)


def test_attachment_to_undefined_exporter_is_rejected() -> None:
    block = "exporters:\n  loki/customer: {}\npipelines:\n  logs: [ghost]\n"
    with pytest.raises(CollectorConfigError, match="undefined exporter"):
        merge_collector_config(BASE_CONFIG, block)


def test_non_list_pipeline_attachment_is_rejected() -> None:
    block = "exporters:\n  loki/customer: {}\npipelines:\n  logs: loki/customer\n"
    with pytest.raises(CollectorConfigError, match="must be a list of exporter names"):
        merge_collector_config(BASE_CONFIG, block)


def test_non_mapping_pipelines_is_rejected() -> None:
    block = "exporters:\n  loki/customer: {}\npipelines: nope\n"
    with pytest.raises(CollectorConfigError, match="'pipelines' must be a mapping"):
        merge_collector_config(BASE_CONFIG, block)


def test_non_mapping_customer_exporters_is_rejected() -> None:
    block = "exporters: notamap\n"
    with pytest.raises(CollectorConfigError, match="must be a mapping of exporter name"):
        merge_collector_config(BASE_CONFIG, block)


def test_blank_customer_block_yaml_returns_base() -> None:
    # A non-None string that parses to None (empty / comment-only) is treated
    # as "no customer blocks".
    result = merge_collector_config(BASE_CONFIG, "# nothing here\n")
    merged = _parse(result.effective_config)
    assert set(merged["exporters"]) == {"logging"}
    assert result.exporter_names == ()


def test_non_string_exporter_name_is_rejected() -> None:
    block = "exporters:\n  123: {}\n"
    with pytest.raises(CollectorConfigError, match="non-string exporter name"):
        merge_collector_config(BASE_CONFIG, block)


def test_attached_base_pipeline_must_be_mapping() -> None:
    base = "exporters:\n  logging: {}\nservice:\n  pipelines:\n    logs: nope\n"
    block = "exporters:\n  datadog: {}\npipelines:\n  logs: [datadog]\n"
    with pytest.raises(CollectorConfigError, match=r"'service\.pipelines\.logs' must be a mapping"):
        merge_collector_config(base, block)


def test_unattached_non_mapping_pipeline_is_skipped_in_validation() -> None:
    # A base pipeline that is not a mapping and is not touched by the customer
    # is skipped by merged validation (it is the Collector's own concern).
    base = (
        "exporters:\n  logging: {}\nservice:\n  pipelines:\n"
        "    logs:\n      exporters: [logging]\n"
        "    metrics: nope\n"
    )
    block = "exporters:\n  datadog: {}\npipelines:\n  logs: [datadog]\n"
    result = merge_collector_config(base, block)
    merged = _parse(result.effective_config)
    assert merged["service"]["pipelines"]["logs"]["exporters"] == ["datadog", "logging"]
    assert merged["service"]["pipelines"]["metrics"] == "nope"


def test_unattached_pipeline_non_list_exporters_is_rejected() -> None:
    base = (
        "exporters:\n  logging: {}\nservice:\n  pipelines:\n"
        "    logs:\n      exporters: nope\n"
        "    metrics:\n      exporters: [logging]\n"
    )
    # Customer attaches to metrics only; the malformed logs pipeline is caught
    # by merged validation, not by attachment.
    block = "exporters:\n  datadog: {}\npipelines:\n  metrics: [datadog]\n"
    with pytest.raises(CollectorConfigError, match=r"merged 'service\.pipelines\.logs\.exporters'"):
        merge_collector_config(base, block)


def test_base_pipeline_referencing_undefined_exporter_is_rejected() -> None:
    base = (
        "exporters:\n  logging: {}\nservice:\n  pipelines:\n    logs:\n      exporters: [ghost]\n"
    )
    with pytest.raises(CollectorConfigError, match="references undefined exporters"):
        merge_collector_config(base, None)


# --------------------------------------------------------------------------- #
# Base-config validation                                                      #
# --------------------------------------------------------------------------- #


def test_non_mapping_base_is_rejected() -> None:
    with pytest.raises(CollectorConfigError, match="base config must be a mapping"):
        merge_collector_config("- a\n- b\n", None)


def test_base_with_non_mapping_service_is_rejected() -> None:
    with pytest.raises(CollectorConfigError, match="'service' must be a mapping"):
        merge_collector_config("service: nope\n", None)


def test_base_with_non_mapping_pipelines_is_rejected() -> None:
    with pytest.raises(CollectorConfigError, match=r"'service\.pipelines' must be a mapping"):
        merge_collector_config("service:\n  pipelines: nope\n", None)


def test_base_with_non_mapping_exporters_is_rejected() -> None:
    base = "exporters: nope\nservice:\n  pipelines: {}\n"
    with pytest.raises(CollectorConfigError, match="base config 'exporters' must be a mapping"):
        merge_collector_config(base, None)


def test_base_pipeline_non_list_exporters_is_rejected() -> None:
    base = "exporters:\n  logging: {}\nservice:\n  pipelines:\n    logs:\n      exporters: nope\n"
    block = "exporters:\n  datadog: {}\npipelines:\n  logs: [datadog]\n"
    with pytest.raises(CollectorConfigError, match="must be a list"):
        merge_collector_config(base, block)


# --------------------------------------------------------------------------- #
# CollectorConfigMerger — last-good retention                                 #
# --------------------------------------------------------------------------- #


def test_merger_initial_last_good_is_base() -> None:
    merger = CollectorConfigMerger(base_config=BASE_CONFIG)
    assert "loki/customer" not in merger.last_good
    assert set(_parse(merger.last_good)["exporters"]) == {"logging"}


def test_merger_apply_good_updates_last_good() -> None:
    merger = CollectorConfigMerger(base_config=BASE_CONFIG)
    outcome = merger.apply(CUSTOMER_BLOCK)
    assert isinstance(outcome, MergeOutcome)
    assert outcome.applied is True
    assert outcome.rejected is False
    assert outcome.rejection_reason is None
    assert "loki/customer" in merger.last_good
    assert outcome.effective_config == merger.last_good


def test_merger_apply_bad_retains_last_good_and_captures_reason() -> None:
    merger = CollectorConfigMerger(base_config=BASE_CONFIG)
    good = merger.apply(CUSTOMER_BLOCK)
    bad = merger.apply("exporters:\n  logging: {}\n")  # collides with base

    assert bad.applied is False
    assert bad.rejected is True
    assert bad.rejection_reason is not None
    assert "redefine base exporters" in bad.rejection_reason
    assert bad.exporter_names == ()
    # Last-good is unchanged — still the previous good merge.
    assert merger.last_good == good.effective_config
    assert bad.effective_config == good.effective_config


def test_merger_construction_validates_base() -> None:
    with pytest.raises(CollectorConfigError):
        CollectorConfigMerger(base_config="- not\n- a mapping\n")
