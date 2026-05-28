# Change: slack-webhook-target-kind

Date: 2026-05-28
Type: component-design
Component: connector-service
Sequence: 016
GitHub Issue: [#313](https://github.com/toddysm/custos/issues/313) (Phase L / CONN-IMPL-030)
Status: closed

## Summary

Add `slack-webhook` to the closed set of Connector Manifest v1 target
kinds so the reference Slack-notifier plugin shipped under
`src/libs/connector-plugins/slack-notifier/` can declare a schema-valid
manifest, and so future webhook-style sink connectors have a curated
target kind to reuse instead of inventing per-plugin shapes.

This extends the existing `target.config` property-bag pattern (see
`2026-05-16-001-target-config-property-bag.md`) with a new closed
sub-schema, `slackWebhookConfig`, selected by `target.kind ==
"slack-webhook"`. No existing kinds (`oci-registry`,
`azure-blob-storage`, `amazon-s3-bucket`) are affected.

## Before

`target.kind` enumerated three values: `oci-registry`,
`azure-blob-storage`, `amazon-s3-bucket`. The connector-service rejected
any manifest declaring a fourth kind with the
`unknown-target-kind` validation error, blocking sink-style plugins
whose target is an opaque webhook endpoint rather than a storage system.

Both validators (`manifest/validator.py` and `instances/validator.py`)
hard-coded the three-kind set in `_TARGET_CONFIG_REQUIRED`.

## After

`target.kind` is extended to a closed four-value enum:

```json
"kind": {
  "type": "string",
  "enum": [
    "oci-registry",
    "azure-blob-storage",
    "amazon-s3-bucket",
    "slack-webhook"
  ]
}
```

When `kind == "slack-webhook"`, the `allOf` `if/then` router requires
`target.config` to match `$defs.slackWebhookConfig`:

```json
{
  "type": "object",
  "additionalProperties": false,
  "required": ["channel"],
  "properties": {
    "channel": {
      "type": "string",
      "pattern": "^#?[a-z0-9][a-z0-9._-]{0,79}$",
      "maxLength": 80
    }
  }
}
```

`channel` accepts an optional leading `#` for readability and is
length-capped at 80 characters total (the regex alone would otherwise
admit 81 when `#` is present; the explicit `maxLength` keeps the bound
identical to the documented contract). The runtime strips the leading
`#` before invoking the incoming-webhook URL; the URL itself is carried
by `spec.target.endpoint` (e.g. `https://hooks.slack.com/services/...`).

Both `_TARGET_CONFIG_REQUIRED` tables grow a matching entry:

```python
"slack-webhook": ("channel",),
```

so publish-time manifest validation and deploy-time instance config
validation agree on the required-key set without re-parsing the schema.

## Impact

* **Connector Service** — `validate-connector-manifest` accepts the new
  kind; `validate-instance-config` enforces `channel` at deploy time.
  Both validator tables (`manifest/validator.py` and
  `instances/validator.py`) are kept in lock-step by the existing drift
  test (`test_instance_config_validator.py`).
* **Sample plugins** — `src/libs/connector-plugins/slack-notifier/` now
  has a schema-valid `connector-manifest.json`. The sink connector
  example in `design.md` is updated to include `config.channel:
  "#deploys"` instead of `config: {}` so the documented example stays
  schema-valid.
* **Docs** — `docs/developers/connections-api.md` documents the new
  kind and its required `channel` field;
  `docs/developers/examples/slack-webhook-azure-managed-identity.json`
  ships a copy-pasteable example manifest.
* **Schemas** — both copies of the schema
  (`design/components/connector-service/schemas/` and
  `src/services/connector-service/src/custos_connector/manifest/_schemas/`)
  are updated together; the existing schema-drift test pins them to be
  byte-identical.
* **No migrations required** — the change is additive: no existing
  manifest is invalidated, no stored data needs to be rewritten.

## Validation

* Schema-drift test (`tests/test_manifest_schema_drift.py`): passes —
  both copies are byte-identical.
* Manifest validator unit tests: extended with a `slack-webhook`
  parametrize entry.
* Connector-service test suite locally: 677 passed at 93.35% coverage.
* Sample-plugin integration test
  (`tests/integration/test_sample_plugins.py`): publishes the new
  manifest as an OCI artifact via the fallback-tag discovery path and
  re-validates the retrieved body — passes against testcontainers
  distribution.
