# Reference slack-notifier sink connector plugin

Minimal sink connector. Used as the worked example for the optional-
`events`-block code path in the [Connector Manifest v1 reference](../../../../docs/developers/connections-api.md)
and as the fallback-tag publish fixture for the connector-service
integration suite ([CONN-IMPL-031](https://github.com/toddysm/custos/issues/314)).

* **Manifest**: [`connector-manifest.json`](connector-manifest.json) — note
  the absence of `spec.events`.
* **Hooks**: implemented in
  [`src/slack_notifier_plugin/plugin.py`](src/slack_notifier_plugin/plugin.py).
* **Wire contract**: documented in
  [`docs/developers/connector-plugin-author.md`](../../../../docs/developers/connector-plugin-author.md).

## Building the image

```sh
docker build -t custos-sample/slack-notifier-plugin:1.0.0 \
    -f src/libs/connector-plugins/slack-notifier/Dockerfile \
    src/libs/connector-plugins/slack-notifier
```

## Tests

```sh
pip install -e .[dev]
pytest -q
```
