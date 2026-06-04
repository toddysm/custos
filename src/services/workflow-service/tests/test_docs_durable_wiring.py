"""WF-IMPL-119 — docs/developers/workflow-durable-wiring.md config test.

The durable-wiring developer doc publishes a Configuration table naming
the four environment variables that drive the durable-vs-in-memory
switch (`WF_CATALOG_ENDPOINT`, `WF_METADATA_STORE`,
`WF_IDEMPOTENCY_KEY_TTL`, `ENVIRONMENT`) plus the default
`WF_IDEMPOTENCY_KEY_TTL` window. This test pins those literals to the
code constants so the doc cannot silently drift from the env surface it
documents.
"""

from __future__ import annotations

from pathlib import Path

from custos_workflow.providers import (
    ENV_CATALOG_APP_ID,
    ENV_ENVIRONMENT,
    ENV_METADATA_STORE,
)
from custos_workflow.validator.idempotency_ledger import (
    DEFAULT_IDEMPOTENCY_KEY_TTL,
    IDEMPOTENCY_TTL_ENV_VAR,
)

_DOC_PATH = (
    Path(__file__).resolve().parents[4] / "docs" / "developers" / "workflow-durable-wiring.md"
)


def _doc_text() -> str:
    assert _DOC_PATH.is_file(), _DOC_PATH
    return _DOC_PATH.read_text(encoding="utf-8")


def test_doc_names_every_durable_wiring_env_var() -> None:
    """Each env-var constant the doc documents must appear verbatim."""
    doc = _doc_text()
    for env_var in (
        ENV_CATALOG_APP_ID,
        ENV_METADATA_STORE,
        ENV_ENVIRONMENT,
        IDEMPOTENCY_TTL_ENV_VAR,
    ):
        assert env_var in doc, f"{env_var} missing from {_DOC_PATH.name}"


def test_doc_idempotency_ttl_default_matches_code() -> None:
    """The doc's stated default TTL must equal the code default (PT24H)."""
    assert DEFAULT_IDEMPOTENCY_KEY_TTL.total_seconds() == 24 * 3600
    assert "`PT24H`" in _doc_text()
