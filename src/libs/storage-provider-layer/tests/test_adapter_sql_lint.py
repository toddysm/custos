"""CI lint: every SQL `WHERE` in an adapter must include `workspace_id`.

Static guard that backs the runtime workspace-scoping middleware: even
if every entry point validates `workspace_id`, an adapter that omits
`workspace_id = ?` from a `WHERE` clause leaks cross-workspace rows.
This test fails CI for any such omission.

Scope:
  - Scans every `.py` file under `src/custos_spl/adapters/`.
  - Skips files under `adapters/auth/` (AuthStoreProvider is exempt
    from workspace scoping) and `adapters/catalog/` (CatalogStore is
    platform-wide).
  - Inspects every string literal; if it contains a SQL keyword that
    can carry a `WHERE` clause AND it contains `WHERE`, it MUST also
    mention `workspace_id`.

The rule is intentionally simple (string-literal scan, not a SQL
parser) — false positives are cheap to silence with `# noqa: SPL008`
on the literal, false negatives are dangerous so the bias is toward
flagging.
"""

from __future__ import annotations

import ast
import re
from collections.abc import Iterable
from pathlib import Path
from typing import NamedTuple

# Adapter package root: <repo>/src/libs/storage-provider-layer/src/custos_spl/adapters
_ADAPTERS_ROOT = (
    Path(__file__).resolve().parents[2] / "src" / "custos_spl" / "adapters"
)

# Subpaths exempt from workspace scoping.
_EXEMPT_DIRS: tuple[str, ...] = ("auth", "catalog")

# SQL statements that can carry a WHERE clause.
_SQL_KEYWORDS = re.compile(
    r"\b(SELECT|UPDATE|DELETE|RETURNING|UPSERT|MERGE)\b", re.IGNORECASE
)
_WHERE_RE = re.compile(r"\bWHERE\b", re.IGNORECASE)
_WORKSPACE_RE = re.compile(r"\bworkspace_id\b", re.IGNORECASE)
_NOQA_RE = re.compile(r"#\s*noqa:\s*SPL008", re.IGNORECASE)


class _Violation(NamedTuple):
    path: Path
    lineno: int
    snippet: str


def _iter_string_literals(
    source: str,
) -> Iterable[tuple[int, str]]:
    """Yield (lineno, value) for every str literal in `source`."""
    try:
        tree = ast.parse(source)
    except SyntaxError:
        return
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str):
            yield node.lineno, node.value


def _is_exempt(path: Path, adapters_root: Path) -> bool:
    parts = path.relative_to(adapters_root).parts
    return bool(parts) and parts[0] in _EXEMPT_DIRS


def find_violations(adapters_root: Path = _ADAPTERS_ROOT) -> list[_Violation]:
    if not adapters_root.exists():
        return []
    violations: list[_Violation] = []
    for path in sorted(adapters_root.rglob("*.py")):
        if path.name == "__init__.py":
            continue
        if _is_exempt(path, adapters_root):
            continue
        source = path.read_text(encoding="utf-8")
        source_lines = source.splitlines()
        for lineno, value in _iter_string_literals(source):
            if not _SQL_KEYWORDS.search(value):
                continue
            if not _WHERE_RE.search(value):
                continue
            if _WORKSPACE_RE.search(value):
                continue
            # honor inline opt-out on the line where the literal starts
            line_text = (
                source_lines[lineno - 1] if 0 < lineno <= len(source_lines) else ""
            )
            if _NOQA_RE.search(line_text):
                continue
            snippet = value.strip().splitlines()[0][:120]
            violations.append(_Violation(path=path, lineno=lineno, snippet=snippet))
    return violations


def test_no_adapter_sql_omits_workspace_filter() -> None:
    """Every `WHERE` in adapter SQL must mention `workspace_id`."""
    violations = find_violations()
    if violations:
        lines = [f"  {v.path}:{v.lineno} — {v.snippet}" for v in violations]
        raise AssertionError(
            "Adapter SQL is missing `workspace_id` in a WHERE clause "
            "(SPL008). Add `WHERE ... AND workspace_id = ?` to these "
            "queries, or annotate the literal with `# noqa: SPL008` if "
            "it is genuinely platform-wide:\n" + "\n".join(lines)
        )


# ----- self-test of the rule itself -----


def test_rule_detects_missing_workspace_id(tmp_path: Path) -> None:
    """Sanity check: the linter actually flags a bad string literal."""
    bad = tmp_path / "bad_adapter.py"
    bad.write_text('SQL = "SELECT id FROM things WHERE tenant_id = ?"\n')
    violations = find_violations(tmp_path)
    assert len(violations) == 1
    assert violations[0].path == bad


def test_rule_accepts_workspace_filter(tmp_path: Path) -> None:
    good = tmp_path / "good_adapter.py"
    good.write_text(
        'SQL = "SELECT id FROM things WHERE workspace_id = ? AND id = ?"\n'
    )
    assert find_violations(tmp_path) == []


def test_rule_accepts_insert_without_where(tmp_path: Path) -> None:
    """INSERTs have no WHERE clause; the rule must not flag them."""
    f = tmp_path / "ins.py"
    f.write_text('SQL = "INSERT INTO things (id, name) VALUES (?, ?)"\n')
    assert find_violations(tmp_path) == []


def test_rule_honors_inline_noqa(tmp_path: Path) -> None:
    f = tmp_path / "ok.py"
    f.write_text(
        'SQL = "SELECT id FROM platform_table WHERE id = ?"  # noqa: SPL008\n'
    )
    assert find_violations(tmp_path) == []


def test_rule_skips_exempt_subdirectories(tmp_path: Path) -> None:
    """`auth/` and `catalog/` subtrees are exempt."""
    (tmp_path / "auth").mkdir()
    (tmp_path / "auth" / "store.py").write_text(
        'SQL = "SELECT id FROM users WHERE email = ?"\n'
    )
    (tmp_path / "catalog").mkdir()
    (tmp_path / "catalog" / "store.py").write_text(
        'SQL = "SELECT id FROM activity_types WHERE namespace = ?"\n'
    )
    assert find_violations(tmp_path) == []
