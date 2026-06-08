"""Tests for the `custos-migrate` CLI.

These tests exercise the command-line surface without touching real
entry points or real adapters. We monkeypatch `_discover_entry_points`
to inject fake adapters, then assert exit codes and human-readable
output.
"""

from __future__ import annotations

import io
from collections.abc import Mapping
from collections.abc import Set as AbstractSet

import pytest

from custos_spl.migrations import cli, runner


class _FakeEntryPoint:
    """Stand-in for `importlib.metadata.EntryPoint`.

    The real CLI only uses `.name`, `.value`, and `.load()`, so a
    duck-typed object works fine.
    """

    def __init__(self, name: str, factory: object) -> None:
        self.name = name
        self.value = f"<fake>:{name}"
        self._factory = factory

    def load(self) -> object:
        return self._factory


class _FakeAdapter:
    def __init__(
        self,
        declared: Mapping[str, AbstractSet[int]],
        *,
        to_apply: list[str] | None = None,
    ) -> None:
        self._declared = dict(declared)
        self._to_apply = to_apply or []
        self.apply_calls = 0

    @property
    def declared_revisions(self) -> Mapping[str, AbstractSet[int]]:
        return self._declared

    async def apply_pending(self) -> list[str]:
        self.apply_calls += 1
        # Simulate that once applied, the adapter advances.
        for iface, revs in self._declared.items():
            self._declared[iface] = set(revs) | set(
                range(1, runner.required_revisions().get(iface, 0) + 1)
            )
        return list(self._to_apply)


def _full_declared() -> dict[str, set[int]]:
    return {iface: set(range(1, rev + 1)) for iface, rev in runner.required_revisions().items()}


# ----- status -----


def test_status_no_adapters_is_clean(monkeypatch: pytest.MonkeyPatch) -> None:
    """With nothing deployed there is nothing to gate, so status is clean.

    The check is scoped to interfaces a deployed adapter owns; an empty
    adapter set owns nothing, so `status` reports no discovered adapters
    and exits 0 rather than listing every required interface as a gap.
    """
    monkeypatch.setattr(cli, "_discover_entry_points", lambda: [])
    out = io.StringIO()
    code = cli.main(["status"], stream=out)
    assert code == 0
    text = out.getvalue()
    assert "No adapters discovered." in text
    assert "All required revisions are present." in text


def test_status_clean_when_revisions_satisfied(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = _FakeAdapter(_full_declared())
    monkeypatch.setattr(
        cli,
        "_discover_entry_points",
        lambda: [_FakeEntryPoint("postgres", lambda: adapter)],
    )
    out = io.StringIO()
    code = cli.main(["status"], stream=out)
    assert code == 0
    assert "All required revisions are present." in out.getvalue()


def test_status_unknown_adapter_name_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = _FakeAdapter(_full_declared())
    monkeypatch.setattr(
        cli,
        "_discover_entry_points",
        lambda: [_FakeEntryPoint("postgres", lambda: adapter)],
    )
    out = io.StringIO()
    code = cli.main(["status", "--adapter", "redis"], stream=out)
    assert code == 1
    assert "no adapter named 'redis'" in out.getvalue()
    assert "postgres" in out.getvalue()  # lists known


# ----- up -----


def test_up_with_no_adapters_exits_nonzero(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli, "_discover_entry_points", lambda: [])
    out = io.StringIO()
    code = cli.main(["up"], stream=out)
    assert code == 1
    assert "No adapters registered" in out.getvalue()


def test_up_applies_pending_then_validates(monkeypatch: pytest.MonkeyPatch) -> None:
    """`up` calls apply_pending and re-checks revisions."""
    # Start with an adapter declaring zero revisions; its apply_pending
    # advances it to the full set.
    adapter = _FakeAdapter(
        {iface: set() for iface in runner.required_revisions()},
        to_apply=["applied rev1", "applied rev2"],
    )
    monkeypatch.setattr(
        cli,
        "_discover_entry_points",
        lambda: [_FakeEntryPoint("postgres", lambda: adapter)],
    )
    out = io.StringIO()
    code = cli.main(["up"], stream=out)
    assert code == 0
    text = out.getvalue()
    assert "[postgres] applying pending migrations..." in text
    assert "applied rev1" in text
    assert "applied rev2" in text
    assert "Migration complete." in text
    assert adapter.apply_calls == 1


def test_up_check_does_not_apply(monkeypatch: pytest.MonkeyPatch) -> None:
    """`--check` reports status without invoking apply_pending."""
    adapter = _FakeAdapter(_full_declared())
    monkeypatch.setattr(
        cli,
        "_discover_entry_points",
        lambda: [_FakeEntryPoint("postgres", lambda: adapter)],
    )
    out = io.StringIO()
    code = cli.main(["up", "--check"], stream=out)
    assert code == 0
    assert adapter.apply_calls == 0
    assert "Already up to date." in out.getvalue()


def test_up_check_reports_gap_with_exit_2(monkeypatch: pytest.MonkeyPatch) -> None:
    adapter = _FakeAdapter({iface: set() for iface in runner.required_revisions()})
    monkeypatch.setattr(
        cli,
        "_discover_entry_points",
        lambda: [_FakeEntryPoint("postgres", lambda: adapter)],
    )
    out = io.StringIO()
    code = cli.main(["up", "--check"], stream=out)
    assert code == 2
    assert adapter.apply_calls == 0
    assert "MigrationRequired" in out.getvalue()


def test_up_rejects_adapter_not_implementing_protocol(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class _Broken:
        pass

    monkeypatch.setattr(
        cli,
        "_discover_entry_points",
        lambda: [_FakeEntryPoint("broken", lambda: _Broken())],
    )
    out = io.StringIO()
    code = cli.main(["up"], stream=out)
    assert code == 1
    assert "does not implement" in out.getvalue()


def test_up_surfaces_factory_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    def _bad_factory() -> object:
        raise RuntimeError("missing CUSTOS_PG_DSN")

    monkeypatch.setattr(
        cli,
        "_discover_entry_points",
        lambda: [_FakeEntryPoint("postgres", _bad_factory)],
    )
    out = io.StringIO()
    code = cli.main(["up"], stream=out)
    assert code == 1
    assert "raised on instantiation" in out.getvalue()
    assert "missing CUSTOS_PG_DSN" in out.getvalue()


# ----- parser shape -----


def test_parser_requires_subcommand() -> None:
    parser = cli.build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([])


def test_parser_has_up_and_status_subcommands() -> None:
    parser = cli.build_parser()
    ns_up = parser.parse_args(["up"])
    assert ns_up.command == "up"
    ns_status = parser.parse_args(["status"])
    assert ns_status.command == "status"
