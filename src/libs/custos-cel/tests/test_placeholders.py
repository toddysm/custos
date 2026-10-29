"""Shared placeholder scanner preserves the existing Workflow lexical contract."""

import pytest

from custos_cel.placeholders import extract_placeholders


@pytest.mark.parametrize(
    ("text", "inners"),
    [
        ("plain", []),
        (r"\${{ literal }}", []),
        ("${{ event.data.value }}", [" event.data.value "]),
        ("prefix-${{ 1 }}-${{ 2 }}", [" 1 ", " 2 "]),
        ("${{ {'key': '}}'}.key }}", [" {'key': '}}'}.key "]),
    ],
)
def test_shared_scanner_preserves_order_and_offsets(text: str, inners: list[str]) -> None:
    """Quoted braces, maps and escaped openers do not change expression boundaries."""
    segments = extract_placeholders(text)
    assert [segment.inner for segment in segments] == inners
    assert all(text[segment.start : segment.end] == segment.token for segment in segments)


def test_unterminated_placeholder_is_explicit() -> None:
    """Malformed templates cannot silently become literal workflow inputs."""
    with pytest.raises(ValueError, match="unterminated"):
        extract_placeholders("${{ event.data.value")
