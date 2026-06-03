"""Kubernetes resource-quantity parsing and comparison (ARM-IMPL-008).

The Resource Limiter reasons about CPU, memory, and ephemeral-storage values
that arrive as Kubernetes *quantity* strings (``250m``, ``1``, ``256Mi``,
``2Gi``). To enforce the "each layer can only tighten within the layer above"
rule the limiter must compare and clamp these values, which means parsing them
into a single canonical numeric space.

This module parses the quantity grammar Custos accepts — a decimal number with
an optional binary (``Ki``/``Mi``/``Gi``/…) or decimal SI (``m``/``k``/``M``/
``G``/…) suffix — into an exact :class:`fractions.Fraction` of base units
(cores for CPU, bytes for memory/storage). Fractions keep the arithmetic exact
so ``250m`` and ``0.25`` compare equal and clamping never drifts.

Only the suffix subset Kubernetes itself documents is accepted; anything else
is a loud :class:`ValueError`, surfaced by the limiter as a configuration /
manifest error rather than silently coerced.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from fractions import Fraction
from typing import Final

__all__ = ["Quantity"]

#: Binary (power-of-two) suffixes → multiplier in base units.
_BINARY_SUFFIXES: Final[dict[str, int]] = {
    "Ki": 1024**1,
    "Mi": 1024**2,
    "Gi": 1024**3,
    "Ti": 1024**4,
    "Pi": 1024**5,
    "Ei": 1024**6,
}

#: Decimal (power-of-ten) suffixes → multiplier in base units. ``m`` is the
#: milli suffix (one-thousandth); the rest are the SI multiples Kubernetes
#: accepts.
_DECIMAL_SUFFIXES: Final[dict[str, Fraction]] = {
    "m": Fraction(1, 1000),
    "k": Fraction(1000),
    "M": Fraction(1000**2),
    "G": Fraction(1000**3),
    "T": Fraction(1000**4),
    "P": Fraction(1000**5),
    "E": Fraction(1000**6),
}

#: A signless decimal number with an optional suffix. The number itself is a
#: plain decimal (``1``, ``0.25``, ``250``) — exponent notation is not part of
#: the quantity grammar Custos accepts.
_QUANTITY_PATTERN: Final[re.Pattern[str]] = re.compile(
    r"^(?P<number>\d+(?:\.\d+)?)(?P<suffix>[a-zA-Z]+)?$"
)


def _multiplier(suffix: str) -> Fraction:
    if suffix in _BINARY_SUFFIXES:
        return Fraction(_BINARY_SUFFIXES[suffix])
    if suffix in _DECIMAL_SUFFIXES:
        return _DECIMAL_SUFFIXES[suffix]
    raise ValueError(
        f"unknown resource-quantity suffix {suffix!r}; expected a binary "
        f"({'/'.join(_BINARY_SUFFIXES)}) or decimal ({'/'.join(_DECIMAL_SUFFIXES)}) suffix"
    )


@dataclass(frozen=True, slots=True, eq=False)
class Quantity:
    """An exact, comparable Kubernetes resource quantity in base units.

    A quantity is stored as a :class:`~fractions.Fraction` of base units
    (cores for CPU, bytes for memory/storage) so that values written with
    different suffixes compare exactly — ``Quantity.parse("250m")`` equals
    ``Quantity.parse("0.25")`` and ``Quantity.parse("1Gi")`` is strictly
    greater than ``Quantity.parse("1000Mi")``.

    Instances are immutable (a frozen dataclass — assignment raises at
    runtime), hashable, and totally ordered; ``str(q)`` returns the original
    source text so a clamped value round-trips back to the same suffix the
    operator wrote.
    """

    _value: Fraction
    _source: str

    @classmethod
    def parse(cls, raw: str) -> Quantity:
        """Parse a Kubernetes quantity string into base units.

        Args:
            raw: A quantity such as ``"250m"``, ``"1"``, ``"256Mi"``.

        Returns:
            The parsed :class:`Quantity`.

        Raises:
            ValueError: ``raw`` is empty, malformed, negative, or carries an
                unrecognised suffix.
        """
        text = raw.strip()
        match = _QUANTITY_PATTERN.match(text)
        if match is None:
            raise ValueError(
                f"invalid resource quantity {raw!r}; expected a decimal number "
                "with an optional binary/decimal suffix (e.g. '250m', '1', '256Mi')"
            )
        number = Fraction(match.group("number"))
        suffix = match.group("suffix")
        value = number * _multiplier(suffix) if suffix else number
        return cls(value, text)

    def __str__(self) -> str:
        return self._source

    def __repr__(self) -> str:
        return f"Quantity({self._source!r})"

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Quantity):
            return NotImplemented
        return self._value == other._value

    def __lt__(self, other: Quantity) -> bool:
        return self._value < other._value

    def __le__(self, other: Quantity) -> bool:
        return self._value <= other._value

    def __gt__(self, other: Quantity) -> bool:
        return self._value > other._value

    def __ge__(self, other: Quantity) -> bool:
        return self._value >= other._value

    def __hash__(self) -> int:
        return hash(self._value)
