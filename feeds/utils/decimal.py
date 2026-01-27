from __future__ import annotations

from decimal import Decimal, getcontext

getcontext().prec = 38


def to_decimal(value) -> Decimal:
    """Konvertiert Zahlen sicher in Decimal."""
    if isinstance(value, Decimal):
        return value
    if isinstance(value, (int, float, str)):
        return Decimal(str(value))
    raise TypeError(f"Kann Wert nicht in Decimal umwandeln: {type(value)!r}")
