"""Formula metrics: arithmetic over statement columns and the latest price.

A comparison metric that starts with ``=`` is a formula. Columns are written
in brackets as ``Table.Column`` (a ``]`` inside a name is doubled), between
numbers, ``+ - * /`` and parentheses, with ``*`` and ``/`` binding first:
``=[Stock_Prices.Price] / [ShareMetrics.Basic earnings (loss) per share]``.
``Stock_Prices.Price`` is the latest split-adjusted price; every other column
is its latest reported value. A missing value or a division by zero leaves
the result empty, as it does in the screener.
"""

from __future__ import annotations

import math
import re
from collections.abc import Callable

PRICE_REF = "Stock_Prices.Price"
_MAX_TOKENS = 60
_NUMBER = re.compile(r"\d+(?:\.\d*)?(?:[eE][+-]?\d+)?|\.\d+(?:[eE][+-]?\d+)?")

Token = tuple[str, str | float]


def _tokenize(text: str) -> list[Token] | None:
    tokens: list[Token] = []
    position = 0
    while position < len(text):
        char = text[position]
        if char.isspace():
            position += 1
        elif char == "[":
            end = position + 1
            name: list[str] = []
            while True:
                close = text.find("]", end)
                if close < 0:
                    return None
                name.append(text[end:close])
                if text.startswith("]]", close):
                    name.append("]")
                    end = close + 2
                    continue
                position = close + 1
                break
            reference = "".join(name)
            table, separator, column = reference.partition(".")
            if not separator or not table.strip() or not column.strip():
                return None
            tokens.append(("column", reference))
        elif char in "+-*/":
            # A sign where a term is expected belongs to the number after it.
            expects_term = not tokens or tokens[-1][0] == "op" or tokens[-1] == ("paren", "(")
            match = _NUMBER.match(text, position + 1) if char == "-" and expects_term else None
            if match:
                tokens.append(("number", -float(match.group())))
                position = match.end()
            else:
                tokens.append(("op", char))
                position += 1
        elif char in "()":
            tokens.append(("paren", char))
            position += 1
        else:
            match = _NUMBER.match(text, position)
            if not match:
                return None
            tokens.append(("number", float(match.group())))
            position = match.end()
    return tokens


def _well_formed(tokens: list[Token]) -> bool:
    """The screener's expression grammar: terms and operators alternate, parentheses match."""
    expect_term = True
    depth = 0
    for kind, value in tokens:
        if kind in ("column", "number"):
            if not expect_term:
                return False
            expect_term = False
        elif kind == "op":
            if expect_term:
                return False
            expect_term = True
        elif value == "(":
            if not expect_term:
                return False
            depth += 1
        else:
            if expect_term or depth == 0:
                return False
            depth -= 1
            expect_term = False
    return not expect_term and depth == 0


def parse_formula(metric: str) -> list[Token] | None:
    """The tokens of a formula metric, or ``None`` when *metric* is not a well-formed one."""
    if not isinstance(metric, str) or not metric.startswith("="):
        return None
    tokens = _tokenize(metric[1:])
    if not tokens or len(tokens) > _MAX_TOKENS or not _well_formed(tokens):
        return None
    if not any(kind == "column" for kind, _ in tokens):
        return None
    return tokens


def formula_columns(tokens: list[Token]) -> list[str]:
    """The ``Table.Column`` references a formula reads, in first-use order."""
    return list(dict.fromkeys(str(value) for kind, value in tokens if kind == "column"))


def evaluate_formula(tokens: list[Token], value_of: Callable[[str], float | None]) -> float | None:
    """The formula's value, reading each column through *value_of*."""
    position = 0

    def peek() -> Token | None:
        return tokens[position] if position < len(tokens) else None

    def factor() -> float | None:
        nonlocal position
        kind, value = tokens[position]
        position += 1
        if kind == "paren":
            inner = expression()
            position += 1  # the closing parenthesis
            return inner
        if kind == "number":
            return float(value)
        found = value_of(str(value))
        return float(found) if isinstance(found, (int, float)) and not isinstance(found, bool) else None

    def term() -> float | None:
        nonlocal position
        result = factor()
        while peek() in (("op", "*"), ("op", "/")):
            operator = tokens[position][1]
            position += 1
            right = factor()
            if result is None or right is None:
                result = None
            elif operator == "*":
                result = result * right
            else:
                result = result / right if right != 0 else None
        return result

    def expression() -> float | None:
        nonlocal position
        result = term()
        while peek() in (("op", "+"), ("op", "-")):
            operator = tokens[position][1]
            position += 1
            right = term()
            if result is None or right is None:
                result = None
            else:
                result = result + right if operator == "+" else result - right
        return result

    result = expression()
    return result if result is not None and math.isfinite(result) else None
