# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unified placeholder context for test description variable resolution.

Provides a single PlaceholderContext that manages all placeholder bindings
across the test lifecycle: variable generation, input substitution, history
matching, and log validation.

Usage:
    ctx = PlaceholderContext()
    ctx.resolve_variables({"RUN_ID": "${GEN_STR:8}"})
    input_value = ctx.substitute("${RUN_ID}")
    # Later, history matcher binds ${ID1} -> "abc-123"
    ctx.bind("ID1", "abc-123")
    # Log patterns can reference any bound placeholder
    pattern = ctx.substitute("${RUN_ID}")
"""

from __future__ import annotations

import re
import string
from secrets import choice
from typing import Any

# Pattern for generator expressions: ${GEN_STR:length}
_GEN_STR_PATTERN: re.Pattern[str] = re.compile(r"^\$\{GEN_STR:(\d+)\}$")

# Pattern for placeholder references in strings: ${NAME}
# Matches ${NAME} where NAME is alphanumeric + underscores.
_PLACEHOLDER_REF_PATTERN: re.Pattern[str] = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")

# Characters used for random string generation
_ALPHANUMERIC: str = string.ascii_lowercase + string.digits


def _generate_random_string(length: int) -> str:
    """Generate a cryptographically random alphanumeric string.

    Args:
        length: Number of characters to generate.

    Returns:
        A random string of lowercase letters and digits.
    """
    return "".join(choice(_ALPHANUMERIC) for _ in range(length))


class PlaceholderContext:
    """Manages placeholder bindings across the test validation lifecycle.

    A single instance is shared across variable generation, input
    substitution, history matching, and log validation. This ensures
    all phases operate on the same set of resolved values.

    Attributes:
        bindings: The current name -> value mapping of all placeholders.
    """

    def __init__(self) -> None:
        self._bindings: dict[str, Any] = {}

    @property
    def bindings(self) -> dict[str, Any]:
        """Read-only view of current placeholder bindings."""
        return dict(self._bindings)

    def bind(self, name: str, value: Any) -> None:
        """Bind a placeholder name to a value.

        Args:
            name: The placeholder name (without ${} wrapper).
            value: The value to bind.

        Raises:
            ValueError: If the name is already bound to a different value.
        """
        if name in self._bindings:
            if self._bindings[name] != value:
                msg: str = (
                    f"Placeholder ${{{name}}} already bound to {self._bindings[name]!r}, cannot rebind to {value!r}"
                )
                raise ValueError(msg)
            return
        self._bindings[name] = value

    def get(self, name: str) -> Any | None:
        """Get the value bound to a placeholder name, or None."""
        return self._bindings.get(name)

    def has(self, name: str) -> bool:
        """Check if a placeholder name is bound."""
        return name in self._bindings

    def resolve_variables(self, variables: dict[str, str] | None) -> None:
        """Process the Variables field from a YAML test spec.

        For each variable, if the value matches a generator expression
        like ${GEN_STR:8}, generates the value and binds it. Otherwise,
        binds the literal value.

        Args:
            variables: The Variables dict from the YAML spec, mapping
                variable names to generator expressions or literal values.
        """
        if not variables:
            return

        for name, value in variables.items():
            if not isinstance(value, str):
                self._bindings[name] = str(value)
                continue

            gen_match: re.Match[str] | None = _GEN_STR_PATTERN.match(value)
            if gen_match:
                length: int = int(gen_match.group(1))
                self._bindings[name] = _generate_random_string(length)
            else:
                self._bindings[name] = value

    def substitute(self, data: Any) -> Any:
        """Recursively substitute ${NAME} placeholders in data structures.

        Only substitutes placeholders whose names are currently bound.
        Unrecognized placeholders are left untouched (they may be resolved
        later by the history matcher).

        Args:
            data: The data structure to process (str, dict, list, or scalar).

        Returns:
            The data structure with known placeholders substituted.
        """
        if not self._bindings:
            return data

        if isinstance(data, str):
            return self._substitute_string(data)

        if isinstance(data, dict):
            return {key: self.substitute(val) for key, val in data.items()}

        if isinstance(data, list):
            return [self.substitute(item) for item in data]

        return data

    def _substitute_string(self, text: str) -> str:
        """Substitute placeholders in a single string.

        Only substitutes placeholders whose names exist in bindings.
        Other ${...} patterns are preserved for downstream processing.

        Args:
            text: The string potentially containing ${NAME} placeholders.

        Returns:
            The string with known placeholders replaced.
        """

        def _replacer(match: re.Match[str]) -> str:
            name: str = match.group(1)
            if name in self._bindings:
                return str(self._bindings[name])
            return match.group(0)

        return _PLACEHOLDER_REF_PATTERN.sub(_replacer, text)
