# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Event history matching for expected vs actual execution events.

Provides pattern matching with placeholders, regex, wildcards, and literal
equality for comparing expected event histories against actual results.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

import yaml

from aws_durable_execution_conformance_tests.variables import PlaceholderContext

# Pattern for placeholder references like ${ID1}, ${Name1}
PLACEHOLDER_PATTERN = re.compile(r"^\$\{(.+)\}$")

# Pattern for regex matchers like ${/pattern/}
REGEX_PATTERN = re.compile(r"^\$\{/(.+)/\}$")


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class MatchResult:
    """Result of comparing expected vs actual event histories."""

    success: bool
    errors: list[str] = field(default_factory=list)
    resolved_placeholders: dict[str, Any] = field(default_factory=dict)

    def __bool__(self) -> bool:
        return self.success


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------


def is_wildcard(value: Any) -> bool:
    """Check if value is '*' (match anything)."""
    return isinstance(value, str) and value == "*"


def is_empty_object(value: Any) -> bool:
    """Check if value is {} (don't care / skip)."""
    return isinstance(value, dict) and len(value) == 0


def get_placeholder_name(value: Any) -> str | None:
    """Extract placeholder name from ${...} pattern, or None.

    Returns None if the value is a regex pattern (${/.../}).
    """
    if not isinstance(value, str):
        return None
    # Don't treat regex patterns as placeholders
    if REGEX_PATTERN.match(value):
        return None
    m = PLACEHOLDER_PATTERN.match(value)
    return m.group(1) if m else None


def get_regex_pattern(value: Any) -> re.Pattern | None:
    """Extract a compiled regex from ${/pattern/} syntax, or None."""
    if not isinstance(value, str):
        return None
    m = REGEX_PATTERN.match(value)
    if not m:
        return None
    return re.compile(m.group(1))


def load_json_file(path: str) -> Any:
    """Load and parse a JSON file."""
    with open(path) as f:
        return json.load(f)


def load_yaml_file(path: str) -> Any:
    """Load and parse a YAML file."""
    with open(path) as f:
        return yaml.safe_load(f)


# ---------------------------------------------------------------------------
# Matcher
# ---------------------------------------------------------------------------


class EventHistoryMatcher:
    """Matches expected event history against actual event history.

    Matching rules:
    - ${Xxx}  : placeholder — all occurrences must resolve to the same actual value
    - ${/re/} : regex — actual value must be a string matching the pattern
    - {}      : empty object means "don't care", field is skipped
    - "*"     : wildcard, any value is accepted
    - Otherwise: literal equality is required

    Expected events are matched to actual events by EventId.
    Extra events in the actual history that are not in the expected list are ignored.
    """

    def __init__(self, context: PlaceholderContext | None = None) -> None:
        self._context: PlaceholderContext = context or PlaceholderContext()
        self._errors: list[str] = []

    @property
    def context(self) -> PlaceholderContext:
        """The placeholder context used by this matcher."""
        return self._context

    def match(
        self,
        expected_events: list[dict[str, Any]],
        actual_events: list[dict[str, Any]],
    ) -> MatchResult:
        """Compare expected events against actual events.

        Args:
            expected_events: list of expected event dicts (may contain placeholders).
            actual_events: list of actual event dicts from the execution.

        Returns:
            MatchResult with success flag, errors, and resolved placeholders.
        """
        self._errors = []

        # Index actual events by EventId for O(1) lookup
        actual_by_id: dict[int, dict[str, Any]] = {}
        for evt in actual_events:
            eid = evt.get("EventId")
            if eid is not None:
                actual_by_id[eid] = evt

        for expected in expected_events:
            eid = expected.get("EventId")
            if eid is None:
                self._errors.append(f"Expected event missing EventId: {expected}")
                continue

            actual = actual_by_id.get(eid)
            if actual is None:
                self._errors.append(f"No actual event found for EventId={eid}")
                continue

            self._match_value(expected, actual, path=f"Event[EventId={eid}]")

        return MatchResult(
            success=len(self._errors) == 0,
            errors=list(self._errors),
            resolved_placeholders=self._context.bindings,
        )

    # ------------------------------------------------------------------
    # Internal recursive matching
    # ------------------------------------------------------------------

    def _match_value(self, expected: Any, actual: Any, path: str) -> None:
        """Recursively match an expected value against an actual value."""
        # Rule: empty object {} → don't care
        if is_empty_object(expected):
            return

        # Rule: "*" → any value
        if is_wildcard(expected):
            return

        # Rule: ${Placeholder}
        placeholder = get_placeholder_name(expected)
        if placeholder is not None:
            self._resolve_placeholder(placeholder, actual, path)
            return

        # Rule: ${/regex/flags}
        regex = get_regex_pattern(expected)
        if regex is not None:
            if not regex.search(str(actual)):
                self._errors.append(f"{path}: value {actual!r} does not match regex pattern {regex.pattern!r}")
            return

        # Both dicts → recurse on expected keys only
        if isinstance(expected, dict) and isinstance(actual, dict):
            for key in expected:
                if key not in actual:
                    self._errors.append(f"{path}.{key}: key missing in actual event")
                    continue
                self._match_value(expected[key], actual[key], f"{path}.{key}")
            return

        # Both lists → match element-wise
        if isinstance(expected, list) and isinstance(actual, list):
            if len(expected) != len(actual):
                self._errors.append(f"{path}: list length mismatch (expected {len(expected)}, got {len(actual)})")
                return
            for i, (e, a) in enumerate(zip(expected, actual, strict=False)):
                self._match_value(e, a, f"{path}[{i}]")
            return

        # Literal comparison — substitute known placeholders first
        resolved_expected: Any = expected
        if isinstance(expected, str):
            resolved_expected = self._context.substitute(expected)
        if resolved_expected != actual:
            self._errors.append(f"{path}: expected {resolved_expected!r}, got {actual!r}")

    def _resolve_placeholder(self, name: str, actual_value: Any, path: str) -> None:
        """Bind or verify a placeholder value."""
        if self._context.has(name):
            if self._context.get(name) != actual_value:
                self._errors.append(
                    f"{path}: placeholder ${{{name}}} was previously "
                    f"bound to {self._context.get(name)!r}, "
                    f"but got {actual_value!r}"
                )
        else:
            self._context.bind(name, actual_value)
