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

# Directive that JSON-decodes the actual (string) value before matching the
# wrapped expected structure against the decoded value, e.g. an expected
# ``Payload: {"${JSON}": {<envelope fields>}}`` decodes the actual Payload
# string and matches the envelope fields against it.
# It exists because container payloads (e.g. a DAG's ContextSucceeded result)
# are carried on the wire as an opaque JSON *string*; without decoding, the
# whole payload could only be compared with a single literal string, so no
# nested field could be pinned while wildcarding just the variable ones
# (timestamps, language-specific error values). The wrapped value is matched
# with the ordinary rules (``*``, ``${...}``, ``${/re/}``, literals, ``${ABSENT}``).
JSON_DECODE_KEY = "${JSON}"

# Directive asserting a mapping key is ABSENT from the actual object, e.g.
#   tasks: ${ABSENT}
# The ordinary matcher recurses on expected keys only and silently ignores
# extra actual keys, so it cannot express "this field must not be present".
# ``${ABSENT}`` fills that gap: it is the only way to pin that a field was
# dropped (e.g. the offloaded DAG envelope omits ``tasks``; the converged
# envelope no longer carries ``completedCount`` / ``terminalTaskNames`` /
# ``summary``). It is meaningful only as a mapping *value*.
ABSENT_DIRECTIVE = "${ABSENT}"

# Sentinel distinguishing "no ${JSON} directive" from a wrapped spec that is
# itself falsy (e.g. an empty object).
_NO_JSON_SPEC = object()


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


def get_json_decode_spec(value: Any) -> Any:
    """Return the wrapped spec of a ``{"${JSON}": <spec>}`` directive.

    Returns the special :data:`_NO_JSON_SPEC` sentinel when ``value`` is not a
    JSON-decode directive, so a wrapped spec that is itself falsy (``{}``,
    ``[]``, ``null``) is still handled.
    """
    if isinstance(value, dict) and len(value) == 1 and JSON_DECODE_KEY in value:
        return value[JSON_DECODE_KEY]
    return _NO_JSON_SPEC


def is_absent_directive(value: Any) -> bool:
    """Check if value is the ``${ABSENT}`` key-absence directive."""
    return isinstance(value, str) and value == ABSENT_DIRECTIVE


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
    - {"${JSON}": <spec>} : JSON-decode the actual string value, then match <spec>
      against the decoded structure with these same rules
    - ${ABSENT} (as a mapping value) : assert the key is NOT present in actual
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

        # Rule: {"${JSON}": <spec>} → decode the actual JSON string, then match
        # <spec> against the decoded structure. Lets a payload carried on the
        # wire as an opaque JSON string be pinned field-by-field.
        json_spec = get_json_decode_spec(expected)
        if json_spec is not _NO_JSON_SPEC:
            if not isinstance(actual, str):
                self._errors.append(
                    f"{path}: ${{JSON}} expects a JSON string to decode, got {type(actual).__name__}"
                )
                return
            try:
                decoded = json.loads(actual)
            except (json.JSONDecodeError, ValueError) as exc:
                self._errors.append(f"{path}: value is not valid JSON ({exc}): {actual!r}")
                return
            self._match_value(json_spec, decoded, f"{path}<json>")
            return

        # Both dicts → recurse on expected keys only
        if isinstance(expected, dict) and isinstance(actual, dict):
            for key in expected:
                expected_child = expected[key]
                # ${ABSENT}: the key must NOT appear in the actual object.
                if is_absent_directive(expected_child):
                    if key in actual:
                        self._errors.append(
                            f"{path}.{key}: expected key to be ABSENT, but it is present "
                            f"with value {actual[key]!r}"
                        )
                    continue
                if key not in actual:
                    self._errors.append(f"{path}.{key}: key missing in actual event")
                    continue
                self._match_value(expected_child, actual[key], f"{path}.{key}")
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
