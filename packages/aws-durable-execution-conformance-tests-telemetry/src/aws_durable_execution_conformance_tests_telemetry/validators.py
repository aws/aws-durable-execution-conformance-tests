# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Pure assertion logic for Durable Execution Telemetry records. No AWS calls."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from aws_durable_execution_conformance_tests.history import get_regex_pattern
from aws_durable_execution_conformance_tests_telemetry.model import TelemetryRecord

_MISSING = object()

# The only key an entry of ``TelemetryAssertions.records`` may carry.
_ENTRY_KEYS = frozenset({"record"})


def validate_record(data: Mapping[str, Any], assertions: Mapping[str, Any]) -> list[str]:
    """Check one record against one entry of ``TelemetryAssertions.records``.

    ``entry.record`` is the complete expected record. It lists every key the
    record must carry, in the order the record must carry them, at every
    nesting level. A key the record carries but the entry does not list is an
    error, because a key one SDK emits and another does not is a cross-SDK
    difference, and this suite exists to detect such differences. Expected
    values follow the rules of :func:`_expectation_errors`. Error strings use
    the form ``<json path>: expected <x>, got <y>``.
    """
    errors: list[str] = []

    unknown = [key for key in assertions if key not in _ENTRY_KEYS]
    if unknown:
        errors.append(f"TelemetryAssertions: unknown entry keys {unknown}")

    record_assertions = assertions.get("record", _MISSING)
    if not isinstance(record_assertions, Mapping):
        errors.append("TelemetryAssertions.record: expected a mapping")
        return errors

    errors.extend(_expectation_errors(record_assertions, data, path=""))
    return errors


def validate_records(records: Sequence[TelemetryRecord], expected_records: Any) -> list[str]:
    """Match fetched records positionally against ``TelemetryAssertions.records``.

    Entry ``i`` of ``expected_records`` is compared to the ``i``-th fetched
    record, in retriever order. The counts must match; when they differ the
    count error is the only error, because positional matching is meaningless.
    Otherwise every record is checked with :func:`validate_record` and each
    error is prefixed ``record[i]:``. An empty list with no fetched records
    passes.
    """
    if not isinstance(expected_records, list):
        return ["TelemetryAssertions.records: expected a list"]

    if len(records) != len(expected_records):
        return [f"records: expected {len(expected_records)} record(s), got {len(records)}"]

    errors: list[str] = []
    for index, (record, entry) in enumerate(zip(records, expected_records, strict=True)):
        prefix = f"record[{index}]: "
        if not isinstance(entry, Mapping):
            errors.append(f"{prefix}TelemetryAssertions.records[{index}]: expected a mapping")
            continue
        errors.extend(prefix + error for error in validate_record(record.data, entry))
    return errors


def _is_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))


def _at(path: str, message: str) -> str:
    # The top-level record has no path of its own, so its messages carry no prefix.
    return f"{path}: {message}" if path else message


def _child(path: str, key: str) -> str:
    return f"{path}.{key}" if path else key


def _expectation_errors(expected: Any, actual: Any, *, path: str) -> list[str]:
    """Compare one expected value against one actual value, recursively.

    The scalar rules are those of the core history matcher and the OTel suite.
    The mapping rule is stricter, so that the expected value describes the
    exact shape of the record:

    - ``'*'``: the field must be present; any value is accepted.
    - ``${/regex/}``: ``str(actual)`` must match the pattern.
    - ``{$integer: {min: N}}``: ``actual`` must be a JSON integer, so a
      number with no fraction and not a boolean, string, or null. ``min``
      is optional and inclusive.
    - mapping: ``actual`` must be a mapping with exactly the expected keys,
      in the expected order. Every key is compared recursively. Keys that
      ``actual`` carries but ``expected`` does not list are reported as
      ``unexpected keys``. The order is checked only when the key sets are
      equal, so one defect produces one error.
    - sequence: ``actual`` must be a sequence of the same length; items are
      compared pairwise.
    - anything else: equality. A boolean matches only a boolean, and a
      number matches only a number of the same JSON type, so ``1`` does not
      match ``1.0``.
    """
    if actual is _MISSING:
        if isinstance(expected, Mapping) and not _is_rule(expected):
            return [_at(path, f"expected mapping with keys {list(expected)}, got <missing>")]
        return [_at(path, f"expected {expected!r}, got <missing>")]
    if expected == "*":
        return []
    pattern = get_regex_pattern(expected)
    if pattern is not None:
        if pattern.search(str(actual)):
            return []
        return [_at(path, f"value {actual!r} does not match regex pattern {pattern.pattern!r}")]
    if _is_rule(expected):
        return _integer_errors(expected["$integer"], actual, path=path)
    if isinstance(expected, Mapping):
        if not isinstance(actual, Mapping):
            return [_at(path, f"expected mapping, got {actual!r}")]
        errors: list[str] = []
        for key, expected_value in expected.items():
            errors.extend(_expectation_errors(expected_value, actual.get(key, _MISSING), path=_child(path, key)))
        unexpected = [key for key in actual if key not in expected]
        if unexpected:
            errors.append(_at(path, f"unexpected keys {unexpected}"))
        elif len(actual) == len(expected) and list(actual) != list(expected):
            errors.append(_at(path, f"expected key order {list(expected)}, got {list(actual)}"))
        return errors
    if _is_sequence(expected):
        if not _is_sequence(actual):
            return [_at(path, f"expected sequence, got {actual!r}")]
        if len(expected) != len(actual):
            return [_at(path, f"expected {len(expected)} item(s), got {len(actual)}")]
        errors = []
        for index, (expected_item, actual_item) in enumerate(zip(expected, actual, strict=True)):
            errors.extend(_expectation_errors(expected_item, actual_item, path=f"{path}[{index}]"))
        return errors
    if not _equal(expected, actual):
        return [_at(path, f"expected {expected!r}, got {actual!r}")]
    return []


def _is_rule(expected: Any) -> bool:
    # ``{$integer: ...}`` is a rule, not a mapping the record must carry.
    return isinstance(expected, Mapping) and set(expected) == {"$integer"}


def _integer_errors(constraints: Any, actual: Any, *, path: str) -> list[str]:
    constraints = constraints or {}
    if not isinstance(constraints, Mapping):
        return [_at(path, "$integer constraints must be a mapping")]
    # json.loads yields int only for a JSON integer literal. bool is an int
    # subclass in Python, so it is excluded explicitly.
    if isinstance(actual, bool) or not isinstance(actual, int):
        return [_at(path, f"expected an integer, got {actual!r}")]
    minimum = constraints.get("min")
    if minimum is not None and actual < minimum:
        return [_at(path, f"expected an integer >= {minimum}, got {actual!r}")]
    return []


def _equal(expected: Any, actual: Any) -> bool:
    # bool is an int subclass in Python. YAML ``count: 1`` must not match
    # JSON ``true``, so booleans compare only with booleans.
    if isinstance(expected, bool) or isinstance(actual, bool):
        return isinstance(expected, bool) and isinstance(actual, bool) and expected is actual
    # json.loads yields int for a JSON integer literal and float for a literal
    # with a fraction or exponent. Python evaluates ``1 == 1.0`` as true, but
    # an SDK that writes ``1.0`` where another writes ``1`` differs. So a
    # number matches only a number of the same type.
    if isinstance(expected, (int, float)) or isinstance(actual, (int, float)):
        return type(expected) is type(actual) and expected == actual
    return bool(expected == actual)
