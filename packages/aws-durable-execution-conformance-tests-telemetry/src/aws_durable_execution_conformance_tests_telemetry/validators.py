# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Pure assertion logic for Durable Execution Telemetry records. No AWS calls."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from aws_durable_execution_conformance_tests.history import get_regex_pattern
from aws_durable_execution_conformance_tests_telemetry.model import (
    ExpectedExecution,
    TelemetryRecord,
)

_MISSING = object()

# Record fields whose expected value comes from the execution ARN rather than
# from the requirement YAML.
_ARN_FIELDS = (
    ("executionArn", "execution_arn"),
    ("functionName", "function_name"),
    ("functionQualifier", "qualifier"),
    ("region", "region"),
    ("accountId", "account_id"),
)


def validate_record(
    data: Mapping[str, Any],
    assertions: Mapping[str, Any],
    expected: ExpectedExecution,
) -> list[str]:
    """Check one record against one entry of ``TelemetryAssertions.records``.

    Every check runs against this record alone. Expected values follow the
    rules of the core history matcher, see :func:`_expectation_errors`.
    Unknown top-level and per-operation keys are ignored, so additive schema
    changes do not fail the check. Error strings use the form
    ``<json path>: expected <x>, got <y>``.
    """
    errors: list[str] = []

    record_assertions = assertions.get("record", {})
    if not isinstance(record_assertions, Mapping):
        errors.append("TelemetryAssertions.record: expected a mapping")
    else:
        for key, expected_value in record_assertions.items():
            errors.extend(_expectation_errors(expected_value, data.get(key, _MISSING), path=key))

    for key, attribute in _ARN_FIELDS:
        errors.extend(_expectation_errors(getattr(expected, attribute), data.get(key, _MISSING), path=key))

    forbid_fields = assertions.get("forbid_fields", ())
    if isinstance(forbid_fields, str):
        forbid_fields = (forbid_fields,)
    for key in forbid_fields:
        if key in data:
            errors.append(f"{key}: expected absent, got {data[key]!r}")

    expected_operations = assertions.get("operations_by_name", _MISSING)
    if expected_operations is not _MISSING:
        errors.extend(_validate_operations(data.get("operationsByName", _MISSING), expected_operations))

    return errors


def validate_records(
    records: Sequence[TelemetryRecord],
    expected_records: Any,
    expected: ExpectedExecution,
) -> list[str]:
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
        errors.extend(prefix + error for error in validate_record(record.data, entry, expected))
    return errors


def _validate_operations(actual: Any, expected_operations: Any) -> list[str]:
    if not isinstance(expected_operations, Mapping):
        return ["TelemetryAssertions.operations_by_name: expected a mapping"]
    if actual is _MISSING:
        return [f"operationsByName: expected mapping with keys {sorted(expected_operations)}, got <missing>"]
    if not isinstance(actual, Mapping):
        return [f"operationsByName: expected mapping with keys {sorted(expected_operations)}, got {actual!r}"]

    errors: list[str] = []
    expected_keys = set(expected_operations)
    actual_keys = set(actual)
    if expected_keys != actual_keys:
        errors.append(f"operationsByName: expected keys {sorted(expected_keys)}, got {sorted(actual_keys)}")

    for name in sorted(expected_keys & actual_keys):
        path = f"operationsByName.{name}"
        expected_summary = expected_operations[name]
        summary = actual[name]
        if not isinstance(expected_summary, Mapping):
            errors.append(f"TelemetryAssertions.operations_by_name.{name}: expected a mapping")
            continue
        if not isinstance(summary, Mapping):
            errors.append(f"{path}: expected mapping, got {summary!r}")
            continue
        for field, expected_value in expected_summary.items():
            errors.extend(_expectation_errors(expected_value, summary.get(field, _MISSING), path=f"{path}.{field}"))
        if "error" in summary:
            errors.append(f"{path}.error: expected absent, got {summary['error']!r}")
    return errors


def _is_sequence(value: Any) -> bool:
    return isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray))


def _expectation_errors(expected: Any, actual: Any, *, path: str) -> list[str]:
    """Compare one expected value against one actual value, recursively.

    The rules are those of the core history matcher and the OTel suite:

    - ``'*'``: the field must be present; any value is accepted.
    - ``${/regex/}``: ``str(actual)`` must match the pattern.
    - mapping: ``actual`` must be a mapping; every expected key must be
      present and is compared recursively. Extra actual keys are ignored.
    - sequence: ``actual`` must be a sequence of the same length; items are
      compared pairwise.
    - anything else: equality, with booleans compared only to booleans.
    """
    if actual is _MISSING:
        return [f"{path}: expected {expected!r}, got <missing>"]
    if expected == "*":
        return []
    pattern = get_regex_pattern(expected)
    if pattern is not None:
        if pattern.search(str(actual)):
            return []
        return [f"{path}: value {actual!r} does not match regex pattern {pattern.pattern!r}"]
    if isinstance(expected, Mapping):
        if not isinstance(actual, Mapping):
            return [f"{path}: expected mapping, got {actual!r}"]
        errors: list[str] = []
        for key, expected_value in expected.items():
            errors.extend(_expectation_errors(expected_value, actual.get(key, _MISSING), path=f"{path}.{key}"))
        return errors
    if _is_sequence(expected):
        if not _is_sequence(actual):
            return [f"{path}: expected sequence, got {actual!r}"]
        if len(expected) != len(actual):
            return [f"{path}: expected {len(expected)} item(s), got {len(actual)}"]
        errors = []
        for index, (expected_item, actual_item) in enumerate(zip(expected, actual, strict=True)):
            errors.extend(_expectation_errors(expected_item, actual_item, path=f"{path}[{index}]"))
        return errors
    if not _equal(expected, actual):
        return [f"{path}: expected {expected!r}, got {actual!r}"]
    return []


def _equal(expected: Any, actual: Any) -> bool:
    # bool is an int subclass in Python. YAML ``count: 1`` must not match
    # JSON ``true``, so booleans compare only with booleans.
    if isinstance(expected, bool) or isinstance(actual, bool):
        return isinstance(expected, bool) and isinstance(actual, bool) and expected is actual
    return bool(expected == actual)
