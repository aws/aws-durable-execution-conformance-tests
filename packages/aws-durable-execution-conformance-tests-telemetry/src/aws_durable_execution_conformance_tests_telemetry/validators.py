# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Pure assertion logic for Durable Execution Telemetry records. No AWS calls."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from aws_durable_execution_conformance_tests_telemetry.model import (
    ExpectedExecution,
    TelemetryRecord,
)

_MISSING = object()

# Record fields whose expected value comes from the execution ARN rather than
# from the requirement YAML.
_ARN_FIELDS = (
    ("functionName", "function_name"),
    ("functionQualifier", "qualifier"),
    ("region", "region"),
    ("accountId", "account_id"),
)

# Per-operation summary fields compared for equality when listed in the YAML.
_OPERATION_FIELDS = ("type", "status", "count", "failedCount")


def validate_record(
    data: Mapping[str, Any],
    assertions: Mapping[str, Any],
    expected: ExpectedExecution,
) -> list[str]:
    """Check one record against the requirement's ``TelemetryAssertions``.

    Every check runs against this record alone. Unknown top-level and
    per-operation keys are ignored, so additive schema changes do not fail
    the check. Error strings use the form ``<json path>: expected <x>, got <y>``.
    """
    errors: list[str] = []

    record_assertions = assertions.get("record", {})
    if not isinstance(record_assertions, Mapping):
        errors.append("TelemetryAssertions.record: expected a mapping")
    else:
        for key, expected_value in record_assertions.items():
            _check_equal(errors, key, expected_value, data.get(key, _MISSING))

    for key, attribute in _ARN_FIELDS:
        _check_equal(errors, key, getattr(expected, attribute), data.get(key, _MISSING))

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


def select_passing_record(
    records: Sequence[TelemetryRecord],
    assertions: Mapping[str, Any],
    expected: ExpectedExecution,
) -> tuple[TelemetryRecord | None, list[str]]:
    """Evaluate each record independently.

    Returns ``(record, [])`` for the first record with no errors. Otherwise
    returns the best candidate and its errors. The best candidate has the
    fewest errors; a tie goes to the latest event timestamp. With no records
    the result is ``(None, [])``.
    """
    best: TelemetryRecord | None = None
    best_errors: list[str] = []
    for record in records:
        errors = validate_record(record.data, assertions, expected)
        if not errors:
            return record, []
        if best is None or _is_better(record, errors, best, best_errors):
            best = record
            best_errors = errors
    return best, best_errors


def _is_better(
    candidate: TelemetryRecord,
    candidate_errors: list[str],
    best: TelemetryRecord,
    best_errors: list[str],
) -> bool:
    if len(candidate_errors) != len(best_errors):
        return len(candidate_errors) < len(best_errors)
    return _timestamp(candidate) > _timestamp(best)


def _timestamp(record: TelemetryRecord) -> float:
    value = record.event.get("timestamp", 0)
    return float(value) if isinstance(value, (int, float)) else 0.0


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
        for field in _OPERATION_FIELDS:
            if field in expected_summary:
                _check_equal(errors, f"{path}.{field}", expected_summary[field], summary.get(field, _MISSING))
        if "error" in summary:
            errors.append(f"{path}.error: expected absent, got {summary['error']!r}")
    return errors


def _check_equal(errors: list[str], path: str, expected: Any, actual: Any) -> None:
    if actual is _MISSING:
        errors.append(f"{path}: expected {expected!r}, got <missing>")
    elif not _equal(expected, actual):
        errors.append(f"{path}: expected {expected!r}, got {actual!r}")


def _equal(expected: Any, actual: Any) -> bool:
    # bool is an int subclass in Python. YAML ``count: 1`` must not match
    # JSON ``true``, so booleans compare only with booleans.
    if isinstance(expected, bool) or isinstance(actual, bool):
        return isinstance(expected, bool) and isinstance(actual, bool) and expected is actual
    return bool(expected == actual)
