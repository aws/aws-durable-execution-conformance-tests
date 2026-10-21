# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Pure record assertion tests."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest

from aws_durable_execution_conformance_tests_telemetry.model import (
    ExpectedExecution,
    TelemetryRecord,
)
from aws_durable_execution_conformance_tests_telemetry.validators import (
    select_passing_record,
    validate_record,
)

ARN = (
    "arn:aws:lambda:us-west-2:123456789012:function:conformance-tests-abc-telemetry-1:$LATEST"
    "/durable-execution/8f1c2d3e-0000-4000-8000-000000000001/inv-1"
)
EXPECTED = ExpectedExecution(
    execution_arn=ARN,
    function_name="conformance-tests-abc-telemetry-1",
    qualifier="$LATEST",
    region="us-west-2",
    account_id="123456789012",
)
ASSERTIONS: dict[str, Any] = {
    "record": {
        "recordType": "DurableExecutionTelemetry",
        "schemaVersion": "1.0",
        "status": "SUCCEEDED",
        "executionArn": ARN,
        "input": {"value": 41},
        "output": {"value": 42},
    },
    "forbid_fields": ["error"],
    "operations_by_name": {
        "increment": {"type": "STEP", "status": "SUCCEEDED", "count": 1, "failedCount": 0},
    },
}


def _record() -> dict[str, Any]:
    return {
        "recordType": "DurableExecutionTelemetry",
        "schemaVersion": "1.0",
        "emittedAt": "2026-10-19T20:00:00.000Z",
        "executionArn": ARN,
        "executionName": "8f1c2d3e-0000-4000-8000-000000000001",
        "functionName": "conformance-tests-abc-telemetry-1",
        "functionQualifier": "$LATEST",
        "region": "us-west-2",
        "accountId": "123456789012",
        "status": "SUCCEEDED",
        "startTime": "2026-10-19T19:59:59.000Z",
        "endTime": "2026-10-19T20:00:00.000Z",
        "durationMs": 1000,
        "input": {"value": 41},
        "output": {"value": 42},
        "operationsByName": {
            "increment": {
                "type": "STEP",
                "subType": "Step",
                "count": 1,
                "failedCount": 0,
                "status": "SUCCEEDED",
                "minDurationMs": 3,
                "maxDurationMs": 3,
                "totalDurationMs": 3,
                "maxAttempt": 1,
            },
        },
    }


def test_valid_record_passes() -> None:
    assert validate_record(_record(), ASSERTIONS, EXPECTED) == []


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        (
            "executionArn",
            "arn:aws:lambda:us-west-2:123456789012:function:x:$LATEST/durable-execution/e/i",
            "executionArn",
        ),
        ("input", {"value": 40}, "input: expected {'value': 41}, got {'value': 40}"),
        ("output", {"value": 43}, "output: expected {'value': 42}, got {'value': 43}"),
        ("status", "FAILED", "status: expected 'SUCCEEDED', got 'FAILED'"),
        ("schemaVersion", "2.0", "schemaVersion: expected '1.0', got '2.0'"),
        ("recordType", "Other", "recordType: expected 'DurableExecutionTelemetry', got 'Other'"),
    ],
)
def test_wrong_record_field_produces_one_error(field: str, value: Any, message: str) -> None:
    record = _record()
    record[field] = value
    errors = validate_record(record, ASSERTIONS, EXPECTED)
    assert len(errors) == 1
    assert message in errors[0]


def test_missing_record_field_reports_missing() -> None:
    record = _record()
    del record["output"]
    assert validate_record(record, ASSERTIONS, EXPECTED) == ["output: expected {'value': 42}, got <missing>"]


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("functionName", "other-function"),
        ("functionQualifier", "1"),
        ("region", "us-east-1"),
        ("accountId", "000000000000"),
    ],
)
def test_arn_derived_field_mismatch_fails(field: str, value: str) -> None:
    record = _record()
    record[field] = value
    errors = validate_record(record, ASSERTIONS, EXPECTED)
    assert len(errors) == 1
    assert errors[0].startswith(f"{field}: expected ")


def test_top_level_error_present_fails() -> None:
    record = _record()
    record["error"] = {"name": "Error", "message": "boom"}
    assert validate_record(record, ASSERTIONS, EXPECTED) == [
        "error: expected absent, got {'name': 'Error', 'message': 'boom'}"
    ]


def test_missing_operations_by_name_fails() -> None:
    record = _record()
    del record["operationsByName"]
    assert validate_record(record, ASSERTIONS, EXPECTED) == [
        "operationsByName: expected mapping with keys ['increment'], got <missing>"
    ]


def test_operations_by_name_not_mapping_fails() -> None:
    record = _record()
    record["operationsByName"] = []
    assert validate_record(record, ASSERTIONS, EXPECTED) == [
        "operationsByName: expected mapping with keys ['increment'], got []"
    ]


def test_extra_operation_key_fails() -> None:
    record = _record()
    record["operationsByName"]["extra"] = deepcopy(record["operationsByName"]["increment"])
    assert validate_record(record, ASSERTIONS, EXPECTED) == [
        "operationsByName: expected keys ['increment'], got ['extra', 'increment']"
    ]


def test_missing_increment_fails() -> None:
    record = _record()
    record["operationsByName"] = {"other": record["operationsByName"]["increment"]}
    assert validate_record(record, ASSERTIONS, EXPECTED) == [
        "operationsByName: expected keys ['increment'], got ['other']"
    ]


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("type", "WAIT", "operationsByName.increment.type: expected 'STEP', got 'WAIT'"),
        ("status", "FAILED", "operationsByName.increment.status: expected 'SUCCEEDED', got 'FAILED'"),
        ("count", 2, "operationsByName.increment.count: expected 1, got 2"),
        ("failedCount", 1, "operationsByName.increment.failedCount: expected 0, got 1"),
        ("count", True, "operationsByName.increment.count: expected 1, got True"),
    ],
)
def test_wrong_operation_field_produces_one_error(field: str, value: Any, message: str) -> None:
    record = _record()
    record["operationsByName"]["increment"][field] = value
    assert validate_record(record, ASSERTIONS, EXPECTED) == [message]


def test_operation_error_present_fails() -> None:
    record = _record()
    record["operationsByName"]["increment"]["error"] = {"name": "Error", "message": "boom"}
    assert validate_record(record, ASSERTIONS, EXPECTED) == [
        "operationsByName.increment.error: expected absent, got {'name': 'Error', 'message': 'boom'}"
    ]


def test_operation_summary_not_mapping_fails() -> None:
    record = _record()
    record["operationsByName"]["increment"] = "SUCCEEDED"
    assert validate_record(record, ASSERTIONS, EXPECTED) == [
        "operationsByName.increment: expected mapping, got 'SUCCEEDED'"
    ]


def test_unknown_additive_fields_pass() -> None:
    record = _record()
    record["truncated"] = False
    record["futureField"] = {"nested": True}
    record["operationsByName"]["increment"]["futureMetric"] = 12
    assert validate_record(record, ASSERTIONS, EXPECTED) == []


def test_multiple_defects_report_every_error() -> None:
    record = _record()
    record["status"] = "FAILED"
    record["error"] = {"name": "Error", "message": "boom"}
    record["operationsByName"]["increment"]["failedCount"] = 1
    errors = validate_record(record, ASSERTIONS, EXPECTED)
    assert len(errors) == 3


def test_malformed_assertions_are_reported_not_raised() -> None:
    assert validate_record(_record(), {"record": "bad"}, EXPECTED) == ["TelemetryAssertions.record: expected a mapping"]
    assert validate_record(_record(), {"operations_by_name": ["increment"]}, EXPECTED) == [
        "TelemetryAssertions.operations_by_name: expected a mapping"
    ]
    assert validate_record(_record(), {"operations_by_name": {"increment": "x"}}, EXPECTED) == [
        "TelemetryAssertions.operations_by_name.increment: expected a mapping"
    ]


def test_forbid_fields_accepts_single_string() -> None:
    record = _record()
    record["error"] = {}
    assertions = {**ASSERTIONS, "forbid_fields": "error"}
    assert validate_record(record, assertions, EXPECTED) == ["error: expected absent, got {}"]


def _telemetry(data: dict[str, Any], timestamp: int) -> TelemetryRecord:
    return TelemetryRecord(event={"timestamp": timestamp, "message": "..."}, data=data)


def test_select_passing_record_does_not_combine_partial_records() -> None:
    # Record A has the right output but no operations. Record B has the right
    # operations but the wrong output. Neither is complete on its own.
    a = _record()
    del a["operationsByName"]
    b = _record()
    b["output"] = {"value": 0}
    passing, errors = select_passing_record([_telemetry(a, 1), _telemetry(b, 2)], ASSERTIONS, EXPECTED)
    assert passing is not None
    assert errors
    assert len(errors) == 1


def test_select_passing_record_prefers_fewest_errors_then_latest() -> None:
    two_errors = _record()
    two_errors["status"] = "FAILED"
    two_errors["output"] = {"value": 0}
    one_error_old = _record()
    one_error_old["output"] = {"value": 0}
    one_error_new = _record()
    one_error_new["status"] = "RUNNING"
    passing, errors = select_passing_record(
        [_telemetry(two_errors, 5), _telemetry(one_error_old, 1), _telemetry(one_error_new, 9)],
        ASSERTIONS,
        EXPECTED,
    )
    assert passing is not None
    assert passing.data is one_error_new
    assert errors == ["status: expected 'SUCCEEDED', got 'RUNNING'"]


def test_select_passing_record_accepts_complete_record_among_incomplete() -> None:
    incomplete = _record()
    incomplete["status"] = "RUNNING"
    complete = _record()
    passing, errors = select_passing_record(
        [_telemetry(incomplete, 1), _telemetry(complete, 2), _telemetry(incomplete, 3)],
        ASSERTIONS,
        EXPECTED,
    )
    assert passing is not None
    assert passing.data is complete
    assert errors == []


def test_select_passing_record_with_no_records() -> None:
    assert select_passing_record([], ASSERTIONS, EXPECTED) == (None, [])
