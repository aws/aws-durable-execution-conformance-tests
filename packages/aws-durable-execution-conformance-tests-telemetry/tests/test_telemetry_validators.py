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
    validate_record,
    validate_records,
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
        "schemaVersion": "1.0",
        "status": "SUCCEEDED",
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
        ("input", {"value": 40}, "input: expected {'value': 41}, got {'value': 40}"),
        ("output", {"value": 43}, "output: expected {'value': 42}, got {'value': 43}"),
        ("status", "FAILED", "status: expected 'SUCCEEDED', got 'FAILED'"),
        ("schemaVersion", "2.0", "schemaVersion: expected '1.0', got '2.0'"),
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
        ("executionArn", "arn:aws:lambda:us-west-2:123456789012:function:x:$LATEST/durable-execution/e/i"),
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


def _telemetry(data: dict[str, Any]) -> TelemetryRecord:
    return TelemetryRecord(event={"timestamp": 1, "message": "..."}, data=data)


def test_validate_records_single_entry_passes() -> None:
    assert validate_records([_telemetry(_record())], [ASSERTIONS], EXPECTED) == []


def test_validate_records_zero_expected_zero_fetched_passes() -> None:
    assert validate_records([], [], EXPECTED) == []


@pytest.mark.parametrize(("fetched", "expected_count"), [(2, 1), (0, 1), (1, 2)])
def test_validate_records_count_mismatch_is_the_only_error(fetched: int, expected_count: int) -> None:
    bad = _record()
    bad["status"] = "FAILED"
    records = [_telemetry(bad) for _ in range(fetched)]
    assert validate_records(records, [ASSERTIONS] * expected_count, EXPECTED) == [
        f"records: expected {expected_count} record(s), got {fetched}"
    ]


def _on_change_fixture() -> tuple[list[TelemetryRecord], list[dict[str, Any]]]:
    running = _record()
    running["status"] = "RUNNING"
    del running["output"]
    del running["operationsByName"]
    succeeded = _record()
    running_entry: dict[str, Any] = {
        "record": {"status": "RUNNING", "input": {"value": 41}},
        "forbid_fields": ["output", "error"],
    }
    return [_telemetry(running), _telemetry(succeeded)], [running_entry, ASSERTIONS]


def test_validate_records_matches_entries_positionally() -> None:
    records, entries = _on_change_fixture()
    assert validate_records(records, entries, EXPECTED) == []


def test_validate_records_swapped_entries_fail_both_positions() -> None:
    records, entries = _on_change_fixture()
    assert validate_records(records, list(reversed(entries)), EXPECTED) == [
        "record[0]: status: expected 'SUCCEEDED', got 'RUNNING'",
        "record[0]: output: expected {'value': 42}, got <missing>",
        "record[0]: operationsByName: expected mapping with keys ['increment'], got <missing>",
        "record[1]: status: expected 'RUNNING', got 'SUCCEEDED'",
        "record[1]: output: expected absent, got {'value': 42}",
    ]


def test_validate_records_rejects_non_list_and_non_mapping_entry() -> None:
    assert validate_records([_telemetry(_record())], {"record": {}}, EXPECTED) == [
        "TelemetryAssertions.records: expected a list"
    ]
    assert validate_records([_telemetry(_record())], None, EXPECTED) == ["TelemetryAssertions.records: expected a list"]
    assert validate_records([_telemetry(_record())], ["bad"], EXPECTED) == [
        "record[0]: TelemetryAssertions.records[0]: expected a mapping"
    ]
