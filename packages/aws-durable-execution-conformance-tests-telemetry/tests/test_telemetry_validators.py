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
    _ARN_FIELDS,
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
    execution_name="8f1c2d3e-0000-4000-8000-000000000001",
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
        ("input", {"value": 40}, "input.value: expected 41, got 40"),
        ("output", {"value": 43}, "output.value: expected 42, got 43"),
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
        ("executionName", "other-execution"),
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


# ---------------------------------------------------------------------------
# Expected-value rules: '*', ${/regex/}, nested mappings and sequences
# ---------------------------------------------------------------------------


def _assertions_with_record(record_assertions: dict[str, Any]) -> dict[str, Any]:
    assertions = deepcopy(ASSERTIONS)
    assertions["record"] = record_assertions
    return assertions


@pytest.mark.parametrize("value", ["2026-10-19T20:00:00.000Z", 0, False, None, {}, []])
def test_wildcard_accepts_any_present_value(value: Any) -> None:
    record = _record()
    record["emittedAt"] = value
    assert validate_record(record, _assertions_with_record({"emittedAt": "*"}), EXPECTED) == []


def test_wildcard_rejects_missing_field() -> None:
    record = _record()
    del record["durationMs"]
    assert validate_record(record, _assertions_with_record({"durationMs": "*"}), EXPECTED) == [
        "durationMs: expected '*', got <missing>"
    ]


ISO_8601 = "${/^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?Z$/}"


def test_regex_matches_iso_timestamp() -> None:
    assert validate_record(_record(), _assertions_with_record({"startTime": ISO_8601}), EXPECTED) == []


@pytest.mark.parametrize("value", ["2026-10-19 20:00:00", 1760904000000, ""])
def test_regex_rejects_non_matching_value(value: Any) -> None:
    record = _record()
    record["startTime"] = value
    errors = validate_record(record, _assertions_with_record({"startTime": ISO_8601}), EXPECTED)
    assert errors == [f"startTime: value {value!r} does not match regex pattern {ISO_8601[3:-2]!r}"]


def test_regex_rejects_missing_field() -> None:
    record = _record()
    del record["endTime"]
    assert validate_record(record, _assertions_with_record({"endTime": ISO_8601}), EXPECTED) == [
        f"endTime: expected {ISO_8601!r}, got <missing>"
    ]


def test_wildcard_inside_nested_mapping() -> None:
    record = _record()
    record["output"] = {"value": 42, "generated": "anything"}
    assertions = _assertions_with_record({"output": {"value": 42, "generated": "*"}})
    assert validate_record(record, assertions, EXPECTED) == []
    del record["output"]["generated"]
    assert validate_record(record, assertions, EXPECTED) == ["output.generated: expected '*', got <missing>"]


def test_nested_mapping_reports_each_leaf() -> None:
    record = _record()
    record["output"] = {"value": 43}
    assert validate_record(record, _assertions_with_record({"output": {"value": 42}}), EXPECTED) == [
        "output.value: expected 42, got 43"
    ]


@pytest.mark.parametrize(
    ("expected", "actual", "message"),
    [
        ({"value": 1}, {"value": True}, "output.value: expected 1, got True"),
        ({"value": True}, {"value": 1}, "output.value: expected True, got 1"),
        ({"value": 0}, {"value": False}, "output.value: expected 0, got False"),
    ],
)
def test_nested_boolean_is_not_equal_to_integer(expected: Any, actual: Any, message: str) -> None:
    record = _record()
    record["output"] = actual
    assert validate_record(record, _assertions_with_record({"output": expected}), EXPECTED) == [message]


def test_mapping_expected_but_scalar_actual() -> None:
    record = _record()
    record["output"] = "42"
    assert validate_record(record, _assertions_with_record({"output": {"value": 42}}), EXPECTED) == [
        "output: expected mapping, got '42'"
    ]


def test_sequence_matches_pairwise_with_rules() -> None:
    record = _record()
    record["output"] = {"items": [1, "x", {"id": "abc"}]}
    assertions = _assertions_with_record({"output": {"items": [1, "*", {"id": "${/^a/}"}]}})
    assert validate_record(record, assertions, EXPECTED) == []
    record["output"]["items"][2]["id"] = "zzz"
    assert validate_record(record, assertions, EXPECTED) == [
        "output.items[2].id: value 'zzz' does not match regex pattern '^a'"
    ]


def test_sequence_length_mismatch_is_one_error() -> None:
    record = _record()
    record["output"] = {"items": [1, 2]}
    assert validate_record(record, _assertions_with_record({"output": {"items": [1]}}), EXPECTED) == [
        "output.items: expected 1 item(s), got 2"
    ]


def test_sequence_expected_but_scalar_actual() -> None:
    record = _record()
    record["output"] = {"items": "1"}
    assert validate_record(record, _assertions_with_record({"output": {"items": [1]}}), EXPECTED) == [
        "output.items: expected sequence, got '1'"
    ]


def test_operation_summary_checks_every_listed_key() -> None:
    assertions = deepcopy(ASSERTIONS)
    assertions["operations_by_name"]["increment"].update(
        {"subType": "Step", "maxAttempt": 1, "minDurationMs": "*", "maxDurationMs": "*", "totalDurationMs": "*"}
    )
    assert validate_record(_record(), assertions, EXPECTED) == []

    record = _record()
    record["operationsByName"]["increment"]["maxAttempt"] = 2
    del record["operationsByName"]["increment"]["minDurationMs"]
    assert validate_record(record, assertions, EXPECTED) == [
        "operationsByName.increment.maxAttempt: expected 1, got 2",
        "operationsByName.increment.minDurationMs: expected '*', got <missing>",
    ]


# ---------------------------------------------------------------------------
# The shipped telemetry-1 requirement against a complete success record
# ---------------------------------------------------------------------------


def _telemetry_1_entry() -> dict[str, Any]:
    from aws_durable_execution_conformance_tests.validate import discover_test_files, load_yaml_file
    from aws_durable_execution_conformance_tests_telemetry.extension import TelemetryExtension

    (suite,) = TelemetryExtension().requirement_suites()
    requirement = load_yaml_file(discover_test_files(suite.root)["telemetry-1"])
    (entry,) = requirement["TelemetryAssertions"]["records"]
    return entry


def test_telemetry_1_accepts_complete_success_record() -> None:
    assert validate_record(_record(), _telemetry_1_entry(), EXPECTED) == []


def _telemetry_1_asserted_paths() -> list[tuple[str, ...]]:
    entry = _telemetry_1_entry()
    paths: list[tuple[str, ...]] = [(key,) for key in entry["record"]]
    paths.extend((key,) for key, _attribute in _ARN_FIELDS)
    for name, summary in entry["operations_by_name"].items():
        paths.extend(("operationsByName", name, key) for key in summary)
    return paths


@pytest.mark.parametrize("path", _telemetry_1_asserted_paths(), ids=".".join)
def test_telemetry_1_rejects_record_missing_each_asserted_field(path: tuple[str, ...]) -> None:
    record = _record()
    parent: dict[str, Any] = record
    for key in path[:-1]:
        parent = parent[key]
    del parent[path[-1]]
    errors = validate_record(record, _telemetry_1_entry(), EXPECTED)
    assert len(errors) == 1
    assert errors[0].startswith(f"{'.'.join(path)}: expected ")
    assert errors[0].endswith(", got <missing>")
