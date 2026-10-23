# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Pure record assertion tests."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from typing import Any

import pytest

from aws_durable_execution_conformance_tests.variables import PlaceholderContext
from aws_durable_execution_conformance_tests_telemetry.model import (
    ExpectedExecution,
    TelemetryRecord,
    execution_placeholders,
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
    execution_name="8f1c2d3e-0000-4000-8000-000000000001",
)

# A record in the shape the JS CloudWatch Logs exporter writes: these keys, in
# this order, at both levels.
TOP_LEVEL_KEYS = [
    "recordType",
    "schemaVersion",
    "emittedAt",
    "executionArn",
    "executionName",
    "functionName",
    "functionQualifier",
    "region",
    "accountId",
    "status",
    "startTime",
    "endTime",
    "durationMs",
    "input",
    "output",
    "operationsByName",
]
SUMMARY_KEYS = [
    "type",
    "count",
    "failedCount",
    "status",
    "subType",
    "minDurationMs",
    "maxDurationMs",
    "totalDurationMs",
    "maxAttempt",
]


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
                "count": 1,
                "failedCount": 0,
                "status": "SUCCEEDED",
                "subType": "Step",
                "minDurationMs": 3,
                "maxDurationMs": 3,
                "totalDurationMs": 3,
                "maxAttempt": 1,
            },
        },
    }


# The entry as the validator sees it: placeholders already substituted. Values
# that vary per run use ``'*'`` here; the shipped requirement pins them.
ENTRY: dict[str, Any] = {
    "record": {
        "recordType": "DurableExecutionTelemetry",
        "schemaVersion": "1.0",
        "emittedAt": "*",
        "executionArn": ARN,
        "executionName": "8f1c2d3e-0000-4000-8000-000000000001",
        "functionName": "conformance-tests-abc-telemetry-1",
        "functionQualifier": "$LATEST",
        "region": "us-west-2",
        "accountId": "123456789012",
        "status": "SUCCEEDED",
        "startTime": "*",
        "endTime": "*",
        "durationMs": "*",
        "input": {"value": 41},
        "output": {"value": 42},
        "operationsByName": {
            "increment": {
                "type": "STEP",
                "count": 1,
                "failedCount": 0,
                "status": "SUCCEEDED",
                "subType": "Step",
                "minDurationMs": 3,
                "maxDurationMs": 3,
                "totalDurationMs": 3,
                "maxAttempt": 1,
            },
        },
    },
}


def _entry(**record_overrides: Any) -> dict[str, Any]:
    entry = deepcopy(ENTRY)
    entry["record"].update(record_overrides)
    return entry


def _set_path(record: dict[str, Any], path: tuple[str, ...], value: Any) -> None:
    parent: dict[str, Any] = record
    for key in path[:-1]:
        parent = parent[key]
    parent[path[-1]] = value


def _del_path(record: dict[str, Any], path: tuple[str, ...]) -> None:
    parent: dict[str, Any] = record
    for key in path[:-1]:
        parent = parent[key]
    del parent[path[-1]]


def test_fixture_record_has_the_js_shape() -> None:
    record = _record()
    assert list(record) == TOP_LEVEL_KEYS
    assert list(record["operationsByName"]["increment"]) == SUMMARY_KEYS
    assert list(ENTRY["record"]) == TOP_LEVEL_KEYS
    assert list(ENTRY["record"]["operationsByName"]["increment"]) == SUMMARY_KEYS


def test_valid_record_passes() -> None:
    assert validate_record(_record(), ENTRY) == []


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("input", {"value": 40}, "input.value: expected 41, got 40"),
        ("output", {"value": 43}, "output.value: expected 42, got 43"),
        ("status", "FAILED", "status: expected 'SUCCEEDED', got 'FAILED'"),
        ("schemaVersion", "2.0", "schemaVersion: expected '1.0', got '2.0'"),
        ("recordType", "Other", "recordType: expected 'DurableExecutionTelemetry', got 'Other'"),
    ],
)
def test_wrong_record_field_produces_one_error(field: str, value: Any, message: str) -> None:
    record = _record()
    record[field] = value
    assert validate_record(record, ENTRY) == [message]


def test_missing_scalar_field_reports_missing() -> None:
    record = _record()
    del record["status"]
    assert validate_record(record, ENTRY) == ["status: expected 'SUCCEEDED', got <missing>"]


def test_missing_mapping_field_reports_its_expected_keys() -> None:
    record = _record()
    del record["output"]
    assert validate_record(record, ENTRY) == ["output: expected mapping with keys ['value'], got <missing>"]


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
def test_identity_field_mismatch_fails(field: str, value: str) -> None:
    record = _record()
    record[field] = value
    errors = validate_record(record, ENTRY)
    assert errors == [f"{field}: expected {ENTRY['record'][field]!r}, got {value!r}"]


# ---------------------------------------------------------------------------
# Exact shape: unlisted keys, key order, numeric type
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("error", {"name": "Error", "message": "boom"}),
        ("operations", [{"id": "op-1", "name": "increment", "type": "STEP", "status": "SUCCEEDED"}]),
        ("truncated", False),
        ("droppedOperations", 0),
        ("futureField", {"nested": True}),
    ],
)
def test_unlisted_top_level_key_fails(key: str, value: Any) -> None:
    record = _record()
    record[key] = value
    assert validate_record(record, ENTRY) == [f"unexpected keys [{key!r}]"]


def test_every_unlisted_top_level_key_is_reported_once_in_record_order() -> None:
    record = _record()
    record["truncated"] = True
    record["droppedOperations"] = 1
    assert validate_record(record, ENTRY) == ["unexpected keys ['truncated', 'droppedOperations']"]


@pytest.mark.parametrize(
    ("key", "value"),
    [
        ("error", {"name": "Error", "message": "boom"}),
        ("result", 42),
        ("result", {"t": "int", "v": 42}),
        ("truncated", True),
        ("futureMetric", 12),
    ],
)
def test_unlisted_operation_key_fails(key: str, value: Any) -> None:
    record = _record()
    record["operationsByName"]["increment"][key] = value
    assert validate_record(record, ENTRY) == [f"operationsByName.increment: unexpected keys [{key!r}]"]


def test_unlisted_nested_payload_key_fails() -> None:
    record = _record()
    record["output"]["extra"] = 1
    assert validate_record(record, ENTRY) == ["output: unexpected keys ['extra']"]


def test_top_level_key_order_is_checked() -> None:
    record = _record()
    status = record.pop("status")
    record["status"] = status
    expected = TOP_LEVEL_KEYS
    actual = [key for key in TOP_LEVEL_KEYS if key != "status"] + ["status"]
    assert validate_record(record, ENTRY) == [f"expected key order {expected}, got {actual}"]


def test_operation_key_order_is_checked() -> None:
    record = _record()
    summary = record["operationsByName"]["increment"]
    sub_type = summary.pop("subType")
    reordered = {"type": summary.pop("type"), "subType": sub_type, **summary}
    record["operationsByName"]["increment"] = reordered
    assert validate_record(record, ENTRY) == [
        f"operationsByName.increment: expected key order {SUMMARY_KEYS}, got {list(reordered)}"
    ]


def test_key_order_is_not_reported_when_the_key_sets_differ() -> None:
    # A missing or unexpected key is already one error; a second error about
    # order would describe the same defect.
    record = _record()
    del record["status"]
    record["extra"] = 1
    assert validate_record(record, ENTRY) == [
        "status: expected 'SUCCEEDED', got <missing>",
        "unexpected keys ['extra']",
    ]


@pytest.mark.parametrize(
    ("path", "value", "message"),
    [
        (("operationsByName", "increment", "count"), 1.0, "operationsByName.increment.count: expected 1, got 1.0"),
        (
            ("operationsByName", "increment", "maxAttempt"),
            1.0,
            "operationsByName.increment.maxAttempt: expected 1, got 1.0",
        ),
        (("output", "value"), 42.0, "output.value: expected 42, got 42.0"),
        (("input", "value"), "41", "input.value: expected 41, got '41'"),
    ],
)
def test_integer_expectation_rejects_other_json_number_types(path: tuple[str, ...], value: Any, message: str) -> None:
    record = _record()
    _set_path(record, path, value)
    assert validate_record(record, ENTRY) == [message]


def test_float_expectation_matches_only_a_float() -> None:
    record = _record()
    record["output"] = {"value": 42.5}
    assert validate_record(record, _entry(output={"value": 42.5})) == []
    record["output"] = {"value": 42}
    assert validate_record(record, _entry(output={"value": 42.5})) == ["output.value: expected 42.5, got 42"]


# ---------------------------------------------------------------------------
# operationsByName through the generic mapping rule
# ---------------------------------------------------------------------------


def test_missing_operations_by_name_fails() -> None:
    record = _record()
    del record["operationsByName"]
    assert validate_record(record, ENTRY) == [
        "operationsByName: expected mapping with keys ['increment'], got <missing>"
    ]


def test_operations_by_name_not_mapping_fails() -> None:
    record = _record()
    record["operationsByName"] = []
    assert validate_record(record, ENTRY) == ["operationsByName: expected mapping, got []"]


def test_extra_operation_name_fails() -> None:
    record = _record()
    record["operationsByName"]["extra"] = deepcopy(record["operationsByName"]["increment"])
    assert validate_record(record, ENTRY) == ["operationsByName: unexpected keys ['extra']"]


def test_renamed_operation_fails_as_missing_and_unexpected() -> None:
    record = _record()
    record["operationsByName"] = {"other": record["operationsByName"]["increment"]}
    assert validate_record(record, ENTRY) == [
        f"operationsByName.increment: expected mapping with keys {SUMMARY_KEYS}, got <missing>",
        "operationsByName: unexpected keys ['other']",
    ]


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("type", "WAIT", "operationsByName.increment.type: expected 'STEP', got 'WAIT'"),
        ("status", "FAILED", "operationsByName.increment.status: expected 'SUCCEEDED', got 'FAILED'"),
        ("count", 2, "operationsByName.increment.count: expected 1, got 2"),
        ("failedCount", 1, "operationsByName.increment.failedCount: expected 0, got 1"),
        ("count", True, "operationsByName.increment.count: expected 1, got True"),
        ("maxAttempt", 2, "operationsByName.increment.maxAttempt: expected 1, got 2"),
    ],
)
def test_wrong_operation_field_produces_one_error(field: str, value: Any, message: str) -> None:
    record = _record()
    record["operationsByName"]["increment"][field] = value
    assert validate_record(record, ENTRY) == [message]


def test_operation_summary_not_mapping_fails() -> None:
    record = _record()
    record["operationsByName"]["increment"] = "SUCCEEDED"
    assert validate_record(record, ENTRY) == ["operationsByName.increment: expected mapping, got 'SUCCEEDED'"]


def test_multiple_defects_report_every_error() -> None:
    record = _record()
    record["status"] = "FAILED"
    record["error"] = {"name": "Error", "message": "boom"}
    record["operationsByName"]["increment"]["failedCount"] = 1
    assert validate_record(record, ENTRY) == [
        "status: expected 'SUCCEEDED', got 'FAILED'",
        "operationsByName.increment.failedCount: expected 0, got 1",
        "unexpected keys ['error']",
    ]


def test_malformed_assertions_are_reported_not_raised() -> None:
    assert validate_record(_record(), {"record": "bad"}) == ["TelemetryAssertions.record: expected a mapping"]
    assert validate_record(_record(), {}) == ["TelemetryAssertions.record: expected a mapping"]


def test_unknown_entry_keys_are_reported() -> None:
    # A misspelled section would otherwise be ignored and weaken the check silently.
    assert validate_record(_record(), {**ENTRY, "extra_section": 1}) == [
        "TelemetryAssertions: unknown entry keys ['extra_section']"
    ]


# ---------------------------------------------------------------------------
# validate_records: positional matching
# ---------------------------------------------------------------------------


def _telemetry(data: dict[str, Any]) -> TelemetryRecord:
    return TelemetryRecord(event={"timestamp": 1, "message": "..."}, data=data)


def test_validate_records_single_entry_passes() -> None:
    assert validate_records([_telemetry(_record())], [ENTRY]) == []


def test_validate_records_zero_expected_zero_fetched_passes() -> None:
    assert validate_records([], []) == []


@pytest.mark.parametrize(("fetched", "expected_count"), [(2, 1), (0, 1), (1, 2)])
def test_validate_records_count_mismatch_is_the_only_error(fetched: int, expected_count: int) -> None:
    bad = _record()
    bad["status"] = "FAILED"
    records = [_telemetry(bad) for _ in range(fetched)]
    assert validate_records(records, [ENTRY] * expected_count) == [
        f"records: expected {expected_count} record(s), got {fetched}"
    ]


def _on_change_fixture() -> tuple[list[TelemetryRecord], list[dict[str, Any]]]:
    # A RUNNING record has no end time, duration, or output yet.
    running = _record()
    running["status"] = "RUNNING"
    for key in ("endTime", "durationMs", "output", "operationsByName"):
        del running[key]
    running_entry = _entry(status="RUNNING")
    for key in ("endTime", "durationMs", "output", "operationsByName"):
        del running_entry["record"][key]
    return [_telemetry(running), _telemetry(_record())], [running_entry, ENTRY]


def test_validate_records_matches_entries_positionally() -> None:
    records, entries = _on_change_fixture()
    assert validate_records(records, entries) == []


def test_validate_records_swapped_entries_fail_both_positions() -> None:
    records, entries = _on_change_fixture()
    assert validate_records(records, list(reversed(entries))) == [
        "record[0]: status: expected 'SUCCEEDED', got 'RUNNING'",
        "record[0]: endTime: expected '*', got <missing>",
        "record[0]: durationMs: expected '*', got <missing>",
        "record[0]: output: expected mapping with keys ['value'], got <missing>",
        "record[0]: operationsByName: expected mapping with keys ['increment'], got <missing>",
        "record[1]: status: expected 'RUNNING', got 'SUCCEEDED'",
        "record[1]: unexpected keys ['endTime', 'durationMs', 'output', 'operationsByName']",
    ]


def test_validate_records_rejects_non_list_and_non_mapping_entry() -> None:
    assert validate_records([_telemetry(_record())], {"record": {}}) == ["TelemetryAssertions.records: expected a list"]
    assert validate_records([_telemetry(_record())], None) == ["TelemetryAssertions.records: expected a list"]
    assert validate_records([_telemetry(_record())], ["bad"]) == [
        "record[0]: TelemetryAssertions.records[0]: expected a mapping"
    ]


# ---------------------------------------------------------------------------
# Expected-value rules: '*', ${/regex/}, $integer, nested mappings and sequences
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("value", ["2026-10-19T20:00:00.000Z", 0, False, None, {}, []])
def test_wildcard_accepts_any_present_value(value: Any) -> None:
    record = _record()
    record["emittedAt"] = value
    assert validate_record(record, _entry(emittedAt="*")) == []


def test_wildcard_rejects_missing_field() -> None:
    record = _record()
    del record["durationMs"]
    assert validate_record(record, _entry(durationMs="*")) == ["durationMs: expected '*', got <missing>"]


ISO_8601 = "${/^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?Z$/}"


def test_regex_matches_iso_timestamp() -> None:
    assert validate_record(_record(), _entry(startTime=ISO_8601)) == []


@pytest.mark.parametrize("value", ["2026-10-19 20:00:00", 1760904000000, ""])
def test_regex_rejects_non_matching_value(value: Any) -> None:
    record = _record()
    record["startTime"] = value
    errors = validate_record(record, _entry(startTime=ISO_8601))
    assert errors == [f"startTime: value {value!r} does not match regex pattern {ISO_8601[3:-2]!r}"]


def test_regex_rejects_missing_field() -> None:
    record = _record()
    del record["endTime"]
    assert validate_record(record, _entry(endTime=ISO_8601)) == [f"endTime: expected {ISO_8601!r}, got <missing>"]


NON_NEGATIVE_INTEGER = {"$integer": {"min": 0}}


@pytest.mark.parametrize("value", [0, 1, 1163])
def test_integer_accepts_json_integer(value: int) -> None:
    record = _record()
    record["durationMs"] = value
    assert validate_record(record, _entry(durationMs=NON_NEGATIVE_INTEGER)) == []


@pytest.mark.parametrize("value", ["1000", "0", 1000.0, 12.5, True, False, None, float("nan"), {}, []])
def test_integer_rejects_non_integer(value: Any) -> None:
    record = _record()
    record["durationMs"] = value
    assert validate_record(record, _entry(durationMs=NON_NEGATIVE_INTEGER)) == [
        f"durationMs: expected an integer, got {value!r}"
    ]


def test_integer_rejects_below_min() -> None:
    record = _record()
    record["durationMs"] = -1
    assert validate_record(record, _entry(durationMs=NON_NEGATIVE_INTEGER)) == [
        "durationMs: expected an integer >= 0, got -1"
    ]


def test_integer_without_constraints_accepts_any_integer() -> None:
    record = _record()
    record["durationMs"] = -5
    assert validate_record(record, _entry(durationMs={"$integer": {}})) == []


def test_integer_rejects_missing_field() -> None:
    record = _record()
    del record["durationMs"]
    assert validate_record(record, _entry(durationMs=NON_NEGATIVE_INTEGER)) == [
        "durationMs: expected {'$integer': {'min': 0}}, got <missing>"
    ]


def test_wildcard_inside_nested_mapping() -> None:
    record = _record()
    record["output"] = {"value": 42, "generated": "anything"}
    entry = _entry(output={"value": 42, "generated": "*"})
    assert validate_record(record, entry) == []
    del record["output"]["generated"]
    assert validate_record(record, entry) == ["output.generated: expected '*', got <missing>"]


def test_nested_mapping_reports_each_leaf() -> None:
    record = _record()
    record["output"] = {"value": 43}
    assert validate_record(record, _entry(output={"value": 42})) == ["output.value: expected 42, got 43"]


def test_nested_mapping_key_order_is_checked() -> None:
    record = _record()
    record["output"] = {"b": 2, "a": 1}
    assert validate_record(record, _entry(output={"a": 1, "b": 2})) == [
        "output: expected key order ['a', 'b'], got ['b', 'a']"
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
    assert validate_record(record, _entry(output=expected)) == [message]


def test_mapping_expected_but_scalar_actual() -> None:
    record = _record()
    record["output"] = "42"
    assert validate_record(record, _entry(output={"value": 42})) == ["output: expected mapping, got '42'"]


def test_sequence_matches_pairwise_with_rules() -> None:
    record = _record()
    record["output"] = {"items": [1, "x", {"id": "abc"}]}
    entry = _entry(output={"items": [1, "*", {"id": "${/^a/}"}]})
    assert validate_record(record, entry) == []
    record["output"]["items"][2]["id"] = "zzz"
    assert validate_record(record, entry) == ["output.items[2].id: value 'zzz' does not match regex pattern '^a'"]


def test_sequence_item_mapping_is_closed() -> None:
    record = _record()
    record["output"] = {"items": [{"id": "abc", "extra": 1}]}
    assert validate_record(record, _entry(output={"items": [{"id": "abc"}]})) == [
        "output.items[0]: unexpected keys ['extra']"
    ]


def test_sequence_length_mismatch_is_one_error() -> None:
    record = _record()
    record["output"] = {"items": [1, 2]}
    assert validate_record(record, _entry(output={"items": [1]})) == ["output.items: expected 1 item(s), got 2"]


def test_sequence_expected_but_scalar_actual() -> None:
    record = _record()
    record["output"] = {"items": "1"}
    assert validate_record(record, _entry(output={"items": [1]})) == ["output.items: expected sequence, got '1'"]


# ---------------------------------------------------------------------------
# The shipped telemetry-1 requirement against the JS CloudWatch Logs record
# ---------------------------------------------------------------------------


def _telemetry_1_entry() -> dict[str, Any]:
    """The shipped entry, with the identity placeholders bound as the extension binds them."""
    from aws_durable_execution_conformance_tests.validate import discover_test_files, load_yaml_file
    from aws_durable_execution_conformance_tests_telemetry.extension import TelemetryExtension

    (suite,) = TelemetryExtension().requirement_suites()
    requirement = load_yaml_file(discover_test_files(suite.root)["telemetry-1"])
    (entry,) = requirement["TelemetryAssertions"]["records"]
    placeholders = PlaceholderContext()
    for name, value in execution_placeholders(EXPECTED).items():
        placeholders.bind(name, value)
    return placeholders.substitute(entry)


def test_telemetry_1_accepts_the_fixture_record() -> None:
    assert validate_record(_record(), _telemetry_1_entry()) == []


def _leaf_paths(mapping: Mapping[str, Any], prefix: tuple[str, ...] = ()) -> list[tuple[str, ...]]:
    paths: list[tuple[str, ...]] = []
    for key, value in mapping.items():
        path = (*prefix, key)
        paths.append(path)
        if isinstance(value, dict) and set(value) != {"$integer"}:
            paths.extend(_leaf_paths(value, path))
    return paths


@pytest.mark.parametrize("path", _leaf_paths(_telemetry_1_entry()["record"]), ids=".".join)
def test_telemetry_1_rejects_record_missing_each_asserted_field(path: tuple[str, ...]) -> None:
    record = _record()
    _del_path(record, path)
    errors = validate_record(record, _telemetry_1_entry())
    assert len(errors) == 1
    assert errors[0].startswith(f"{'.'.join(path)}: expected ")
    assert errors[0].endswith(", got <missing>")


@pytest.mark.parametrize(
    ("field", "attribute"),
    [
        ("executionArn", "execution_arn"),
        ("executionName", "execution_name"),
        ("functionName", "function_name"),
        ("functionQualifier", "qualifier"),
        ("region", "region"),
        ("accountId", "account_id"),
    ],
)
def test_telemetry_1_checks_each_identity_field_against_the_arn(field: str, attribute: str) -> None:
    record = _record()
    record[field] = "other"
    errors = validate_record(record, _telemetry_1_entry())
    assert errors == [f"{field}: expected {getattr(EXPECTED, attribute)!r}, got 'other'"]


def test_telemetry_1_rejects_reordered_keys() -> None:
    record = _record()
    reordered = {key: record[key] for key in reversed(TOP_LEVEL_KEYS)}
    assert validate_record(reordered, _telemetry_1_entry()) == [
        f"expected key order {TOP_LEVEL_KEYS}, got {list(reversed(TOP_LEVEL_KEYS))}"
    ]


_TIMESTAMP_PATHS = [("emittedAt",), ("startTime",), ("endTime",)]
_TIMESTAMP_PATTERN = r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$"


@pytest.mark.parametrize("path", _TIMESTAMP_PATHS, ids=".".join)
def test_telemetry_1_accepts_timestamp_with_three_fraction_digits(path: tuple[str, ...]) -> None:
    record = _record()
    _set_path(record, path, "2026-10-23T02:15:43.000Z")
    assert validate_record(record, _telemetry_1_entry()) == []


# The requirement pins exactly three fraction digits with a Z suffix. The
# values below cover a missing fraction, other widths, a numeric offset, and a
# missing suffix. Each must produce one error.
@pytest.mark.parametrize("path", _TIMESTAMP_PATHS, ids=".".join)
@pytest.mark.parametrize(
    "value",
    [
        "2026-10-23T02:15:43Z",
        "2026-10-23T02:15:43.0Z",
        "2026-10-23T02:15:43.031123Z",
        "2026-10-23T02:15:43.031123456Z",
        "2026-10-23T02:15:43.031+00:00",
        "2026-10-23T02:15:43.031",
    ],
)
def test_telemetry_1_rejects_other_timestamp_formats(path: tuple[str, ...], value: str) -> None:
    record = _record()
    _set_path(record, path, value)
    assert validate_record(record, _telemetry_1_entry()) == [
        f"{'.'.join(path)}: value {value!r} does not match regex pattern {_TIMESTAMP_PATTERN!r}"
    ]


# Every SDK emits durations as a non-negative integer millisecond count. The
# requirement therefore rejects null, booleans, strings, negatives, and
# non-integers so that an exporter with broken duration serialization fails
# the check.
_TELEMETRY_1_DURATION_PATHS: list[tuple[str, ...]] = [
    ("durationMs",),
    ("operationsByName", "increment", "minDurationMs"),
    ("operationsByName", "increment", "maxDurationMs"),
    ("operationsByName", "increment", "totalDurationMs"),
]


@pytest.mark.parametrize("path", _TELEMETRY_1_DURATION_PATHS, ids=".".join)
@pytest.mark.parametrize("value", [0, 1, 1000])
def test_telemetry_1_accepts_non_negative_integer_duration(path: tuple[str, ...], value: int) -> None:
    record = _record()
    _set_path(record, path, value)
    assert validate_record(record, _telemetry_1_entry()) == []


@pytest.mark.parametrize("path", _TELEMETRY_1_DURATION_PATHS, ids=".".join)
@pytest.mark.parametrize("value", [None, True, False, "1000", "", 12.5, 1000.0, float("nan"), float("inf")])
def test_telemetry_1_rejects_non_integer_duration(path: tuple[str, ...], value: Any) -> None:
    record = _record()
    _set_path(record, path, value)
    assert validate_record(record, _telemetry_1_entry()) == [f"{'.'.join(path)}: expected an integer, got {value!r}"]


@pytest.mark.parametrize("path", _TELEMETRY_1_DURATION_PATHS, ids=".".join)
def test_telemetry_1_rejects_negative_duration(path: tuple[str, ...]) -> None:
    record = _record()
    _set_path(record, path, -1)
    assert validate_record(record, _telemetry_1_entry()) == [f"{'.'.join(path)}: expected an integer >= 0, got -1"]
