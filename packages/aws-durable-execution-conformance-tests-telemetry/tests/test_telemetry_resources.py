# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Packaged Durable Execution Telemetry requirement resource tests."""

from __future__ import annotations

from pathlib import Path

import pytest

from aws_durable_execution_conformance_tests.validate import (
    discover_test_files,
    load_yaml_file,
)
from aws_durable_execution_conformance_tests_telemetry.extension import TelemetryExtension


def _suite_root() -> Path:
    (suite,) = TelemetryExtension().requirement_suites()
    return suite.root


def _requirements() -> dict[str, str]:
    return discover_test_files(_suite_root())


def test_requirement_set() -> None:
    assert set(_requirements()) == {"telemetry-1", "telemetry-2", "telemetry-3", "telemetry-4"}


def test_telemetry_1_loads_with_core_sections() -> None:
    requirement = load_yaml_file(_requirements()["telemetry-1"])
    assert requirement["Input"] == {"value": 41}
    assert requirement["ExpectedResult"] == {"ExecutionStatus": "SUCCEEDED", "Result": {"value": 42}}
    assert [event["EventType"] for event in requirement["ExpectedExecutionHistory"]] == [
        "StepStarted",
        "StepSucceeded",
        "InvocationCompleted",
    ]


def test_telemetry_2_loads_with_core_sections() -> None:
    requirement = load_yaml_file(_requirements()["telemetry-2"])
    assert requirement["Input"] == {"value": 41}
    assert requirement["ExpectedResult"] == {
        "ExecutionStatus": "FAILED",
        "Error": {"ErrorType": "*", "ErrorMessage": "payment failed"},
    }
    assert [event["EventType"] for event in requirement["ExpectedExecutionHistory"]] == [
        "StepStarted",
        "StepFailed",
        "InvocationCompleted",
    ]
    step_failed = requirement["ExpectedExecutionHistory"][1]
    assert step_failed["StepFailedDetails"]["Error"]["Payload"]["ErrorMessage"] == "card declined"


def test_telemetry_3_loads_with_core_sections() -> None:
    requirement = load_yaml_file(_requirements()["telemetry-3"])
    assert requirement["Input"] == {"userId": "u-123"}
    # The workflow returns the step result unchanged. So the execution result
    # carries every key, and only the telemetry record is filtered.
    assert requirement["ExpectedResult"] == {
        "ExecutionStatus": "SUCCEEDED",
        "Result": {"userId": "u-123", "email": "test@example.com", "token": "tok-456"},
    }
    assert [event["EventType"] for event in requirement["ExpectedExecutionHistory"]] == [
        "StepStarted",
        "StepSucceeded",
        "InvocationCompleted",
    ]
    assert requirement["ExpectedExecutionHistory"][0]["Name"] == "fetch-user"


def test_telemetry_4_loads_with_core_sections() -> None:
    requirement = load_yaml_file(_requirements()["telemetry-4"])
    assert requirement["Input"] == [1, 2]
    assert requirement["ExpectedResult"] == {"ExecutionStatus": "SUCCEEDED", "Result": ["Hello, 1!", "Hello, 2!"]}
    events = requirement["ExpectedExecutionHistory"]
    # map/9-1's event sequence: the map, then per item an iteration that wraps one step.
    assert [event["EventType"] for event in events] == [
        "ContextStarted",
        "ContextStarted",
        "StepStarted",
        "StepSucceeded",
        "ContextSucceeded",
        "ContextStarted",
        "StepStarted",
        "StepSucceeded",
        "ContextSucceeded",
        "ContextSucceeded",
        "InvocationCompleted",
    ]
    by_id = {event["EventId"]: event for event in events}
    # map/9-13's names: the map and both iterations are named, so each reaches the record.
    assert by_id[2]["Name"] == by_id[11]["Name"] == "process-items"
    assert (by_id[3]["Name"], by_id[7]["Name"]) == ("item-1", "item-2")
    assert (by_id[4]["Name"], by_id[8]["Name"]) == ("greet-1", "greet-2")
    # The two-layer parent chain: step -> iteration -> map.
    assert by_id[4]["ParentId"] == by_id[5]["ParentId"] == by_id[3]["Id"]
    assert by_id[8]["ParentId"] == by_id[9]["ParentId"] == by_id[7]["Id"]
    assert by_id[3]["ParentId"] == by_id[7]["ParentId"] == by_id[2]["Id"]


@pytest.mark.parametrize(
    ("requirement_id", "operation_names"),
    [
        ("telemetry-1", {"increment"}),
        ("telemetry-2", {"charge"}),
        ("telemetry-3", {"fetch-user"}),
        ("telemetry-4", {"process-items", "item-1", "item-2", "greet-1", "greet-2"}),
    ],
)
def test_telemetry_assertions_expect_one_complete_record(requirement_id: str, operation_names: set[str]) -> None:
    requirement = load_yaml_file(_requirements()[requirement_id])
    assertions = requirement["TelemetryAssertions"]
    assert set(assertions) == {"records"}
    assert len(assertions["records"]) == 1
    entry = assertions["records"][0]
    assert set(entry) == {"record"}
    record = entry["record"]
    assert record["recordType"] == "DurableExecutionTelemetry"
    assert record["schemaVersion"] == "1.0"
    assert {"emittedAt", "startTime", "endTime", "durationMs"} <= set(record)
    assert set(record["operationsByName"]) == operation_names


def test_telemetry_2_describes_a_failure_record_without_output() -> None:
    record = load_yaml_file(_requirements()["telemetry-2"])["TelemetryAssertions"]["records"][0]["record"]
    assert record["status"] == "FAILED"
    assert "output" not in record
    summary = record["operationsByName"]["charge"]
    assert summary["status"] == "FAILED"
    assert summary["failedCount"] == 1
    # Error names and cause depth differ per SDK by design. So both errors are
    # asserted as present objects only; the messages are pinned by the core
    # ExpectedResult and ExpectedExecutionHistory sections.
    assert record["error"] == {"$object": {}}
    assert summary["error"] == {"$object": {}}


def test_telemetry_3_selects_one_result_key_and_disables_execution_payloads() -> None:
    record = load_yaml_file(_requirements()["telemetry-3"])["TelemetryAssertions"]["records"][0]["record"]
    assert record["status"] == "SUCCEEDED"
    # Execution input and output are disabled in the plugin. The mapping is
    # closed, so listing neither key makes an exported payload a failure.
    assert "input" not in record
    assert "output" not in record
    summary = record["operationsByName"]["fetch-user"]
    # The selected key is pinned as a one-key mapping, not a rule. So the full
    # three-key result, a masked extra key, a JSON string, and a `{value: ...}`
    # wrapper all fail the closed-mapping check.
    assert summary["result"] == {"userId": "u-123"}
    assert summary["maxAttempt"] == 1


def test_telemetry_4_describes_the_map_its_iterations_and_their_steps() -> None:
    record = load_yaml_file(_requirements()["telemetry-4"])["TelemetryAssertions"]["records"][0]["record"]
    assert record["status"] == "SUCCEEDED"
    assert record["input"] == [1, 2]
    assert record["output"] == ["Hello, 1!", "Hello, 2!"]
    operations = record["operationsByName"]
    assert (operations["process-items"]["type"], operations["process-items"]["subType"]) == ("CONTEXT", "Map")
    for name in ("item-1", "item-2"):
        assert (operations[name]["type"], operations[name]["subType"]) == ("CONTEXT", "MapIteration")
    for name in ("greet-1", "greet-2"):
        assert (operations[name]["type"], operations[name]["subType"]) == ("STEP", "Step")
        assert operations[name]["maxAttempt"] == 1
    # Every SDK reads `attempt` from the step details only. So a context has no
    # `maxAttempt`, and the closed mapping fails a plugin that writes one.
    for name in ("process-items", "item-1", "item-2"):
        assert "maxAttempt" not in operations[name]
    for summary in operations.values():
        assert summary["count"] == 1
        assert summary["failedCount"] == 0
        assert {"minDurationMs", "maxDurationMs", "totalDurationMs"} <= set(summary)
        assert "result" not in summary
        assert "error" not in summary


@pytest.mark.parametrize("requirement_id", ["telemetry-1", "telemetry-2", "telemetry-3", "telemetry-4"])
def test_identity_fields_are_named_through_placeholders(requirement_id: str) -> None:
    record = load_yaml_file(_requirements()[requirement_id])["TelemetryAssertions"]["records"][0]["record"]
    assert {field: record[field] for field in ("executionArn", "executionName", "functionName")} == {
        "executionArn": "${EXECUTION_ARN}",
        "executionName": "${EXECUTION_NAME}",
        "functionName": "${FUNCTION_NAME}",
    }
    assert {field: record[field] for field in ("functionQualifier", "region", "accountId")} == {
        "functionQualifier": "${FUNCTION_QUALIFIER}",
        "region": "${REGION}",
        "accountId": "${ACCOUNT_ID}",
    }


@pytest.mark.parametrize("requirement_id", ["telemetry-1", "telemetry-2", "telemetry-3", "telemetry-4"])
def test_timestamps_are_pinned_to_three_fraction_digits(requirement_id: str) -> None:
    record = load_yaml_file(_requirements()[requirement_id])["TelemetryAssertions"]["records"][0]["record"]
    pattern = "${/^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z$/}"
    assert record["emittedAt"] == record["startTime"] == record["endTime"] == pattern
