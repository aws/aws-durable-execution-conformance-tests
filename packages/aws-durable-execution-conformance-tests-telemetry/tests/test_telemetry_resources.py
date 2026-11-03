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
    assert set(_requirements()) == {"telemetry-1", "telemetry-2"}


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


@pytest.mark.parametrize(
    ("requirement_id", "operation_name"),
    [("telemetry-1", "increment"), ("telemetry-2", "charge")],
)
def test_telemetry_assertions_expect_one_complete_record(requirement_id: str, operation_name: str) -> None:
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
    assert set(record["operationsByName"]) == {operation_name}


def test_telemetry_2_describes_a_failure_record_without_output() -> None:
    record = load_yaml_file(_requirements()["telemetry-2"])["TelemetryAssertions"]["records"][0]["record"]
    assert record["status"] == "FAILED"
    assert "output" not in record
    assert "error" in record
    summary = record["operationsByName"]["charge"]
    assert summary["status"] == "FAILED"
    assert summary["failedCount"] == 1
    assert "error" in summary


def _chain(error: dict[str, object]) -> list[dict[str, object]]:
    levels: list[dict[str, object]] = []
    node: object = error
    while isinstance(node, dict):
        levels.append(node)
        node = node.get("cause")
    return levels


def test_telemetry_2_error_chain_has_wildcard_names_and_exact_messages() -> None:
    record = load_yaml_file(_requirements()["telemetry-2"])["TelemetryAssertions"]["records"][0]["record"]
    execution_levels = _chain(record["error"])
    step_levels = _chain(record["operationsByName"]["charge"]["error"])
    assert [level["message"] for level in execution_levels] == ["payment failed", "card declined", "card declined"]
    assert [level["message"] for level in step_levels] == ["card declined", "card declined"]
    for level in [*execution_levels, *step_levels]:
        assert level["name"] == "*"
        assert set(level) <= {"name", "message", "cause"}
    # The execution error's cause is the step error. So the two chains agree below the top level.
    assert execution_levels[1:] == step_levels


@pytest.mark.parametrize("requirement_id", ["telemetry-1", "telemetry-2"])
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


@pytest.mark.parametrize("requirement_id", ["telemetry-1", "telemetry-2"])
def test_timestamps_are_pinned_to_three_fraction_digits(requirement_id: str) -> None:
    record = load_yaml_file(_requirements()[requirement_id])["TelemetryAssertions"]["records"][0]["record"]
    pattern = "${/^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z$/}"
    assert record["emittedAt"] == record["startTime"] == record["endTime"] == pattern
