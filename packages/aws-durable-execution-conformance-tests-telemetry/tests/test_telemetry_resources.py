# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Packaged Durable Execution Telemetry requirement resource tests."""

from __future__ import annotations

from pathlib import Path

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
    assert set(_requirements()) == {"telemetry-1"}


def test_telemetry_1_loads_with_core_sections() -> None:
    requirement = load_yaml_file(_requirements()["telemetry-1"])
    assert requirement["Input"] == {"value": 41}
    assert requirement["ExpectedResult"] == {"ExecutionStatus": "SUCCEEDED", "Result": {"value": 42}}
    assert [event["EventType"] for event in requirement["ExpectedExecutionHistory"]] == [
        "StepStarted",
        "StepSucceeded",
        "InvocationCompleted",
    ]


def test_telemetry_assertions_expect_one_record_with_three_sections() -> None:
    requirement = load_yaml_file(_requirements()["telemetry-1"])
    assertions = requirement["TelemetryAssertions"]
    assert set(assertions) == {"records"}
    assert len(assertions["records"]) == 1
    entry = assertions["records"][0]
    assert set(entry) == {"record", "forbid_fields", "operations_by_name"}
    assert entry["record"]["schemaVersion"] == "1.0"
    assert entry["forbid_fields"] == ["error"]
    assert set(entry["operations_by_name"]) == {"increment"}
