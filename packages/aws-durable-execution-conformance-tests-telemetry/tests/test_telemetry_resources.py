# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Packaged Durable Execution Telemetry requirement resource tests."""

from __future__ import annotations

import re
from pathlib import Path

from aws_durable_execution_conformance_tests.validate import (
    discover_test_files,
    load_yaml_file,
)
from aws_durable_execution_conformance_tests_telemetry.extension import TelemetryExtension

PLACEHOLDER = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _suite_root() -> Path:
    (suite,) = TelemetryExtension().requirement_suites()
    return suite.root


def _requirements() -> dict[str, str]:
    return discover_test_files(_suite_root())


def _placeholders(value: object) -> set[str]:
    if isinstance(value, str):
        return set(PLACEHOLDER.findall(value))
    if isinstance(value, dict):
        return {name for item in value.values() for name in _placeholders(item)}
    if isinstance(value, list):
        return {name for item in value for name in _placeholders(item)}
    return set()


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


def test_telemetry_assertions_have_exactly_the_three_sections() -> None:
    requirement = load_yaml_file(_requirements()["telemetry-1"])
    assertions = requirement["TelemetryAssertions"]
    assert set(assertions) == {"record", "forbid_fields", "operations_by_name"}
    assert assertions["record"]["schemaVersion"] == "1.0"
    assert assertions["forbid_fields"] == ["error"]
    assert set(assertions["operations_by_name"]) == {"increment"}


def test_telemetry_assertions_reference_only_execution_arn() -> None:
    requirement = load_yaml_file(_requirements()["telemetry-1"])
    assert _placeholders(requirement["TelemetryAssertions"]) == {"EXECUTION_ARN"}
    assert _placeholders(requirement["ExpectedExecutionHistory"]) == {"ID1"}
