# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Contract tests for the movable Python OTel conformance examples."""

from __future__ import annotations

import ast
from pathlib import Path

from aws_durable_execution_conformance_tests.validate import (
    parse_function_descriptions,
)

EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples" / "python"
EXPECTED_MAPPINGS = [
    ("Otel1Success", "otel-1"),
    ("Otel2WaitResume", "otel-2"),
    ("Otel3Retry", "otel-3"),
    ("Otel4TerminalFailure", "otel-4"),
]
REQUIRED_OTEL_PARAMETERS = {
    "LambdaExecutionRoleArn",
    "OtelLayerArn",
    "OtelExecWrapper",
    "OtelServiceName",
    "OtelTracesExporter",
    "OtelExporterEndpoint",
    "OtelExporterHeaders",
    "OtelSecretEnvironmentNames",
}


def test_python_example_template_maps_every_otel_requirement() -> None:
    template_path = EXAMPLES_DIR / "template.yaml"
    mappings = parse_function_descriptions(str(template_path))

    assert mappings == EXPECTED_MAPPINGS


def test_python_example_template_accepts_runner_parameters() -> None:
    template = (EXAMPLES_DIR / "template.yaml").read_text(encoding="utf-8")

    for parameter in REQUIRED_OTEL_PARAMETERS:
        assert f"  {parameter}:" in template
    assert "    NoEcho: true" in template
    assert template.count("      Role: !Ref LambdaExecutionRoleArn") == len(EXPECTED_MAPPINGS)
    assert template.count("BuildMethod: makefile") == len(EXPECTED_MAPPINGS)
    for case_number in range(1, 5):
        assert f'FunctionName: !Sub "otel-{case_number}-${{AWS::StackName}}"' in template


def test_python_example_handlers_are_valid_python() -> None:
    source_dir = EXAMPLES_DIR / "src"
    modules = {path.stem for path in source_dir.glob("*.py")}

    assert modules == {
        "common",
        "otel_1_success",
        "otel_2_wait_resume",
        "otel_3_retry",
        "otel_4_terminal_failure",
    }
    for path in source_dir.glob("*.py"):
        ast.parse(path.read_text(encoding="utf-8"), filename=str(path))


def test_python_examples_track_both_sdk_packages_from_main() -> None:
    requirements = (EXAMPLES_DIR / "src" / "requirements.txt").read_text(encoding="utf-8")

    assert (
        "aws-durable-execution-sdk-python @ "
        "git+https://github.com/aws/aws-durable-execution-sdk-python.git@main"
        "#subdirectory=packages/aws-durable-execution-sdk-python"
    ) in requirements
    assert (
        "aws-durable-execution-sdk-python-otel @ "
        "git+https://github.com/aws/aws-durable-execution-sdk-python.git@main"
        "#subdirectory=packages/aws-durable-execution-sdk-python-otel"
    ) in requirements
