# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Contract tests for the movable Python OTel conformance examples."""

from __future__ import annotations

import ast
from pathlib import Path

from aws_durable_execution_conformance_tests.validate import (
    ExpectedFailure,
    parse_expected_failures,
    parse_function_descriptions,
)

EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples" / "python"
EXPECTED_MAPPINGS = [
    ("Otel1Success", "otel-1"),
    ("Otel2WaitResume", "otel-2"),
    ("Otel3Retry", "otel-3"),
    ("Otel4TerminalFailure", "otel-4"),
    ("Otel5ChildContext", "otel-5"),
    ("Otel6Parallel", "otel-6"),
    ("Otel7Map", "otel-7"),
    ("Otel8HandledFailure", "otel-8"),
    ("Otel9WaitForCondition", "otel-9"),
    ("Otel10WaitForCallback", "otel-10"),
    ("Otel11ChainedInvoke", "otel-11"),
    ("Otel12ChildContextFailure", "otel-12"),
    ("Otel13ParallelFailure", "otel-13"),
    ("Otel14MapFailure", "otel-14"),
    ("Otel15WaitInterrupted", "otel-15"),
    ("Otel16WaitForConditionFailure", "otel-16"),
    ("Otel17WaitForCallbackFailure", "otel-17"),
    ("Otel18ChainedInvokeFailure", "otel-18"),
    ("Otel19ExecutionFailure", "otel-19"),
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


def test_python_example_template_declares_only_known_sdk_failures() -> None:
    template_path = EXAMPLES_DIR / "template.yaml"

    assert parse_expected_failures(str(template_path)) == {
        "otel-3": ExpectedFailure(
            reason=(
                "Waiting for the retry span fix in https://github.com/aws/aws-durable-execution-sdk-python/pull/568"
            ),
            errors=("OpenTelemetry: span_assertions[1].select matched no spans",),
        ),
        "otel-9": ExpectedFailure(
            reason=(
                "Waiting for the wait-for-condition span fix in "
                "https://github.com/aws/aws-durable-execution-sdk-python/pull/568"
            ),
            errors=("OpenTelemetry: span_assertions[1].select matched no spans",),
        ),
    }


def test_python_example_template_accepts_runner_parameters() -> None:
    template = (EXAMPLES_DIR / "template.yaml").read_text(encoding="utf-8")

    for parameter in REQUIRED_OTEL_PARAMETERS:
        assert f"  {parameter}:" in template
    assert "    NoEcho: true" in template
    assert template.count("      Role: !Ref LambdaExecutionRoleArn") == len(EXPECTED_MAPPINGS) + 2
    assert template.count("BuildMethod: makefile") == len(EXPECTED_MAPPINGS) + 2
    for case_number in range(1, 20):
        assert f'FunctionName: !Sub "${{AWS::StackName}}-otel-{case_number}"' in template
    assert 'FunctionName: !Sub "${AWS::StackName}-otel-11-target"' in template
    assert 'FunctionName: !Sub "${AWS::StackName}-otel-18-target"' in template
    assert 'OTEL_INVOKE_TARGET_FUNCTION_NAME: !Sub "${Otel11InvokeTarget.Arn}:$LATEST"' in template
    assert 'OTEL_INVOKE_TARGET_FUNCTION_NAME: !Sub "${Otel18InvokeTarget.Arn}:$LATEST"' in template
    assert "ExecutionTimeout: 5" in template

    makefile = (EXAMPLES_DIR / "src" / "Makefile").read_text(encoding="utf-8")
    for logical_id, _description_id in EXPECTED_MAPPINGS:
        assert f"build-{logical_id}" in makefile
    assert "build-Otel11InvokeTarget" in makefile
    assert "build-Otel18InvokeTarget" in makefile


def test_python_example_handlers_are_valid_python() -> None:
    source_dir = EXAMPLES_DIR / "src"
    modules = {path.stem for path in source_dir.glob("*.py")}

    assert modules == {
        "common",
        "otel_1_success",
        "otel_2_wait_resume",
        "otel_3_retry",
        "otel_4_terminal_failure",
        "otel_5_child_context",
        "otel_6_parallel",
        "otel_7_map",
        "otel_8_handled_failure",
        "otel_9_wait_for_condition",
        "otel_10_wait_for_callback",
        "otel_11_chained_invoke",
        "otel_12_child_context_failure",
        "otel_13_parallel_failure",
        "otel_14_map_failure",
        "otel_15_wait_interrupted",
        "otel_16_wait_for_condition_failure",
        "otel_17_wait_for_callback_failure",
        "otel_18_chained_invoke_failure",
        "otel_19_execution_failure",
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
