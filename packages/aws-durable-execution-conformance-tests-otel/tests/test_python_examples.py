# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Contract tests for the movable Python OTel conformance examples."""

from __future__ import annotations

import ast
from pathlib import Path

from aws_durable_execution_conformance_tests.validate import (
    parse_function_descriptions,
    parse_not_implemented,
)

EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples" / "python"
WORKFLOW_PATH = EXAMPLES_DIR.parents[3] / ".github" / "workflows" / "python-opentelemetry.yml"
COLLECTOR_BUILD_SCRIPT = "packages/aws-durable-execution-conformance-tests-otel/collector/build-lambda-layer.sh"
EXPECTED_MAPPINGS = [
    ("Otel1Success", "otel-invocation-1"),
    ("Otel2WaitResume", "otel-invocation-2"),
    ("Otel3Retry", "otel-invocation-3"),
    ("Otel4TerminalFailure", "otel-invocation-4"),
    ("Otel5ChildContext", "otel-invocation-5"),
    ("Otel6Parallel", "otel-invocation-6"),
    ("Otel7Map", "otel-invocation-7"),
    ("Otel8HandledFailure", "otel-invocation-8"),
    ("Otel9WaitForCondition", "otel-invocation-9"),
    ("Otel10WaitForCallback", "otel-invocation-10"),
    ("Otel11ChainedInvoke", "otel-invocation-11"),
    ("Otel12ChildContextFailure", "otel-invocation-12"),
    ("Otel13ParallelFailure", "otel-invocation-13"),
    ("Otel14MapFailure", "otel-invocation-14"),
    ("Otel15WaitInterrupted", "otel-invocation-15"),
    ("Otel16WaitForConditionFailure", "otel-invocation-16"),
    ("Otel17WaitForCallbackFailure", "otel-invocation-17"),
    ("Otel18ChainedInvokeFailure", "otel-invocation-18"),
    ("Otel19ExecutionFailure", "otel-invocation-19"),
    ("OtelExecution1Success", "otel-execution-1"),
    ("OtelExecution2WaitResume", "otel-execution-2"),
    ("OtelExecution3Retry", "otel-execution-3"),
    ("OtelExecution4TerminalFailure", "otel-execution-4"),
    ("OtelExecution5ChildContext", "otel-execution-5"),
    ("OtelExecution6Parallel", "otel-execution-6"),
    ("OtelExecution7Map", "otel-execution-7"),
    ("OtelExecution8HandledFailure", "otel-execution-8"),
    ("OtelExecution9WaitForCondition", "otel-execution-9"),
    ("OtelExecution10WaitForCallback", "otel-execution-10"),
    ("OtelExecution11ChainedInvoke", "otel-execution-11"),
    ("OtelExecution12ChildContextFailure", "otel-execution-12"),
    ("OtelExecution13ParallelFailure", "otel-execution-13"),
    ("OtelExecution14MapFailure", "otel-execution-14"),
    ("OtelExecution16WaitForConditionFailure", "otel-execution-16"),
    ("OtelExecution17WaitForCallbackFailure", "otel-execution-17"),
    ("OtelExecution18ChainedInvokeFailure", "otel-execution-18"),
]
EXECUTION_CASES = (*range(1, 15), 16, 17, 18)
REQUIRED_OTEL_PARAMETERS = {
    "LambdaExecutionRoleArn",
    "OtelCollectorBucket",
    "OtelCollectorLayerArn",
    "OtelCollectorPrefix",
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


def test_python_example_declares_execution_plugin_lifecycle_gaps() -> None:
    assert parse_not_implemented(str(EXAMPLES_DIR / "template.yaml")) == {
        "otel-execution-15": (
            "ExecutionOtelPlugin cannot export a terminal workflow after a pending invocation times out externally"
        ),
        "otel-execution-19": "ExecutionOtelPlugin discards the workflow after the handler invocation ends with RETRY",
    }


def test_python_example_template_accepts_runner_parameters() -> None:
    template = (EXAMPLES_DIR / "template.yaml").read_text(encoding="utf-8")

    for parameter in REQUIRED_OTEL_PARAMETERS:
        assert f"  {parameter}:" in template
    assert "    NoEcho: true" in template
    assert template.count("      Role: !Ref LambdaExecutionRoleArn") == len(EXPECTED_MAPPINGS) + 4
    assert template.count("BuildMethod: makefile") == len(EXPECTED_MAPPINGS) + 4
    for case_number in range(1, 20):
        assert f'FunctionName: !Sub "${{AWS::StackName}}-otel-invocation-{case_number}"' in template
    for case_number in EXECUTION_CASES:
        assert f'FunctionName: !Sub "${{AWS::StackName}}-otel-execution-{case_number}"' in template
    assert 'FunctionName: !Sub "${AWS::StackName}-otel-invocation-11-target"' in template
    assert 'FunctionName: !Sub "${AWS::StackName}-otel-invocation-18-target"' in template
    assert 'FunctionName: !Sub "${AWS::StackName}-otel-execution-11-target"' in template
    assert 'FunctionName: !Sub "${AWS::StackName}-otel-execution-18-target"' in template
    assert 'OTEL_INVOKE_TARGET_FUNCTION_NAME: !Sub "${Otel11InvokeTarget.Arn}:$LATEST"' in template
    assert 'OTEL_INVOKE_TARGET_FUNCTION_NAME: !Sub "${Otel18InvokeTarget.Arn}:$LATEST"' in template
    assert 'OTEL_INVOKE_TARGET_FUNCTION_NAME: !Sub "${OtelExecution11InvokeTarget.Arn}:$LATEST"' in template
    assert 'OTEL_INVOKE_TARGET_FUNCTION_NAME: !Sub "${OtelExecution18InvokeTarget.Arn}:$LATEST"' in template
    assert "ExecutionTimeout: 5" in template
    assert "HasOtelCollectorLayer: !Not" in template
    assert '!Ref "AWS::NoValue"' in template
    assert "OTEL_S3_BUCKET: !Ref OtelCollectorBucket" in template
    assert "OTEL_S3_PREFIX: !Ref OtelCollectorPrefix" in template
    assert "/opt/collector-config/config-s3.yaml" in template
    assert template.count("          OTEL_PLUGIN_MODE: execution") == len(EXECUTION_CASES) + 2

    makefile = (EXAMPLES_DIR / "src" / "Makefile").read_text(encoding="utf-8")
    for logical_id, _description_id in EXPECTED_MAPPINGS:
        assert f"build-{logical_id}" in makefile
    assert "build-Otel11InvokeTarget" in makefile
    assert "build-Otel18InvokeTarget" in makefile
    assert "build-OtelExecution11InvokeTarget" in makefile
    assert "build-OtelExecution18InvokeTarget" in makefile


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


def test_python_examples_pin_both_sdk_packages_to_execution_plugin_pr() -> None:
    requirements = (EXAMPLES_DIR / "src" / "requirements.txt").read_text(encoding="utf-8")
    sdk_ref = "6ff4e2e8fecd4530e55959a479c20ca458d307e6"

    assert (
        "aws-durable-execution-sdk-python @ "
        f"git+https://github.com/aws/aws-durable-execution-sdk-python.git@{sdk_ref}"
        "#subdirectory=packages/aws-durable-execution-sdk-python"
    ) in requirements
    assert (
        "aws-durable-execution-sdk-python-otel @ "
        f"git+https://github.com/aws/aws-durable-execution-sdk-python.git@{sdk_ref}"
        "#subdirectory=packages/aws-durable-execution-sdk-python-otel"
    ) in requirements

    common = (EXAMPLES_DIR / "src" / "common.py").read_text(encoding="utf-8")
    assert "ExecutionOtelPlugin" in common
    assert "InvocationOtelPlugin" in common
    assert 'os.environ.get("OTEL_PLUGIN_MODE") == "execution"' in common


def test_python_s3_job_builds_and_queries_the_collector() -> None:
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert "  collector:" in workflow
    assert "  s3_collector:" in workflow
    assert "github.base_ref == 'main'" in workflow
    assert "open-telemetry/opentelemetry-lambda" in workflow
    assert "layer-collector/0.22.0" in workflow
    assert COLLECTOR_BUILD_SCRIPT in workflow
    assert "--compatible-runtimes python3.13" in workflow
    assert "--language python" in workflow
    assert "--otel-exporter community" in workflow
    assert "--otel-endpoint http://localhost:4318" in workflow
    assert "--otel-backend collector" in workflow
    assert '--otel-backend-endpoint "$OTEL_S3_URI"' in workflow
    assert "OtelCollectorLayerArn=$COLLECTOR_LAYER_ARN" in workflow
    assert "OtelCollectorBucket=$OTEL_S3_BUCKET" in workflow
    assert "OtelCollectorPrefix=$OTEL_S3_PREFIX" in workflow
    assert "delete-layer-version" in workflow
    assert '--suite "$OTEL_SUITE"' in workflow
