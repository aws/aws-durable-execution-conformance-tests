# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Contract tests for the TypeScript OTel conformance examples."""

from __future__ import annotations

import json
from pathlib import Path

import yaml

from aws_durable_execution_conformance_tests.config import STACK_NAME_PREFIX
from aws_durable_execution_conformance_tests.validate import (
    _CfnSafeLoader,
    parse_function_descriptions,
    parse_not_implemented,
)

EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples" / "typescript"
ENTRY_WORKFLOW_PATH = EXAMPLES_DIR.parents[3] / ".github" / "workflows" / "typescript-opentelemetry.yml"
WORKFLOW_PATH = EXAMPLES_DIR.parents[3] / ".github" / "workflows" / "typescript-opentelemetry-suite.yml"
LONG_RUNNING_WORKFLOW_PATH = (
    EXAMPLES_DIR.parents[3] / ".github" / "workflows" / "typescript-opentelemetry-long-running.yml"
)
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
    ("OtelExecution15WaitInterrupted", "otel-execution-15"),
    ("OtelExecution16WaitForConditionFailure", "otel-execution-16"),
    ("OtelExecution17WaitForCallbackFailure", "otel-execution-17"),
    ("OtelExecution18ChainedInvokeFailure", "otel-execution-18"),
    ("OtelExecution19ExecutionFailure", "otel-execution-19"),
]
EXECUTION_CASES = tuple(range(1, 20))
REQUIRED_OTEL_PARAMETERS = {
    "LambdaExecutionRoleArn",
    "OtelCollectorBucket",
    "OtelCollectorLayerArn",
    "OtelCollectorPrefix",
    "OtelLayerArn",
    "OtelSuite",
    "OtelExecWrapper",
    "OtelServiceName",
    "OtelTracesExporter",
    "OtelExporterEndpoint",
    "OtelExporterHeaders",
    "OtelSecretEnvironmentNames",
}


def test_typescript_example_template_maps_every_otel_requirement() -> None:
    assert parse_function_descriptions(str(EXAMPLES_DIR / "template.yaml")) == EXPECTED_MAPPINGS


def test_typescript_example_declares_no_execution_plugin_gaps() -> None:
    assert parse_not_implemented(str(EXAMPLES_DIR / "template.yaml")) == {}


def test_wait_interrupted_functions_use_short_execution_timeout() -> None:
    with (EXAMPLES_DIR / "template.yaml").open(encoding="utf-8") as stream:
        resources = yaml.load(stream, Loader=_CfnSafeLoader)["Resources"]

    assert resources["Otel15WaitInterrupted"]["Properties"]["DurableConfig"]["ExecutionTimeout"] == 5
    assert resources["OtelExecution15WaitInterrupted"]["Properties"]["DurableConfig"]["ExecutionTimeout"] == 5


def test_typescript_template_deploys_only_the_selected_otel_view() -> None:
    with (EXAMPLES_DIR / "template.yaml").open(encoding="utf-8") as stream:
        template = yaml.load(stream, Loader=_CfnSafeLoader)

    assert template["Parameters"]["OtelSuite"] == {
        "Type": "String",
        "Default": "all",
        "AllowedValues": ["all", "otel-invocation", "otel-execution"],
        "Description": "OpenTelemetry view whose functions should be deployed",
    }
    assert {"DeployInvocationView", "DeployExecutionView"} <= template["Conditions"].keys()
    for logical_id, resource in template["Resources"].items():
        expected_condition = "DeployExecutionView" if logical_id.startswith("OtelExecution") else "DeployInvocationView"
        assert resource["Condition"] == expected_condition


def test_typescript_example_template_accepts_runner_parameters() -> None:
    template = (EXAMPLES_DIR / "template.yaml").read_text(encoding="utf-8")

    for parameter in REQUIRED_OTEL_PARAMETERS:
        assert f"  {parameter}:" in template
    assert "    NoEcho: true" in template
    assert template.count("      Role: !Ref LambdaExecutionRoleArn") == len(EXPECTED_MAPPINGS) + 4
    assert template.count("      CodeUri: dist/") == len(EXPECTED_MAPPINGS) + 4
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
    assert "Runtime: nodejs22.x" in template
    assert "AWS_LAMBDA_EXEC_WRAPPER: !Ref OtelExecWrapper" in template
    assert "Default: /opt/otel-instrument" in template
    assert "HasOtelCollectorLayer: !Not" in template
    assert '!Ref "AWS::NoValue"' in template
    assert "/opt/collector-config/config-s3.yaml" in template
    assert "OTEL_S3_BUCKET: !Ref OtelCollectorBucket" in template
    assert "OTEL_S3_PREFIX: !Ref OtelCollectorPrefix" in template
    assert template.count("        OTEL_PLUGIN_MODE: invocation") == 1
    assert template.count("          OTEL_PLUGIN_MODE: execution") == len(EXECUTION_CASES) + 2


def test_typescript_template_handlers_have_sources() -> None:
    template = (EXAMPLES_DIR / "template.yaml").read_text(encoding="utf-8")
    source_dir = EXAMPLES_DIR / "handlers"
    expected_modules = {
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
        "otel_20_long_wait",
        "otel_21_long_retry",
        "otel_22_long_callback",
        "otel_23_long_chained_invoke",
    }

    assert {path.stem for path in source_dir.glob("*.ts")} == expected_modules
    handlers = {
        line.strip().removeprefix("Handler: ").split(".")[0]
        for line in template.splitlines()
        if line.strip().startswith("Handler: ")
    }
    assert handlers == expected_modules - {
        "common",
        "otel_20_long_wait",
        "otel_21_long_retry",
        "otel_22_long_callback",
        "otel_23_long_chained_invoke",
    }


def test_typescript_examples_build_sdk_packages_from_main() -> None:
    package = json.loads((EXAMPLES_DIR / "package.json").read_text(encoding="utf-8"))
    bootstrap = (EXAMPLES_DIR / "scripts" / "install-sdk-main.sh").read_text(encoding="utf-8")
    common = (EXAMPLES_DIR / "handlers" / "common.ts").read_text(encoding="utf-8")

    assert "@aws/durable-execution-sdk-js" not in package["dependencies"]
    assert "@aws/durable-execution-sdk-js-otel" not in package["dependencies"]
    assert "git clone --depth 1 --branch main" in bootstrap
    assert "--workspace packages/aws-durable-execution-sdk-js" in bootstrap
    assert "--workspace packages/aws-durable-execution-sdk-js-otel" in bootstrap
    assert "InvocationOtelPlugin({ useDefaultTracerProvider: true })" in common
    assert "ExecutionOtelPlugin({ useDefaultTracerProvider: true })" in common
    assert 'process.env.OTEL_PLUGIN_MODE === "execution"' in common


def test_typescript_workflow_uses_current_adot_distro() -> None:
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert "AWSOpenTelemetryDistroJs" in workflow
    assert "aws-observability/aws-otel-js-instrumentation/releases/latest" in workflow
    assert "npm run install-sdk-main" in workflow
    assert "--language javascript" in workflow
    assert '--suite "$OTEL_SUITE"' in workflow
    assert workflow.count("--otel-service-name durable-execution-conformance") == 3


def test_typescript_workflow_resolves_main_once_and_propagates_the_commit() -> None:
    entry_workflow = ENTRY_WORKFLOW_PATH.read_text(encoding="utf-8")
    suite_workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
    long_running_workflow = LONG_RUNNING_WORKFLOW_PATH.read_text(encoding="utf-8")

    assert entry_workflow.count("git ls-remote") == 1
    assert "refs/heads/main" in entry_workflow
    assert 'echo "ref=$TYPESCRIPT_SDK_REF" >> "$GITHUB_OUTPUT"' in entry_workflow
    assert entry_workflow.count("needs: resolve-sdk-main") == 4
    assert entry_workflow.count("typescript_sdk_ref: ${{ needs.resolve-sdk-main.outputs.typescript_sdk_ref }}") == 4
    for workflow in (suite_workflow, long_running_workflow):
        assert "TYPESCRIPT_SDK_REF: ${{ inputs.typescript_sdk_ref }}" in workflow
        assert "      typescript_sdk_ref:" in workflow
        assert "        required: true" in workflow
    assert suite_workflow.count("repository: aws/aws-durable-execution-sdk-js") == 2
    assert suite_workflow.count("ref: ${{ env.TYPESCRIPT_SDK_REF }}") == 2
    assert suite_workflow.count("SDK_SOURCE_DIR: ${{ github.workspace }}/.build/aws-durable-execution-sdk-js") == 2
    assert long_running_workflow.count("repository: aws/aws-durable-execution-sdk-js") == 1
    assert long_running_workflow.count("ref: ${{ env.TYPESCRIPT_SDK_REF }}") == 1
    assert (
        long_running_workflow.count("SDK_SOURCE_DIR: ${{ github.workspace }}/.build/aws-durable-execution-sdk-js") == 1
    )


def test_typescript_s3_job_builds_and_queries_the_collector() -> None:
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert "  s3_collector:" in workflow
    assert "open-telemetry/opentelemetry-lambda" in workflow
    assert "layer-collector/0.22.0" in workflow
    assert COLLECTOR_BUILD_SCRIPT in workflow
    assert "--otel-exporter community" in workflow
    assert "--otel-backend collector" in workflow
    assert '--otel-backend-endpoint "$OTEL_S3_URI"' in workflow
    assert "npm run install-sdk-main" in workflow
    assert "--language javascript" in workflow


def test_typescript_workflow_uses_lambda_compatible_function_names() -> None:
    for backend in ("xray", "s3"):
        for view, suite in (("inv", "otel-invocation"), ("exec", "otel-execution")):
            stack_name = f"{STACK_NAME_PREFIX}-typescript-{backend}-{view}"

            assert len(f"{stack_name}-{suite}-18-target") <= 64


def test_typescript_bundle_uses_the_external_collector_layer() -> None:
    rollup = (EXAMPLES_DIR / "rollup.config.mjs").read_text(encoding="utf-8")

    assert "collector-config" not in rollup
