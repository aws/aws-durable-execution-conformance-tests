# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Contract tests for the movable Python OTel conformance examples."""

from __future__ import annotations

import ast
from pathlib import Path

import yaml

from aws_durable_execution_conformance_tests.validate import (
    _CfnSafeLoader,
    parse_function_descriptions,
    parse_not_implemented,
)

EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples" / "python"
ENTRY_WORKFLOW_PATH = EXAMPLES_DIR.parents[3] / ".github" / "workflows" / "python-opentelemetry.yml"
WORKFLOW_PATH = EXAMPLES_DIR.parents[3] / ".github" / "workflows" / "python-opentelemetry-suite.yml"
LONG_RUNNING_WORKFLOW_PATH = EXAMPLES_DIR.parents[3] / ".github" / "workflows" / "python-opentelemetry-long-running.yml"
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


def test_python_example_template_maps_every_otel_requirement() -> None:
    template_path = EXAMPLES_DIR / "template.yaml"
    mappings = parse_function_descriptions(str(template_path))

    assert mappings == EXPECTED_MAPPINGS


def test_python_example_declares_no_execution_plugin_lifecycle_gaps() -> None:
    assert parse_not_implemented(str(EXAMPLES_DIR / "template.yaml")) == {}


def test_python_template_deploys_only_the_selected_otel_view() -> None:
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
    assert template["Resources"]["OtelExecution15WaitInterrupted"]["Properties"]["DurableConfig"] == {
        "ExecutionTimeout": 5,
        "RetentionPeriodInDays": 1,
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
    assert template.count("        OTEL_PLUGIN_MODE: invocation") == 1
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
        "otel_long_running_1_wait",
        "otel_long_running_2_retry",
        "otel_long_running_3_callback",
        "otel_long_running_4_chained_invoke",
    }
    for path in source_dir.glob("*.py"):
        ast.parse(path.read_text(encoding="utf-8"), filename=str(path))

    makefile = (source_dir / "Makefile").read_text(encoding="utf-8")
    for logical_id in (
        "OtelLongRunning1Wait",
        "OtelLongRunning2Retry",
        "OtelLongRunning3Callback",
        "OtelLongRunning4ChainedInvoke",
        "OtelLongRunning4InvokeTarget",
    ):
        assert f"build-{logical_id}" in makefile


def test_python_examples_install_both_sdk_packages_from_one_resolved_main_commit() -> None:
    requirements = (EXAMPLES_DIR / "src" / "requirements.txt").read_text(encoding="utf-8")

    assert (
        "aws-durable-execution-sdk-python @ "
        "git+https://github.com/aws/aws-durable-execution-sdk-python.git@${PYTHON_SDK_REF}"
        "#subdirectory=packages/aws-durable-execution-sdk-python"
    ) in requirements
    assert (
        "aws-durable-execution-sdk-python-otel @ "
        "git+https://github.com/aws/aws-durable-execution-sdk-python.git@${PYTHON_SDK_REF}"
        "#subdirectory=packages/aws-durable-execution-sdk-python-otel"
    ) in requirements

    common = (EXAMPLES_DIR / "src" / "common.py").read_text(encoding="utf-8")
    assert "ExecutionOtelPlugin" in common
    assert "InvocationOtelPlugin" in common
    assert "provider_source=ProviderSource.GLOBAL" in common
    assert 'os.environ.get("OTEL_PLUGIN_MODE") == "execution"' in common


def test_python_workflow_resolves_and_propagates_test_commits() -> None:
    entry_workflow = ENTRY_WORKFLOW_PATH.read_text(encoding="utf-8")
    entry_workflow_config = yaml.safe_load(entry_workflow)
    suite_workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
    long_running_workflow = LONG_RUNNING_WORKFLOW_PATH.read_text(encoding="utf-8")

    assert "  workflow_call:" in entry_workflow
    triggers = entry_workflow_config.get("on") or entry_workflow_config[True]
    for trigger in ("workflow_call", "workflow_dispatch"):
        assert triggers[trigger]["inputs"]["conformance_test_ref"] == {
            "description": "Optional conformance test commit SHA or branch name",
            "required": False,
            "type": "string",
        }
    assert "github.repository == job.workflow_repository && job.workflow_sha" in entry_workflow
    assert "|| 'main'" in entry_workflow
    assert "REQUESTED_PYTHON_SDK_REF: ${{ inputs.python_sdk_ref }}" in entry_workflow
    assert entry_workflow.count("git ls-remote") == 2
    assert "refs/heads/main" in entry_workflow
    assert 'echo "sha=$CONFORMANCE_TEST_SHA" >> "$GITHUB_OUTPUT"' in entry_workflow
    assert 'echo "ref=$PYTHON_SDK_REF" >> "$GITHUB_OUTPUT"' in entry_workflow
    assert entry_workflow.count("needs: resolve-sdk-main") == 4
    assert entry_workflow.count("conformance_test_sha: ${{ needs.resolve-sdk-main.outputs.conformance_test_sha }}") == 6
    assert entry_workflow.count("python_sdk_ref: ${{ needs.resolve-sdk-main.outputs.python_sdk_ref }}") == 6
    for secret in (
        "CONFORMANCE_TEST_ROLE_ARN",
        "CONFORMANCE_TEST_ACCOUNT_ID",
        "CONFORMANCE_TEST_LAMBDA_EXECUTION_ROLE_ARN",
    ):
        assert f"      {secret}:" in entry_workflow
        assert f"secrets.{secret}" in suite_workflow
        assert f"secrets.{secret}" in long_running_workflow
    for retired_secret in (
        "PYTHON_TEST_ROLE_ARN",
        "PYTHON_TEST_ACCOUNT_ID",
        "PYTHON_TEST_LAMBDA_EXECUTION_ROLE_ARN",
    ):
        assert retired_secret not in entry_workflow
        assert retired_secret not in suite_workflow
        assert retired_secret not in long_running_workflow
    for workflow in (suite_workflow, long_running_workflow):
        assert "PYTHON_SDK_REF: ${{ inputs.python_sdk_ref }}" in workflow
        assert 'PYTHONUNBUFFERED: "1"' in workflow
        assert "      conformance_test_sha:" in workflow
        assert "      python_sdk_ref:" in workflow
        assert "        required: true" in workflow


def test_python_workflow_uses_the_resource_service_name() -> None:
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert workflow.count("--otel-service-name durable-execution-conformance") == 3


def test_python_workflow_validates_reusable_inputs() -> None:
    workflow = yaml.safe_load(ENTRY_WORKFLOW_PATH.read_text(encoding="utf-8"))
    resolve_steps = workflow["jobs"]["resolve-sdk-main"]["steps"]

    phase_validation = next(step for step in resolve_steps if step.get("name") == "Validate phase")
    assert 'case "$REQUESTED_PHASE" in' in phase_validation["run"]
    assert "short|launch|check)" in phase_validation["run"]

    sdk_resolution = next(step for step in resolve_steps if step.get("name") == "Resolve Python SDK commit")
    assert '[[ ! "$PYTHON_SDK_REF" =~ ^[0-9a-f]{40}$ ]]' in sdk_resolution["run"]
    assert "python_sdk_ref must be a full 40-character commit SHA" in sdk_resolution["run"]

    conformance_resolution = next(
        step for step in resolve_steps if step.get("name") == "Resolve conformance test commit"
    )
    assert 'CONFORMANCE_TEST_REF="refs/heads/$CONFORMANCE_TEST_REF"' in conformance_resolution["run"]
    assert "aws/aws-durable-execution-conformance-tests.git" in conformance_resolution["run"]
    assert 'echo "sha=$CONFORMANCE_TEST_SHA" >> "$GITHUB_OUTPUT"' in conformance_resolution["run"]


def test_python_workflows_check_out_the_resolved_conformance_revision() -> None:
    suite_workflow = yaml.safe_load(WORKFLOW_PATH.read_text(encoding="utf-8"))
    long_running_workflow = yaml.safe_load(LONG_RUNNING_WORKFLOW_PATH.read_text(encoding="utf-8"))

    suite_checkouts = [
        step
        for job in suite_workflow["jobs"].values()
        for step in job["steps"]
        if step.get("name") == "Check out repository"
    ]
    assert len(suite_checkouts) == 4
    for checkout in suite_checkouts:
        assert checkout["with"]["repository"] == "${{ job.workflow_repository }}"
        assert checkout["with"]["ref"] == "${{ inputs.conformance_test_sha }}"

    long_running_checkout = next(
        step for step in long_running_workflow["jobs"]["run"]["steps"] if step.get("name") == "Check out repository"
    )
    assert long_running_checkout["with"]["repository"] == "${{ job.workflow_repository }}"
    assert long_running_checkout["with"]["ref"] == (
        "${{ steps.state.outputs.source_revision || inputs.conformance_test_sha }}"
    )
    assert '--source-revision "$CONFORMANCE_REF"' in "\n".join(
        step.get("run", "") for step in long_running_workflow["jobs"]["run"]["steps"]
    )


def test_python_s3_job_builds_and_queries_the_collector() -> None:
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert "  collector:" not in workflow
    assert "Community layer + test collector" not in workflow
    assert "collector_query_endpoint" not in workflow
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
    assert "delete-layer-version" not in workflow
    assert '--suite "$OTEL_SUITE"' in workflow
