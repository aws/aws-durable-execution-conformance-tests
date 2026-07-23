# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Contract tests for the TypeScript OTel conformance examples."""

from __future__ import annotations

import json
from pathlib import Path

from aws_durable_execution_conformance_tests.config import STACK_NAME_PREFIX
from aws_durable_execution_conformance_tests.validate import parse_function_descriptions

EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples" / "typescript"
WORKFLOW_PATH = EXAMPLES_DIR.parents[3] / ".github" / "workflows" / "typescript-opentelemetry.yml"
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


def test_typescript_example_template_maps_every_otel_requirement() -> None:
    assert parse_function_descriptions(str(EXAMPLES_DIR / "template.yaml")) == EXPECTED_MAPPINGS


def test_typescript_example_template_accepts_runner_parameters() -> None:
    template = (EXAMPLES_DIR / "template.yaml").read_text(encoding="utf-8")

    for parameter in REQUIRED_OTEL_PARAMETERS:
        assert f"  {parameter}:" in template
    assert "    NoEcho: true" in template
    assert template.count("      Role: !Ref LambdaExecutionRoleArn") == len(EXPECTED_MAPPINGS) + 2
    assert template.count("      CodeUri: dist/") == len(EXPECTED_MAPPINGS) + 2
    for case_number in range(1, 20):
        assert f'FunctionName: !Sub "${{AWS::StackName}}-otel-{case_number}"' in template
    assert 'FunctionName: !Sub "${AWS::StackName}-otel-11-target"' in template
    assert 'FunctionName: !Sub "${AWS::StackName}-otel-18-target"' in template
    assert 'OTEL_INVOKE_TARGET_FUNCTION_NAME: !Sub "${Otel11InvokeTarget.Arn}:$LATEST"' in template
    assert 'OTEL_INVOKE_TARGET_FUNCTION_NAME: !Sub "${Otel18InvokeTarget.Arn}:$LATEST"' in template
    assert "ExecutionTimeout: 5" in template
    assert "Runtime: nodejs22.x" in template
    assert "AWS_LAMBDA_EXEC_WRAPPER: !Ref OtelExecWrapper" in template
    assert "Default: /opt/otel-handler" in template
    assert "OPENTELEMETRY_COLLECTOR_CONFIG_URI: /var/task/collector.yaml" in template
    assert "OTEL_S3_BUCKET: !Ref OtelCollectorBucket" in template
    assert "OTEL_S3_PREFIX: !Ref OtelCollectorPrefix" in template


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
    }

    assert {path.stem for path in source_dir.glob("*.ts")} == expected_modules
    handlers = {
        line.strip().removeprefix("Handler: ").split(".")[0]
        for line in template.splitlines()
        if line.strip().startswith("Handler: ")
    }
    assert handlers == expected_modules - {"common"}


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


def test_typescript_workflow_builds_and_queries_the_s3_collector() -> None:
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert "open-telemetry/opentelemetry-lambda" in workflow
    assert "layer-collector/0.22.0" in workflow
    assert "build-lambda-layer.sh" in workflow
    assert "--otel-exporter community" in workflow
    assert "--otel-backend collector" in workflow
    assert '--otel-backend-endpoint "$OTEL_S3_URI"' in workflow
    assert "npm run install-sdk-main" in workflow
    assert "--language javascript" in workflow


def test_typescript_workflow_uses_lambda_compatible_function_names() -> None:
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
    test_name = next(
        line.split(":", maxsplit=1)[1].strip() for line in workflow.splitlines() if line.startswith("  TEST_NAME:")
    )
    stack_name = f"{STACK_NAME_PREFIX}-{test_name}"

    assert f"  TEST_STACK_NAME: {stack_name}" in workflow
    assert len(f"{stack_name}-otel-18-target") <= 64


def test_typescript_bundle_includes_the_s3_collector_config() -> None:
    rollup = (EXAMPLES_DIR / "rollup.config.mjs").read_text(encoding="utf-8")
    config = (EXAMPLES_DIR.parent / "collector" / "config.yaml").read_text(encoding="utf-8")
    build_script = (EXAMPLES_DIR.parent / "collector" / "build-lambda-layer.sh").read_text(encoding="utf-8")
    component = (EXAMPLES_DIR.parent / "collector" / "lambda" / "awss3.go").read_text(encoding="utf-8")

    assert 'copyFileSync("../collector/config.yaml", resolve(dir, "collector.yaml"))' in rollup
    assert "endpoint: localhost:4318" in config
    assert "marshaler: otlp_json" in config
    assert "compression: gzip" in config
    assert "awss3_version=v0.151.0" in build_script
    assert "awss3exporter@$awss3_version" in build_script
    assert "lambdacomponents.exporter.awss3" in build_script
    assert "awss3exporter.NewFactory()" in component
