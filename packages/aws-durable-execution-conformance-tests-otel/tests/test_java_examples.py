# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Contract tests for the Java OTel conformance examples."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

from aws_durable_execution_conformance_tests.validate import (
    parse_function_descriptions,
    parse_not_implemented,
)

EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples" / "java"
ENTRY_WORKFLOW_PATH = EXAMPLES_DIR.parents[3] / ".github" / "workflows" / "java-opentelemetry.yml"
WORKFLOW_PATH = EXAMPLES_DIR.parents[3] / ".github" / "workflows" / "java-opentelemetry-suite.yml"
LONG_RUNNING_WORKFLOW_PATH = EXAMPLES_DIR.parents[3] / ".github" / "workflows" / "java-opentelemetry-long-running.yml"
COLLECTOR_BUILD_SCRIPT = "packages/aws-durable-execution-conformance-tests-otel/collector/build-lambda-layer.sh"
SOURCE_DIR = (
    EXAMPLES_DIR / "src" / "main" / "java" / "software" / "amazon" / "lambda" / "durable" / "conformance" / "otel"
)
EXPECTED_INVOCATION_MAPPINGS = [
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
]
EXPECTED_EXECUTION_MAPPINGS = [
    (logical_id.replace("Otel", "OtelExecution", 1), description_id.replace("invocation", "execution"))
    for logical_id, description_id in EXPECTED_INVOCATION_MAPPINGS
]
EXPECTED_MAPPINGS = EXPECTED_INVOCATION_MAPPINGS + EXPECTED_EXECUTION_MAPPINGS
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
    "OtelSuite",
}


def test_java_example_template_maps_every_otel_requirement() -> None:
    mappings = parse_function_descriptions(str(EXAMPLES_DIR / "template.yaml"))

    assert mappings == EXPECTED_MAPPINGS


def test_java_example_implements_execution_view() -> None:
    assert parse_not_implemented(str(EXAMPLES_DIR / "template.yaml")) == {}


def test_java_example_template_accepts_runner_parameters() -> None:
    template = (EXAMPLES_DIR / "template.yaml").read_text(encoding="utf-8")

    for parameter in REQUIRED_OTEL_PARAMETERS:
        assert f"  {parameter}:" in template
    assert "    NoEcho: true" in template
    assert template.count("      Role: !Ref LambdaExecutionRoleArn") == len(EXPECTED_MAPPINGS) + 4
    assert template.count("      CodeUri: .") == len(EXPECTED_MAPPINGS) + 4
    for case_number in range(1, 20):
        assert f'FunctionName: !Sub "${{AWS::StackName}}-otel-invocation-{case_number}"' in template
        assert f'FunctionName: !Sub "${{AWS::StackName}}-otel-execution-{case_number}"' in template
    assert 'FunctionName: !Sub "${AWS::StackName}-otel-invocation-11-target"' in template
    assert 'FunctionName: !Sub "${AWS::StackName}-otel-invocation-18-target"' in template
    assert 'FunctionName: !Sub "${AWS::StackName}-otel-execution-11-target"' in template
    assert 'FunctionName: !Sub "${AWS::StackName}-otel-execution-18-target"' in template
    assert 'OTEL_INVOKE_TARGET_FUNCTION_NAME: !Sub "${Otel11InvokeTarget.Arn}:$LATEST"' in template
    assert 'OTEL_INVOKE_TARGET_FUNCTION_NAME: !Sub "${Otel18InvokeTarget.Arn}:$LATEST"' in template
    assert 'OTEL_INVOKE_TARGET_FUNCTION_NAME: !Sub "${OtelExecution11InvokeTarget.Arn}:$LATEST"' in template
    assert 'OTEL_INVOKE_TARGET_FUNCTION_NAME: !Sub "${OtelExecution18InvokeTarget.Arn}:$LATEST"' in template
    assert template.count("        OTEL_PLUGIN_MODE: invocation") == 1
    assert template.count("          OTEL_PLUGIN_MODE: execution") == len(EXPECTED_EXECUTION_MAPPINGS) + 2
    assert "ExecutionTimeout: 5" in template
    assert "Runtime: java21" in template
    assert "Tracing: Active" in template
    assert "AWS_LAMBDA_EXEC_WRAPPER" not in template
    assert "Default: /opt/otel-instrument" in template
    assert "HasOtelCollectorLayer: !Not" in template
    assert '!Ref "AWS::NoValue"' in template
    assert "OTEL_S3_BUCKET: !Ref OtelCollectorBucket" in template
    assert "OTEL_S3_PREFIX: !Ref OtelCollectorPrefix" in template
    assert "/opt/collector-config/config-s3.yaml" in template


def test_java_example_template_handlers_have_sources() -> None:
    template = (EXAMPLES_DIR / "template.yaml").read_text(encoding="utf-8")
    expected_classes = {
        "OtelConformanceHandler",
        *(logical_id for logical_id, _description_id in EXPECTED_INVOCATION_MAPPINGS),
        "Otel11InvokeTarget",
        "Otel18InvokeTarget",
        "OtelLongRunning1Wait",
        "OtelLongRunning2Retry",
        "OtelLongRunning3Callback",
        "OtelLongRunning4ChainedInvoke",
        "OtelLongRunning4InvokeTarget",
    }

    assert {path.stem for path in SOURCE_DIR.glob("*.java")} == expected_classes
    for class_name in expected_classes - {
        "OtelConformanceHandler",
        "OtelLongRunning1Wait",
        "OtelLongRunning2Retry",
        "OtelLongRunning3Callback",
        "OtelLongRunning4ChainedInvoke",
        "OtelLongRunning4InvokeTarget",
    }:
        handler = f"software.amazon.lambda.durable.conformance.otel.{class_name}"
        assert f"      Handler: {handler}" in template


def test_java_examples_require_sdk_main_version_and_otel_plugin() -> None:
    pom_path = EXAMPLES_DIR / "pom.xml"
    root = ET.parse(pom_path).getroot()
    namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
    properties = root.find("m:properties", namespace)

    assert properties is not None
    assert properties.findtext("m:durable.sdk.version", namespaces=namespace) == "${env.JAVA_SDK_VERSION}"
    assert properties.findtext("m:maven.compiler.target", namespaces=namespace) == "17"
    dependencies = root.findall("m:dependencies/m:dependency", namespace)
    artifacts = {element.findtext("m:artifactId", namespaces=namespace) for element in dependencies}
    assert {
        "aws-durable-execution-sdk-java",
        "aws-durable-execution-sdk-java-plugin-otel",
        "aws-distro-opentelemetry-xray-udp-span-exporter",
        "opentelemetry-exporter-otlp",
    } <= artifacts
    sdk_versions = {
        element.findtext("m:version", namespaces=namespace)
        for element in dependencies
        if element.findtext("m:artifactId", namespaces=namespace)
        in {
            "aws-durable-execution-sdk-java",
            "aws-durable-execution-sdk-java-plugin-otel",
        }
    }
    assert sdk_versions == {"${durable.sdk.version}"}
    handler = (SOURCE_DIR / "OtelConformanceHandler.java").read_text(encoding="utf-8")
    assert ".setResource(resource)" in handler
    assert 'AttributeKey.stringKey("service.name")' in handler
    assert '"durable-execution-conformance"' in handler
    assert "AwsXrayUdpSpanExporterBuilder" in handler
    assert '"AWS_XRAY_DAEMON_ADDRESS"' in handler
    assert "OtlpGrpcSpanExporter" in handler
    assert '"OTEL_EXPORTER_OTLP_ENDPOINT"' in handler
    assert '"OTEL_EXPORTER_OTLP_HEADERS"' in handler
    assert "URLDecoder.decode" in handler
    assert "builder::addHeader" in handler
    assert '"software.amazon.lambda.durable.otel.InvocationOtelPlugin"' in handler
    assert '"software.amazon.lambda.durable.otel.ExecutionOtelPlugin"' in handler
    assert '"software.amazon.lambda.durable.otel.OtelPlugin"' in handler
    assert '"OTEL_PLUGIN_MODE"' in handler


def test_java_workflow_uses_current_adot_distro_with_agent_disabled() -> None:
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert "AWSOpenTelemetryDistroJava" in workflow
    assert "aws-observability/aws-otel-java-instrumentation/releases/latest" in workflow
    assert "github.base_ref == 'main'" in workflow
    assert "--otel-allow-missing-span-identity-attributes" not in workflow


def test_java_workflow_builds_handlers_with_sdk_main() -> None:
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert "repository: aws/aws-durable-execution-sdk-java" in workflow
    assert workflow.count("ref: ${{ env.JAVA_SDK_REF }}") == 2
    assert "--projects sdk,otel-plugin" in workflow
    assert "-Dexpression=project.version" in workflow
    assert '-Ddurable.sdk.version="$JAVA_SDK_VERSION"' in workflow
    assert workflow.count('"OtelSuite=$OTEL_SUITE"') == workflow.count("hatch run validate")
    assert workflow.count('"OtelServiceName=$OTEL_RESOURCE_SERVICE_NAME"') == workflow.count("hatch run validate")
    assert workflow.count('--otel-service-name "$OTEL_RESOURCE_SERVICE_NAME"') == workflow.count("hatch run validate")
    assert "OTEL_XRAY_DISCOVERY_SERVICE_NAME" not in workflow
    assert "OTEL_RESOURCE_SERVICE_NAME: durable-execution-conformance" in workflow
    assert "${OTEL_SUITE}-${case_number}-target" in workflow


def test_java_workflow_resolves_main_once_and_propagates_the_commit() -> None:
    entry_workflow = ENTRY_WORKFLOW_PATH.read_text(encoding="utf-8")
    suite_workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
    long_running_workflow = LONG_RUNNING_WORKFLOW_PATH.read_text(encoding="utf-8")

    assert entry_workflow.count("git ls-remote") == 1
    assert "refs/heads/main" in entry_workflow
    assert 'echo "ref=$JAVA_SDK_REF" >> "$GITHUB_OUTPUT"' in entry_workflow
    assert entry_workflow.count("needs: resolve-sdk-main") == 4
    assert entry_workflow.count("java_sdk_ref: ${{ needs.resolve-sdk-main.outputs.java_sdk_ref }}") == 4
    for workflow in (suite_workflow, long_running_workflow):
        assert "JAVA_SDK_REF: ${{ inputs.java_sdk_ref }}" in workflow
        assert "      java_sdk_ref:" in workflow
        assert "        required: true" in workflow
    assert suite_workflow.count("ref: ${{ env.JAVA_SDK_REF }}") == 2
    assert long_running_workflow.count("ref: ${{ env.JAVA_SDK_REF }}") == 1


def test_java_s3_job_builds_and_queries_the_collector() -> None:
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert "  s3_collector:" in workflow
    assert "open-telemetry/opentelemetry-lambda" in workflow
    assert "layer-collector/0.22.0" in workflow
    assert COLLECTOR_BUILD_SCRIPT in workflow
    assert "--compatible-runtimes java21" in workflow
    assert "--language java" in workflow
    assert "--otel-exporter community" in workflow
    assert "--otel-endpoint http://localhost:4317" in workflow
    assert "--otel-backend collector" in workflow
    assert '--otel-backend-endpoint "$OTEL_S3_URI"' in workflow
    assert "OtelCollectorLayerArn=$COLLECTOR_LAYER_ARN" in workflow
    assert "OtelCollectorBucket=$OTEL_S3_BUCKET" in workflow
    assert "OtelCollectorPrefix=$OTEL_S3_PREFIX" in workflow
    assert "delete-layer-version" not in workflow
    assert '--suite "$OTEL_SUITE"' in workflow


def test_map_iteration_names_are_cross_sdk_compatible() -> None:
    requirements_dir = EXAMPLES_DIR.parents[1] / "test-requirements" / "otel-invocation"
    python_source = EXAMPLES_DIR.parent / "python" / "src"

    success_requirement = (requirements_dir / "otel-invocation-7.yaml").read_text(encoding="utf-8")
    failure_requirement = (requirements_dir / "otel-invocation-14.yaml").read_text(encoding="utf-8")
    assert "otel-map-iteration-0" in success_requirement
    assert "otel-map-iteration-1" in success_requirement
    assert "otel-failed-map-iteration-0" in failure_requirement
    assert "otel-map-iteration-{index}" in (python_source / "otel_7_map.py").read_text(encoding="utf-8")
    assert "otel-failed-map-iteration-{index}" in (python_source / "otel_14_map_failure.py").read_text(encoding="utf-8")
