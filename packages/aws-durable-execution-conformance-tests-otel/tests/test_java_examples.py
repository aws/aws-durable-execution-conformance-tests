# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Contract tests for the Java OTel conformance examples."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

import yaml

from aws_durable_execution_conformance_tests.validate import (
    _CfnSafeLoader,
    parse_function_descriptions,
    parse_not_implemented,
)

EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples" / "java"
ENTRY_WORKFLOW_PATH = EXAMPLES_DIR.parents[3] / ".github" / "workflows" / "java-opentelemetry.yml"
ORCHESTRATOR_PATH = EXAMPLES_DIR.parents[3] / ".github" / "workflows" / "opentelemetry-orchestrator.yml"
RESOLVER_PATH = EXAMPLES_DIR.parents[3] / ".github" / "workflows" / "opentelemetry-resolve.yml"
WORKFLOW_PATH = EXAMPLES_DIR.parents[3] / ".github" / "workflows" / "opentelemetry-suite.yml"
LONG_RUNNING_WORKFLOW_PATH = EXAMPLES_DIR.parents[3] / ".github" / "workflows" / "opentelemetry-long-running.yml"
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
    "JavaSdkVersion",
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


def test_java_example_selects_otlp_protocol_by_exporter_profile() -> None:
    with (EXAMPLES_DIR / "template.yaml").open(encoding="utf-8") as stream:
        template = yaml.load(stream, Loader=_CfnSafeLoader)

    environment = template["Globals"]["Function"]["Environment"]["Variables"]
    assert environment["OTEL_EXPORTER_OTLP_PROTOCOL"] == {"If": ["HasOtelExporterEndpoint", "http/protobuf", "grpc"]}


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
    assert template.count("        ExecutionTimeout: 15") == 2
    assert "Runtime: java21" in template
    assert "Tracing: Active" in template
    assert "AWS_LAMBDA_EXEC_WRAPPER: !Ref OtelExecWrapper" in template
    assert "Default: /opt/otel-instrument" in template
    assert "JAVA_TOOL_OPTIONS" not in template
    assert (
        'OTEL_JAVAAGENT_EXTENSIONS: !Sub "/var/task/lib/software.amazon.lambda.durable.'
        'aws-durable-execution-sdk-java-plugin-otel-${JavaSdkVersion}.jar"'
    ) in template
    assert "HasOtelCollectorLayer: !Not" in template
    assert "HasOtelExporterEndpoint: !Not" in template
    assert "HasOtelExporterHeaders: !Not" in template
    assert "          - HasOtelExporterEndpoint" in template
    assert "          - HasOtelExporterHeaders" in template
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


def test_java_long_running_template_enables_agent_extension() -> None:
    template = (EXAMPLES_DIR / "template-long-running.yaml").read_text(encoding="utf-8")

    assert "AWS_LAMBDA_EXEC_WRAPPER: !Ref OtelExecWrapper" in template
    assert "JAVA_TOOL_OPTIONS" not in template
    assert (
        'OTEL_JAVAAGENT_EXTENSIONS: !Sub "/var/task/lib/software.amazon.lambda.durable.'
        'aws-durable-execution-sdk-java-plugin-otel-${JavaSdkVersion}.jar"'
    ) in template


def test_java_examples_use_agent_initialized_otel_plugin() -> None:
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
        "opentelemetry-sdk",
    } <= artifacts
    assert "opentelemetry-exporter-otlp" not in artifacts
    assert "aws-distro-opentelemetry-xray-udp-span-exporter" not in artifacts
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
    assert "SdkTracerProvider" not in handler
    assert "AwsXrayUdpSpanExporterBuilder" not in handler
    assert '"AWS_XRAY_DAEMON_ADDRESS"' not in handler
    assert "OtlpGrpcSpanExporter" not in handler
    assert '"software.amazon.lambda.durable.otel.InvocationOtelPlugin"' in handler
    assert '"software.amazon.lambda.durable.otel.ExecutionOtelPlugin"' in handler
    assert '"software.amazon.lambda.durable.otel.OtelPlugin"' in handler
    assert '"OTEL_PLUGIN_MODE"' in handler
    assert "pluginClass.getConstructor().newInstance()" in handler
    pom = pom_path.read_text(encoding="utf-8")
    assert "maven-dependency-plugin" not in pom
    assert "otel-plugin-extension.jar" not in pom


def test_java_workflow_uses_current_adot_distro_with_agent_enabled() -> None:
    entry_workflow = ENTRY_WORKFLOW_PATH.read_text(encoding="utf-8")
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert "adot_release_repository: aws-observability/aws-otel-java-instrumentation" in entry_workflow
    assert 'gh api "repos/$RELEASE_REPOSITORY/releases/latest"' in workflow
    assert "github.base_ref == 'main'" in workflow
    assert "--otel-allow-missing-span-identity-attributes" not in workflow
    assert "with its Java agent disabled" not in workflow


def test_java_workflow_builds_handlers_with_sdk_main() -> None:
    entry_workflow = ENTRY_WORKFLOW_PATH.read_text(encoding="utf-8")
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert "sdk_repository: aws/aws-durable-execution-sdk-java" in entry_workflow
    assert "--projects sdk,otel-plugin" in entry_workflow
    assert "-Dexpression=project.version" in entry_workflow
    assert '-Ddurable.sdk.version="$JAVA_SDK_VERSION"' in entry_workflow
    assert workflow.count('"JavaSdkVersion=$JAVA_SDK_VERSION"') == workflow.count("hatch run validate")
    assert workflow.count('"OtelSuite=$OTEL_SUITE"') == workflow.count("hatch run validate")
    assert workflow.count('"OtelServiceName=$OTEL_RESOURCE_SERVICE_NAME"') == workflow.count("hatch run validate")
    assert workflow.count('--otel-service-name "$OTEL_RESOURCE_SERVICE_NAME"') == workflow.count("hatch run validate")
    assert "OTEL_XRAY_DISCOVERY_SERVICE_NAME" not in workflow
    assert "OTEL_RESOURCE_SERVICE_NAME: durable-execution-conformance" in workflow
    assert "${OTEL_SUITE}-${case_number}-target" in workflow


def test_java_workflows_share_revision_resolution_and_propagate_the_commit() -> None:
    entry_workflow = yaml.safe_load(ENTRY_WORKFLOW_PATH.read_text(encoding="utf-8"))
    orchestrator = ORCHESTRATOR_PATH.read_text(encoding="utf-8")
    resolver = RESOLVER_PATH.read_text(encoding="utf-8")
    suite_workflow = WORKFLOW_PATH.read_text(encoding="utf-8")
    long_running_workflow = LONG_RUNNING_WORKFLOW_PATH.read_text(encoding="utf-8")

    preset = entry_workflow["jobs"]["conformance"]["with"]
    assert preset["sdk_repository"] == "aws/aws-durable-execution-sdk-java"
    assert "refs/heads/main" in resolver
    assert 'echo "ref=$SDK_REF" >> "$GITHUB_OUTPUT"' in resolver
    assert "uses: ./.github/workflows/opentelemetry-resolve.yml" in orchestrator
    assert orchestrator.count("sdk_ref: ${{ needs.resolve.outputs.sdk_ref }}") == 4
    assert set(entry_workflow["jobs"]) == {"conformance"}
    assert "needs" not in entry_workflow["jobs"]["conformance"]
    for workflow in (suite_workflow, long_running_workflow):
        assert "SDK_REF: ${{ inputs.sdk_ref }}" in workflow
        assert "      sdk_ref:" in workflow
        assert "        required: true" in workflow


def test_java_s3_job_builds_and_queries_the_collector() -> None:
    entry_workflow = ENTRY_WORKFLOW_PATH.read_text(encoding="utf-8")
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert "  s3_collector:" in workflow
    assert "open-telemetry/opentelemetry-lambda" in workflow
    assert "layer-collector/0.22.0" in workflow
    assert COLLECTOR_BUILD_SCRIPT in workflow
    assert "collector_compatible_runtime: java21" in entry_workflow
    assert "collector_otlp_endpoint: http://localhost:4318" in entry_workflow
    assert '--compatible-runtimes "${{ inputs.collector_compatible_runtime }}"' in workflow
    assert '--language "$LANGUAGE"' in workflow
    assert "--otel-exporter community" in workflow
    assert '--otel-endpoint "${{ inputs.collector_otlp_endpoint }}"' in workflow
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
