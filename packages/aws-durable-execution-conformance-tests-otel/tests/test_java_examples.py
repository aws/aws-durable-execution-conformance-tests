# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Contract tests for the Java OTel conformance examples."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

from aws_durable_execution_conformance_tests.validate import (
    parse_function_descriptions,
)

EXAMPLES_DIR = Path(__file__).resolve().parents[1] / "examples" / "java"
WORKFLOW_PATH = EXAMPLES_DIR.parents[3] / ".github" / "workflows" / "java-otel-integration.yml"
SOURCE_DIR = (
    EXAMPLES_DIR / "src" / "main" / "java" / "software" / "amazon" / "lambda" / "durable" / "conformance" / "otel"
)
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


def test_java_example_template_maps_every_otel_requirement() -> None:
    mappings = parse_function_descriptions(str(EXAMPLES_DIR / "template.yaml"))

    assert mappings == EXPECTED_MAPPINGS


def test_java_example_template_accepts_runner_parameters() -> None:
    template = (EXAMPLES_DIR / "template.yaml").read_text(encoding="utf-8")

    for parameter in REQUIRED_OTEL_PARAMETERS:
        assert f"  {parameter}:" in template
    assert "    NoEcho: true" in template
    assert template.count("      Role: !Ref LambdaExecutionRoleArn") == len(EXPECTED_MAPPINGS) + 2
    assert template.count("      CodeUri: .") == len(EXPECTED_MAPPINGS) + 2
    for case_number in range(1, 20):
        assert f'FunctionName: !Sub "${{AWS::StackName}}-otel-{case_number}"' in template
    assert 'FunctionName: !Sub "${AWS::StackName}-otel-11-target"' in template
    assert 'FunctionName: !Sub "${AWS::StackName}-otel-18-target"' in template
    assert 'OTEL_INVOKE_TARGET_FUNCTION_NAME: !Sub "${Otel11InvokeTarget.Arn}:$LATEST"' in template
    assert 'OTEL_INVOKE_TARGET_FUNCTION_NAME: !Sub "${Otel18InvokeTarget.Arn}:$LATEST"' in template
    assert "ExecutionTimeout: 5" in template
    assert "Runtime: java21" in template
    assert "Tracing: Active" in template
    assert "AWS_LAMBDA_EXEC_WRAPPER" not in template


def test_java_example_template_handlers_have_sources() -> None:
    template = (EXAMPLES_DIR / "template.yaml").read_text(encoding="utf-8")
    expected_classes = {
        "OtelConformanceHandler",
        *(logical_id for logical_id, _description_id in EXPECTED_MAPPINGS),
        "Otel11InvokeTarget",
        "Otel18InvokeTarget",
    }

    assert {path.stem for path in SOURCE_DIR.glob("*.java")} == expected_classes
    for class_name in expected_classes - {"OtelConformanceHandler"}:
        handler = f"software.amazon.lambda.durable.conformance.otel.{class_name}"
        assert f"      Handler: {handler}" in template


def test_java_examples_use_released_sdk_and_otel_plugin() -> None:
    pom_path = EXAMPLES_DIR / "pom.xml"
    root = ET.parse(pom_path).getroot()
    namespace = {"m": "http://maven.apache.org/POM/4.0.0"}
    properties = root.find("m:properties", namespace)

    assert properties is not None
    assert properties.findtext("m:durable.sdk.version", namespaces=namespace) == "2.1.0"
    assert properties.findtext("m:maven.compiler.target", namespaces=namespace) == "17"
    artifacts = {element.text for element in root.findall("m:dependencies/m:dependency/m:artifactId", namespace)}
    assert {
        "aws-durable-execution-sdk-java",
        "aws-durable-execution-sdk-java-plugin-otel",
        "opentelemetry-exporter-otlp",
    } <= artifacts


def test_java_workflow_uses_supported_adot_distro_layer() -> None:
    workflow = WORKFLOW_PATH.read_text(encoding="utf-8")

    assert "AWSOpenTelemetryDistroJava" in workflow
    assert "615299751070" in workflow
    assert "list-layer-versions" in workflow
    assert "aws-otel-java-agent" not in workflow


def test_map_iteration_names_are_cross_sdk_compatible() -> None:
    requirements_dir = EXAMPLES_DIR.parents[1] / "test-requirements" / "otel"
    python_source = EXAMPLES_DIR.parent / "python" / "src"

    success_requirement = (requirements_dir / "otel-7.yaml").read_text(encoding="utf-8")
    failure_requirement = (requirements_dir / "otel-14.yaml").read_text(encoding="utf-8")
    assert "otel-map-iteration-0" in success_requirement
    assert "otel-map-iteration-1" in success_requirement
    assert "otel-failed-map-iteration-0" in failure_requirement
    assert "otel-map-iteration-{index}" in (python_source / "otel_7_map.py").read_text(encoding="utf-8")
    assert "otel-failed-map-iteration-{index}" in (python_source / "otel_14_map_failure.py").read_text(encoding="utf-8")
