# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Exporter profile and support-matrix tests."""

from __future__ import annotations

import argparse
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from aws_durable_execution_conformance_tests.extensions import ValidationContext
from aws_durable_execution_conformance_tests_otel import extension as extension_module
from aws_durable_execution_conformance_tests_otel.exporters import (
    AdotExporterProfile,
    CommunityExporterProfile,
    ExporterOptions,
)
from aws_durable_execution_conformance_tests_otel.extension import OtelExtension
from aws_durable_execution_conformance_tests_otel.model import Trace
from aws_durable_execution_conformance_tests_otel.polling import (
    BackendFeatureDisparity,
)


def _options(runtime: str = "python", *, layer_arn: str | None = None) -> ExporterOptions:
    return ExporterOptions(
        runtime=runtime,
        region="us-west-2",
        endpoint="https://collector.example/v1/traces",
        service_name="conformance",
        layer_arn=layer_arn,
    )


@pytest.mark.parametrize(
    ("runtime", "expected_layer"),
    [
        ("java", "arn:aws:lambda:us-west-2:615299751070:layer:AWSOpenTelemetryDistroJava:16"),
        ("javascript", "arn:aws:lambda:us-west-2:615299751070:layer:AWSOpenTelemetryDistroJs:15"),
        ("python", "arn:aws:lambda:us-west-2:615299751070:layer:AWSOpenTelemetryDistroPython:33"),
    ],
)
def test_adot_configures_each_supported_runtime(runtime: str, expected_layer: str) -> None:
    config = AdotExporterProfile().configure(_options(runtime, layer_arn=expected_layer))

    assert config.layer_arns == (expected_layer,)
    assert config.environment["OTEL_TRACES_EXPORTER"] == "otlp"
    assert config.environment["OTEL_PROPAGATORS"] == "xray"
    assert config.parameter_overrides["OtelTracesExporter"] == "otlp"
    assert "OTEL_EXPORTER_OTLP_HEADERS" not in config.environment


def test_adot_java_uses_current_distro_wrapper_parameter() -> None:
    config = AdotExporterProfile().configure(
        _options(
            "java",
            layer_arn="arn:aws:lambda:us-west-2:615299751070:layer:AWSOpenTelemetryDistroJava:16",
        )
    )

    assert config.environment["AWS_LAMBDA_EXEC_WRAPPER"] == "/opt/otel-instrument"
    assert config.parameter_overrides["OtelExecWrapper"] == "/opt/otel-instrument"


def test_adot_javascript_uses_current_distro_wrapper() -> None:
    config = AdotExporterProfile().configure(
        _options(
            "javascript",
            layer_arn="arn:aws:lambda:us-west-2:615299751070:layer:AWSOpenTelemetryDistroJs:15",
        )
    )

    assert config.environment["AWS_LAMBDA_EXEC_WRAPPER"] == "/opt/otel-instrument"
    assert config.parameter_overrides["OtelExecWrapper"] == "/opt/otel-instrument"


def test_adot_requires_explicit_layer_arn() -> None:
    with pytest.raises(ValueError, match="pass --otel-layer-arn or set ADOT_PYTHON_LAYER_ARN"):
        AdotExporterProfile().configure(_options())


@pytest.mark.parametrize("runtime", ["java", "js", "python"])
def test_community_configures_each_supported_runtime(runtime: str) -> None:
    config = CommunityExporterProfile().configure(_options(runtime))

    assert config.environment["OTEL_EXPORTER_OTLP_ENDPOINT"].startswith("https://")
    assert config.secret_environment_names == ("OTEL_EXPORTER_OTLP_HEADERS",)
    assert "OtelSecretEnvironmentNames" in config.parameter_overrides


def test_community_requires_endpoint() -> None:
    options = ExporterOptions(
        runtime="python",
        region="us-west-2",
        endpoint=None,
        service_name="test",
    )
    with pytest.raises(ValueError, match="requires an OTLP endpoint"):
        CommunityExporterProfile().configure(options)


def test_unknown_runtime_is_actionable() -> None:
    with pytest.raises(ValueError, match="supported runtimes"):
        AdotExporterProfile().configure(_options("ruby"))


def _args(exporter: str, backend: str) -> argparse.Namespace:
    return argparse.Namespace(
        suite=["otel"],
        language="python",
        region="us-west-2",
        otel_exporter=exporter,
        otel_backend=backend,
        otel_endpoint="http://collector:4318",
        otel_backend_endpoint=None,
        otel_service_name="test",
        otel_layer_arn="arn:aws:lambda:us-west-2:123456789012:layer:adot:1" if exporter == "adot" else None,
        otel_poll_timeout=10.0,
        otel_poll_interval=0.0,
        otel_poll_attempts=3,
    )


@pytest.mark.parametrize(
    ("exporter", "backend"),
    [
        ("adot", "xray"),
        ("community", "datadog"),
        ("community", "dash0"),
        ("community", "collector"),
    ],
)
def test_required_support_matrix_is_accepted(
    exporter: str,
    backend: str,
) -> None:
    OtelExtension().validate_configuration(_args(exporter, backend))


def test_unsupported_combination_fails_before_deployment() -> None:
    with pytest.raises(ValueError, match="Unsupported OpenTelemetry"):
        OtelExtension().validate_configuration(_args("adot", "datadog"))


def test_secret_otlp_headers_are_returned_as_redacted_deployment_input(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("OTEL_EXPORTER_OTLP_HEADERS", "authorization=secret")

    secrets = OtelExtension().deployment_secrets(_args("community", "collector"))

    assert secrets == {"OtelExporterHeaders": "authorization=secret"}


def test_telemetry_assertions_resolve_history_and_execution_variables(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    trace = Trace(trace_id="1" * 32, spans=())
    received: dict[str, Any] = {}
    received_disparities: list[object] = []

    def capture_assertions(
        _trace: Trace,
        assertions: dict[str, Any],
        _query: object,
        *,
        feature_disparities: object,
    ) -> list[str]:
        received.update(assertions)
        received_disparities.append(feature_disparities)
        return []

    def find_trace(_query: object, _policy: object, *, accept: Any) -> Trace:
        assert accept(trace)
        return trace

    disparities = frozenset({BackendFeatureDisparity.UNSET_STATUS})
    backend = SimpleNamespace(
        feature_disparities=disparities,
        find_trace=find_trace,
        name="xray",
    )
    received_clients: list[object] = []

    def create_with_clients(
        _options: object,
        *,
        region: str,
        aws_clients: object,
    ) -> object:
        assert region == "us-west-2"
        received_clients.append(aws_clients)
        return backend

    factory = SimpleNamespace(create_with_clients=create_with_clients)
    monkeypatch.setattr(OtelExtension, "_backends", staticmethod(lambda: {"xray": factory}))
    monkeypatch.setattr(extension_module, "validate_trace", capture_assertions)

    errors = OtelExtension().validate_telemetry(
        ValidationContext(
            description_id="otel-5",
            function_name="function",
            execution_arn="arn:execution",
            invocation_started_at_ms=1,
            invocation_finished_at_ms=2,
            region="us-west-2",
            language="python",
            requirement={
                "TelemetryAssertions": {
                    "span_assertions": {
                        "select": {
                            "attributes": {
                                "durable.execution.arn": "${EXECUTION_ARN}",
                                "durable.operation.id": "${STEP1}",
                            },
                        },
                        "expect": {},
                    },
                },
            },
            execution_history={},
            output_dir=tmp_path,
            placeholders={
                "EXECUTION_ARN": "arn:execution",
                "STEP1": "step-id",
            },
            options=vars(_args("adot", "xray")),
            aws_clients={"xray": object()},
        )
    )

    assert errors == []
    assert capsys.readouterr().out == "  OpenTelemetry backend feature disparity flags enabled for xray: UNSET_STATUS\n"
    assert received["span_assertions"]["select"]["attributes"] == {
        "durable.execution.arn": "arn:execution",
        "durable.operation.id": "step-id",
    }
    assert received_disparities == [disparities, disparities]
    assert len(received_clients) == 1
