# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Lambda-layer exporter profiles."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import ClassVar


@dataclass(frozen=True)
class ExporterOptions:
    runtime: str
    region: str
    endpoint: str | None
    service_name: str
    layer_arn: str | None = None


@dataclass(frozen=True)
class ExporterConfiguration:
    layer_arns: tuple[str, ...]
    environment: Mapping[str, str]
    secret_environment_names: tuple[str, ...] = ()
    parameter_overrides: Mapping[str, str] = field(default_factory=dict)


_RUNTIME_ALIASES = {
    "js": "javascript",
    "node": "javascript",
    "nodejs": "javascript",
    "py": "python",
}


def normalize_runtime(runtime: str) -> str:
    normalized = runtime.lower().strip()
    return _RUNTIME_ALIASES.get(normalized, normalized)


class AdotExporterProfile:
    name = "adot"
    supported_backends = frozenset({"xray"})

    _WRAPPERS: ClassVar[dict[str, str]] = {
        "python": "/opt/otel-instrument",
        "java": "/opt/otel-instrument",
        "javascript": "/opt/otel-instrument",
    }

    def configure(
        self,
        options: ExporterOptions,
        *,
        environ: Mapping[str, str] | None = None,
    ) -> ExporterConfiguration:
        runtime = normalize_runtime(options.runtime)
        if runtime not in self._WRAPPERS:
            raise ValueError(
                f"ADOT does not define a Lambda layer for runtime {options.runtime!r}; "
                "supported runtimes: java, javascript, python"
            )
        source = environ or os.environ
        override_name = f"ADOT_{runtime.upper()}_LAYER_ARN"
        layer = options.layer_arn or source.get(override_name)
        if not layer:
            raise ValueError(
                f"ADOT requires an explicit {runtime} Lambda layer ARN; pass --otel-layer-arn or set {override_name}"
            )
        environment = {
            "AWS_LAMBDA_EXEC_WRAPPER": self._WRAPPERS[runtime],
            "OTEL_SERVICE_NAME": options.service_name,
            "OTEL_TRACES_EXPORTER": "otlp",
            "OTEL_PROPAGATORS": "xray",
        }
        return ExporterConfiguration(
            layer_arns=(layer,),
            environment=environment,
            parameter_overrides=_parameters(layer, environment, ()),
        )


class CommunityExporterProfile:
    name = "community"
    supported_backends = frozenset({"datadog", "dash0", "collector"})

    _LAYERS: ClassVar[dict[str, str]] = {
        "python": "opentelemetry-python-0_13_0:1",
        "java": "opentelemetry-javaagent-0_20_0:1",
        "javascript": "opentelemetry-nodejs-0_18_0:1",
    }
    _WRAPPERS: ClassVar[dict[str, str]] = {
        "python": "/opt/otel-instrument",
        "java": "/opt/otel-handler",
        "javascript": "/opt/otel-handler",
    }

    def configure(
        self,
        options: ExporterOptions,
        *,
        environ: Mapping[str, str] | None = None,
    ) -> ExporterConfiguration:
        runtime = normalize_runtime(options.runtime)
        if runtime not in self._LAYERS:
            raise ValueError(
                "The OpenTelemetry community layer does not define runtime "
                f"{options.runtime!r}; supported runtimes: java, javascript, python"
            )
        if not options.endpoint:
            raise ValueError("The community exporter requires an OTLP endpoint")
        source = environ or os.environ
        override_name = f"OTEL_COMMUNITY_{runtime.upper()}_LAYER_ARN"
        layer = options.layer_arn or source.get(override_name)
        if not layer:
            layer = f"arn:aws:lambda:{options.region}:184161586896:layer:{self._LAYERS[runtime]}"
        environment = {
            "AWS_LAMBDA_EXEC_WRAPPER": self._WRAPPERS[runtime],
            "OTEL_SERVICE_NAME": options.service_name,
            "OTEL_EXPORTER_OTLP_ENDPOINT": options.endpoint,
            "OTEL_TRACES_EXPORTER": "otlp",
        }
        secret_names = ("OTEL_EXPORTER_OTLP_HEADERS",)
        return ExporterConfiguration(
            layer_arns=(layer,),
            environment=environment,
            secret_environment_names=secret_names,
            parameter_overrides=_parameters(layer, environment, secret_names),
        )


def _parameters(
    layer: str,
    environment: Mapping[str, str],
    secret_names: tuple[str, ...],
) -> dict[str, str]:
    """Map a profile onto a stable SAM-template parameter contract."""

    parameters = {
        "OtelLayerArn": layer,
        "OtelExecWrapper": environment["AWS_LAMBDA_EXEC_WRAPPER"],
        "OtelServiceName": environment["OTEL_SERVICE_NAME"],
        "OtelTracesExporter": environment["OTEL_TRACES_EXPORTER"],
    }
    if endpoint := environment.get("OTEL_EXPORTER_OTLP_ENDPOINT"):
        parameters["OtelExporterEndpoint"] = endpoint
    if secret_names:
        parameters["OtelSecretEnvironmentNames"] = ",".join(secret_names)
    return parameters
