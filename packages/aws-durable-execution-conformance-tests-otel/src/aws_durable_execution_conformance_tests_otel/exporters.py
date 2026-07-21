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


@dataclass(frozen=True)
class _AdotRegionLayers:
    account_id: str
    python: int | None
    java: int | None
    javascript: int | None

    def version_for(self, runtime: str) -> int | None:
        if runtime == "python":
            return self.python
        if runtime == "java":
            return self.java
        return self.javascript


_ADOT_REGION_LAYERS = {
    "af-south-1": _AdotRegionLayers("904233096616", 23, 16, 15),
    "ap-east-1": _AdotRegionLayers("888577020596", 23, 16, 15),
    "ap-east-2": _AdotRegionLayers("412664885777", 4, 5, 4),
    "ap-northeast-1": _AdotRegionLayers("615299751070", 26, 16, 15),
    "ap-northeast-2": _AdotRegionLayers("615299751070", 26, 16, 15),
    "ap-northeast-3": _AdotRegionLayers("615299751070", 26, 16, 15),
    "ap-south-1": _AdotRegionLayers("615299751070", 26, 16, 15),
    "ap-south-2": _AdotRegionLayers("796973505492", 23, 16, 15),
    "ap-southeast-1": _AdotRegionLayers("615299751070", 25, 16, 15),
    "ap-southeast-2": _AdotRegionLayers("615299751070", 26, 16, 15),
    "ap-southeast-3": _AdotRegionLayers("039612877180", 23, 16, 15),
    "ap-southeast-4": _AdotRegionLayers("713881805771", 23, 16, 15),
    "ap-southeast-5": _AdotRegionLayers("152034782359", 14, 13, 8),
    "ap-southeast-6": _AdotRegionLayers("313828097273", 3, 5, 3),
    "ap-southeast-7": _AdotRegionLayers("980416031188", 14, 13, 8),
    "ca-central-1": _AdotRegionLayers("615299751070", 26, 16, 15),
    "ca-west-1": _AdotRegionLayers("595944127152", 14, 13, 8),
    "cn-north-1": _AdotRegionLayers("440179912924", 14, 13, 8),
    "cn-northwest-1": _AdotRegionLayers("440180067931", 14, 13, 8),
    "eu-central-1": _AdotRegionLayers("615299751070", 26, 16, 15),
    "eu-central-2": _AdotRegionLayers("156041407956", 23, 16, 15),
    "eu-north-1": _AdotRegionLayers("615299751070", 26, 16, 15),
    "eu-south-1": _AdotRegionLayers("257394471194", 23, 16, 15),
    "eu-south-2": _AdotRegionLayers("490004653786", 23, 16, 15),
    "eu-west-1": _AdotRegionLayers("615299751070", 26, 16, 15),
    "eu-west-2": _AdotRegionLayers("615299751070", 26, 16, 15),
    "eu-west-3": _AdotRegionLayers("615299751070", 26, 16, 15),
    "il-central-1": _AdotRegionLayers("746669239226", 23, 16, 15),
    "me-central-1": _AdotRegionLayers("739275441131", 22, 16, 15),
    "me-south-1": _AdotRegionLayers("980921751758", None, None, 12),
    "mx-central-1": _AdotRegionLayers("610118373846", 14, 13, 8),
    "sa-east-1": _AdotRegionLayers("615299751070", 26, 16, 15),
    "us-east-1": _AdotRegionLayers("615299751070", 29, 16, 15),
    "us-east-2": _AdotRegionLayers("615299751070", 26, 16, 15),
    "us-west-1": _AdotRegionLayers("615299751070", 33, 16, 15),
    "us-west-2": _AdotRegionLayers("615299751070", 33, 16, 15),
}


class AdotExporterProfile:
    name = "adot"
    supported_backends = frozenset({"xray"})

    _LAYERS: ClassVar[dict[str, str]] = {
        "python": "AWSOpenTelemetryDistroPython",
        "java": "AWSOpenTelemetryDistroJava",
        "javascript": "AWSOpenTelemetryDistroJs",
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
                f"ADOT does not define a Lambda layer for runtime {options.runtime!r}; "
                "supported runtimes: java, javascript, python"
            )
        source = environ or os.environ
        override_name = f"ADOT_{runtime.upper()}_LAYER_ARN"
        layer = options.layer_arn or source.get(override_name)
        if not layer:
            region_layers = _ADOT_REGION_LAYERS.get(options.region)
            if not region_layers:
                raise ValueError(
                    f"ADOT does not define a default Lambda layer in region {options.region!r}; "
                    "provide an explicit layer ARN"
                )
            version = region_layers.version_for(runtime)
            if version is None:
                raise ValueError(
                    f"ADOT does not define a {runtime} Lambda layer in region {options.region!r}; "
                    "provide an explicit layer ARN"
                )
            partition = "aws-cn" if options.region.startswith("cn-") else "aws"
            layer = (
                f"arn:{partition}:lambda:{options.region}:{region_layers.account_id}:"
                f"layer:{self._LAYERS[runtime]}:{version}"
            )
        environment = {
            "AWS_LAMBDA_EXEC_WRAPPER": self._WRAPPERS[runtime],
            "OTEL_SERVICE_NAME": options.service_name,
            "OTEL_TRACES_EXPORTER": "xray",
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
        "java": "opentelemetry-javaagent:1",
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
