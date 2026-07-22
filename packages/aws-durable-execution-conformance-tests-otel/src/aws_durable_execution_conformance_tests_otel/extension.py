# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Core-runner extension that exposes the ``otel`` suite."""

from __future__ import annotations

import argparse
import json
import os
from collections.abc import Mapping
from datetime import UTC, datetime, timedelta
from importlib.resources import files
from pathlib import Path
from typing import Any

from aws_durable_execution_conformance_tests.extensions import (
    RequirementSuite,
    ValidationContext,
)
from aws_durable_execution_conformance_tests.variables import PlaceholderContext

from aws_durable_execution_conformance_tests_otel.backends import BUILTIN_BACKENDS
from aws_durable_execution_conformance_tests_otel.discovery import (
    BACKEND_ENTRY_POINT_GROUP,
    EXPORTER_ENTRY_POINT_GROUP,
    PluginDiscoveryError,
    discover_plugins,
)
from aws_durable_execution_conformance_tests_otel.exporters import (
    AdotExporterProfile,
    CommunityExporterProfile,
    ExporterOptions,
)
from aws_durable_execution_conformance_tests_otel.model import (
    TelemetryQuery,
    trace_to_dict,
)
from aws_durable_execution_conformance_tests_otel.polling import (
    BackendError,
    PollingPolicy,
)
from aws_durable_execution_conformance_tests_otel.redaction import redact
from aws_durable_execution_conformance_tests_otel.validators import validate_trace

BUILTIN_EXPORTERS = {
    "adot": AdotExporterProfile,
    "community": CommunityExporterProfile,
}

SUPPORT_MATRIX = frozenset(
    {
        ("adot", "xray"),
        ("community", "datadog"),
        ("community", "dash0"),
        ("community", "collector"),
    }
)


class OtelExtension:
    name = "otel"
    requires_core = ">=0.2.0,<0.3.0"

    def __init__(self) -> None:
        self._reported_disparity_backends: set[str] = set()

    def requirement_suites(self) -> tuple[RequirementSuite, ...]:
        project_root = Path(__file__).resolve().parent.parent.parent
        source_root = project_root / "test-requirements" / "otel"
        package_root = Path(
            str(
                files("aws_durable_execution_conformance_tests_otel").joinpath(
                    "test_requirements",
                    "otel",
                )
            )
        )
        root = source_root if source_root.is_dir() else package_root
        return (
            RequirementSuite(
                name="otel",
                root=root,
                validation_hook=self.validate_telemetry,
                provider=self.name,
            ),
        )

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        exporters = self._exporters()
        backends = self._backends()
        group = parser.add_argument_group("OpenTelemetry suite")
        group.add_argument(
            "--otel-exporter",
            default="community",
            choices=sorted(exporters),
            help="OpenTelemetry Lambda-layer exporter profile.",
        )
        group.add_argument(
            "--otel-backend",
            default="collector",
            choices=sorted(backends),
            help="Backend used to retrieve conformance telemetry.",
        )
        group.add_argument(
            "--otel-endpoint",
            default=os.environ.get(
                "OTEL_EXPORTER_OTLP_ENDPOINT",
                "http://127.0.0.1:4318",
            ),
            help="Non-secret OTLP export endpoint configured on the test function.",
        )
        group.add_argument(
            "--otel-backend-endpoint",
            default=None,
            help="Backend query endpoint (for collector, s3://bucket/prefix).",
        )
        group.add_argument(
            "--otel-service-name",
            default="durable-execution-conformance",
            help="Service name used to correlate telemetry.",
        )
        group.add_argument(
            "--otel-layer-arn",
            default=None,
            help="Lambda layer ARN override for the selected runtime.",
        )
        group.add_argument(
            "--otel-poll-timeout",
            type=float,
            default=60.0,
            help="Maximum seconds to wait for backend ingestion.",
        )
        group.add_argument(
            "--otel-poll-interval",
            type=float,
            default=2.0,
            help="Seconds between backend lookup attempts.",
        )
        group.add_argument(
            "--otel-poll-attempts",
            type=int,
            default=30,
            help="Maximum backend lookup attempts.",
        )

    def validate_configuration(self, args: argparse.Namespace) -> None:
        exporters = self._exporters()
        backends = self._backends()
        combination = (args.otel_exporter, args.otel_backend)
        support_matrix = set(SUPPORT_MATRIX)
        for exporter_name, profile in exporters.items():
            support_matrix.update(
                (exporter_name, backend_name) for backend_name in getattr(profile, "supported_backends", ())
            )
        for backend_name, factory in backends.items():
            support_matrix.update(
                (exporter_name, backend_name) for exporter_name in getattr(factory, "supported_exporters", ())
            )
        if combination not in support_matrix:
            supported = ", ".join(f"{exporter}+{backend}" for exporter, backend in sorted(support_matrix))
            raise ValueError(
                f"Unsupported OpenTelemetry exporter/backend combination "
                f"{args.otel_exporter}+{args.otel_backend}; supported: {supported}"
            )
        PollingPolicy(
            timeout_seconds=args.otel_poll_timeout,
            interval_seconds=args.otel_poll_interval,
            max_attempts=args.otel_poll_attempts,
        )
        exporters[args.otel_exporter].configure(self._exporter_options(args))
        if args.otel_backend not in backends:
            raise ValueError(f"Unknown OpenTelemetry backend {args.otel_backend!r}")

    def deployment_parameters(self, args: argparse.Namespace) -> Mapping[str, str]:
        profile = self._exporters()[args.otel_exporter]
        return profile.configure(self._exporter_options(args)).parameter_overrides

    def deployment_secrets(self, args: argparse.Namespace) -> Mapping[str, str]:
        config = self._exporters()[args.otel_exporter].configure(self._exporter_options(args))
        headers = os.environ.get("OTEL_EXPORTER_OTLP_HEADERS")
        if "OTEL_EXPORTER_OTLP_HEADERS" not in config.secret_environment_names or not headers:
            return {}
        return {"OtelExporterHeaders": headers}

    def validate_telemetry(self, context: ValidationContext) -> list[str]:
        options = context.options
        try:
            factories = self._backends()
            backend_name = str(options["otel_backend"])
            backend = factories[backend_name].create(options, region=context.region)
            feature_disparities = (
                ", ".join(sorted(disparity.name for disparity in backend.feature_disparities)) or "none"
            )
            if backend.name not in self._reported_disparity_backends:
                print(
                    f"  OpenTelemetry backend feature disparity flags enabled for {backend.name}: {feature_disparities}"
                )
                self._reported_disparity_backends.add(backend.name)
            timeout = float(options["otel_poll_timeout"])
            query = TelemetryQuery(
                execution_arn=context.execution_arn,
                service_name=str(options["otel_service_name"]),
                started_at=datetime.fromtimestamp(
                    context.invocation_started_at_ms / 1000,
                    tz=UTC,
                )
                - timedelta(seconds=30),
                ended_at=datetime.fromtimestamp(
                    context.invocation_finished_at_ms / 1000,
                    tz=UTC,
                )
                + timedelta(seconds=timeout),
            )
            raw_assertions = context.requirement.get("TelemetryAssertions", {})
            if not isinstance(raw_assertions, Mapping):
                return ["TelemetryAssertions must be a mapping"]
            placeholders = PlaceholderContext()
            for name, value in context.placeholders.items():
                placeholders.bind(name, value)
            assertions = placeholders.substitute(raw_assertions)
            trace = backend.find_trace(
                query,
                PollingPolicy(
                    timeout_seconds=timeout,
                    interval_seconds=float(options["otel_poll_interval"]),
                    max_attempts=int(options["otel_poll_attempts"]),
                ),
                accept=lambda candidate: not validate_trace(
                    candidate,
                    assertions,
                    query,
                    feature_disparities=backend.feature_disparities,
                ),
            )
            errors = validate_trace(
                trace,
                assertions,
                query,
                feature_disparities=backend.feature_disparities,
            )
            if errors:
                self._write_artifact(context, trace_to_dict(trace))
            return [f"OpenTelemetry: {error}" for error in errors]
        except (BackendError, PluginDiscoveryError, KeyError, ValueError) as exc:
            return [f"OpenTelemetry backend validation failed: {redact(str(exc))}"]

    def _exporter_options(self, args: argparse.Namespace) -> ExporterOptions:
        return ExporterOptions(
            runtime=args.language,
            region=args.region,
            endpoint=args.otel_endpoint,
            service_name=args.otel_service_name,
            layer_arn=args.otel_layer_arn,
        )

    @staticmethod
    def _exporters() -> dict[str, Any]:
        return discover_plugins(EXPORTER_ENTRY_POINT_GROUP, BUILTIN_EXPORTERS)

    @staticmethod
    def _backends() -> dict[str, Any]:
        return discover_plugins(BACKEND_ENTRY_POINT_GROUP, BUILTIN_BACKENDS)

    @staticmethod
    def _write_artifact(
        context: ValidationContext,
        payload: Mapping[str, Any],
    ) -> None:
        context.output_dir.mkdir(parents=True, exist_ok=True)
        path = context.output_dir / f"{context.description_id}-otel.json"
        with path.open("w", encoding="utf-8") as stream:
            json.dump(redact(payload), stream, indent=2, default=str)
