# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Core-runner extension exposing the Workflow Insight suite."""

from __future__ import annotations

import argparse
import json
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
from aws_durable_execution_conformance_tests_insight.discovery import (
    SINK_ENTRY_POINT_GROUP,
    PluginDiscoveryError,
    discover_plugins,
)
from aws_durable_execution_conformance_tests_insight.model import RecordQuery, records_to_dicts
from aws_durable_execution_conformance_tests_insight.polling import (
    PollingPolicy,
    SinkCapability,
    SinkError,
)
from aws_durable_execution_conformance_tests_insight.sinks import BUILTIN_SINKS
from aws_durable_execution_conformance_tests_insight.validators import validate_insight_records

_SUITE_NAME = "insight"


def _required_capabilities(raw: Any) -> set[SinkCapability]:
    """Parse an assertions ``requires`` value into a capability set."""

    if raw is None:
        return set()
    values = [raw] if isinstance(raw, str) else list(raw) if isinstance(raw, (list, tuple)) else None
    if values is None:
        raise ValueError("InsightAssertions.requires must be a string or sequence of strings")
    capabilities: set[SinkCapability] = set()
    for value in values:
        try:
            capabilities.add(SinkCapability(str(value)))
        except ValueError as exc:
            raise ValueError(f"Unknown insight capability {value!r} in requires") from exc
    return capabilities


class InsightExtension:
    name = "insight"
    requires_core = ">=1.0.0,<2.0.0"

    # -- suites ----------------------------------------------------------------

    def requirement_suites(self) -> tuple[RequirementSuite, ...]:
        project_root = Path(__file__).resolve().parent.parent.parent
        source_root = project_root / "test-requirements" / _SUITE_NAME
        package_root = files("aws_durable_execution_conformance_tests_insight").joinpath("test_requirements")
        installed_root = Path(str(package_root.joinpath(_SUITE_NAME)))
        root = source_root if source_root.is_dir() else installed_root
        return (
            RequirementSuite(
                name=_SUITE_NAME,
                root=root,
                validation_hook=self.validate_insight,
                provider=self.name,
            ),
        )

    # -- CLI -------------------------------------------------------------------

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        sinks = self._sinks()
        group = parser.add_argument_group("Workflow Insight suite")
        group.add_argument(
            "--insight-sink",
            default="s3",
            choices=sorted(sinks),
            help="Sink used to retrieve emitted Workflow Insight records.",
        )
        group.add_argument(
            "--insight-sink-endpoint",
            default=None,
            help="Sink location (s3://bucket/prefix for s3, or a CloudWatch log group name).",
        )
        group.add_argument(
            "--insight-poll-timeout",
            type=float,
            default=60.0,
            help="Maximum seconds to wait for record ingestion.",
        )
        group.add_argument(
            "--insight-poll-interval",
            type=float,
            default=2.0,
            help="Seconds between sink lookup attempts.",
        )
        group.add_argument(
            "--insight-poll-attempts",
            type=int,
            default=30,
            help="Maximum sink lookup attempts.",
        )

    def validate_configuration(self, args: argparse.Namespace) -> None:
        sinks = self._sinks()
        if args.insight_sink not in sinks:
            raise ValueError(f"Unknown insight sink {args.insight_sink!r}")
        PollingPolicy(
            timeout_seconds=args.insight_poll_timeout,
            interval_seconds=args.insight_poll_interval,
            max_attempts=args.insight_poll_attempts,
        )
        validate = getattr(sinks[args.insight_sink], "validate_configuration", None)
        if validate is not None:
            validate(args)

    def deployment_parameters(self, args: argparse.Namespace) -> Mapping[str, str]:
        provider = getattr(self._sinks()[args.insight_sink], "deployment_parameters", None)
        return provider(args) if provider is not None else {}

    def deployment_secrets(self, args: argparse.Namespace) -> Mapping[str, str]:
        del args
        return {}

    def validation_client_services(self, args: argparse.Namespace) -> tuple[str, ...]:
        return tuple(getattr(self._sinks()[args.insight_sink], "client_services", ()))

    # -- validation hook -------------------------------------------------------

    def validate_insight(self, context: ValidationContext) -> list[str]:
        options = context.options
        try:
            factory = self._sinks()[str(options["insight_sink"])]
            sink = factory.create_with_clients(
                options,
                region=context.region,
                function_name=context.function_name,
                aws_clients=context.aws_clients,
            )

            raw_assertions = context.requirement.get("InsightAssertions", {})
            if not isinstance(raw_assertions, Mapping):
                return ["InsightAssertions must be a mapping"]

            placeholders = PlaceholderContext()
            for name, value in context.placeholders.items():
                placeholders.bind(name, value)
            assertions = placeholders.substitute(raw_assertions)

            required = _required_capabilities(assertions.get("requires"))
            if required and sink.capability not in required:
                print(
                    f"  Workflow Insight: requirement {context.description_id} requires "
                    f"{', '.join(sorted(capability.value for capability in required))}; "
                    f"sink {sink.name!r} provides {sink.capability.value} -> reported UNCOVERED (skipped)"
                )
                return []

            timeout = float(options["insight_poll_timeout"])
            query = RecordQuery(
                execution_arn=context.execution_arn,
                started_at=datetime.fromtimestamp(context.invocation_started_at_ms / 1000, tz=UTC)
                - timedelta(seconds=30),
                ended_at=datetime.fromtimestamp(context.invocation_finished_at_ms / 1000, tz=UTC)
                + timedelta(seconds=timeout),
            )
            policy = PollingPolicy(
                timeout_seconds=timeout,
                interval_seconds=float(options["insight_poll_interval"]),
                max_attempts=int(options["insight_poll_attempts"]),
            )
            records = sink.find_records(
                query,
                policy,
                accept=lambda fetched: not validate_insight_records(fetched, assertions, query),
            )
            errors = validate_insight_records(records, assertions, query)
            if errors:
                self._write_artifact(context, records_to_dicts(records))
            return [f"Workflow Insight: {error}" for error in errors]
        except (SinkError, PluginDiscoveryError, KeyError, ValueError) as exc:
            return [f"Workflow Insight sink validation failed: {exc}"]

    # -- helpers ---------------------------------------------------------------

    @staticmethod
    def _sinks() -> dict[str, Any]:
        return discover_plugins(SINK_ENTRY_POINT_GROUP, BUILTIN_SINKS)

    @staticmethod
    def _write_artifact(context: ValidationContext, payload: list[dict[str, Any]]) -> None:
        context.output_dir.mkdir(parents=True, exist_ok=True)
        path = context.output_dir / f"{context.description_id}-insight.json"
        with path.open("w", encoding="utf-8") as stream:
            json.dump(payload, stream, indent=2, default=str)
