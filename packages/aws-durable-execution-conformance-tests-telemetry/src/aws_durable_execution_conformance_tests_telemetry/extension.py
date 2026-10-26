# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Core-runner extension that exposes the Durable Execution Telemetry suite."""

from __future__ import annotations

import argparse
import json
from collections.abc import Mapping
from importlib.resources import files
from pathlib import Path
from typing import Any

from aws_durable_execution_conformance_tests.cloudwatch import CloudWatchLogRetriever
from aws_durable_execution_conformance_tests.config import STACK_NAME_PREFIX
from aws_durable_execution_conformance_tests.extensions import (
    RequirementSuite,
    ValidationContext,
)
from aws_durable_execution_conformance_tests.variables import PlaceholderContext
from aws_durable_execution_conformance_tests_telemetry.model import (
    execution_placeholders,
    parse_execution_arn,
)
from aws_durable_execution_conformance_tests_telemetry.retrieval import (
    FetchedRecords,
    TelemetryAccessError,
    fetch_records,
)
from aws_durable_execution_conformance_tests_telemetry.validators import (
    validate_records,
)

SUITE_NAME = "telemetry"
ASSERTIONS_KEY = "TelemetryAssertions"
LOG_GROUP_PARAMETER = "TelemetryLogGroupName"
DEFAULT_WAIT_SECONDS = 10
ERROR_PREFIX = "Durable Execution Telemetry: "

# The retriever's start time is moved back by this margin so clock skew
# between the runner and Lambda cannot exclude the record.
START_TIME_MARGIN_MS = 30_000


class TelemetryExtension:
    name = SUITE_NAME
    requires_core = ">=1.0.0,<2.0.0"

    def requirement_suites(self) -> tuple[RequirementSuite, ...]:
        project_root = Path(__file__).resolve().parent.parent.parent
        source_root = project_root / "test-requirements" / SUITE_NAME
        installed_root = Path(
            str(
                files("aws_durable_execution_conformance_tests_telemetry").joinpath(
                    "test_requirements",
                    SUITE_NAME,
                )
            )
        )
        return (
            RequirementSuite(
                name=SUITE_NAME,
                root=source_root if source_root.is_dir() else installed_root,
                validation_hook=self.validate_telemetry,
                provider=self.name,
            ),
        )

    def add_arguments(self, parser: argparse.ArgumentParser) -> None:
        group = parser.add_argument_group("Durable Execution Telemetry suite")
        group.add_argument(
            "--telemetry-log-group",
            default=None,
            help="CloudWatch Logs group that receives Durable Execution Telemetry records. "
            f"Defaults to /durable-execution-conformance/{STACK_NAME_PREFIX}-<name>/durable-execution-telemetry. "
            f"Equivalent to --parameter-overrides {LOG_GROUP_PARAMETER}=<group>; "
            "an explicit parameter override takes precedence.",
        )
        group.add_argument(
            "--telemetry-wait-seconds",
            type=int,
            default=DEFAULT_WAIT_SECONDS,
            help="Seconds to wait before the first CloudWatch Logs query.",
        )

    def validate_configuration(self, args: argparse.Namespace) -> None:
        if args.telemetry_wait_seconds < 0:
            raise ValueError("--telemetry-wait-seconds must be zero or greater")
        if args.telemetry_log_group is not None and not args.telemetry_log_group.strip():
            raise ValueError("--telemetry-log-group must not be empty")

    def deployment_parameters(self, args: argparse.Namespace) -> Mapping[str, str]:
        return {LOG_GROUP_PARAMETER: self._log_group(args)}

    def deployment_secrets(self, args: argparse.Namespace) -> Mapping[str, str]:
        return {}

    def validation_client_services(self, args: argparse.Namespace) -> tuple[str, ...]:
        # "logs" and "cloudformation" are core clients; nothing extra is needed.
        return ()

    def validate_telemetry(self, context: ValidationContext) -> list[str]:
        raw_assertions = context.requirement.get(ASSERTIONS_KEY)
        if not isinstance(raw_assertions, Mapping):
            return [f"{ASSERTIONS_KEY} must be a mapping"]

        try:
            expected = parse_execution_arn(context.execution_arn)
        except ValueError as exc:
            return [f"{ERROR_PREFIX}{exc}"]
        if expected.region != context.region:
            return [
                f"{ERROR_PREFIX}execution ARN region {expected.region!r} differs from run region {context.region!r}"
            ]

        # The expected record names the identity fields through placeholders.
        # Core binds the placeholders it resolved from the execution history;
        # the suite adds the ARN-derived ones. A requirement that binds one of
        # those names itself, under ``Variables``, collides here and fails.
        placeholders = PlaceholderContext()
        try:
            for name, value in context.placeholders.items():
                placeholders.bind(name, value)
            for name, value in execution_placeholders(expected).items():
                placeholders.bind(name, value)
        except ValueError as exc:
            return [f"{ERROR_PREFIX}{exc}"]
        assertions = placeholders.substitute(raw_assertions)

        options = context.options
        log_group = _resolve_log_group(options)
        retriever = CloudWatchLogRetriever(
            context.aws_clients["cloudformation"],
            context.aws_clients["logs"],
        )
        try:
            fetched = fetch_records(
                retriever,
                log_group,
                context.execution_arn,
                start_time_ms=context.invocation_started_at_ms - START_TIME_MARGIN_MS,
                wait_seconds=int(options.get("telemetry_wait_seconds", DEFAULT_WAIT_SECONDS)),
            )
        except TelemetryAccessError as exc:
            return [f"{ERROR_PREFIX}access failure: {exc}"]

        expected_records = assertions.get("records")
        errors = [f"{ERROR_PREFIX}{error}" for error in validate_records(fetched.records, expected_records)]
        self._write_artifact(context, _artifact(context, log_group, fetched, errors))
        return errors

    def _log_group(self, args: argparse.Namespace) -> str:
        return _resolve_log_group(vars(args))

    @staticmethod
    def _write_artifact(context: ValidationContext, payload: Mapping[str, Any]) -> None:
        # The caller writes retrieved data whether assertions pass or fail.
        # Configuration and retrieval failures return before this write.
        # ``output_dir`` is the runner's ``--history-dir``, so no suite-specific
        # flag is needed.
        context.output_dir.mkdir(parents=True, exist_ok=True)
        path = context.output_dir / f"{context.description_id}-telemetry.json"
        with path.open("w", encoding="utf-8") as stream:
            json.dump(payload, stream, indent=2, default=str)


def default_log_group(run_name: str) -> str:
    """Log group used when ``--telemetry-log-group`` is not given."""
    return f"/durable-execution-conformance/{STACK_NAME_PREFIX}-{run_name}/durable-execution-telemetry"


def _resolve_log_group(options: Mapping[str, Any]) -> str:
    # Core applies --parameter-overrides after extension parameters, so an
    # explicit TelemetryLogGroupName is the group the stack deploys. The
    # validator must query that same group, so it wins here as well.
    explicit = _explicit_log_group(options)
    if explicit is not None:
        return explicit
    override = options.get("telemetry_log_group")
    if isinstance(override, str) and override.strip():
        return override
    return default_log_group(str(options["name"]))


def _explicit_log_group(options: Mapping[str, Any]) -> str | None:
    # ``parameter_overrides`` is the list of (key, value) tuples that argparse
    # builds from --parameter-overrides KEY=VALUE. Core converts it with
    # dict(...) before deploying, so a key given twice deploys its later value.
    # Convert it the same way so this suite queries the group core deployed.
    overrides = dict(options.get("parameter_overrides") or ())
    value = overrides.get(LOG_GROUP_PARAMETER)
    return value if isinstance(value, str) else None


def _artifact(
    context: ValidationContext,
    log_group: str,
    fetched: FetchedRecords,
    errors: list[str],
) -> dict[str, Any]:
    return {
        "execution_arn": context.execution_arn,
        "log_group": log_group,
        "events": [dict(event) for event in fetched.events],
        "records": [dict(record.data) for record in fetched.records],
        "malformed": [event.get("message") for event in fetched.malformed],
        "errors": errors,
        "execution_history": context.execution_history,
    }


__all__ = ["TelemetryExtension", "default_log_group"]
