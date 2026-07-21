# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the command-line interface."""

from __future__ import annotations

import argparse
from typing import TYPE_CHECKING

import pytest
from aws_durable_execution_conformance_tests.app import (
    _run_extension_validation,
    parse_args,
)
from aws_durable_execution_conformance_tests.config import DEFAULT_REGION
from aws_durable_execution_conformance_tests.validate import DescriptionResult

if TYPE_CHECKING:
    from pathlib import Path

    from aws_durable_execution_conformance_tests.extensions import ValidationContext


def test_region_defaults_to_configured_region() -> None:
    args = parse_args(["--template", "template.yaml", "--language", "python"])

    assert args.region == DEFAULT_REGION


def test_region_accepts_override() -> None:
    args = parse_args(
        [
            "--template",
            "template.yaml",
            "--language",
            "python",
            "--region",
            "eu-west-1",
        ]
    )

    assert args.region == "eu-west-1"


def test_suite_defaults_to_all() -> None:
    args = parse_args(["--template", "template.yaml", "--language", "python"])

    assert args.suite == ["all"]


def test_suite_accepts_discovered_suite() -> None:
    args = parse_args(
        [
            "--template",
            "template.yaml",
            "--language",
            "python",
            "--suite",
            "step",
        ]
    )

    assert args.suite == ["step"]


def test_cleanup_enabled_by_default() -> None:
    args = parse_args(["--template", "template.yaml", "--language", "python"])

    assert args.cleanup is True


def test_cleanup_can_be_disabled() -> None:
    args = parse_args(
        [
            "--template",
            "template.yaml",
            "--language",
            "python",
            "--no-cleanup",
        ]
    )

    assert args.cleanup is False


def test_cleanup_explicit_enable() -> None:
    args = parse_args(
        [
            "--template",
            "template.yaml",
            "--language",
            "python",
            "--cleanup",
        ]
    )

    assert args.cleanup is True


def test_parameter_overrides_accept_repeated_key_value_pairs() -> None:
    args = parse_args(
        [
            "--template",
            "template.yaml",
            "--language",
            "python",
            "--parameter-overrides",
            "LambdaExecutionRoleArn=arn:aws:iam::123456789012:role/lambda",
            "OptionalValue=",
        ]
    )

    assert args.parameter_overrides == [
        ("LambdaExecutionRoleArn", "arn:aws:iam::123456789012:role/lambda"),
        ("OptionalValue", ""),
    ]


def test_parameter_overrides_reject_invalid_values() -> None:
    with pytest.raises(SystemExit):
        parse_args(
            [
                "--template",
                "template.yaml",
                "--language",
                "python",
                "--parameter-overrides",
                "MissingEquals",
            ]
        )


def test_extension_hook_failure_uses_core_description_result(tmp_path: Path) -> None:
    requirement = tmp_path / "otel-1.yaml"
    requirement.write_text("description: telemetry\n", encoding="utf-8")
    received: list[ValidationContext] = []

    def _hook(context: ValidationContext) -> list[str]:
        received.append(context)
        return ["trace was not correlated"]

    result = _run_extension_validation(
        result=DescriptionResult(
            description_id="otel-1",
            function_name="TelemetrySuccess",
            passed=True,
            execution_arn="arn:test",
            invocation_started_at_ms=1,
            invocation_finished_at_ms=2,
            execution_history={"Events": []},
        ),
        hook=_hook,
        requirement_path=requirement,
        args=argparse.Namespace(
            region="us-west-2",
            language="python",
            history_dir=str(tmp_path),
        ),
    )

    assert result.passed is False
    assert result.errors == ["trace was not correlated"]
    assert received[0].execution_arn == "arn:test"
