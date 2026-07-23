# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the command-line interface."""

from __future__ import annotations

import argparse
import time
from threading import Barrier, Lock
from typing import TYPE_CHECKING

import aws_durable_execution_conformance_tests.app as app_module
import pytest
from aws_durable_execution_conformance_tests.app import (
    _run_extension_validation,
    _validate_descriptions,
    parse_args,
)
from aws_durable_execution_conformance_tests.config import DEFAULT_MAX_WORKERS, DEFAULT_REGION
from aws_durable_execution_conformance_tests.extensions import RequirementCase, RequirementSuite
from aws_durable_execution_conformance_tests.sam import Invoker
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


def test_max_workers_defaults_to_configured_value() -> None:
    args = parse_args(["--template", "template.yaml", "--language", "python"])

    assert args.max_workers == DEFAULT_MAX_WORKERS


def test_max_workers_accepts_positive_override() -> None:
    args = parse_args(
        [
            "--template",
            "template.yaml",
            "--language",
            "python",
            "--max-workers",
            "2",
        ]
    )

    assert args.max_workers == 2


def test_max_workers_rejects_non_positive_values() -> None:
    with pytest.raises(SystemExit):
        parse_args(
            [
                "--template",
                "template.yaml",
                "--language",
                "python",
                "--max-workers",
                "0",
            ]
        )


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
            placeholders={"STEP1": "step-id"},
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
    assert received[0].placeholders == {
        "STEP1": "step-id",
        "EXECUTION_ARN": "arn:test",
    }


def test_validates_descriptions_concurrently_and_preserves_order(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    suite = RequirementSuite(name="test", root=tmp_path)
    requirements: dict[str, RequirementCase] = {}
    for description_id in ("test-1", "test-2"):
        path = tmp_path / f"{description_id}.yaml"
        path.write_text("description: test\n", encoding="utf-8")
        requirements[description_id] = RequirementCase(description_id, path, suite)

    barrier = Barrier(2)
    lock = Lock()
    active = 0
    max_active = 0
    completion_order: list[str] = []

    def _validate_description(
        function_name: str,
        description_id: str,
        test_file: str,
        invoker: Invoker,
        tmp_dir: str,
        region: str,
        output_dir: str | None = None,
    ) -> DescriptionResult:
        del test_file, invoker, tmp_dir, region, output_dir
        nonlocal active, max_active
        with lock:
            active += 1
            max_active = max(max_active, active)
        barrier.wait(timeout=1)
        if description_id == "test-1":
            time.sleep(0.02)
        with lock:
            active -= 1
            completion_order.append(description_id)
        return DescriptionResult(
            description_id=description_id,
            function_name=function_name,
            passed=True,
        )

    monkeypatch.setattr(app_module, "validate_description", _validate_description)

    results = _validate_descriptions(
        [("Function1", "test-1"), ("Function2", "test-2")],
        requirements=requirements,
        invoker=Invoker(stack_name="test-stack"),
        tmp_dir=str(tmp_path),
        args=argparse.Namespace(
            history_dir=str(tmp_path),
            language="python",
            max_workers=2,
            region="us-west-2",
        ),
    )

    assert max_active == 2
    assert completion_order == ["test-2", "test-1"]
    assert [result.description_id for result in results] == ["test-1", "test-2"]
