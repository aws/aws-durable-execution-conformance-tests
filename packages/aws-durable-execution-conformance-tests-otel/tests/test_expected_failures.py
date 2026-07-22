# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for strict expected-failure report handling."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from aws_durable_execution_conformance_tests_otel.expected_failures import (
    evaluate_expected_failures,
    main,
)

if TYPE_CHECKING:
    from pathlib import Path


def _report(*results: tuple[str, str]) -> dict[str, Any]:
    return {
        "results": [
            {
                "id": description_id,
                "status": status,
            }
            for description_id, status in results
        ]
    }


def test_accepts_only_the_two_expected_failures() -> None:
    evaluation = evaluate_expected_failures(
        _report(
            ("otel-1", "PASSED"),
            ("otel-3", "FAILED"),
            ("otel-9", "FAILED"),
        ),
        {"otel-3", "otel-9"},
    )

    assert evaluation.passed
    assert evaluation.expected_failures == ("otel-3", "otel-9")


def test_rejects_failure_outside_the_expected_set() -> None:
    evaluation = evaluate_expected_failures(
        _report(
            ("otel-3", "FAILED"),
            ("otel-8", "FAILED"),
            ("otel-9", "FAILED"),
        ),
        {"otel-3", "otel-9"},
    )

    assert evaluation.errors == ("Unexpected conformance failure: 'otel-8'",)


def test_rejects_unexpected_pass() -> None:
    evaluation = evaluate_expected_failures(
        _report(
            ("otel-3", "PASSED"),
            ("otel-9", "FAILED"),
        ),
        {"otel-3", "otel-9"},
    )

    assert evaluation.errors == ("Expected-failure case 'otel-3' unexpectedly passed; remove its exemption",)


def test_cli_returns_success_for_expected_validator_failure(
    tmp_path: Path,
    capsys: Any,
) -> None:
    report_path = tmp_path / "report.json"
    report_path.write_text(
        json.dumps(_report(("otel-3", "FAILED"), ("otel-9", "FAILED"))),
        encoding="utf-8",
    )

    exit_code = main(
        [
            str(report_path),
            "--validator-exit-code",
            "1",
            "--expected-failure",
            "otel-3",
            "--expected-failure",
            "otel-9",
        ]
    )

    assert exit_code == 0
    assert capsys.readouterr().out == (
        "  XFAIL otel-3: failed as expected\n"
        "  XFAIL otel-9: failed as expected\n"
        "Only expected conformance failures were observed.\n"
    )
