# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Apply a strict expected-failure policy to a conformance JSON report."""

from __future__ import annotations

import argparse
import json
import sys
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class ExpectedFailureEvaluation:
    """Result of comparing a conformance report with known failures."""

    expected_failures: tuple[str, ...]
    errors: tuple[str, ...]

    @property
    def passed(self) -> bool:
        return not self.errors


def evaluate_expected_failures(
    report: Mapping[str, Any],
    expected_failure_ids: Collection[str],
) -> ExpectedFailureEvaluation:
    """Require each expected case to fail and every other case not to fail."""

    expected = frozenset(expected_failure_ids)
    raw_results = report.get("results")
    if not isinstance(raw_results, list):
        return ExpectedFailureEvaluation((), ("Conformance report has no results list",))

    statuses: dict[str, str] = {}
    errors: list[str] = []
    for raw_result in raw_results:
        if not isinstance(raw_result, Mapping):
            errors.append("Conformance report contains a non-mapping result")
            continue
        description_id = raw_result.get("id")
        status = raw_result.get("status")
        if not isinstance(description_id, str) or not isinstance(status, str):
            errors.append("Conformance report result is missing a string id or status")
            continue
        if description_id in statuses:
            errors.append(f"Conformance report contains duplicate result {description_id!r}")
            continue
        statuses[description_id] = status

    observed_expected_failures: list[str] = []
    for description_id in sorted(expected):
        status = statuses.get(description_id)
        if status is None:
            errors.append(f"Expected-failure case {description_id!r} is missing from the report")
        elif status == "FAILED":
            observed_expected_failures.append(description_id)
        elif status == "PASSED":
            errors.append(f"Expected-failure case {description_id!r} unexpectedly passed; remove its exemption")
        else:
            errors.append(f"Expected-failure case {description_id!r} reported {status}, not the required FAILED status")

    for description_id, status in sorted(statuses.items()):
        if status == "FAILED" and description_id not in expected:
            errors.append(f"Unexpected conformance failure: {description_id!r}")

    return ExpectedFailureEvaluation(
        expected_failures=tuple(observed_expected_failures),
        errors=tuple(errors),
    )


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Accept only explicitly expected failures in a conformance JSON report.",
    )
    parser.add_argument("report", type=Path, help="Conformance JSON report path.")
    parser.add_argument(
        "--expected-failure",
        action="append",
        required=True,
        dest="expected_failures",
        metavar="CASE_ID",
        help="Case that must report FAILED. Repeat for multiple cases.",
    )
    parser.add_argument(
        "--validator-exit-code",
        type=int,
        required=True,
        help="Exit code returned by the conformance validator.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        report = json.loads(args.report.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        print(f"Could not read conformance report {args.report}: {exc}", file=sys.stderr)
        return 1
    if not isinstance(report, Mapping):
        print("Conformance report root must be a mapping", file=sys.stderr)
        return 1

    evaluation = evaluate_expected_failures(report, args.expected_failures)
    errors = list(evaluation.errors)
    if evaluation.passed and args.validator_exit_code != 1:
        errors.append(
            "Conformance validator exit code was "
            f"{args.validator_exit_code}; expected 1 for the reported expected failures"
        )

    for description_id in evaluation.expected_failures:
        print(f"  XFAIL {description_id}: failed as expected")
    for error in errors:
        print(f"  ERROR {error}", file=sys.stderr)
    if errors:
        return 1

    print("Only expected conformance failures were observed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
