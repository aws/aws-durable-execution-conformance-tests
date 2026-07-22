# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the report model."""

from __future__ import annotations

from aws_durable_execution_conformance_tests.report import (
    FAIL_ON_FAILED,
    FAIL_ON_FAILED_UNCOVERED,
    Report,
    ReportEntry,
    ReportStatus,
    RunMetadata,
)


def _entry(description_id: str, status: ReportStatus, **kwargs) -> ReportEntry:
    return ReportEntry(id=description_id, suite="parallel", status=status, **kwargs)


def _report(entries: list[ReportEntry], fail_on: str = FAIL_ON_FAILED) -> Report:
    run = RunMetadata(name="run", template="t.yaml", region="us-west-2", language="java")
    return Report(run=run, entries=list(entries), fail_on=fail_on)


def test_summary_counts_each_status() -> None:
    report = _report(
        [
            _entry("1-1", ReportStatus.PASSED),
            _entry("1-2", ReportStatus.PASSED),
            _entry("1-3", ReportStatus.FAILED, errors=["boom"]),
            _entry("1-4", ReportStatus.EXPECTED_FAILED, reason="known", errors=["expected"]),
            _entry("1-5", ReportStatus.UNEXPECTED_PASSED, reason="fixed"),
            _entry("1-6", ReportStatus.OPTIONAL_FAILED),
            _entry("1-7", ReportStatus.NOT_IMPLEMENTED, reason="gap"),
            _entry("1-8", ReportStatus.UNCOVERED),
        ]
    )
    assert report.summary() == {
        "total": 8,
        "passed": 2,
        "failed": 1,
        "expected_failed": 1,
        "unexpected_passed": 1,
        "optional_failed": 1,
        "not_implemented": 1,
        "uncovered": 1,
    }


def test_exit_code_blocks_on_failed_by_default() -> None:
    report = _report([_entry("1-1", ReportStatus.PASSED), _entry("1-2", ReportStatus.FAILED)])
    assert report.blocking_count() == 1
    assert report.exit_code() == 1


def test_non_blocking_statuses_do_not_fail_by_default() -> None:
    report = _report(
        [
            _entry("1-1", ReportStatus.PASSED),
            _entry("1-2", ReportStatus.EXPECTED_FAILED, reason="known"),
            _entry("1-3", ReportStatus.OPTIONAL_FAILED),
            _entry("1-4", ReportStatus.NOT_IMPLEMENTED, reason="gap"),
            _entry("1-5", ReportStatus.UNCOVERED),
        ]
    )
    assert report.blocking_count() == 0
    assert report.exit_code() == 0


def test_fail_on_uncovered_policy_blocks_uncovered() -> None:
    report = _report(
        [_entry("1-1", ReportStatus.PASSED), _entry("1-2", ReportStatus.UNCOVERED)],
        fail_on=FAIL_ON_FAILED_UNCOVERED,
    )
    assert report.blocking_count() == 1
    assert report.exit_code() == 1


def test_unexpected_pass_always_blocks() -> None:
    report = _report([_entry("1-1", ReportStatus.UNEXPECTED_PASSED, reason="fixed")])

    assert report.blocking_count() == 1
    assert report.exit_code() == 1


def test_unknown_fail_on_policy_defaults_to_failed_only() -> None:
    report = _report([_entry("1-1", ReportStatus.UNCOVERED)], fail_on="bogus")
    # Unknown policy falls back to the default blocking statuses, so UNCOVERED passes.
    assert report.exit_code() == 0


def test_entries_by_status_preserves_order() -> None:
    report = _report(
        [
            _entry("1-1", ReportStatus.NOT_IMPLEMENTED, reason="a"),
            _entry("1-2", ReportStatus.PASSED),
            _entry("1-3", ReportStatus.NOT_IMPLEMENTED, reason="b"),
        ]
    )
    ni = report.entries_by_status(ReportStatus.NOT_IMPLEMENTED)
    assert [e.id for e in ni] == ["1-1", "1-3"]


def test_to_dict_schema_shape() -> None:
    report = _report(
        [
            _entry("8-13", ReportStatus.NOT_IMPLEMENTED, function=None, reason="pct rejected"),
            _entry("8-1", ReportStatus.PASSED, function="ParallelBasic", description="Parallel basic"),
        ]
    )
    data = report.to_dict()

    assert data["schema_version"] == "1.1"
    assert data["run"]["language"] == "java"
    assert data["run"]["suites"] == ["all"]
    assert data["summary"]["not_implemented"] == 1
    assert data["ci"] == {
        "fail_on": FAIL_ON_FAILED,
        "blocking_statuses": ["FAILED", "UNEXPECTED_PASSED"],
        "blocking_count": 0,
        "exit_code": 0,
    }
    first = data["results"][0]
    assert first["id"] == "8-13"
    assert first["suite"] == "parallel"
    assert first["status"] == "NOT_IMPLEMENTED"
    assert first["reason"] == "pct rejected"
    assert first["function"] is None
    assert data["results"][1]["description"] == "Parallel basic"


def test_status_values_are_strings() -> None:
    # ReportStatus is a str-enum so json.dumps of .value works directly.
    assert ReportStatus.PASSED.value == "PASSED"
    assert ReportStatus.EXPECTED_FAILED.value == "EXPECTED_FAILED"
    assert ReportStatus.UNEXPECTED_PASSED.value == "UNEXPECTED_PASSED"
    assert ReportStatus.NOT_IMPLEMENTED.value == "NOT_IMPLEMENTED"


def test_warnings_default_empty_in_to_dict() -> None:
    data = _report([_entry("1-1", ReportStatus.PASSED)]).to_dict()
    assert data["warnings"] == []


def test_warnings_serialized_in_to_dict() -> None:
    run = RunMetadata(name="run", template="t.yaml", region="us-west-2", language="java")
    report = Report(
        run=run,
        entries=[_entry("1-1", ReportStatus.PASSED)],
        warnings=["'8-13' is declared NotImplemented but is covered by an example"],
    )
    data = report.to_dict()
    assert data["warnings"] == ["'8-13' is declared NotImplemented but is covered by an example"]
