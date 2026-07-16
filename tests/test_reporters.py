# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Unit tests for the report writers."""

from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from pathlib import Path

import pytest

from aws_durable_execution_sdk_testing.report import (
    Report,
    ReportEntry,
    ReportStatus,
    RunMetadata,
)
from aws_durable_execution_sdk_testing.reporters import (
    render_console,
    render_json,
    render_junit,
    write_report,
)


def _report() -> Report:
    run = RunMetadata(name="run", template="t.yaml", region="us-west-2", language="java")
    return Report(
        run=run,
        entries=[
            ReportEntry(
                id="8-1",
                suite="parallel",
                status=ReportStatus.PASSED,
                function="ParallelBasic",
                description="Parallel basic (all succeed)",
            ),
            ReportEntry(
                id="8-2", suite="parallel", status=ReportStatus.FAILED, function="ParallelFail", errors=["boom"]
            ),
            ReportEntry(id="8-13", suite="parallel", status=ReportStatus.NOT_IMPLEMENTED, reason="pct rejected"),
            ReportEntry(id="8-9", suite="parallel", status=ReportStatus.UNCOVERED),
        ],
    )


def test_console_includes_all_sections() -> None:
    text = render_console(_report())
    assert "1 passed, 1 failed" in text
    assert "8-1" in text
    assert "8-2" in text
    assert "8-13" in text
    assert "8-9" in text
    assert "boom" in text  # failed errors shown
    assert "pct rejected" in text  # not-implemented reason shown
    assert "Parallel basic (all succeed)" in text  # description shown
    assert "Exit code: 1" in text  # one FAILED blocks


def test_console_shows_warnings() -> None:
    report = _report()
    report.warnings.append("'8-13' is declared NotImplemented but is covered by an example")
    text = render_console(report)
    assert "Warnings:" in text
    assert "declared NotImplemented but is covered" in text


def test_json_is_valid_and_schema_versioned() -> None:
    data = json.loads(render_json(_report()))
    assert data["schema_version"] == "1.0"
    assert data["summary"]["failed"] == 1
    assert data["ci"]["exit_code"] == 1
    assert {r["id"] for r in data["results"]} == {"8-1", "8-2", "8-13", "8-9"}


def test_junit_maps_failed_to_failure_and_rest_to_skipped() -> None:
    root = ET.fromstring(render_junit(_report()))
    assert root.tag == "testsuite"
    assert root.attrib["tests"] == "4"
    assert root.attrib["failures"] == "1"
    assert root.attrib["skipped"] == "2"  # not_implemented + uncovered

    by_name = {tc.attrib["name"]: tc for tc in root.findall("testcase")}
    assert by_name["8-1"].find("failure") is None
    assert by_name["8-1"].find("skipped") is None
    assert by_name["8-2"].find("failure") is not None
    ni_skip = by_name["8-13"].find("skipped")
    assert ni_skip is not None
    assert "NOT_IMPLEMENTED" in ni_skip.attrib["message"]
    assert by_name["8-9"].find("skipped") is not None


def test_junit_classname_encodes_language_and_suite() -> None:
    root = ET.fromstring(render_junit(_report()))
    tc = root.find("testcase")
    assert tc is not None
    assert tc.attrib["classname"] == "java.parallel"


def test_junit_properties_and_system_out() -> None:
    root = ET.fromstring(render_junit(_report()))
    by_name = {tc.attrib["name"]: tc for tc in root.findall("testcase")}

    # 8-1 has a description + function -> full properties + system-out.
    tc = by_name["8-1"]
    props = {p.attrib["name"]: p.attrib["value"] for p in tc.findall("properties/property")}
    assert props["requirement_id"] == "8-1"
    assert props["language"] == "java"
    assert props["description"] == "Parallel basic (all succeed)"
    assert props["example"] == "ParallelBasic"
    system_out = tc.find("system-out")
    assert system_out is not None
    assert system_out.text == "Parallel basic (all succeed)"

    # 8-13 (NOT_IMPLEMENTED) has no function -> no 'example' property, no system-out.
    ni = by_name["8-13"]
    ni_props = {p.attrib["name"] for p in ni.findall("properties/property")}
    assert "example" not in ni_props
    assert ni.find("system-out") is None


def test_write_report_json_and_junit(tmp_path: Path) -> None:
    json_path = write_report(_report(), "json", str(tmp_path / "out" / "report"))
    xml_path = write_report(_report(), "junit", str(tmp_path / "out" / "report"))

    assert json_path.endswith(".json")
    assert xml_path.endswith(".xml")
    assert Path(json_path).is_file()
    assert Path(xml_path).is_file()
    # Round-trips parse cleanly.
    json.loads(Path(json_path).read_text())
    ET.fromstring(Path(xml_path).read_text())


def test_write_report_unknown_format_raises(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Unknown machine report format"):
        write_report(_report(), "yaml", str(tmp_path / "x"))
