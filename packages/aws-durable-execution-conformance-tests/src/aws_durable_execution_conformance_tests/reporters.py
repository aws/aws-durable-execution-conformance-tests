# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Report writers: render a :class:`Report` as console text, JSON, or JUnit XML.

All writers are pure functions of a ``Report`` (plus, for the file variants, a
path). Non-blocking statuses (OPTIONAL_FAILED / NOT_IMPLEMENTED / UNCOVERED) map
to JUnit ``<skipped>`` so CI renders them yellow, not red; only FAILED maps to
``<failure>``.
"""

from __future__ import annotations

import html
import json
import xml.etree.ElementTree as ET
from pathlib import Path

from aws_durable_execution_conformance_tests.report import Report, ReportStatus

# Console glyphs per status.
_GLYPH: dict[ReportStatus, str] = {
    ReportStatus.PASSED: "✅",
    ReportStatus.FAILED: "❌",
    ReportStatus.OPTIONAL_FAILED: "⚠️ ",
    ReportStatus.NOT_IMPLEMENTED: "🚧",
    ReportStatus.UNCOVERED: "◻️ ",
}


def render_console(report: Report) -> str:
    """Render a human-readable console report as a string."""
    summary = report.summary()
    lines: list[str] = []
    lines.append("=" * 50)
    lines.append(
        f"RESULTS: {summary['passed']} passed, {summary['failed']} failed, "
        f"{summary['optional_failed']} optional failed, "
        f"{summary['not_implemented']} not implemented, "
        f"{summary['uncovered']} uncovered, {summary['total']} total"
    )
    lines.append("=" * 50)

    if report.warnings:
        lines.append("\nWarnings:")
        lines.extend(f"  ⚠️  {warning}" for warning in report.warnings)

    def _section(title: str, status: ReportStatus, *, show_errors: bool = False, show_reason: bool = False) -> None:
        entries = report.entries_by_status(status)
        if not entries:
            return
        lines.append(f"\n{title}:")
        for entry in entries:
            fn = f" ({entry.function})" if entry.function else ""
            optional = " [optional]" if entry.optional and status != ReportStatus.OPTIONAL_FAILED else ""
            lines.append(f"  {_GLYPH[status]} {entry.id}{fn}{optional}")
            if entry.description:
                lines.append(f"       {entry.description}")
            if show_reason and entry.reason:
                lines.append(f"       {entry.reason}")
            if show_errors:
                lines.extend(f"       {err}" for err in entry.errors)

    _section("Passed", ReportStatus.PASSED)
    _section("Failed", ReportStatus.FAILED, show_errors=True)
    _section("Optional (failed, non-blocking)", ReportStatus.OPTIONAL_FAILED, show_errors=True)
    _section("Not implemented (declared SDK gap, non-blocking)", ReportStatus.NOT_IMPLEMENTED, show_reason=True)
    _section("Uncovered (no example found)", ReportStatus.UNCOVERED)

    lines.append(f"\nExit code: {report.exit_code()} (fail-on: {report.fail_on})")
    return "\n".join(lines)


def render_json(report: Report) -> str:
    """Render the report as a pretty-printed JSON string (schema 1.0)."""
    return json.dumps(report.to_dict(), indent=2)


def render_github_summary(report: Report) -> str:
    """Render a GitHub Actions job summary with blocking failure details."""
    summary = report.summary()
    lines = [
        "## Conformance test summary",
        "",
        f"**Run:** `{html.escape(report.run.name)}`  ",
        f"**Language:** `{html.escape(report.run.language or 'unknown')}`  ",
        f"**Suites:** `{html.escape(', '.join(report.run.suites))}`",
        "",
        "| Passed | Failed | Optional failed | Not implemented | Uncovered | Total |",
        "| ---: | ---: | ---: | ---: | ---: | ---: |",
        (
            f"| {summary['passed']} | {summary['failed']} | {summary['optional_failed']} | "
            f"{summary['not_implemented']} | {summary['uncovered']} | {summary['total']} |"
        ),
    ]

    blocking_entries = [
        entry for entry in report.entries if entry.status in report.blocking_statuses() and not entry.optional
    ]
    if not blocking_entries:
        lines.extend(["", "No blocking test failures."])
        return "\n".join(lines)

    lines.extend(["", "### Blocking failures"])
    for entry in blocking_entries:
        description = f" - {html.escape(entry.description)}" if entry.description else ""
        lines.extend(
            [
                "",
                f"<details><summary><code>{html.escape(entry.id)}</code> ({entry.status.value}){description}</summary>",
                "",
            ]
        )
        if entry.function:
            lines.append(f"**Function:** `{html.escape(entry.function)}`")
        details = entry.errors or ([entry.reason] if entry.reason else [])
        if details:
            escaped_details = html.escape("\n".join(details))
            lines.extend(["", f"<pre>{escaped_details}</pre>"])
        lines.extend(["", "</details>"])

    return "\n".join(lines)


def render_junit(report: Report) -> str:
    """Render the report as JUnit XML.

    One ``<testcase>`` per requirement (``classname`` = suite, ``name`` = id).
    FAILED -> ``<failure>``; every other non-passing status -> ``<skipped>`` with
    a reason/message so CI shows it as skipped rather than failed.
    """
    summary = report.summary()
    testsuite = ET.Element(
        "testsuite",
        {
            "name": report.run.name,
            "tests": str(summary["total"]),
            "failures": str(summary["failed"]),
            "skipped": str(summary["optional_failed"] + summary["not_implemented"] + summary["uncovered"]),
            "time": f"{report.run.duration_seconds:.3f}",
        },
    )
    for entry in report.entries:
        suite = entry.suite or "unknown"
        # classname encodes language + suite so that JUnit files from all SDKs
        # can be merged and still grouped/filtered per language, while `name`
        # stays the bare requirement id -- the stable join key across the
        # per-SDK files.
        classname = f"{report.run.language}.{suite}" if report.run.language else suite
        testcase = ET.SubElement(
            testsuite,
            "testcase",
            {
                "classname": classname,
                "name": entry.id,
                "time": f"{entry.duration_seconds:.3f}",
            },
        )

        # Structured metadata: correlate requirement id, description, language,
        # and the per-language example handler on every testcase.
        prop_values: list[tuple[str, str | None]] = [
            ("requirement_id", entry.id),
            ("description", entry.description),
            ("language", report.run.language),
            ("example", entry.function),
        ]
        properties = ET.SubElement(testcase, "properties")
        for prop_name, prop_value in prop_values:
            if prop_value:
                ET.SubElement(properties, "property", {"name": prop_name, "value": prop_value})

        if entry.status == ReportStatus.FAILED:
            failure = ET.SubElement(testcase, "failure", {"message": "; ".join(entry.errors) or "assertion failed"})
            failure.text = "\n".join(entry.errors)
        elif entry.status != ReportStatus.PASSED:
            message = f"{entry.status.value}: {entry.reason}" if entry.reason else entry.status.value
            skipped = ET.SubElement(testcase, "skipped", {"message": message})
            if entry.errors:
                skipped.text = "\n".join(entry.errors)

        # Mirror the description into system-out for reporters that surface
        # console output but not <properties>.
        if entry.description:
            system_out = ET.SubElement(testcase, "system-out")
            system_out.text = entry.description

    ET.indent(testsuite, space="  ")
    return ET.tostring(testsuite, encoding="unicode")


# --- File emission ----------------------------------------------------------

_EXT: dict[str, str] = {"json": "json", "junit": "xml"}


def write_report(report: Report, fmt: str, report_base: str) -> str:
    """Write a machine report to disk and return the path written.

    The format's extension is appended to ``report_base`` via :data:`_EXT`, so
    the format->extension mapping lives in exactly one place.

    Args:
        report: The report to serialize.
        fmt: One of ``"json"`` or ``"junit"``.
        report_base: Destination path without extension (e.g. ``build/report``).

    Returns:
        The path written.

    Raises:
        ValueError: If ``fmt`` is not a known machine format.
    """
    if fmt == "json":
        content = render_json(report)
    elif fmt == "junit":
        content = render_junit(report)
    else:
        msg = f"Unknown machine report format: {fmt!r}"
        raise ValueError(msg)

    path = Path(f"{report_base}.{_EXT[fmt]}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")
    return str(path)


def append_github_summary(report: Report, summary_path: str) -> str:
    """Append a rendered report to the GitHub Actions job summary."""
    path = Path(summary_path)
    with path.open("a", encoding="utf-8") as summary_file:
        summary_file.write(render_github_summary(report))
        summary_file.write("\n")
    return str(path)
