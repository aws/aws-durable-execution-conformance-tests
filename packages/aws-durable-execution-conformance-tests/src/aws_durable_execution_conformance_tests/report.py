# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Conformance test report model.

Pure data structures describing the outcome of a validation run. Report writers
(console / json / junit) consume a :class:`Report`; nothing in this module does
I/O or AWS calls, so it is cheap to unit-test.

Status semantics:

- ``PASSED``            -- history + result matched.
- ``FAILED``            -- a real mismatch or error (blocking by default).
- ``EXPECTED_FAILED``   -- matched a declared failure and error signature (non-blocking).
- ``UNEXPECTED_PASSED`` -- a declared failure passed and its declaration is stale (blocking).
- ``OPTIONAL_FAILED``   -- failed but the requirement is marked ``optional`` (non-blocking).
- ``NOT_IMPLEMENTED``   -- a declared, intentional SDK gap with a reason (non-blocking).
- ``UNCOVERED``         -- no example found and not declared (non-blocking by default; warn).

Exit-code policy is controlled by ``fail_on``: ``FAILED`` and
``UNEXPECTED_PASSED`` always block, and ``failed+uncovered`` additionally blocks
on ``UNCOVERED``.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class ReportStatus(str, Enum):
    """Terminal status of a single validated requirement."""

    PASSED = "PASSED"
    FAILED = "FAILED"
    EXPECTED_FAILED = "EXPECTED_FAILED"
    UNEXPECTED_PASSED = "UNEXPECTED_PASSED"
    OPTIONAL_FAILED = "OPTIONAL_FAILED"
    NOT_IMPLEMENTED = "NOT_IMPLEMENTED"
    UNCOVERED = "UNCOVERED"


# --- Exit-code policies (--fail-on) -----------------------------------------

FAIL_ON_FAILED = "failed"
FAIL_ON_FAILED_UNCOVERED = "failed+uncovered"

_BLOCKING_BY_POLICY: dict[str, frozenset[ReportStatus]] = {
    FAIL_ON_FAILED: frozenset(
        {
            ReportStatus.FAILED,
            ReportStatus.UNEXPECTED_PASSED,
        }
    ),
    FAIL_ON_FAILED_UNCOVERED: frozenset(
        {
            ReportStatus.FAILED,
            ReportStatus.UNEXPECTED_PASSED,
            ReportStatus.UNCOVERED,
        }
    ),
}


@dataclass(frozen=True)
class ReportEntry:
    """Outcome of a single requirement.

    Attributes:
        id: Requirement id (e.g. ``"8-13"``).
        suite: Requirement suite (e.g. ``"parallel"``), or None if unknown.
        status: The terminal :class:`ReportStatus`.
        function: Logical SAM function name that satisfied the requirement, or
            None (e.g. for NOT_IMPLEMENTED / UNCOVERED there is no function).
        description: One-line requirement description (from the requirement YAML).
        reason: Human-readable explanation for declared non-passing outcomes.
        errors: Assertion error messages for failing outcomes.
        duration_seconds: Wall-clock time spent validating this requirement.
    """

    id: str
    suite: str | None
    status: ReportStatus
    function: str | None = None
    description: str | None = None
    reason: str | None = None
    errors: list[str] = field(default_factory=list)
    duration_seconds: float = 0.0

    @property
    def optional(self) -> bool:
        """True when this entry is a non-blocking optional failure.

        Derived from ``status`` so it can never contradict it.
        """
        return self.status == ReportStatus.OPTIONAL_FAILED

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-friendly dict."""
        return {
            "id": self.id,
            "function": self.function,
            "suite": self.suite,
            "description": self.description,
            "status": self.status.value,
            "reason": self.reason,
            "optional": self.optional,
            "errors": list(self.errors),
            "duration_seconds": round(self.duration_seconds, 3),
        }


@dataclass(frozen=True)
class RunMetadata:
    """Context describing a single validation run."""

    name: str
    template: str
    region: str
    language: str | None = None
    suites: list[str] = field(default_factory=lambda: ["all"])
    started_at: str | None = None
    finished_at: str | None = None
    duration_seconds: float = 0.0

    def to_dict(self) -> dict[str, Any]:
        """Serialize to a JSON-friendly dict."""
        return {
            "name": self.name,
            "template": self.template,
            "region": self.region,
            "language": self.language,
            "suites": list(self.suites),
            "started_at": self.started_at,
            "finished_at": self.finished_at,
            "duration_seconds": round(self.duration_seconds, 3),
        }


@dataclass
class Report:
    """A full validation run: metadata + per-requirement entries.

    Computes summary counts and the CI exit code from the entries and the
    ``fail_on`` policy. Pure — construct it, then hand it to a writer.
    """

    run: RunMetadata
    entries: list[ReportEntry] = field(default_factory=list)
    fail_on: str = FAIL_ON_FAILED
    schema_version: str = "1.1"
    warnings: list[str] = field(default_factory=list)

    def add(self, entry: ReportEntry) -> None:
        """Append a requirement outcome."""
        self.entries.append(entry)

    def counts(self) -> dict[ReportStatus, int]:
        """Return a count per :class:`ReportStatus` (all statuses present)."""
        result: dict[ReportStatus, int] = dict.fromkeys(ReportStatus, 0)
        for entry in self.entries:
            result[entry.status] += 1
        return result

    def summary(self) -> dict[str, int]:
        """Return a JSON-friendly summary of status counts."""
        counts = self.counts()
        return {
            "total": len(self.entries),
            "passed": counts[ReportStatus.PASSED],
            "failed": counts[ReportStatus.FAILED],
            "expected_failed": counts[ReportStatus.EXPECTED_FAILED],
            "unexpected_passed": counts[ReportStatus.UNEXPECTED_PASSED],
            "optional_failed": counts[ReportStatus.OPTIONAL_FAILED],
            "not_implemented": counts[ReportStatus.NOT_IMPLEMENTED],
            "uncovered": counts[ReportStatus.UNCOVERED],
        }

    def blocking_statuses(self) -> frozenset[ReportStatus]:
        """Statuses that block CI under the current ``fail_on`` policy."""
        return _BLOCKING_BY_POLICY.get(
            self.fail_on,
            frozenset(
                {
                    ReportStatus.FAILED,
                    ReportStatus.UNEXPECTED_PASSED,
                }
            ),
        )

    def blocking_count(self) -> int:
        """Number of entries whose status blocks CI."""
        blocking = self.blocking_statuses()
        return sum(1 for entry in self.entries if entry.status in blocking)

    def exit_code(self) -> int:
        """Process exit code: 1 if any blocking entry exists, else 0."""
        return 1 if self.blocking_count() > 0 else 0

    def entries_by_status(self, status: ReportStatus) -> list[ReportEntry]:
        """All entries with the given status, in insertion order."""
        return [entry for entry in self.entries if entry.status == status]

    def to_dict(self) -> dict[str, Any]:
        """Serialize the whole report to a JSON-friendly dict (schema 1.1)."""
        return {
            "schema_version": self.schema_version,
            "run": self.run.to_dict(),
            "warnings": list(self.warnings),
            "summary": self.summary(),
            "results": [entry.to_dict() for entry in self.entries],
            "ci": {
                "fail_on": self.fail_on,
                "blocking_statuses": sorted(s.value for s in self.blocking_statuses()),
                "blocking_count": self.blocking_count(),
                "exit_code": self.exit_code(),
            },
        }
