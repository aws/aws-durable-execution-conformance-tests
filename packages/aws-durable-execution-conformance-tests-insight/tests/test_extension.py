# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Extension validation-hook tests (capability gating + record diffing)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

from aws_durable_execution_conformance_tests.extensions import ValidationContext
from aws_durable_execution_conformance_tests_insight.extension import InsightExtension
from aws_durable_execution_conformance_tests_insight.model import (
    InsightOperation,
    InsightRecord,
    RecordQuery,
)
from aws_durable_execution_conformance_tests_insight.polling import (
    PollingPolicy,
    SinkCapability,
)


class _FakeSink:
    def __init__(self, capability: SinkCapability, records: list[InsightRecord]) -> None:
        self.name = "fake"
        self.capability = capability
        self._records = records

    def find_records(
        self,
        query: RecordQuery,
        policy: PollingPolicy,
        *,
        accept: Any = None,
    ) -> list[InsightRecord]:
        del query, policy, accept
        return self._records


class _FakeFactory:
    def __init__(self, sink: _FakeSink) -> None:
        self._sink = sink

    def create_with_clients(self, options: Any, *, region: str, function_name: str, aws_clients: Any) -> _FakeSink:
        del options, region, function_name, aws_clients
        return self._sink


def _context(tmp_path: Path, requirement: dict[str, Any]) -> ValidationContext:
    return ValidationContext(
        description_id="insight-1",
        function_name="fn",
        execution_arn="arn:test",
        invocation_started_at_ms=1_000,
        invocation_finished_at_ms=2_000,
        region="us-west-2",
        language="js",
        requirement=requirement,
        execution_history={},
        output_dir=tmp_path,
        placeholders={"EXECUTION_ARN": "arn:test"},
        options={
            "insight_sink": "fake",
            "insight_poll_timeout": 5.0,
            "insight_poll_interval": 0.0,
            "insight_poll_attempts": 1,
        },
        aws_clients={},
    )


def _patch_sink(monkeypatch: Any, sink: _FakeSink) -> None:
    monkeypatch.setattr(InsightExtension, "_sinks", staticmethod(lambda: {"fake": _FakeFactory(sink)}))


def test_capability_gate_skips_when_sink_cannot_express(monkeypatch: Any, tmp_path: Path, capsys: Any) -> None:
    sink = _FakeSink(SinkCapability.OPERATIONS_BY_NAME, [])
    _patch_sink(monkeypatch, sink)
    requirement = {"InsightAssertions": {"requires": ["OPERATIONS_ARRAY"], "record_count": 1}}
    errors = InsightExtension().validate_insight(_context(tmp_path, requirement))
    assert errors == []  # gated out -> not a failure
    assert "UNCOVERED" in capsys.readouterr().out


def test_hook_passes_when_records_satisfy(monkeypatch: Any, tmp_path: Path) -> None:
    record = InsightRecord(
        record_type="WorkflowInsight",
        execution_arn="arn:test",
        status="SUCCEEDED",
        operations=(InsightOperation(name="greet", type="STEP", status="SUCCEEDED"),),
    )
    _patch_sink(monkeypatch, _FakeSink(SinkCapability.OPERATIONS_ARRAY, [record]))
    requirement = {
        "InsightAssertions": {
            "requires": ["OPERATIONS_ARRAY"],
            "record_count": 1,
            "records": [{"expect": {"status": "SUCCEEDED"}}],
        }
    }
    assert InsightExtension().validate_insight(_context(tmp_path, requirement)) == []


def test_hook_reports_errors_and_writes_artifact(monkeypatch: Any, tmp_path: Path) -> None:
    record = InsightRecord(record_type="WorkflowInsight", execution_arn="arn:test", status="FAILED")
    _patch_sink(monkeypatch, _FakeSink(SinkCapability.OPERATIONS_ARRAY, [record]))
    requirement = {"InsightAssertions": {"records": [{"expect": {"status": "SUCCEEDED"}}]}}
    errors = InsightExtension().validate_insight(_context(tmp_path, requirement))
    assert errors
    assert all(error.startswith("Workflow Insight: ") for error in errors)
    assert (tmp_path / "insight-1-insight.json").is_file()


def test_execution_arn_placeholder_is_substituted(monkeypatch: Any, tmp_path: Path) -> None:
    record = InsightRecord(record_type="WorkflowInsight", execution_arn="arn:test", status="SUCCEEDED")
    _patch_sink(monkeypatch, _FakeSink(SinkCapability.OPERATIONS_ARRAY, [record]))
    requirement = {"InsightAssertions": {"records": [{"expect": {"executionArn": "${EXECUTION_ARN}"}}]}}
    assert InsightExtension().validate_insight(_context(tmp_path, requirement)) == []
