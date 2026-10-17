# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Canonical Workflow Insight record model.

Mirrors ``WorkflowInsightRecord`` / ``OperationRecord`` / ``OperationSummary``
from the JS ``aws-durable-execution-sdk-js-insight`` plugin. Python attribute
names are snake_case; :meth:`to_dict` re-emits the exact camelCase *wire* names
so requirement YAML reads like the emitted record.

A record carries **exactly one** of ``operations`` (the lossless array emitted by
array-native sinks such as S3) or ``operations_by_name`` (the name-keyed summary
map emitted by point-access sinks such as CloudWatch Logs) -- whichever the sink
actually emitted. The other stays ``None``. The map is **never** derived from the
array here: doing so would reimplement the plugin's ``buildOperationsByName`` and
hide bugs in it.

Every optional wire field distinguishes *absent* (key never present) from
*null* (key present with value ``None``). Absent fields default to
:data:`MISSING` and are omitted by :meth:`to_dict`; a present ``None`` is
preserved as ``null``. This distinction is what lets requirements assert
``absent: [...]`` separately from an explicit null value.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any, Final


class _Missing:
    """Sentinel marking a wire field that was never present."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "MISSING"

    def __bool__(self) -> bool:
        return False


MISSING: Final = _Missing()


def _emit(data: dict[str, Any], wire: str, value: Any) -> None:
    """Add ``wire`` to ``data`` unless the value is absent (``MISSING``)."""

    if value is not MISSING:
        data[wire] = value


# Python attribute -> camelCase wire name. Ordering follows the JS record.
_OPERATION_WIRE: Final[tuple[tuple[str, str], ...]] = (
    ("id", "id"),
    ("name", "name"),
    ("type", "type"),
    ("sub_type", "subType"),
    ("parent_id", "parentId"),
    ("status", "status"),
    ("start_time", "startTime"),
    ("end_time", "endTime"),
    ("duration_ms", "durationMs"),
    ("attempt", "attempt"),
    ("error", "error"),
    ("result", "result"),
    ("truncated", "truncated"),
)

_SUMMARY_WIRE: Final[tuple[tuple[str, str], ...]] = (
    ("type", "type"),
    ("sub_type", "subType"),
    ("count", "count"),
    ("min_duration_ms", "minDurationMs"),
    ("max_duration_ms", "maxDurationMs"),
    ("total_duration_ms", "totalDurationMs"),
    ("failed_count", "failedCount"),
    ("max_attempt", "maxAttempt"),
    ("status", "status"),
    ("result", "result"),
    ("error", "error"),
)

# Record-level scalar fields (operations / operations_by_name handled separately).
_RECORD_WIRE: Final[tuple[tuple[str, str], ...]] = (
    ("record_type", "recordType"),
    ("schema_version", "schemaVersion"),
    ("emitted_at", "emittedAt"),
    ("execution_arn", "executionArn"),
    ("execution_name", "executionName"),
    ("function_name", "functionName"),
    ("function_qualifier", "functionQualifier"),
    ("region", "region"),
    ("account_id", "accountId"),
    ("status", "status"),
    ("start_time", "startTime"),
    ("end_time", "endTime"),
    ("duration_ms", "durationMs"),
    ("input", "input"),
    ("output", "output"),
    ("error", "error"),
    ("truncated", "truncated"),
    ("dropped_operations", "droppedOperations"),
    ("dropped_input", "droppedInput"),
    ("dropped_output", "droppedOutput"),
)


@dataclass(frozen=True)
class InsightOperation:
    """A single operation within an execution (mirrors ``OperationRecord``)."""

    id: Any = MISSING
    name: Any = MISSING
    type: Any = MISSING
    sub_type: Any = MISSING
    parent_id: Any = MISSING
    status: Any = MISSING
    start_time: Any = MISSING
    end_time: Any = MISSING
    duration_ms: Any = MISSING
    attempt: Any = MISSING
    error: Any = MISSING
    result: Any = MISSING
    truncated: Any = MISSING

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {}
        for attr, wire in _OPERATION_WIRE:
            _emit(data, wire, getattr(self, attr))
        return data


@dataclass(frozen=True)
class InsightOperationSummary:
    """A per-operation-name summary (mirrors ``OperationSummary``)."""

    type: Any = MISSING
    sub_type: Any = MISSING
    count: Any = MISSING
    min_duration_ms: Any = MISSING
    max_duration_ms: Any = MISSING
    total_duration_ms: Any = MISSING
    failed_count: Any = MISSING
    max_attempt: Any = MISSING
    status: Any = MISSING
    result: Any = MISSING
    error: Any = MISSING

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {}
        for attr, wire in _SUMMARY_WIRE:
            _emit(data, wire, getattr(self, attr))
        return data


@dataclass(frozen=True)
class InsightRecord:
    """A curated execution record (mirrors ``WorkflowInsightRecord``)."""

    record_type: Any = MISSING
    schema_version: Any = MISSING
    emitted_at: Any = MISSING
    execution_arn: Any = MISSING
    execution_name: Any = MISSING
    function_name: Any = MISSING
    function_qualifier: Any = MISSING
    region: Any = MISSING
    account_id: Any = MISSING
    status: Any = MISSING
    start_time: Any = MISSING
    end_time: Any = MISSING
    duration_ms: Any = MISSING
    input: Any = MISSING
    output: Any = MISSING
    error: Any = MISSING
    truncated: Any = MISSING
    dropped_operations: Any = MISSING
    dropped_input: Any = MISSING
    dropped_output: Any = MISSING
    operations: tuple[InsightOperation, ...] | None = None
    operations_by_name: Mapping[str, InsightOperationSummary] | None = None

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {}
        for attr, wire in _RECORD_WIRE:
            _emit(data, wire, getattr(self, attr))
        if self.operations is not None:
            data["operations"] = [operation.to_dict() for operation in self.operations]
        if self.operations_by_name is not None:
            data["operationsByName"] = {name: summary.to_dict() for name, summary in self.operations_by_name.items()}
        return data


def record_to_dict(record: InsightRecord) -> dict[str, Any]:
    """Serialize a canonical record to its camelCase wire dict."""

    return record.to_dict()


def records_to_dicts(records: tuple[InsightRecord, ...] | list[InsightRecord]) -> list[dict[str, Any]]:
    """Serialize an ordered collection of records for diagnostic artifacts."""

    return [record.to_dict() for record in records]


@dataclass(frozen=True)
class RecordQuery:
    """Execution-scoped record retrieval window (mirrors otel's TelemetryQuery)."""

    execution_arn: str
    started_at: Any = field(default=None)
    ended_at: Any = field(default=None)
