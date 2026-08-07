# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Normalize raw emitted Workflow Insight JSON into the canonical model.

Handles both wire shapes the built-in sinks observe:

* the **S3 array** shape -- ``JSON.stringify(record)`` with a lossless
  ``operations`` array (from ``S3Exporter``); and
* the **CloudWatch ``operationsByName`` map** shape -- one
  ``console.log(JSON.stringify(withOperationsByName(record)))`` line, where the
  plugin has replaced ``operations`` with the name-keyed summary map (from
  ``LambdaLogExporter``).

Only the keys actually present in the payload are populated; every absent field
is left as :data:`~aws_durable_execution_conformance_tests_insight.model.MISSING`
so the matcher can tell *absent* from *null*. The ``operationsByName`` map is read
verbatim -- never rebuilt from an ``operations`` array.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from aws_durable_execution_conformance_tests_insight.model import (
    MISSING,
    InsightOperation,
    InsightOperationSummary,
    InsightRecord,
)

_OPERATION_FIELDS: tuple[tuple[str, str], ...] = (
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

_SUMMARY_FIELDS: tuple[tuple[str, str], ...] = (
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

_RECORD_FIELDS: tuple[tuple[str, str], ...] = (
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


class NormalizationError(ValueError):
    """Raised when a payload is not a Workflow Insight record object."""


def _present(payload: Mapping[str, Any], wire: str) -> Any:
    return payload.get(wire, MISSING)


def _fields(payload: Mapping[str, Any], mapping: tuple[tuple[str, str], ...]) -> dict[str, Any]:
    return {attr: _present(payload, wire) for attr, wire in mapping}


def normalize_operation(payload: Mapping[str, Any]) -> InsightOperation:
    """Normalize one ``OperationRecord`` object."""

    if not isinstance(payload, Mapping):
        raise NormalizationError("operation entry must be a JSON object")
    return InsightOperation(**_fields(payload, _OPERATION_FIELDS))


def normalize_summary(payload: Mapping[str, Any]) -> InsightOperationSummary:
    """Normalize one ``OperationSummary`` object."""

    if not isinstance(payload, Mapping):
        raise NormalizationError("operationsByName entry must be a JSON object")
    return InsightOperationSummary(**_fields(payload, _SUMMARY_FIELDS))


def normalize_record(payload: Mapping[str, Any]) -> InsightRecord:
    """Normalize a raw emitted record into the canonical model.

    Populates ``operations`` when the payload carries the ``operations`` array,
    and/or ``operations_by_name`` when it carries the ``operationsByName`` map.
    The two shapes are read exactly as emitted; the map is never derived from
    the array.
    """

    if not isinstance(payload, Mapping):
        raise NormalizationError("insight record must be a JSON object")

    operations: tuple[InsightOperation, ...] | None = None
    raw_operations = payload.get("operations", MISSING)
    if "operations" in payload:
        if not isinstance(raw_operations, list):
            raise NormalizationError("'operations' must be a JSON array")
        operations = tuple(normalize_operation(item) for item in raw_operations)

    operations_by_name: dict[str, InsightOperationSummary] | None = None
    if "operationsByName" in payload:
        raw_by_name = payload["operationsByName"]
        if not isinstance(raw_by_name, Mapping):
            raise NormalizationError("'operationsByName' must be a JSON object")
        operations_by_name = {str(name): normalize_summary(summary) for name, summary in raw_by_name.items()}

    return InsightRecord(
        operations=operations,
        operations_by_name=operations_by_name,
        **_fields(payload, _RECORD_FIELDS),
    )
