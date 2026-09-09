# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Canonical record normalization tests (both wire shapes)."""

from __future__ import annotations

import pytest

from aws_durable_execution_conformance_tests_insight.model import MISSING
from aws_durable_execution_conformance_tests_insight.normalizers import (
    NormalizationError,
    normalize_record,
)


def test_normalizes_s3_operations_array_shape() -> None:
    payload = {
        "recordType": "WorkflowInsight",
        "schemaVersion": "1.0",
        "executionArn": "arn:test",
        "functionName": "fn",
        "status": "SUCCEEDED",
        "input": "World",
        "output": "Hello, World!",
        "operations": [
            {
                "id": "0123456789abcdef",
                "name": "greet",
                "type": "STEP",
                "subType": "Step",
                "status": "SUCCEEDED",
                "attempt": 1,
            }
        ],
    }

    record = normalize_record(payload)

    assert record.operations is not None
    assert record.operations_by_name is None
    assert len(record.operations) == 1
    assert record.operations[0].sub_type == "Step"
    assert record.operations[0].parent_id is MISSING  # absent, not None
    assert record.to_dict()["operations"][0]["subType"] == "Step"
    # Absent optional fields are omitted from the wire dict.
    assert "error" not in record.to_dict()
    assert "truncated" not in record.to_dict()


def test_normalizes_cloudwatch_operations_by_name_shape() -> None:
    payload = {
        "recordType": "WorkflowInsight",
        "schemaVersion": "1.0",
        "executionArn": "arn:test",
        "functionName": "fn",
        "status": "SUCCEEDED",
        "operationsByName": {
            "greet": {
                "type": "STEP",
                "count": 2,
                "failedCount": 0,
                "status": "SUCCEEDED",
            }
        },
    }

    record = normalize_record(payload)

    assert record.operations is None
    assert record.operations_by_name is not None
    assert record.operations_by_name["greet"].count == 2
    assert record.operations_by_name["greet"].result is MISSING
    wire = record.to_dict()
    assert "operations" not in wire
    assert wire["operationsByName"]["greet"]["failedCount"] == 0


def test_present_null_is_preserved() -> None:
    payload = {"executionArn": "arn:test", "input": None}
    record = normalize_record(payload)
    assert record.input is None
    assert "input" in record.to_dict()
    assert record.to_dict()["input"] is None


def test_rejects_non_object_payload() -> None:
    with pytest.raises(NormalizationError):
        normalize_record(["not", "an", "object"])


def test_rejects_bad_operations_shape() -> None:
    with pytest.raises(NormalizationError):
        normalize_record({"executionArn": "arn:test", "operations": {"not": "a list"}})
