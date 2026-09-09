# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""InsightAssertions matcher tests."""

from __future__ import annotations

from aws_durable_execution_conformance_tests_insight.model import (
    InsightOperation,
    InsightOperationSummary,
    InsightRecord,
)
from aws_durable_execution_conformance_tests_insight.validators import validate_insight_records


def _success_record() -> InsightRecord:
    return InsightRecord(
        record_type="WorkflowInsight",
        schema_version="1.0",
        execution_arn="arn:test",
        function_name="fn",
        status="SUCCEEDED",
        input="World",
        output="Hello, World!",
        operations=(
            InsightOperation(
                id="0123456789abcdef",
                name="greet",
                type="STEP",
                sub_type="Step",
                status="SUCCEEDED",
                attempt=1,
            ),
        ),
    )


def test_basic_success_passes() -> None:
    assertions = {
        "record_count": 1,
        "records": [
            {
                "expect": {
                    "recordType": "WorkflowInsight",
                    "schemaVersion": "1.0",
                    "status": "SUCCEEDED",
                    "functionName": "*",
                    "input": "World",
                    "output": "Hello, World!",
                },
                "absent": ["error", "truncated", "droppedInput", "droppedOutput"],
                "operations": [
                    {
                        "select": {"name": "greet"},
                        "count": 1,
                        "expect": {
                            "type": "STEP",
                            "subType": "Step",
                            "status": "SUCCEEDED",
                            "attempt": 1,
                            "id": "${OP1}",
                        },
                        "absent": ["parentId", "result"],
                    }
                ],
            }
        ],
    }
    assert validate_insight_records([_success_record()], assertions) == []


def test_absent_key_that_is_present_fails() -> None:
    record = InsightRecord(execution_arn="arn:test", truncated=True)
    errors = validate_insight_records([record], {"records": [{"absent": ["truncated"]}]})
    assert any("truncated" in error for error in errors)


def test_absent_distinguishes_present_null_from_missing() -> None:
    # input present with an explicit null value -> "absent" must fail.
    present_null = InsightRecord(execution_arn="arn:test", input=None)
    assert validate_insight_records([present_null], {"records": [{"absent": ["input"]}]})

    # input never present -> "absent" passes.
    truly_absent = InsightRecord(execution_arn="arn:test")
    assert validate_insight_records([truly_absent], {"records": [{"absent": ["input"]}]}) == []


def test_null_value_can_be_matched_explicitly() -> None:
    record = InsightRecord(execution_arn="arn:test", output=None)
    assert validate_insight_records([record], {"records": [{"expect": {"output": None}}]}) == []


def test_placeholder_binds_and_stays_consistent() -> None:
    record = InsightRecord(
        execution_arn="arn:test",
        operations=(
            InsightOperation(id="aaaa", name="a", parent_id="root"),
            InsightOperation(id="bbbb", name="b", parent_id="root"),
        ),
    )
    # ${PARENT} is bound by the first op and must equal the second op's value.
    assertions = {
        "records": [
            {
                "operations": [
                    {"select": {"name": "a"}, "expect": {"parentId": "${PARENT}"}},
                    {"select": {"name": "b"}, "expect": {"parentId": "${PARENT}"}},
                ]
            }
        ]
    }
    assert validate_insight_records([record], assertions) == []


def test_placeholder_inconsistency_fails() -> None:
    record = InsightRecord(
        execution_arn="arn:test",
        operations=(
            InsightOperation(id="aaaa", name="a", parent_id="root-1"),
            InsightOperation(id="bbbb", name="b", parent_id="root-2"),
        ),
    )
    assertions = {
        "records": [
            {
                "operations": [
                    {"select": {"name": "a"}, "expect": {"parentId": "${PARENT}"}},
                    {"select": {"name": "b"}, "expect": {"parentId": "${PARENT}"}},
                ]
            }
        ]
    }
    errors = validate_insight_records([record], assertions)
    assert any("PARENT" in error for error in errors)


def test_record_count_zero_passes_for_empty() -> None:
    assert validate_insight_records([], {"record_count": 0}) == []


def test_record_count_mismatch_fails() -> None:
    errors = validate_insight_records([], {"record_count": 1})
    assert any("record_count" in error for error in errors)


def test_min_and_max_record_count() -> None:
    records = [InsightRecord(execution_arn="arn:test")]
    assert validate_insight_records(records, {"min_record_count": 1, "max_record_count": 2}) == []
    assert validate_insight_records(records, {"min_record_count": 2})
    assert validate_insight_records(records * 3, {"max_record_count": 2})


def test_regex_and_any_of_and_wildcard() -> None:
    record = InsightRecord(
        execution_arn="arn:test",
        status="FAILED",
        operations=(InsightOperation(id="0123456789abcdef", name="greet", status="FAILED"),),
    )
    assertions = {
        "records": [
            {
                "expect": {"status": {"$any_of": ["SUCCEEDED", "FAILED"]}, "executionArn": "*"},
                "operations": [
                    {"select": {"name": "greet"}, "expect": {"id": "/^[0-9a-f]{16}$/"}},
                ],
            }
        ]
    }
    assert validate_insight_records([record], assertions) == []


def test_operations_by_name_expect_and_absent() -> None:
    record = InsightRecord(
        execution_arn="arn:test",
        operations_by_name={
            "greet": InsightOperationSummary(
                type="STEP",
                count=1,
                failed_count=0,
                status="SUCCEEDED",
            )
        },
    )
    assertions = {
        "records": [
            {
                "operations_by_name": {
                    "greet": {
                        "expect": {"type": "STEP", "count": 1, "failedCount": 0, "status": "SUCCEEDED"},
                        "absent": ["result", "error"],
                    }
                }
            }
        ]
    }
    assert validate_insight_records([record], assertions) == []


def test_operations_array_required_but_missing() -> None:
    record = InsightRecord(execution_arn="arn:test", operations_by_name={})
    errors = validate_insight_records([record], {"records": [{"operations": [{"select": {"name": "x"}}]}]})
    assert any("operations" in error for error in errors)


def test_operation_count_mismatch_fails() -> None:
    record = InsightRecord(
        execution_arn="arn:test",
        operations=(
            InsightOperation(name="loop"),
            InsightOperation(name="loop"),
        ),
    )
    errors = validate_insight_records([record], {"records": [{"operations": [{"select": {"name": "loop"}}]}]})
    assert any("expected exactly one" in error for error in errors)
    assert (
        validate_insight_records([record], {"records": [{"operations": [{"select": {"name": "loop"}, "count": 2}]}]})
        == []
    )
