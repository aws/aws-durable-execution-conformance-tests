# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Execution ARN parsing and record parsing tests."""

from __future__ import annotations

import json

import pytest

from aws_durable_execution_conformance_tests_telemetry.model import (
    ExpectedExecution,
    parse_execution_arn,
    parse_record,
)

ARN = (
    "arn:aws:lambda:us-west-2:123456789012:function:conformance-tests-abc-telemetry-1:$LATEST"
    "/durable-execution/8f1c2d3e-0000-4000-8000-000000000001/inv-1"
)


def test_parse_execution_arn_yields_identity_fields() -> None:
    assert parse_execution_arn(ARN) == ExpectedExecution(
        execution_arn=ARN,
        function_name="conformance-tests-abc-telemetry-1",
        qualifier="$LATEST",
        region="us-west-2",
        account_id="123456789012",
    )


def test_parse_execution_arn_accepts_numeric_qualifier() -> None:
    arn = "arn:aws:lambda:eu-west-1:123456789012:function:fn:7/durable-execution/e/i"
    assert parse_execution_arn(arn).qualifier == "7"


@pytest.mark.parametrize(
    "arn",
    [
        "arn:aws:lambda:us-west-2:123456789012:function:fn",
        "arn:aws:lambda::123456789012:function:fn:$LATEST/durable-execution/e/i",
        "arn:aws:lambda:us-west-2::function:fn:$LATEST/durable-execution/e/i",
        "arn:aws:lambda:us-west-2:123456789012:function::$LATEST/durable-execution/e/i",
        "arn:aws:lambda:us-west-2:123456789012:function:fn:/durable-execution/e/i",
        "not-an-arn",
    ],
)
def test_parse_execution_arn_rejects_malformed(arn: str) -> None:
    with pytest.raises(ValueError, match="Execution ARN"):
        parse_execution_arn(arn)


def test_parse_record_accepts_workflow_telemetry_object() -> None:
    event = {"timestamp": 1, "message": json.dumps({"recordType": "DurableExecutionTelemetry", "executionArn": ARN})}
    record = parse_record(event)
    assert record is not None
    assert record.event is event
    assert record.data["executionArn"] == ARN


@pytest.mark.parametrize(
    "message",
    [
        "plain text",
        json.dumps(["DurableExecutionTelemetry"]),
        json.dumps("DurableExecutionTelemetry"),
        json.dumps({"recordType": "Other"}),
        json.dumps({"executionArn": ARN}),
    ],
)
def test_parse_record_rejects_non_telemetry_payloads(message: str) -> None:
    assert parse_record({"message": message}) is None


def test_parse_record_rejects_missing_or_non_string_message() -> None:
    assert parse_record({}) is None
    assert parse_record({"message": {"recordType": "DurableExecutionTelemetry"}}) is None
