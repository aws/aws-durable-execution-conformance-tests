# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Sink retrieval tests using mocked boto3 clients."""

from __future__ import annotations

import io
import json
from typing import Any

from aws_durable_execution_conformance_tests_insight.model import RecordQuery
from aws_durable_execution_conformance_tests_insight.polling import (
    PollingPolicy,
    SinkCapability,
)
from aws_durable_execution_conformance_tests_insight.sinks.cloudwatch import CloudWatchSink
from aws_durable_execution_conformance_tests_insight.sinks.s3 import S3Sink

_NO_WAIT = {"monotonic": lambda: 0.0, "sleep": lambda _seconds: None}


def _policy() -> PollingPolicy:
    return PollingPolicy(timeout_seconds=10, interval_seconds=0, max_attempts=2)


def _query(arn: str = "arn:test") -> RecordQuery:
    return RecordQuery(execution_arn=arn)


class _FakeS3Client:
    def __init__(self, objects: dict[str, Any]) -> None:
        self._objects = objects

    def list_objects_v2(self, **_kwargs: Any) -> dict[str, Any]:
        return {"Contents": [{"Key": key} for key in self._objects], "IsTruncated": False}

    def get_object(self, *, Bucket: str, Key: str) -> dict[str, Any]:
        del Bucket
        body = json.dumps(self._objects[Key]).encode("utf-8")
        return {"Body": io.BytesIO(body)}


def test_s3_sink_returns_only_matching_execution() -> None:
    objects = {
        "workflow-insight/other.json": {"recordType": "WorkflowInsight", "executionArn": "arn:other"},
        "workflow-insight/mine.json": {
            "recordType": "WorkflowInsight",
            "executionArn": "arn:test",
            "status": "SUCCEEDED",
            "operations": [{"id": "1", "name": "greet", "type": "STEP", "status": "SUCCEEDED"}],
        },
    }
    sink = S3Sink(_FakeS3Client(objects), "bucket", "workflow-insight/", **_NO_WAIT)
    records = sink.find_records(_query(), _policy())
    assert len(records) == 1
    assert records[0].execution_arn == "arn:test"
    assert records[0].operations is not None
    assert sink.capability is SinkCapability.OPERATIONS_ARRAY


def test_s3_sink_absence_returns_empty_list() -> None:
    sink = S3Sink(_FakeS3Client({}), "bucket", "", **_NO_WAIT)
    assert sink.find_records(_query(), _policy()) == []


class _FakeLogsClient:
    def __init__(self, messages: list[str]) -> None:
        self._messages = messages
        self.calls: list[dict[str, Any]] = []

    def filter_log_events(self, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(kwargs)
        return {"events": [{"message": message} for message in self._messages]}


def test_cloudwatch_sink_filters_records_by_execution_and_record_type() -> None:
    messages = [
        "START RequestId: abc",  # non-JSON runtime line, ignored
        json.dumps({"recordType": "WorkflowInsight", "executionArn": "arn:other"}),
        json.dumps(
            {
                "recordType": "WorkflowInsight",
                "executionArn": "arn:test",
                "status": "SUCCEEDED",
                "operationsByName": {"greet": {"type": "STEP", "count": 1, "failedCount": 0, "status": "SUCCEEDED"}},
            }
        ),
    ]
    client = _FakeLogsClient(messages)
    sink = CloudWatchSink(client, "/aws/lambda/fn", **_NO_WAIT)
    records = sink.find_records(_query(), _policy())
    assert len(records) == 1
    assert records[0].operations_by_name is not None
    assert records[0].operations_by_name["greet"].count == 1
    assert sink.capability is SinkCapability.OPERATIONS_BY_NAME
    assert client.calls[0]["logGroupName"] == "/aws/lambda/fn"


def test_cloudwatch_sink_absence_returns_empty_list() -> None:
    sink = CloudWatchSink(_FakeLogsClient([]), "/aws/lambda/fn", **_NO_WAIT)
    assert sink.find_records(_query(), _policy()) == []


def test_cloudwatch_sink_missing_log_group_is_retryable_not_fatal() -> None:
    """A lazily-created log group must read as 'no records yet', not a SinkError."""
    from botocore.exceptions import ClientError

    class _MissingLogGroupClient:
        def filter_log_events(self, **_kwargs: Any) -> dict[str, Any]:
            raise ClientError(
                {"Error": {"Code": "ResourceNotFoundException", "Message": "no group"}},
                "FilterLogEvents",
            )

    sink = CloudWatchSink(_MissingLogGroupClient(), "/aws/lambda/fn", **_NO_WAIT)
    # Poll budget elapses without records -> absence, not an exception.
    assert sink.find_records(_query(), _policy()) == []


def test_cloudwatch_sink_unwraps_lambda_structured_logging_envelope() -> None:
    """nodejs18+ wraps console.log output as {"timestamp",...,"message": "<record>"}."""
    inner = json.dumps(
        {
            "recordType": "WorkflowInsight",
            "executionArn": "arn:test",
            "status": "SUCCEEDED",
            "operationsByName": {"task": {"type": "STEP", "count": 3, "failedCount": 0, "status": "SUCCEEDED"}},
        }
    )
    envelope = json.dumps(
        {"timestamp": "2026-08-05T21:47:47.007Z", "level": "INFO", "requestId": "r-1", "message": inner}
    )
    sink = CloudWatchSink(_FakeLogsClient([envelope]), "/aws/lambda/fn", **_NO_WAIT)
    records = sink.find_records(_query(), _policy())
    assert len(records) == 1
    assert records[0].operations_by_name is not None
    assert records[0].operations_by_name["task"].count == 3


def test_cloudwatch_log_group_resolution_uses_core_retriever_and_prefixed_stack() -> None:
    """Resolution must query CFN with the core's prefixed stack name, not the raw --name."""
    from aws_durable_execution_conformance_tests.config import STACK_NAME_PREFIX

    from aws_durable_execution_conformance_tests_insight.sinks.cloudwatch import (
        _resolve_log_group,
    )

    calls: list[dict[str, str]] = []

    class _FakeCfn:
        def describe_stack_resource(self, **kwargs: str) -> dict[str, Any]:
            calls.append(kwargs)
            return {"StackResourceDetail": {"PhysicalResourceId": "my-stack-insight-7"}}

    group = _resolve_log_group(
        {"name": "insight-js-cw-local"},
        "Insight7RepeatedOperationName",
        {"cloudformation": _FakeCfn(), "logs": object()},
    )
    assert group == "/aws/lambda/my-stack-insight-7"
    assert calls[0]["StackName"] == f"{STACK_NAME_PREFIX}-insight-js-cw-local"
    assert calls[0]["LogicalResourceId"] == "Insight7RepeatedOperationName"


def test_cloudwatch_log_group_resolution_failure_is_loud() -> None:
    """CFN resolution failure must raise SinkError, never fall back to a guessed group."""
    import pytest
    from botocore.exceptions import ClientError

    from aws_durable_execution_conformance_tests_insight.polling import SinkError
    from aws_durable_execution_conformance_tests_insight.sinks.cloudwatch import (
        _resolve_log_group,
    )

    class _FailingCfn:
        def describe_stack_resource(self, **_kwargs: str) -> dict[str, Any]:
            raise ClientError(
                {"Error": {"Code": "ValidationError", "Message": "Stack does not exist"}},
                "DescribeStackResource",
            )

    with pytest.raises(SinkError):
        _resolve_log_group(
            {"name": "insight-js-cw-local"},
            "Insight7RepeatedOperationName",
            {"cloudformation": _FailingCfn(), "logs": object()},
        )
