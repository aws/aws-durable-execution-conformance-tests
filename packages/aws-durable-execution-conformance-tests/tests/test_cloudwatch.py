# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for CloudWatch log retrieval and validation."""

from __future__ import annotations

from typing import Any

import pytest
from botocore.exceptions import ClientError

import aws_durable_execution_conformance_tests.cloudwatch as cloudwatch_module
from aws_durable_execution_conformance_tests.cloudwatch import (
    CloudWatchLogError,
    CloudWatchLogRetriever,
)


class _LogsClient:
    def __init__(self, responses: list[dict[str, Any]]) -> None:
        self._responses = iter(responses)
        self.filter_log_events_calls: list[dict[str, Any]] = []

    def filter_log_events(self, **kwargs: Any) -> dict[str, Any]:
        self.filter_log_events_calls.append(kwargs)
        return next(self._responses)


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.now += seconds


def test_queries_logs_for_one_durable_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    execution_arn = "arn:aws:lambda:us-west-2:123456789012:function:test:$LATEST/durable-execution/execution/name"
    event = {
        "timestamp": 1_500_000,
        "message": f'{{"executionArn":"{execution_arn}","message":"step executed"}}',
    }
    logs_client = _LogsClient(
        [
            {"events": []},
            {"events": [event]},
            {"events": [event]},
            {"events": [event]},
        ]
    )
    clock = _Clock()
    monkeypatch.setattr(cloudwatch_module.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(cloudwatch_module.time, "sleep", clock.sleep)
    retriever = CloudWatchLogRetriever(
        cloudformation_client=object(),
        logs_client=logs_client,
    )

    events = retriever.get_execution_log_events(
        log_group_name="/aws/lambda/test",
        execution_arn=execution_arn,
        start_time_ms=1_000_123,
        end_time_ms=2_000_456,
        wait_seconds=0,
    )

    assert events == [event]
    expected_call = {
        "logGroupName": "/aws/lambda/test",
        "startTime": 1_000_123,
        "endTime": 2_000_456,
        "filterPattern": f'{{ ($.durableExecutionArn = "{execution_arn}") || ($.executionArn = "{execution_arn}") }}',
    }
    assert logs_client.filter_log_events_calls == [expected_call] * 4


def test_waits_for_all_execution_log_events_to_stabilize(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_event = {"timestamp": 1_500_000, "message": "first"}
    second_event = {"timestamp": 1_600_000, "message": "second"}
    complete_events = [first_event, second_event]
    logs_client = _LogsClient(
        [
            {"events": []},
            {"events": [first_event]},
            {"events": [first_event]},
            {"events": complete_events},
            {"events": complete_events},
            {"events": complete_events},
        ]
    )
    clock = _Clock()
    monkeypatch.setattr(cloudwatch_module.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(cloudwatch_module.time, "sleep", clock.sleep)
    retriever = CloudWatchLogRetriever(
        cloudformation_client=object(),
        logs_client=logs_client,
    )

    events = retriever.get_execution_log_events(
        log_group_name="/aws/lambda/test",
        execution_arn="arn:execution",
        start_time_ms=1_000,
        end_time_ms=2_000,
        wait_seconds=0,
    )

    assert events == complete_events
    assert len(logs_client.filter_log_events_calls) == 6


def test_returns_empty_execution_logs_at_poll_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = _Clock()
    logs_client = _LogsClient([{"events": []}, {"events": []}, {"events": []}])
    monkeypatch.setattr(cloudwatch_module.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(cloudwatch_module.time, "sleep", clock.sleep)
    monkeypatch.setattr(CloudWatchLogRetriever, "EVENT_POLL_TIMEOUT_SECONDS", 2.0)
    retriever = CloudWatchLogRetriever(
        cloudformation_client=object(),
        logs_client=logs_client,
    )

    events = retriever.get_execution_log_events(
        log_group_name="/aws/lambda/test",
        execution_arn="arn:execution",
        start_time_ms=1_000,
        end_time_ms=2_000,
        wait_seconds=0,
    )

    assert events == []
    assert len(logs_client.filter_log_events_calls) == 3


def test_raises_when_filter_log_events_fails() -> None:
    class _FailingLogsClient:
        def filter_log_events(self, **_kwargs: Any) -> dict[str, Any]:
            raise ClientError(
                {"Error": {"Code": "InvalidParameterException", "Message": "failed"}},
                "FilterLogEvents",
            )

    logs_client = _FailingLogsClient()
    retriever = CloudWatchLogRetriever(
        cloudformation_client=object(),
        logs_client=logs_client,
    )

    with pytest.raises(CloudWatchLogError, match="filter-log-events failed"):
        retriever.get_execution_log_events(
            log_group_name="/aws/lambda/test",
            execution_arn="arn:execution",
            start_time_ms=1_000,
            end_time_ms=2_000,
            wait_seconds=0,
        )
