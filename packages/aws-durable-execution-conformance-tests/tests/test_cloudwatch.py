# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for CloudWatch log retrieval and validation."""

from __future__ import annotations

from typing import Any

import pytest

import aws_durable_execution_conformance_tests.cloudwatch as cloudwatch_module
from aws_durable_execution_conformance_tests.cloudwatch import (
    CloudWatchLogError,
    CloudWatchLogRetriever,
)


class _LogsClient:
    def __init__(self, query_results: list[dict[str, Any]]) -> None:
        self._query_results = iter(query_results)
        self.start_query_calls: list[dict[str, Any]] = []
        self.get_query_results_calls: list[dict[str, Any]] = []

    def start_query(self, **kwargs: Any) -> dict[str, str]:
        self.start_query_calls.append(kwargs)
        return {"queryId": "query-123"}

    def get_query_results(self, **kwargs: Any) -> dict[str, Any]:
        self.get_query_results_calls.append(kwargs)
        return next(self._query_results)


def test_queries_logs_for_one_durable_execution(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    execution_arn = "arn:aws:lambda:us-west-2:123456789012:function:test:$LATEST/durable-execution/execution/name"
    logs_client = _LogsClient(
        [
            {"status": "Running"},
            {
                "status": "Complete",
                "results": [
                    [
                        {"field": "@timestamp", "value": "2026-07-23 22:32:33.000"},
                        {"field": "@message", "value": "step executed"},
                        {"field": "@ptr", "value": "pointer"},
                    ]
                ],
            },
        ]
    )
    monkeypatch.setattr(cloudwatch_module.time, "sleep", lambda _seconds: None)
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

    assert events == [
        {
            "timestamp": "2026-07-23 22:32:33.000",
            "message": "step executed",
        }
    ]
    assert logs_client.start_query_calls == [
        {
            "queryLanguage": "CWLI",
            "logGroupName": "/aws/lambda/test",
            "startTime": 1000,
            "endTime": 2001,
            "queryString": (
                "fields @timestamp, @message\n"
                f'| filter coalesce(durableExecutionArn, executionArn) like "{execution_arn}"\n'
                "| sort @timestamp asc"
            ),
            "limit": 10_000,
        }
    ]
    assert logs_client.get_query_results_calls == [
        {"queryId": "query-123"},
        {"queryId": "query-123"},
    ]


def test_raises_when_logs_insights_query_fails() -> None:
    logs_client = _LogsClient([{"status": "Failed"}])
    retriever = CloudWatchLogRetriever(
        cloudformation_client=object(),
        logs_client=logs_client,
    )

    with pytest.raises(CloudWatchLogError, match="status Failed"):
        retriever.get_execution_log_events(
            log_group_name="/aws/lambda/test",
            execution_arn="arn:execution",
            start_time_ms=1_000,
            end_time_ms=2_000,
            wait_seconds=0,
        )
