# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Record retrieval tests with a scripted retriever."""

from __future__ import annotations

import json
from typing import Any

import pytest

from aws_durable_execution_conformance_tests.cloudwatch import CloudWatchLogError
from aws_durable_execution_conformance_tests_telemetry.retrieval import (
    TelemetryAccessError,
    fetch_records,
)

ARN = "arn:aws:lambda:us-west-2:123456789012:function:fn:$LATEST/durable-execution/e1/i1"
OTHER_ARN = "arn:aws:lambda:us-west-2:123456789012:function:fn:$LATEST/durable-execution/e2/i1"
LOG_GROUP = "/durable-execution-conformance/conformance-tests-abc/durable-execution-telemetry"


class _FakeRetriever:
    def __init__(self, events: list[dict[str, Any]] | Exception) -> None:
        self._events = events
        self.calls: list[dict[str, Any]] = []

    def get_execution_log_events(self, **kwargs: Any) -> list[dict[str, Any]]:
        self.calls.append(kwargs)
        if isinstance(self._events, Exception):
            raise self._events
        return self._events


def _event(payload: Any, timestamp: int = 1) -> dict[str, Any]:
    return {"timestamp": timestamp, "ingestionTime": timestamp, "message": json.dumps(payload)}


def _fetch(retriever: Any) -> Any:
    return fetch_records(retriever, LOG_GROUP, ARN, start_time_ms=1_000, wait_seconds=3)


def test_telemetry_record_parsed() -> None:
    retriever = _FakeRetriever(
        [_event({"recordType": "DurableExecutionTelemetry", "executionArn": ARN, "status": "SUCCEEDED"})]
    )
    fetched = _fetch(retriever)
    assert len(fetched.events) == 1
    assert len(fetched.records) == 1
    assert fetched.records[0].data["status"] == "SUCCEEDED"
    assert fetched.malformed == ()


def test_non_json_and_non_telemetry_events_are_malformed() -> None:
    events = [
        {"timestamp": 1, "message": "START RequestId: abc"},
        _event({"recordType": "Other", "executionArn": ARN}),
        _event({"executionArn": ARN}),
        _event({"recordType": "DurableExecutionTelemetry", "executionArn": ARN}),
    ]
    fetched = _fetch(_FakeRetriever(events))
    assert len(fetched.records) == 1
    assert len(fetched.malformed) == 3
    assert fetched.malformed[0]["message"] == "START RequestId: abc"


def test_record_for_another_execution_is_dropped() -> None:
    events = [
        _event({"recordType": "DurableExecutionTelemetry", "executionArn": OTHER_ARN}),
        _event({"recordType": "DurableExecutionTelemetry", "executionArn": ARN}),
        _event({"recordType": "DurableExecutionTelemetry"}),
    ]
    fetched = _fetch(_FakeRetriever(events))
    assert [record.data["executionArn"] for record in fetched.records] == [ARN]
    assert fetched.malformed == ()
    assert len(fetched.events) == 3


def test_identical_messages_are_kept_as_separate_records() -> None:
    payload = {"recordType": "DurableExecutionTelemetry", "executionArn": ARN}
    fetched = _fetch(_FakeRetriever([_event(payload, 1), _event(payload, 2)]))
    assert len(fetched.records) == 2
    assert [record.event["timestamp"] for record in fetched.records] == [1, 2]


def test_cloudwatch_error_becomes_access_error() -> None:
    retriever = _FakeRetriever(CloudWatchLogError(LOG_GROUP, "filter-log-events failed: AccessDeniedException"))
    with pytest.raises(TelemetryAccessError, match="AccessDeniedException") as info:
        _fetch(retriever)
    assert isinstance(info.value.__cause__, CloudWatchLogError)


def test_empty_response_yields_no_records() -> None:
    fetched = _fetch(_FakeRetriever([]))
    assert fetched == fetched.__class__(events=(), records=(), malformed=())


@pytest.mark.parametrize(
    ("timestamps", "ingestion_times"),
    [((1, 2), (30, 20)), ((1, 1), (20, 30))],
)
def test_records_sorted_independently_of_discovery_order(
    timestamps: tuple[int, int], ingestion_times: tuple[int, int]
) -> None:
    payload = {"recordType": "DurableExecutionTelemetry", "executionArn": ARN}
    earlier = {**_event(payload, timestamps[0]), "ingestionTime": ingestion_times[0]}
    later = {**_event(payload, timestamps[1]), "ingestionTime": ingestion_times[1]}
    fetched = _fetch(_FakeRetriever([later, earlier]))
    assert [record.event for record in fetched.records] == [earlier, later]
    assert fetched.events == (later, earlier)


def test_timestamp_ties_preserve_discovery_order_and_distinct_events() -> None:
    payload = {"recordType": "DurableExecutionTelemetry", "executionArn": ARN}
    first = {**_event(payload), "eventId": "first"}
    second = {**_event(payload), "eventId": "second"}
    fetched = _fetch(_FakeRetriever([second, first]))
    assert [record.event["eventId"] for record in fetched.records] == ["second", "first"]
