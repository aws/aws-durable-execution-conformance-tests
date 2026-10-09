# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Regression coverage for log groups propagating during execution polling."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import pytest
from botocore.exceptions import ClientError, EndpointConnectionError

import aws_durable_execution_conformance_tests.cloudwatch as cloudwatch_module
from aws_durable_execution_conformance_tests.cloudwatch import (
    CloudWatchLogError,
    CloudWatchLogRetriever,
    CloudWatchLogValidator,
)


class _Clock:
    def __init__(self) -> None:
        self.now = 0.0
        self.sleeps: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.now += seconds


@pytest.fixture
def fake_clock(monkeypatch: pytest.MonkeyPatch) -> _Clock:
    clock = _Clock()
    monkeypatch.setattr(cloudwatch_module.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(cloudwatch_module.time, "sleep", clock.sleep)
    return clock


class _LogsClient:
    def __init__(self, responses: Sequence[dict[str, Any] | Exception]) -> None:
        self.responses = responses
        self.requests: list[dict[str, Any]] = []

    def filter_log_events(self, **kwargs: Any) -> dict[str, Any]:
        response = self.responses[min(len(self.requests), len(self.responses) - 1)]
        self.requests.append(kwargs)
        if isinstance(response, Exception):
            raise response
        return response


def _client_error(code: str = "ResourceNotFoundException") -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": "log group is unavailable"}}, "FilterLogEvents")


def _collect(retriever: CloudWatchLogRetriever, **kwargs: Any) -> list[dict]:
    return retriever.get_execution_log_events(
        log_group_name="/aws/lambda/new-function",
        execution_arn="arn:execution",
        start_time_ms=1_000,
        wait_seconds=0,
        **kwargs,
    )


def test_retries_new_log_group_until_records_are_available(fake_clock: _Clock) -> None:
    event = {"eventId": "first", "message": "done"}
    logs = _LogsClient([_client_error(), _client_error(), {"events": [event]}])
    retriever = CloudWatchLogRetriever(object(), logs, event_poll_timeout_seconds=2, event_poll_interval_seconds=0.5)

    assert _collect(retriever) == [event]
    assert fake_clock.now == 2
    assert fake_clock.sleeps == [0.5] * 4
    assert len(logs.requests) == 5


def test_retains_records_across_a_missing_log_group_response(fake_clock: _Clock) -> None:
    first = {"eventId": "first", "message": "first"}
    second = {"eventId": "second", "message": "second"}
    logs = _LogsClient([{"events": [first]}, _client_error(), {"events": [second]}])
    retriever = CloudWatchLogRetriever(object(), logs, event_poll_timeout_seconds=2)

    assert _collect(retriever) == [first, second]
    assert fake_clock.now == 2


@pytest.mark.parametrize(
    ("timeout", "expected_sleeps"),
    [(0.0, []), (0.25, [0.25]), (1.25, [1.0, 0.25]), (25.0, [1.0] * 25)],
)
def test_persistent_missing_group_raises_at_deadline_without_passing_negative_expectation(
    fake_clock: _Clock, timeout: float, expected_sleeps: list[float]
) -> None:
    failure = _client_error()
    logs = _LogsClient([failure])
    retriever = CloudWatchLogRetriever(object(), logs, event_poll_timeout_seconds=timeout)
    checks: list[list[dict]] = []

    def completion_check(events: list[dict]) -> bool:
        checks.append(events)
        return bool(CloudWatchLogValidator().validate([{"match": {"message": "forbidden"}, "count": 0}], events))

    with pytest.raises(CloudWatchLogError) as exc:
        _collect(retriever, completion_check=completion_check)

    assert exc.value.__cause__ is failure
    assert fake_clock.now == timeout
    assert fake_clock.sleeps == expected_sleeps
    assert len(logs.requests) == len(expected_sleeps) + 1
    assert checks == []


def test_missing_group_at_deadline_raises_even_with_previously_observed_records(fake_clock: _Clock) -> None:
    failure = _client_error()
    logs = _LogsClient([{"events": [{"eventId": "first", "message": "done"}]}, failure])
    retriever = CloudWatchLogRetriever(object(), logs, event_poll_timeout_seconds=1)

    with pytest.raises(CloudWatchLogError) as exc:
        _collect(retriever)

    assert exc.value.__cause__ is failure
    assert fake_clock.now == 1
    assert len(logs.requests) == 2


@pytest.mark.parametrize(("interval", "missing_polls", "finished_at"), [(1.0, 26, 36.0), (10.0, 3, 40.0)])
def test_negative_expectation_waits_for_quiet_window_after_successful_empty_recovery(
    fake_clock: _Clock, interval: float, missing_polls: int, finished_at: float
) -> None:
    logs = _LogsClient([_client_error()] * missing_polls + [{"events": []}])
    retriever = CloudWatchLogRetriever(
        object(), logs, event_poll_timeout_seconds=60, event_poll_interval_seconds=interval
    )
    check_times: list[float] = []

    def completion_check(events: list[dict]) -> bool:
        check_times.append(fake_clock.now)
        return bool(CloudWatchLogValidator().validate([{"match": {"message": "forbidden"}, "count": 0}], events))

    assert _collect(retriever, completion_check=completion_check) == []
    assert fake_clock.now == finished_at
    assert check_times == [finished_at]


def test_missing_poll_cannot_finish_passing_snapshot_and_recovery_restarts_quiet_window(fake_clock: _Clock) -> None:
    event = {"eventId": "first", "message": "done"}
    logs = _LogsClient([{"events": [event]}] * 20 + [_client_error(), {"events": []}])
    retriever = CloudWatchLogRetriever(object(), logs, event_poll_timeout_seconds=60)

    events = _collect(retriever, completion_check=bool)

    assert events == [event]
    assert fake_clock.now == 31
    assert len(logs.requests) == 32


@pytest.mark.parametrize(
    ("initial", "partial", "expected_count", "actual_count"),
    [
        ([], {"eventId": "forbidden", "message": "record"}, 0, 1),
        ([{"eventId": "first", "message": "record"}], {"eventId": "duplicate", "message": "record"}, 1, 2),
    ],
    ids=["forbidden", "duplicate"],
)
def test_retains_partial_page_violations_and_restarts_pagination_after_missing_group(
    fake_clock: _Clock, initial: list[dict], partial: dict, expected_count: int, actual_count: int
) -> None:
    logs = _LogsClient(
        [
            {"events": initial},
            {"events": [partial], "nextToken": "page-2"},
            _client_error(),
            {"events": []},
        ]
    )
    retriever = CloudWatchLogRetriever(object(), logs, event_poll_timeout_seconds=3)

    events = _collect(retriever)
    result = CloudWatchLogValidator().validate([{"match": {"message": "record"}, "count": expected_count}], events)

    assert events == [*initial, partial]
    assert not result
    assert f"expected exactly {expected_count} match(es), got {actual_count}" in result.errors[0]
    assert [request.get("nextToken") for request in logs.requests] == [None, None, "page-2", None, None]
    assert fake_clock.now == 3


@pytest.mark.parametrize(
    "failure",
    [
        _client_error("AccessDeniedException"),
        _client_error("ThrottlingException"),
        EndpointConnectionError(endpoint_url="https://logs.invalid"),
    ],
    ids=["access-denied", "throttled", "connection-error"],
)
def test_other_errors_still_fail_immediately_without_retrying(fake_clock: _Clock, failure: Exception) -> None:
    logs = _LogsClient([failure])
    retriever = CloudWatchLogRetriever(object(), logs)

    with pytest.raises(CloudWatchLogError) as exc:
        _collect(retriever)

    assert exc.value.__cause__ is failure
    assert fake_clock.now == 0
    assert fake_clock.sleeps == []
    assert len(logs.requests) == 1


def test_direct_log_retrieval_still_raises_for_missing_group(fake_clock: _Clock) -> None:
    failure = _client_error()
    logs = _LogsClient([failure])
    retriever = CloudWatchLogRetriever(object(), logs)

    with pytest.raises(CloudWatchLogError) as exc:
        retriever.get_log_events("/aws/lambda/new-function", start_time_ms=1_000, wait_seconds=0)

    assert exc.value.__cause__ is failure
    assert fake_clock.now == 0
    assert fake_clock.sleeps == []
    assert len(logs.requests) == 1
