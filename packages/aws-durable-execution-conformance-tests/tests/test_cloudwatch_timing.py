# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Validate polling durations before any CloudWatch request can be made."""

from __future__ import annotations

from typing import Any

import pytest

import aws_durable_execution_conformance_tests.cloudwatch as cloudwatch_module
from aws_durable_execution_conformance_tests.cloudwatch import CloudWatchLogRetriever


@pytest.mark.parametrize("timeout", [-0.25, float("nan"), float("inf"), float("-inf")])
def test_rejects_invalid_timeout(timeout: float) -> None:
    with pytest.raises(ValueError, match="event_poll_timeout_seconds must be finite and non-negative"):
        CloudWatchLogRetriever(object(), object(), event_poll_timeout_seconds=timeout)


@pytest.mark.parametrize("interval", [0.0, -0.25, float("nan"), float("inf"), float("-inf")])
def test_rejects_invalid_poll_interval(interval: float) -> None:
    with pytest.raises(ValueError, match="event_poll_interval_seconds must be finite and positive"):
        CloudWatchLogRetriever(object(), object(), event_poll_interval_seconds=interval)


@pytest.mark.parametrize("interval", [0.0, -0.25, float("nan"), float("inf"), float("-inf")])
def test_validates_resolved_default_poll_interval(monkeypatch: pytest.MonkeyPatch, interval: float) -> None:
    monkeypatch.setattr(CloudWatchLogRetriever, "EVENT_POLL_INTERVAL_SECONDS", interval)

    with pytest.raises(ValueError, match="event_poll_interval_seconds must be finite and positive"):
        CloudWatchLogRetriever(object(), object())


@pytest.mark.parametrize(
    ("timeout", "interval", "expected_sleeps"),
    [
        (0.0, None, []),
        (0.0, 0.125, []),
        (0.25, None, [0.25]),
        (0.25, 0.125, [0.125, 0.125]),
    ],
)
def test_accepts_zero_timeout_and_positive_fractional_durations(
    monkeypatch: pytest.MonkeyPatch,
    timeout: float,
    interval: float | None,
    expected_sleeps: list[float],
) -> None:
    now = 0.0
    sleeps: list[float] = []
    requests: list[dict[str, Any]] = []

    def sleep(seconds: float) -> None:
        nonlocal now
        sleeps.append(seconds)
        now += seconds

    class LogsClient:
        def filter_log_events(self, **kwargs: Any) -> dict[str, Any]:
            requests.append(kwargs)
            return {"events": []}

    monkeypatch.setattr(cloudwatch_module.time, "monotonic", lambda: now)
    monkeypatch.setattr(cloudwatch_module.time, "sleep", sleep)
    retriever = CloudWatchLogRetriever(
        object(),
        LogsClient(),
        event_poll_timeout_seconds=timeout,
        event_poll_interval_seconds=interval,
    )

    events = retriever.get_execution_log_events(
        log_group_name="/aws/lambda/test",
        execution_arn="arn:execution",
        start_time_ms=1_000,
        wait_seconds=0,
    )

    assert events == []
    assert now == timeout
    assert sleeps == expected_sleeps
    assert len(requests) == len(expected_sleeps) + 1
