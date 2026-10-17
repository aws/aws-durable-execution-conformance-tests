# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Sink polling behavior tests."""

from __future__ import annotations

import pytest

from aws_durable_execution_conformance_tests_insight.model import InsightRecord, RecordQuery
from aws_durable_execution_conformance_tests_insight.polling import (
    PollingPolicy,
    PollingSink,
    SinkCapability,
)


class _Sink(PollingSink):
    name = "fake"
    capability = SinkCapability.OPERATIONS_ARRAY

    def __init__(self, responses: list[list[InsightRecord] | None]) -> None:
        super().__init__(monotonic=lambda: 0.0, sleep=lambda _seconds: None)
        self.responses = responses
        self.attempts = 0

    def _lookup(self, query: RecordQuery) -> list[InsightRecord] | None:
        del query
        response = self.responses[self.attempts]
        self.attempts += 1
        return response


def _query() -> RecordQuery:
    return RecordQuery(execution_arn="arn:test")


def test_returns_after_ingestion() -> None:
    found = [InsightRecord(execution_arn="arn:test")]
    sink = _Sink([None, found])
    result = sink.find_records(_query(), PollingPolicy(timeout_seconds=10, interval_seconds=0, max_attempts=3))
    assert result is found
    assert sink.attempts == 2


def test_waits_for_acceptable_records() -> None:
    partial = [InsightRecord(execution_arn="arn:test")]
    complete = [InsightRecord(execution_arn="arn:test"), InsightRecord(execution_arn="arn:test")]
    sink = _Sink([partial, complete])
    result = sink.find_records(
        _query(),
        PollingPolicy(timeout_seconds=10, interval_seconds=0, max_attempts=3),
        accept=lambda records: len(records) == 2,
    )
    assert result is complete
    assert sink.attempts == 2


def test_absence_returns_empty_after_budget() -> None:
    sink = _Sink([None, None])
    result = sink.find_records(
        _query(),
        PollingPolicy(timeout_seconds=10, interval_seconds=0, max_attempts=2),
        accept=lambda records: not records,
    )
    assert result == []
    assert sink.attempts == 2


@pytest.mark.parametrize(
    "policy",
    [{"timeout_seconds": 0}, {"interval_seconds": -1}, {"max_attempts": 0}],
)
def test_invalid_polling_limits_are_rejected(policy: dict) -> None:
    with pytest.raises(ValueError):
        PollingPolicy(**policy)
