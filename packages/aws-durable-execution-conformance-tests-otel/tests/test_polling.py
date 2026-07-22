# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Backend polling behavior tests."""

from __future__ import annotations

from datetime import UTC, datetime

import pytest
from aws_durable_execution_conformance_tests_otel.model import TelemetryQuery, Trace
from aws_durable_execution_conformance_tests_otel.polling import (
    PollingBackend,
    PollingPolicy,
    TelemetryTimeout,
)


class _Backend(PollingBackend):
    name = "fake"

    def __init__(self, responses: list[Trace | None]) -> None:
        super().__init__(monotonic=lambda: 0.0, sleep=lambda _seconds: None)
        self.responses = responses
        self.attempts = 0

    def _lookup(self, query: TelemetryQuery) -> Trace | None:
        del query
        response = self.responses[self.attempts]
        self.attempts += 1
        return response


def _query() -> TelemetryQuery:
    now = datetime.now(UTC)
    return TelemetryQuery("arn:test", "service", now, now)


def test_polling_returns_after_ingestion() -> None:
    expected = Trace(trace_id="1" * 32, spans=())
    backend = _Backend([None, expected])

    actual = backend.find_trace(
        _query(),
        PollingPolicy(timeout_seconds=10, interval_seconds=0, max_attempts=3),
    )

    assert actual is expected
    assert backend.attempts == 2


def test_polling_waits_for_an_acceptable_trace() -> None:
    incomplete = Trace(trace_id="1" * 32, spans=())
    complete = Trace(trace_id="1" * 32, spans=())
    backend = _Backend([incomplete, complete])

    actual = backend.find_trace(
        _query(),
        PollingPolicy(timeout_seconds=10, interval_seconds=0, max_attempts=3),
        accept=lambda trace: trace is complete,
    )

    assert actual is complete
    assert backend.attempts == 2


def test_polling_returns_latest_trace_when_none_are_acceptable() -> None:
    first = Trace(trace_id="1" * 32, spans=())
    latest = Trace(trace_id="1" * 32, spans=())
    backend = _Backend([first, latest])

    actual = backend.find_trace(
        _query(),
        PollingPolicy(timeout_seconds=10, interval_seconds=0, max_attempts=2),
        accept=lambda _trace: False,
    )

    assert actual is latest
    assert backend.attempts == 2


def test_polling_timeout_has_provider_neutral_context() -> None:
    backend = _Backend([None, None, None])
    with pytest.raises(TelemetryTimeout, match="execution='arn:test'"):
        backend.find_trace(
            _query(),
            PollingPolicy(timeout_seconds=10, interval_seconds=0, max_attempts=3),
        )
    assert backend.attempts == 3


@pytest.mark.parametrize(
    "policy",
    [
        {"timeout_seconds": 0},
        {"interval_seconds": -1},
        {"max_attempts": 0},
    ],
)
def test_invalid_polling_limits_are_rejected(policy: dict) -> None:
    with pytest.raises(ValueError):
        PollingPolicy(**policy)
