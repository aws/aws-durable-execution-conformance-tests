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
    CloudWatchLogValidator,
    LogExpectation,
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
    logs_client = _LogsClient([{"events": []}] + [{"events": [event]}] * 10)
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
    assert logs_client.filter_log_events_calls == [expected_call] * 11


def test_polls_through_partial_execution_log_results(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    first_event = {"timestamp": 1_500_000, "message": "first"}
    second_event = {"timestamp": 1_600_000, "message": "second"}
    complete_events = [first_event, second_event]
    logs_client = _LogsClient([{"events": []}] + [{"events": [first_event]}] * 3 + [{"events": complete_events}] * 7)
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
    assert len(logs_client.filter_log_events_calls) == 11


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


# region Validator (ordered-by-default ExpectedLogs)


def _events(*messages: str) -> list[dict]:
    """Build fake CloudWatch events with increasing timestamps."""
    return [{"message": msg, "timestamp": 1000 + i, "ingestionTime": 2000 + i} for i, msg in enumerate(messages)]


# region LogExpectation parsing


def test_from_dict_defaults():
    exp = LogExpectation.from_dict({"pattern": "foo"})
    assert exp.match == "contains"
    assert exp.count is None
    assert exp.unordered is False
    assert exp.is_ordered


def test_absence_entries_are_not_ordered():
    assert not LogExpectation.from_dict({"pattern": "x", "count": 0}).is_ordered
    assert not LogExpectation.from_dict({"pattern": "x", "max_count": 0}).is_ordered


def test_unordered_flag():
    exp = LogExpectation.from_dict({"pattern": "x", "unordered": True})
    assert not exp.is_ordered


# endregion


# region Ordered matching


def test_ordered_patterns_in_emission_order_pass():
    validator = CloudWatchLogValidator()
    result = validator.validate(
        [{"pattern": "start"}, {"pattern": "middle"}, {"pattern": "end"}],
        _events("start", "middle", "end"),
    )
    assert result.success, result.errors


def test_ordered_patterns_out_of_order_fail():
    validator = CloudWatchLogValidator()
    result = validator.validate(
        [{"pattern": "end"}, {"pattern": "start"}],
        _events("start", "end"),
    )
    assert not result.success
    assert any("start" in e for e in result.errors)


def test_events_sorted_by_timestamp_before_matching():
    validator = CloudWatchLogValidator()
    # Events arrive out of order; timestamps define the real order.
    events = [
        {"message": "second", "timestamp": 2, "ingestionTime": 0},
        {"message": "first", "timestamp": 1, "ingestionTime": 0},
    ]
    result = validator.validate([{"pattern": "first"}, {"pattern": "second"}], events)
    assert result.success, result.errors


def test_equal_timestamps_tiebreak_by_ingestion_time():
    validator = CloudWatchLogValidator()
    # Same-millisecond events: ingestionTime breaks the tie.
    events = [
        {"message": "second", "timestamp": 5, "ingestionTime": 20},
        {"message": "first", "timestamp": 5, "ingestionTime": 10},
    ]
    result = validator.validate([{"pattern": "first"}, {"pattern": "second"}], events)
    assert result.success, result.errors


def test_string_timestamps_from_logs_insights_sort_correctly():
    validator = CloudWatchLogValidator()
    # The Logs Insights path yields string @timestamp values and no
    # ingestionTime; ordering must still work (ISO-ish strings sort).
    events = [
        {"message": "second", "timestamp": "2026-07-23 22:32:34.000"},
        {"message": "first", "timestamp": "2026-07-23 22:32:33.000"},
    ]
    result = validator.validate([{"pattern": "first"}, {"pattern": "second"}], events)
    assert result.success, result.errors


def test_min_and_max_count_constraints_on_ordered_entry():
    validator = CloudWatchLogValidator()
    events = _events("x", "x", "x")
    ok = validator.validate([{"pattern": "x", "min_count": 2, "max_count": 3}], events)
    assert ok.success, ok.errors
    too_few = validator.validate([{"pattern": "x", "min_count": 4}], events)
    assert not too_few.success
    too_many = validator.validate([{"pattern": "x", "max_count": 2}], events)
    assert not too_many.success


def test_exact_count_is_strict_over_remainder():
    validator = CloudWatchLogValidator()
    # Strictness guard: a later duplicate of an earlier pattern still counts
    # against an exact-count entry ("executed exactly once" stays strong).
    result = validator.validate(
        [{"pattern": "a", "count": 1}, {"pattern": "b", "count": 1}],
        _events("a", "b", "a"),
    )
    assert not result.success


def test_repeating_sequences_use_distinct_patterns():
    validator = CloudWatchLogValidator()
    # Idiom for interleaved repeats: make each line distinct (e.g. attempt
    # numbers) instead of repeating the same pattern.
    result = validator.validate(
        [
            {"pattern": "attempt n=1", "count": 1},
            {"pattern": "outcome n=1 FAILED", "count": 1},
            {"pattern": "attempt n=2", "count": 1},
            {"pattern": "outcome n=2 SUCCEEDED", "count": 1},
        ],
        _events("attempt n=1", "outcome n=1 FAILED", "attempt n=2", "outcome n=2 SUCCEEDED"),
    )
    assert result.success, result.errors


def test_ordered_count_multiple_matches():
    validator = CloudWatchLogValidator()
    result = validator.validate(
        [{"pattern": "attempt", "count": 3}, {"pattern": "done", "count": 1}],
        _events("attempt", "attempt", "attempt", "done"),
    )
    assert result.success, result.errors


def test_ordered_count_mismatch_fails():
    validator = CloudWatchLogValidator()
    result = validator.validate(
        [{"pattern": "attempt", "count": 3}],
        _events("attempt", "attempt"),
    )
    assert not result.success


def test_single_entry_degenerates_to_global_count():
    # Backwards compatibility: single positive entry == old global semantics.
    validator = CloudWatchLogValidator()
    result = validator.validate(
        [{"pattern": "x", "count": 2}],
        _events("x", "y", "x"),
    )
    assert result.success, result.errors


# endregion


# region Absence + unordered entries


def test_absence_entry_is_position_neutral():
    validator = CloudWatchLogValidator()
    # count: 0 in the middle must not break ordering of surrounding entries.
    result = validator.validate(
        [{"pattern": "start"}, {"pattern": "ERROR", "count": 0}, {"pattern": "end"}],
        _events("start", "end"),
    )
    assert result.success, result.errors


def test_absence_entry_checked_globally():
    validator = CloudWatchLogValidator()
    # The ERROR line is BEFORE the current scan position; absence must
    # still be checked over the whole stream and fail.
    result = validator.validate(
        [{"pattern": "start"}, {"pattern": "ERROR", "count": 0}],
        _events("ERROR boom", "start"),
    )
    assert not result.success


def test_unordered_entry_matches_anywhere_and_is_position_neutral():
    validator = CloudWatchLogValidator()
    result = validator.validate(
        [
            {"pattern": "start"},
            {"pattern": "concurrent", "count": 2, "unordered": True},
            {"pattern": "end"},
        ],
        _events("concurrent", "start", "end", "concurrent"),
    )
    assert result.success, result.errors


# endregion


# region Match modes


def test_exact_and_regex_modes():
    validator = CloudWatchLogValidator()
    result = validator.validate(
        [
            {"pattern": "hello", "match": "exact"},
            {"pattern": r"wor\w+", "match": "regex"},
        ],
        _events("hello", "world"),
    )
    assert result.success, result.errors


def test_exact_mode_strips_message_whitespace():
    validator = CloudWatchLogValidator()
    # CloudWatch messages carry trailing newlines; exact mode strips them.
    result = validator.validate([{"pattern": "hello", "match": "exact"}], _events("  hello\n"))
    assert result.success, result.errors


def test_invalid_entry_reports_error():
    validator = CloudWatchLogValidator()
    result = validator.validate([{"match": "contains"}], _events("x"))
    assert not result.success
    assert "invalid entry" in result.errors[0]


# endregion
