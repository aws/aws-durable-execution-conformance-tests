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
    CloudWatchLogValidator,
    LogExpectation,
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
