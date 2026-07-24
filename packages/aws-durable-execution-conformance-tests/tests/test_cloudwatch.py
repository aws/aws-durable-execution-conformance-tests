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


# region Validator (global cardinality + before/after ordering)


def _events(*messages: str) -> list[dict]:
    """Build fake CloudWatch events with increasing timestamps."""
    return [{"message": msg, "timestamp": 1000 + i, "ingestionTime": 2000 + i} for i, msg in enumerate(messages)]


def _validator():
    return CloudWatchLogValidator()


# --- LogExpectation parsing ---


def test_from_dict_defaults():
    exp = LogExpectation.from_dict({"pattern": "foo"})
    assert exp.match == "contains"
    assert exp.count is None
    assert exp.before is None
    assert exp.after is None


def test_from_dict_before_after():
    exp = LogExpectation.from_dict({"pattern": "b", "after": "a", "before": "c"})
    assert exp.after == "a"
    assert exp.before == "c"


def test_invalid_entry_reports_error():
    result = _validator().validate([{"match": "contains"}], _events("x"))
    assert not result.success
    assert "invalid entry" in result.errors[0]


# --- Cardinality (always global) ---


def test_default_requires_at_least_one_match():
    assert _validator().validate([{"pattern": "hit"}], _events("hit")).success
    assert not _validator().validate([{"pattern": "miss"}], _events("hit")).success


def test_exact_count_is_global():
    # A duplicate anywhere in the stream counts against an exact-count entry.
    result = _validator().validate(
        [{"pattern": "a", "count": 1}, {"pattern": "b", "count": 1}],
        _events("a", "b", "a"),
    )
    assert not result.success


def test_duplicate_anywhere_fails_global_count():
    # end,start,end with start(1), end(1): the duplicated 'end' fails globally.
    result = _validator().validate(
        [{"pattern": "start", "count": 1}, {"pattern": "end", "count": 1}],
        _events("end", "start", "end"),
    )
    assert not result.success
    assert any("exactly 1" in e for e in result.errors)


def test_min_and_max_count_constraints():
    events = _events("x", "x", "x")
    assert _validator().validate([{"pattern": "x", "min_count": 2, "max_count": 3}], events).success
    assert not _validator().validate([{"pattern": "x", "min_count": 4}], events).success
    assert not _validator().validate([{"pattern": "x", "max_count": 2}], events).success


def test_entries_without_before_after_assert_counts_only():
    # No implicit list-order chain: reversed emission order still passes.
    result = _validator().validate(
        [{"pattern": "first", "count": 1}, {"pattern": "second", "count": 1}],
        _events("second", "first"),
    )
    assert result.success, result.errors


def test_absence_entry():
    assert _validator().validate([{"pattern": "ERROR", "count": 0}], _events("ok")).success
    assert not _validator().validate([{"pattern": "ERROR", "count": 0}], _events("ERROR boom")).success


def test_max_only_entry_permits_zero_matches():
    result = _validator().validate(
        [{"pattern": "optional", "max_count": 1}, {"pattern": "end", "count": 1}],
        _events("end"),
    )
    assert result.success, result.errors


# --- Ordering via before/after ---


def test_after_satisfied_in_emission_order():
    result = _validator().validate(
        [
            {"pattern": "start", "count": 1},
            {"pattern": "end", "count": 1, "after": "start"},
        ],
        _events("start", "end"),
    )
    assert result.success, result.errors


def test_after_violated_reports_out_of_order():
    result = _validator().validate(
        [
            {"pattern": "start", "count": 1},
            {"pattern": "end", "count": 1, "after": "start"},
        ],
        _events("end", "start"),
    )
    assert not result.success
    assert any("out of order" in e for e in result.errors)


def test_before_field_mirror_semantics():
    spec = [
        {"pattern": "start", "count": 1, "before": "end"},
        {"pattern": "end", "count": 1},
    ]
    assert _validator().validate(spec, _events("start", "end")).success
    assert not _validator().validate(spec, _events("end", "start")).success


def test_after_chain_three_entries():
    spec = [
        {"pattern": "a", "count": 1},
        {"pattern": "b", "count": 1, "after": "a"},
        {"pattern": "c", "count": 1, "after": "b"},
    ]
    assert _validator().validate(spec, _events("a", "b", "c")).success
    assert not _validator().validate(spec, _events("a", "c", "b")).success


def test_after_requires_all_matches_after_reference():
    # One of the two 'end' matches precedes 'start': violation.
    result = _validator().validate(
        [
            {"pattern": "start", "count": 1},
            {"pattern": "end", "count": 2, "after": "start"},
        ],
        _events("end", "start", "end"),
    )
    assert not result.success


def test_same_timestamp_ties_are_concurrent():
    # CloudWatch has no sub-millisecond order: identical sort keys satisfy
    # either direction, so adjacent same-ms hook lines never flake.
    events = [
        {"message": "end", "timestamp": 5, "ingestionTime": 0},
        {"message": "start", "timestamp": 5, "ingestionTime": 0},
    ]
    result = _validator().validate(
        [
            {"pattern": "start", "count": 1},
            {"pattern": "end", "count": 1, "after": "start"},
        ],
        events,
    )
    assert result.success, result.errors


def test_dangling_reference_is_an_error():
    result = _validator().validate(
        [{"pattern": "end", "count": 1, "after": "nonexistent"}],
        _events("end"),
    )
    assert not result.success
    assert any("does not match the pattern of any other entry" in e for e in result.errors)


def test_self_reference_is_an_error():
    result = _validator().validate(
        [{"pattern": "x", "count": 1, "after": "x"}],
        _events("x"),
    )
    assert not result.success
    assert any("own pattern" in e for e in result.errors)


def test_ordering_vacuous_when_reference_has_no_matches():
    # The referenced entry's own count check reports the miss; the
    # ordering constraint itself is vacuous.
    result = _validator().validate(
        [
            {"pattern": "start", "count": 0},
            {"pattern": "end", "count": 1, "after": "start"},
        ],
        _events("end"),
    )
    assert result.success, result.errors


# --- Event sorting ---


def test_events_sorted_by_timestamp_before_matching():
    events = [
        {"message": "second", "timestamp": 2, "ingestionTime": 0},
        {"message": "first", "timestamp": 1, "ingestionTime": 0},
    ]
    result = _validator().validate(
        [{"pattern": "first", "count": 1}, {"pattern": "second", "count": 1, "after": "first"}],
        events,
    )
    assert result.success, result.errors


def test_equal_timestamps_tiebreak_by_ingestion_time():
    events = [
        {"message": "second", "timestamp": 5, "ingestionTime": 20},
        {"message": "first", "timestamp": 5, "ingestionTime": 10},
    ]
    result = _validator().validate(
        [{"pattern": "first", "count": 1}, {"pattern": "second", "count": 1, "after": "first"}],
        events,
    )
    assert result.success, result.errors


def test_string_timestamps_from_logs_insights_sort_correctly():
    # The Logs Insights path yields string @timestamp values and no
    # ingestionTime; before/after ordering must still work.
    events = [
        {"message": "second", "timestamp": "2026-07-23 22:32:34.000"},
        {"message": "first", "timestamp": "2026-07-23 22:32:33.000"},
    ]
    result = _validator().validate(
        [{"pattern": "first", "count": 1}, {"pattern": "second", "count": 1, "after": "first"}],
        events,
    )
    assert result.success, result.errors


# --- Match modes ---


def test_exact_and_regex_modes():
    result = _validator().validate(
        [
            {"pattern": "hello", "match": "exact"},
            {"pattern": r"wor\w+", "match": "regex"},
        ],
        _events("hello", "world"),
    )
    assert result.success, result.errors


def test_exact_mode_strips_message_whitespace():
    result = _validator().validate([{"pattern": "hello", "match": "exact"}], _events("  hello\n"))
    assert result.success, result.errors


# endregion
