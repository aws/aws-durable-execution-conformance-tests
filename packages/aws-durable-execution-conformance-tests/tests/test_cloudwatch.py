# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for CloudWatch log retrieval and validation."""

from __future__ import annotations

import json
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

# region Retriever


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
        event_poll_timeout_seconds=10.0,
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
        event_poll_timeout_seconds=10.0,
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
    retriever = CloudWatchLogRetriever(
        cloudformation_client=object(),
        logs_client=logs_client,
        event_poll_timeout_seconds=2.0,
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


def test_event_poll_timeout_override_is_honored(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A per-instance timeout controls how long polling continues."""
    clock = _Clock()
    logs_client = _LogsClient([{"events": []}] * 10)
    monkeypatch.setattr(cloudwatch_module.time, "monotonic", clock.monotonic)
    monkeypatch.setattr(cloudwatch_module.time, "sleep", clock.sleep)
    retriever = CloudWatchLogRetriever(
        cloudformation_client=object(),
        logs_client=logs_client,
        event_poll_timeout_seconds=3.0,
    )

    events = retriever.get_execution_log_events(
        log_group_name="/aws/lambda/test",
        execution_arn="arn:execution",
        start_time_ms=1_000,
        end_time_ms=2_000,
        wait_seconds=0,
    )

    assert events == []
    # 3s deadline with a 1s interval: queries at t=0,1,2,3 -> 4 calls.
    assert len(logs_client.filter_log_events_calls) == 4


def test_legacy_two_client_constructor_uses_default_timeout(fake_clock: _Clock) -> None:
    logs_client = _LogsClient([{"events": []}] * 121)
    retriever = CloudWatchLogRetriever(object(), logs_client)

    events = retriever.get_execution_log_events(
        log_group_name="/aws/lambda/test",
        execution_arn="arn:execution",
        start_time_ms=1_000,
        wait_seconds=0,
    )

    assert events == []
    assert fake_clock.now == 120.0
    assert len(logs_client.filter_log_events_calls) == 121


def test_accumulates_events_missing_from_later_polls(fake_clock: _Clock) -> None:
    first_event = {"eventId": "first", "timestamp": 1_500, "message": "first"}
    second_event = {"eventId": "second", "timestamp": 1_600, "message": "second"}
    logs_client = _LogsClient([{"events": [first_event]}, {"events": [second_event]}, {"events": []}])
    retriever = CloudWatchLogRetriever(object(), logs_client, event_poll_timeout_seconds=2.0)

    events = retriever.get_execution_log_events(
        log_group_name="/aws/lambda/test",
        execution_arn="arn:execution",
        start_time_ms=1_000,
        wait_seconds=0,
    )

    assert events == [first_event, second_event]
    assert fake_clock.now == 2.0


def test_deduplicates_event_ids_across_pages_and_polls(fake_clock: _Clock) -> None:
    first_event = {"eventId": "first", "logStreamName": "stream", "message": "first"}
    second_event = {"eventId": "second", "logStreamName": "stream", "message": "second"}
    logs_client = _LogsClient(
        [
            {"events": [first_event], "nextToken": "page-2"},
            {"events": [first_event, second_event]},
            {"events": [first_event, second_event]},
        ]
    )
    retriever = CloudWatchLogRetriever(object(), logs_client, event_poll_timeout_seconds=1.0)

    events = retriever.get_execution_log_events(
        log_group_name="/aws/lambda/test",
        execution_arn="arn:execution",
        start_time_ms=1_000,
        wait_seconds=0,
    )

    assert events == [first_event, second_event]
    assert [call.get("nextToken") for call in logs_client.filter_log_events_calls] == [None, "page-2", None]
    assert fake_clock.now == 1.0


def test_preserves_identical_payloads_with_distinct_event_ids(fake_clock: _Clock) -> None:
    first_event = {
        "eventId": "first",
        "logStreamName": "stream",
        "timestamp": 1_500,
        "ingestionTime": 2_000,
        "message": "step executed",
    }
    second_event = {**first_event, "eventId": "second"}
    logs_client = _LogsClient([{"events": [first_event]}, {"events": [first_event, second_event]}])
    retriever = CloudWatchLogRetriever(object(), logs_client, event_poll_timeout_seconds=1.0)

    events = retriever.get_execution_log_events(
        log_group_name="/aws/lambda/test",
        execution_arn="arn:execution",
        start_time_ms=1_000,
        wait_seconds=0,
    )

    assert events == [first_event, second_event]
    result = CloudWatchLogValidator().validate([{"match": {"message": "step executed"}, "count": 1}], events)
    assert not result.success
    assert "got 2" in result.errors[0]
    assert fake_clock.now == 1.0


def test_preserves_multiplicity_when_clients_omit_event_ids(fake_clock: _Clock) -> None:
    event = {"timestamp": 1_500, "ingestionTime": 2_000, "message": "same record"}
    logs_client = _LogsClient(
        [
            {"events": [event]},
            {"events": [event, dict(event)]},
            {"events": [event]},
            {"events": []},
        ]
    )
    retriever = CloudWatchLogRetriever(object(), logs_client, event_poll_timeout_seconds=3.0)

    events = retriever.get_execution_log_events(
        log_group_name="/aws/lambda/test",
        execution_arn="arn:execution",
        start_time_ms=1_000,
        wait_seconds=0,
    )

    assert events == [event, event]
    assert fake_clock.now == 3.0


@pytest.mark.parametrize(("timeout", "expected_sleeps"), [(0.0, []), (0.25, [0.25]), (1.25, [1.0, 0.25])])
def test_polling_respects_zero_and_fractional_timeout(
    fake_clock: _Clock, timeout: float, expected_sleeps: list[float]
) -> None:
    logs_client = _LogsClient([{"events": []}] * (len(expected_sleeps) + 1))
    retriever = CloudWatchLogRetriever(object(), logs_client, event_poll_timeout_seconds=timeout)

    events = retriever.get_execution_log_events(
        log_group_name="/aws/lambda/test",
        execution_arn="arn:execution",
        start_time_ms=1_000,
        wait_seconds=0,
    )

    assert events == []
    assert fake_clock.now == timeout
    assert fake_clock.sleeps == expected_sleeps
    assert len(logs_client.filter_log_events_calls) == len(expected_sleeps) + 1


def test_poll_interval_override_is_honored(fake_clock: _Clock) -> None:
    logs_client = _LogsClient([{"events": []}] * 4)
    retriever = CloudWatchLogRetriever(
        object(), logs_client, event_poll_timeout_seconds=1.25, event_poll_interval_seconds=0.5
    )

    retriever.get_execution_log_events(
        log_group_name="/aws/lambda/test",
        execution_arn="arn:execution",
        start_time_ms=1_000,
        wait_seconds=0,
    )

    assert fake_clock.sleeps == [0.5, 0.5, 0.25]
    assert fake_clock.now == 1.25
    assert len(logs_client.filter_log_events_calls) == 4


def test_completion_check_uses_accumulated_events_after_settling(fake_clock: _Clock) -> None:
    event = {"eventId": "first", "message": "first"}
    logs_client = _LogsClient([{"events": [event]}] + [{"events": []}] * 20)
    retriever = CloudWatchLogRetriever(object(), logs_client)

    events = retriever.get_execution_log_events(
        log_group_name="/aws/lambda/test",
        execution_arn="arn:execution",
        start_time_ms=1_000,
        wait_seconds=0,
        completion_check=lambda observed: observed == [event],
    )

    assert events == [event]
    assert fake_clock.now == 20.0
    assert len(logs_client.filter_log_events_calls) == 21


def test_new_events_restart_settling_window(fake_clock: _Clock) -> None:
    first_event = {"eventId": "first", "message": "first"}
    second_event = {"eventId": "second", "message": "second"}
    logs_client = _LogsClient([{"events": [first_event]}] * 15 + [{"events": [first_event, second_event]}] * 11)
    retriever = CloudWatchLogRetriever(object(), logs_client)

    events = retriever.get_execution_log_events(
        log_group_name="/aws/lambda/test",
        execution_arn="arn:execution",
        start_time_ms=1_000,
        wait_seconds=0,
        completion_check=bool,
    )

    assert events == [first_event, second_event]
    assert fake_clock.now == 25.0
    assert len(logs_client.filter_log_events_calls) == 26


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
        event_poll_timeout_seconds=1.0,
    )

    with pytest.raises(CloudWatchLogError, match="filter-log-events failed"):
        retriever.get_execution_log_events(
            log_group_name="/aws/lambda/test",
            execution_arn="arn:execution",
            start_time_ms=1_000,
            end_time_ms=2_000,
            wait_seconds=0,
        )


# endregion


# region Validator (structured field matchers + before/after anchors)


def _events(*messages: str) -> list[dict]:
    """Build fake CloudWatch events with increasing timestamps."""
    return [{"message": msg, "timestamp": 1000 + i, "ingestionTime": 2000 + i} for i, msg in enumerate(messages)]


def _validator() -> CloudWatchLogValidator:
    return CloudWatchLogValidator()


# --- Entry parsing ---


def test_from_dict_requires_match_mapping():
    with pytest.raises(TypeError):
        LogExpectation.from_dict({"match": "not-a-dict"})
    with pytest.raises(TypeError):
        LogExpectation.from_dict({"match": {}})
    with pytest.raises(KeyError):
        LogExpectation.from_dict({"count": 1})


def test_from_dict_anchor_normalization():
    exp = LogExpectation.from_dict({"match": {"message": "b"}, "after": {"message": "a"}})
    assert exp.after == ({"message": "a"},)
    exp = LogExpectation.from_dict({"match": {"message": "c"}, "after": [{"message": "a"}, {"message": "b"}]})
    assert len(exp.after) == 2
    with pytest.raises(TypeError):
        LogExpectation.from_dict({"match": {"message": "b"}, "after": "a-plain-string"})


def test_invalid_entry_reports_error():
    result = _validator().validate([{"count": 1}], _events("x"))
    assert not result.success
    assert "invalid entry" in result.errors[0]


# --- Field extraction + exact matching ---


def test_plain_line_exposes_only_message_exact():
    v = _validator()
    assert v.validate([{"match": {"message": "hello world"}}], _events("hello world")).success
    # exact: substring must NOT match (the n=1 vs n=12 hazard)
    assert not v.validate([{"match": {"message": "attempt-start n=1"}}], _events("attempt-start n=12")).success
    # whitespace stripped
    assert v.validate([{"match": {"message": "hello"}}], _events("  hello\n")).success


def test_json_line_exposes_fields():
    line = json.dumps({"message": "Greeting step completed", "level": "INFO", "operationId": "abc123", "attempt": 1})
    v = _validator()
    assert v.validate(
        [{"match": {"message": "Greeting step completed", "operationId": "abc123", "attempt": 1, "level": "INFO"}}],
        _events(line),
    ).success
    # wrong field value fails
    assert not v.validate([{"match": {"operationId": "other"}}], _events(line)).success
    # missing field fails
    assert not v.validate([{"match": {"nonexistent": "x"}}], _events(line)).success


def test_regex_value_matching():
    v = _validator()
    assert v.validate([{"match": {"message": "/attempt-start n=\\d+/"}}], _events("attempt-start n=7")).success
    assert v.validate([{"match": {"message": "/ERROR/"}, "count": 0}], _events("all fine")).success
    assert not v.validate([{"match": {"message": "/ERROR/"}, "count": 0}], _events("ERROR boom")).success


def test_retrieval_pipeline_contract_top_level_arn_required(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Retrieval-level contract: the execution-scoped CloudWatch filter
    matches TOP-LEVEL ARN fields, so plugin records must be raw top-level
    JSON. A plugin JSON object nested inside a runtime envelope's message
    string is never retrieved — which is why the Node.js handlers emit via
    process.stdout.write instead of console.log."""
    arn = "arn:aws:lambda:us-west-2:1:function:f:$LATEST/durable-execution/x/e1"

    class _FilteringLogsClient:
        """Simulates CloudWatch's server-side JSON filter pattern."""

        def filter_log_events(self, **kwargs: Any) -> dict[str, Any]:
            assert "filterPattern" in kwargs  # retrieval must be execution-scoped
            events = [
                # raw top-level plugin record -> matched by the filter
                {
                    "message": json.dumps(
                        {"plugin": "CONFPLUGIN", "hook": "invocation-start", "first": True, "durableExecutionArn": arn}
                    ),
                    "timestamp": 1,
                },
                # plugin JSON nested in an envelope message -> NOT matched (no top-level ARN)
                {
                    "message": json.dumps(
                        {
                            "level": "INFO",
                            "message": json.dumps(
                                {
                                    "plugin": "CONFPLUGIN",
                                    "hook": "invocation-end",
                                    "status": "SUCCEEDED",
                                    "durableExecutionArn": arn,
                                }
                            ),
                        }
                    ),
                    "timestamp": 2,
                },
            ]
            kept = []
            for evt in events:
                try:
                    rec = json.loads(str(evt["message"]))
                except ValueError:
                    continue
                if isinstance(rec, dict) and arn in (str(rec.get("durableExecutionArn")), str(rec.get("executionArn"))):
                    kept.append(evt)
            return {"events": kept}

    monkeypatch.setattr(cloudwatch_module.time, "sleep", lambda _s: None)
    retriever = CloudWatchLogRetriever(
        cloudformation_client=object(),
        logs_client=_FilteringLogsClient(),
        event_poll_timeout_seconds=0.0,
    )
    events = retriever.get_execution_log_events(
        log_group_name="/aws/lambda/test", execution_arn=arn, start_time_ms=0, end_time_ms=10, wait_seconds=0
    )

    v = _validator()
    # the top-level record survives retrieval and matches
    assert v.validate([{"match": {"plugin": "CONFPLUGIN", "hook": "invocation-start", "first": True}}], events).success
    # the nested record was never retrieved: asserting it must fail
    assert not v.validate([{"match": {"plugin": "CONFPLUGIN", "hook": "invocation-end"}}], events).success


def test_plugin_json_printed_raw_is_assertable():
    """Runtimes that do not wrap stdout (println/print) yield the plugin
    JSON as the whole line."""
    line = json.dumps({"plugin": "CONFPLUGIN-A", "hook": "invocation-start", "first": True})
    v = _validator()
    assert v.validate(
        [{"match": {"plugin": "CONFPLUGIN-A", "hook": "invocation-start", "first": True}}], _events(line)
    ).success
    assert not v.validate([{"match": {"first": False}}], _events(line)).success


def test_non_json_message_string_stays_plain():
    envelope = json.dumps({"level": "INFO", "message": "CONFPLUGIN invocation-start first=true"})
    v = _validator()
    assert v.validate([{"match": {"message": "CONFPLUGIN invocation-start first=true"}}], _events(envelope)).success


# --- Cardinality (always global) ---


def test_default_requires_at_least_one_match():
    assert _validator().validate([{"match": {"message": "hit"}}], _events("hit")).success
    assert not _validator().validate([{"match": {"message": "miss"}}], _events("hit")).success


def test_exact_count_is_global():
    result = _validator().validate(
        [{"match": {"message": "a"}, "count": 1}, {"match": {"message": "b"}, "count": 1}],
        _events("a", "b", "a"),
    )
    assert not result.success


def test_min_and_max_count_constraints():
    events = _events("x", "x", "x")
    assert _validator().validate([{"match": {"message": "x"}, "min_count": 2, "max_count": 3}], events).success
    assert not _validator().validate([{"match": {"message": "x"}, "min_count": 4}], events).success
    assert not _validator().validate([{"match": {"message": "x"}, "max_count": 2}], events).success


def test_entries_without_anchors_assert_counts_only():
    result = _validator().validate(
        [{"match": {"message": "first"}, "count": 1}, {"match": {"message": "second"}, "count": 1}],
        _events("second", "first"),
    )
    assert result.success, result.errors


# --- Ordering via before/after anchors ---


def test_after_satisfied_in_emission_order():
    spec = [
        {"match": {"message": "start"}, "count": 1},
        {"match": {"message": "end"}, "count": 1, "after": {"message": "start"}},
    ]
    assert _validator().validate(spec, _events("start", "end")).success


def test_after_violated_reports_out_of_order():
    spec = [
        {"match": {"message": "start"}, "count": 1},
        {"match": {"message": "end"}, "count": 1, "after": {"message": "start"}},
    ]
    result = _validator().validate(spec, _events("end", "start"))
    assert not result.success
    assert any("out of order" in e for e in result.errors)


def test_before_mirror_semantics():
    spec = [
        {"match": {"message": "start"}, "count": 1, "before": {"message": "end"}},
        {"match": {"message": "end"}, "count": 1},
    ]
    assert _validator().validate(spec, _events("start", "end")).success
    assert not _validator().validate(spec, _events("end", "start")).success


def test_after_multiple_anchors_all_required():
    spec = [
        {"match": {"message": "c"}, "count": 1, "after": [{"message": "a"}, {"message": "b"}]},
    ]
    assert _validator().validate(spec, _events("a", "b", "c")).success
    assert _validator().validate(spec, _events("b", "a", "c")).success
    assert not _validator().validate(spec, _events("a", "c", "b")).success


def test_after_requires_all_own_matches_after_anchor():
    spec = [
        {"match": {"message": "end"}, "count": 2, "after": {"message": "start"}},
    ]
    assert not _validator().validate(spec, _events("end", "start", "end")).success


def test_same_timestamp_ties_are_concurrent():
    events = [
        {"message": "end", "timestamp": 5, "ingestionTime": 0},
        {"message": "start", "timestamp": 5, "ingestionTime": 0},
    ]
    spec = [
        {"match": {"message": "start"}, "count": 1},
        {"match": {"message": "end"}, "count": 1, "after": {"message": "start"}},
    ]
    assert _validator().validate(spec, events).success


def test_zero_match_anchor_is_an_error():
    result = _validator().validate(
        [{"match": {"message": "end"}, "count": 1, "after": {"message": "nonexistent"}}],
        _events("end"),
    )
    assert not result.success
    assert any("matched no log records" in e for e in result.errors)


def test_anchor_on_json_field():
    lines = [
        json.dumps({"message": "hook fired", "operationId": "op1"}),
        json.dumps({"message": "hook done", "operationId": "op1"}),
    ]
    spec = [
        {"match": {"message": "hook done"}, "count": 1, "after": {"operationId": "op1", "message": "hook fired"}},
    ]
    assert _validator().validate(spec, _events(*lines)).success


# --- Event sorting ---


def test_events_sorted_by_timestamp_before_matching():
    events = [
        {"message": "second", "timestamp": 2, "ingestionTime": 0},
        {"message": "first", "timestamp": 1, "ingestionTime": 0},
    ]
    spec = [
        {"match": {"message": "first"}, "count": 1},
        {"match": {"message": "second"}, "count": 1, "after": {"message": "first"}},
    ]
    assert _validator().validate(spec, events).success


def test_equal_timestamps_tiebreak_by_ingestion_time():
    events = [
        {"message": "second", "timestamp": 5, "ingestionTime": 20},
        {"message": "first", "timestamp": 5, "ingestionTime": 10},
    ]
    spec = [
        {"match": {"message": "first"}, "count": 1},
        {"match": {"message": "second"}, "count": 1, "after": {"message": "first"}},
    ]
    assert _validator().validate(spec, events).success


def test_string_timestamps_sort_correctly():
    events = [
        {"message": "second", "timestamp": "2026-07-23 22:32:34.000"},
        {"message": "first", "timestamp": "2026-07-23 22:32:33.000"},
    ]
    spec = [
        {"match": {"message": "first"}, "count": 1},
        {"match": {"message": "second"}, "count": 1, "after": {"message": "first"}},
    ]
    assert _validator().validate(spec, events).success


# endregion
