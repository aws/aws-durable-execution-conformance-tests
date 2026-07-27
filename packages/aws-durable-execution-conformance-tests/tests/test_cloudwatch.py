# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Tests for CloudWatch log retrieval and validation (v3 schema)."""

from __future__ import annotations

import json
from typing import Any

import pytest

import aws_durable_execution_conformance_tests.cloudwatch as cloudwatch_module
from aws_durable_execution_conformance_tests.cloudwatch import (
    CloudWatchLogError,
    CloudWatchLogRetriever,
    CloudWatchLogValidator,
    LogExpectation,
)

# region Retriever


class _LogsClient:
    def __init__(self, pages: list[dict[str, Any]]) -> None:
        self._pages = pages
        self._i = 0
        self.filter_calls: list[dict[str, Any]] = []

    def filter_log_events(self, **kwargs: Any) -> dict[str, Any]:
        self.filter_calls.append(kwargs)
        page = self._pages[min(self._i, len(self._pages) - 1)]
        self._i += 1
        return page


def _expire_poll_deadline(monkeypatch: pytest.MonkeyPatch) -> None:
    """Advance time.monotonic() by 100s per call so the bounded ingestion
    polling window expires right after the first fetch."""
    clock = {"t": 0.0}

    def _monotonic() -> float:
        clock["t"] += 100.0
        return clock["t"]

    monkeypatch.setattr(cloudwatch_module.time, "monotonic", _monotonic)


def test_execution_log_events_keeps_unattributed_and_matching_records(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    execution_arn = "arn:aws:lambda:us-west-2:1:function:f:$LATEST/durable-execution/x/e1"
    other_arn = "arn:aws:lambda:us-west-2:1:function:f:$LATEST/durable-execution/x/e2"
    events = [
        {"message": "CONFPLUGIN invocation-start first=true", "timestamp": 1},
        {"message": json.dumps({"message": "enriched mine", "executionArn": execution_arn}), "timestamp": 2},
        {"message": json.dumps({"message": "enriched other", "durableExecutionArn": other_arn}), "timestamp": 3},
        {"message": json.dumps({"message": "no arn field", "level": "INFO"}), "timestamp": 4},
    ]
    logs_client = _LogsClient([{"events": events}])
    monkeypatch.setattr(cloudwatch_module.time, "sleep", lambda _s: None)
    _expire_poll_deadline(monkeypatch)
    retriever = CloudWatchLogRetriever(cloudformation_client=object(), logs_client=logs_client)

    result = retriever.get_execution_log_events(
        log_group_name="/aws/lambda/test",
        execution_arn=execution_arn,
        start_time_ms=1_000,
        end_time_ms=2_000,
        wait_seconds=0,
    )

    kept = [evt["message"] for evt in result]
    assert "CONFPLUGIN invocation-start first=true" in kept  # plain stdout kept
    assert any("enriched mine" in m for m in kept)  # own execution kept
    assert not any("enriched other" in m for m in kept)  # other execution dropped
    assert any("no arn field" in m for m in kept)  # unattributed JSON kept
    assert logs_client.filter_calls[0]["logGroupName"] == "/aws/lambda/test"
    assert logs_client.filter_calls[0]["startTime"] == 1_000


def test_execution_log_events_paginates(monkeypatch: pytest.MonkeyPatch) -> None:
    logs_client = _LogsClient(
        [
            {"events": [{"message": "a", "timestamp": 1}], "nextToken": "t1"},
            {"events": [{"message": "b", "timestamp": 2}]},
        ]
    )
    monkeypatch.setattr(cloudwatch_module.time, "sleep", lambda _s: None)
    _expire_poll_deadline(monkeypatch)
    retriever = CloudWatchLogRetriever(cloudformation_client=object(), logs_client=logs_client)

    result = retriever.get_execution_log_events(
        log_group_name="/aws/lambda/test",
        execution_arn="arn:whatever",
        start_time_ms=0,
        end_time_ms=10,
        wait_seconds=0,
    )

    assert [evt["message"] for evt in result] == ["a", "b"]
    assert logs_client.filter_calls[1]["nextToken"] == "t1"


def test_get_log_group_name_raises_on_empty_physical_id() -> None:
    class _Cfn:
        def describe_stack_resource(self, **kwargs):
            return {"StackResourceDetail": {"PhysicalResourceId": ""}}

    retriever = CloudWatchLogRetriever(cloudformation_client=_Cfn(), logs_client=object())
    with pytest.raises(CloudWatchLogError, match="Empty physical resource ID"):
        retriever.get_log_group_name("stack", "Fn")


# endregion


# region Validator (v3: structured matchers + before/after anchors)


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


def test_plugin_json_nested_in_runtime_envelope():
    """A plugin printing JSON through a wrapping runtime (Node.js JSON log
    format) nests its object as a string in the envelope's message field:
    inner fields must be merged and assertable."""
    inner = {"plugin": "CONFPLUGIN", "hook": "attempt-end", "n": 1, "outcome": "FAILED", "op": "abc123"}
    envelope = json.dumps(
        {"timestamp": "2026-07-27T18:00:00Z", "level": "INFO", "requestId": "r-1", "message": json.dumps(inner)}
    )
    v = _validator()
    assert v.validate(
        [{"match": {"plugin": "CONFPLUGIN", "hook": "attempt-end", "n": 1, "outcome": "FAILED", "op": "abc123"}}],
        _events(envelope),
    ).success
    # envelope fields still visible when not shadowed by inner keys
    assert v.validate([{"match": {"level": "INFO", "hook": "attempt-end"}}], _events(envelope)).success


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
