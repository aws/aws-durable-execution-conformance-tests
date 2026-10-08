# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Requirement-oracle controls, not evidence of SDK/service scenario reachability."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta
from unittest.mock import Mock

import pytest

from aws_durable_execution_conformance_tests.callback import CallbackAction, CallbackSender
from aws_durable_execution_conformance_tests.history import EventHistoryMatcher
from aws_durable_execution_conformance_tests.validate import discover_test_files, load_yaml_file
from aws_durable_execution_conformance_tests.variables import PlaceholderContext
from aws_durable_execution_conformance_tests_otel.extension import OtelExtension
from aws_durable_execution_conformance_tests_otel.model import Span, SpanLink, TelemetryQuery, Trace
from aws_durable_execution_conformance_tests_otel.polling import BackendFeatureDisparity
from aws_durable_execution_conformance_tests_otel.validators import validate_trace

TRACE_ID = "1" * 32
EXECUTION_ARN = "arn:test:no-error-callback"
SERVICE = "durable-execution-conformance"
START = datetime(2026, 10, 7, tzinfo=UTC)


def _requirement(view: str, case: int = 25) -> dict:
    suite = next(item for item in OtelExtension().requirement_suites() if item.name == f"otel-{view}")
    return load_yaml_file(discover_test_files(suite.root, suite="all")[f"otel-{view}-{case}"])


def _assertions(view: str, case: int = 25) -> dict:
    context = PlaceholderContext()
    for key, value in {
        "EXECUTION_ARN": EXECUTION_ARN,
        "SERVICE_NAME": SERVICE,
        "FAILED_CALLBACK_CONTEXT": "context",
        "FAILED_CALLBACK": "callback",
        "CALLBACK_SUBMITTER": "submitter",
    }.items():
        context.bind(key, value)
    return context.substitute(_requirement(view, case)["TelemetryAssertions"])


def _operation(identifier: str, operation_type: str, subtype: str, name: str, **extra: object) -> dict:
    return {
        "durable.operation.id": identifier,
        "durable.operation.type": operation_type,
        "durable.operation.subtype": subtype,
        "durable.operation.name": name,
        **extra,
    }


def _span(
    number: int, name: str, start: float, end: float, parent: int | None, status: str, attributes: dict, links=()
) -> Span:
    return Span(
        trace_id=TRACE_ID,
        span_id=f"{number:016x}",
        name=name,
        start_time=START + timedelta(seconds=start),
        end_time=START + timedelta(seconds=end),
        kind="INTERNAL",
        parent_span_id=f"{parent:016x}" if parent else "f" * 16,
        status=status,
        service_name=SERVICE,
        attributes={"durable.execution.arn": EXECUTION_ARN, **attributes},
        links=tuple(SpanLink(TRACE_ID, f"{link:016x}") for link in links),
    )


def _trace(view: str, context_status: str = "ERROR", callback_status: str = "UNSET") -> Trace:
    context = _operation("context", "CONTEXT", "WaitForCallback", "otel-failed-callback")
    callback = _operation("callback", "CALLBACK", "Callback", "otel-failed-callback create callback id")
    step = _operation("submitter", "STEP", "Step", "otel-failed-callback submitter")
    spans = [
        _span(1, "Workflow", 0, 5, None, "ERROR", {"durable.execution.status": "FAILED"}),
        _span(
            2,
            "Invocation",
            0,
            2,
            None,
            "OK",
            {"durable.invocation.first": True, "durable.invocation.status": "PENDING"},
        ),
        _span(
            3,
            "Invocation",
            3,
            5,
            None,
            "ERROR",
            {"durable.invocation.first": False, "durable.invocation.status": "FAILED"},
        ),
    ]
    if view == "invocation":
        spans.extend(
            [
                _span(
                    4,
                    "otel-failed-callback",
                    0.1,
                    1.9,
                    2,
                    "UNSET",
                    {**context, "durable.operation.status": "STARTED"},
                    (1,),
                ),
                _span(
                    5,
                    "otel-failed-callback",
                    3.1,
                    4.9,
                    3,
                    context_status,
                    {**context, "durable.operation.status": "FAILED"},
                    (4, 1),
                ),
                _span(
                    6,
                    "otel-failed-callback create callback id",
                    0.2,
                    1.5,
                    4,
                    "UNSET",
                    {**callback, "durable.operation.status": "STARTED"},
                    (1,),
                ),
                _span(
                    7,
                    "otel-failed-callback create callback id",
                    3.2,
                    4.8,
                    5,
                    callback_status,
                    {**callback, "durable.operation.status": "FAILED"},
                    (6, 1),
                ),
                _span(
                    8,
                    "otel-failed-callback submitter",
                    0.3,
                    0.9,
                    4,
                    "OK",
                    {**step, "durable.operation.status": "SUCCEEDED", "durable.attempt.number": 1},
                    (1,),
                ),
                _span(
                    9,
                    "otel-failed-callback submitter attempt 1",
                    0.4,
                    0.8,
                    8,
                    "OK",
                    {**step, "durable.attempt.number": 1, "durable.attempt.outcome": "SUCCEEDED"},
                    (1,),
                ),
            ]
        )
    else:
        spans.extend(
            [
                _span(
                    5,
                    "otel-failed-callback",
                    3.1,
                    4.9,
                    1,
                    context_status,
                    {**context, "durable.operation.status": "FAILED"},
                    (3,),
                ),
                _span(
                    7,
                    "otel-failed-callback create callback id",
                    3.2,
                    4.8,
                    5,
                    callback_status,
                    {**callback, "durable.operation.status": "FAILED"},
                    (3,),
                ),
                _span(
                    8,
                    "otel-failed-callback submitter",
                    0.3,
                    0.9,
                    5,
                    "OK",
                    {**step, "durable.operation.status": "SUCCEEDED", "durable.attempt.number": 1},
                    (2,),
                ),
                _span(
                    9,
                    "otel-failed-callback submitter attempt 1",
                    0.4,
                    0.8,
                    8,
                    "OK",
                    {**step, "durable.attempt.number": 1, "durable.attempt.outcome": "SUCCEEDED"},
                    (2,),
                ),
            ]
        )
    return Trace(TRACE_ID, tuple(spans))


def _query() -> TelemetryQuery:
    return TelemetryQuery(EXECUTION_ARN, SERVICE, START, START + timedelta(seconds=5))


@pytest.mark.parametrize("view", ["invocation", "execution"])
@pytest.mark.parametrize("context_status", ["UNSET", "ERROR"])
def test_case25_targets_callback_without_normalizing_sdk_aggregate_errors(view: str, context_status: str) -> None:
    assert validate_trace(_trace(view, context_status), _assertions(view), _query()) == []


@pytest.mark.parametrize("view", ["invocation", "execution"])
@pytest.mark.parametrize("callback_status", ["OK", "ERROR"])
def test_case25_rejects_wrong_terminal_callback_status_on_lossless_backend(view: str, callback_status: str) -> None:
    errors = validate_trace(_trace(view, callback_status=callback_status), _assertions(view), _query())
    assert any("status" in error for error in errors)


@pytest.mark.parametrize("view", ["invocation", "execution"])
@pytest.mark.parametrize("corruption", ["missing", "duplicate", "wrong-operation-status"])
def test_case25_requires_exact_failed_callback_leaf(view: str, corruption: str) -> None:
    trace = _trace(view)
    terminal = next(
        s
        for s in trace.spans
        if s.attributes.get("durable.operation.id") == "callback"
        and s.attributes.get("durable.operation.status") == "FAILED"
    )
    if corruption == "missing":
        spans = tuple(s for s in trace.spans if s is not terminal)
    elif corruption == "duplicate":
        spans = (*trace.spans, terminal)
    else:
        spans = tuple(
            replace(s, attributes={**s.attributes, "durable.operation.status": "SUCCEEDED"}) if s is terminal else s
            for s in trace.spans
        )
    assert validate_trace(replace(trace, spans=spans), _assertions(view), _query())


@pytest.mark.parametrize("view", ["invocation", "execution"])
def test_case17_retains_strict_rich_error_mapping(view: str) -> None:
    assert validate_trace(_trace(view, callback_status="ERROR"), _assertions(view, 17), _query()) == []
    assert validate_trace(_trace(view), _assertions(view, 17), _query())


@pytest.mark.parametrize("view", ["invocation", "execution"])
def test_case25_records_xray_unset_normalization_limit(view: str) -> None:
    # An X-Ray OK value cannot distinguish genuine OK from exporter-normalized UNSET.
    assert (
        validate_trace(
            _trace(view, callback_status="OK"),
            _assertions(view),
            _query(),
            feature_disparities=(BackendFeatureDisparity.UNSET_STATUS,),
        )
        == []
    )


@pytest.mark.parametrize("view", ["invocation", "execution"])
def test_case25_failure_request_omits_error_and_requires_failed_history_event(view: str) -> None:
    requirement = _requirement(view)
    assert "Payload" not in requirement["CallbackActions"][0]
    assert requirement["CallbackActions"][0]["AfterInvocationCompleted"] is True
    assert {"EventId": 6, "EventType": "InvocationCompleted"} in requirement["ExpectedExecutionHistory"]
    sender_client = Mock()
    CallbackSender(sender_client).send("callback-id", CallbackAction.from_dict(requirement["CallbackActions"][0]))
    sender_client.send_durable_execution_callback_failure.assert_called_once_with(CallbackId="callback-id")
    event = next(e for e in requirement["ExpectedExecutionHistory"] if e["EventType"] == "CallbackFailed")
    assert event["$absent"] == [
        ["CallbackFailedDetails", "Error", "Payload", key]
        for key in ["ErrorType", "ErrorMessage", "ErrorData", "StackTrace"]
    ]
    actual = {
        "EventId": 7,
        "EventType": "CallbackFailed",
        "SubType": "Callback",
        "Id": "callback",
        "ParentId": "context",
        "Name": "otel-failed-callback create callback id",
        "CallbackFailedDetails": {"Error": {"Payload": {}, "Truncated": False}},
    }
    assert EventHistoryMatcher().match([event], [actual]).success
    assert not EventHistoryMatcher().match([event], []).success
    for error in [None, {}, {"Payload": {"ErrorType": "RejectedError"}, "Truncated": False}]:
        assert not EventHistoryMatcher().match([event], [{**actual, "CallbackFailedDetails": {"Error": error}}]).success


@pytest.mark.parametrize("view", ["invocation", "execution"])
@pytest.mark.parametrize(
    "payload", [None, "{}", [], {"ErrorType": None}, {"ErrorMessage": "failure"}, {"Unexpected": 1}]
)
def test_case25_empty_service_error_payload_rejects_nonempty_or_nonobject(view: str, payload: object) -> None:
    expected = next(e for e in _requirement(view)["ExpectedExecutionHistory"] if e["EventType"] == "CallbackFailed")
    actual = {
        "EventId": 7,
        "EventType": "CallbackFailed",
        "SubType": "Callback",
        "Id": "cb",
        "ParentId": "ctx",
        "Name": "otel-failed-callback create callback id",
        "CallbackFailedDetails": {"Error": {"Payload": payload, "Truncated": False}},
    }
    assert not EventHistoryMatcher().match([expected], [actual]).success


@pytest.mark.parametrize("view", ["invocation", "execution"])
@pytest.mark.parametrize("error", [{"Payload": {}}, {"Payload": {}, "Truncated": True}])
def test_case25_requires_untruncated_empty_error_evidence(view: str, error: dict) -> None:
    expected = next(e for e in _requirement(view)["ExpectedExecutionHistory"] if e["EventType"] == "CallbackFailed")
    actual = {
        "EventId": 7,
        "EventType": "CallbackFailed",
        "SubType": "Callback",
        "Id": "cb",
        "ParentId": "ctx",
        "Name": "otel-failed-callback create callback id",
        "CallbackFailedDetails": {"Error": error},
    }
    assert not EventHistoryMatcher().match([expected], [actual]).success


@pytest.mark.parametrize("view", ["invocation", "execution"])
def test_case25_unnamed_failed_child_retains_strict_identity(view: str) -> None:
    requirement = _requirement(view)
    expected = next(event for event in requirement["ExpectedExecutionHistory"] if event["EventId"] == 7)
    assert "Name" not in expected
    assert all(
        event["Name"] == "otel-failed-callback"
        for event in requirement["ExpectedExecutionHistory"]
        if event["EventType"] in {"ContextStarted", "ContextFailed"}
    )
    actual = {
        "EventId": 7,
        "EventType": "CallbackFailed",
        "SubType": "Callback",
        "Id": "callback",
        "ParentId": "context",
        "CallbackFailedDetails": {"Error": {"Payload": {}, "Truncated": False}},
    }
    context = PlaceholderContext()
    context.bind("FAILED_CALLBACK", "callback")
    context.bind("FAILED_CALLBACK_CONTEXT", "context")
    matcher = EventHistoryMatcher(context)
    assert matcher.match([expected], [actual]).success
    for field, value in [
        ("Id", "wrong"),
        ("ParentId", "wrong"),
        ("SubType", "wrong"),
        ("EventType", "CallbackSucceeded"),
    ]:
        assert not matcher.match([expected], [{**actual, field: value}]).success


@pytest.mark.parametrize("view", ["invocation", "execution"])
@pytest.mark.parametrize("case", [25, 26])
def test_new_cases_cannot_pass_before_callbacks_appear(view: str, case: int) -> None:
    assert (
        not EventHistoryMatcher()
        .match(_requirement(view, case)["ExpectedExecutionHistory"], [{"EventId": 1, "EventType": "ExecutionStarted"}])
        .success
    )
