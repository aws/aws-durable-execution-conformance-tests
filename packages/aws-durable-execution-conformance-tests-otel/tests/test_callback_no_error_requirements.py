# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Requirement-oracle controls, not evidence of SDK/service scenario reachability."""

import json
from copy import deepcopy
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from unittest.mock import Mock

import pytest

from aws_durable_execution_conformance_tests.callback import CallbackAction, CallbackSender
from aws_durable_execution_conformance_tests.history import EventHistoryMatcher
from aws_durable_execution_conformance_tests.validate import discover_test_files, load_yaml_file
from aws_durable_execution_conformance_tests.variables import PlaceholderContext
from aws_durable_execution_conformance_tests_otel.extension import OtelExtension
from aws_durable_execution_conformance_tests_otel.model import Span, SpanLink, TelemetryQuery, Trace
from aws_durable_execution_conformance_tests_otel.polling import BackendFeatureDisparity, PollingBackend, PollingPolicy
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
        "FAILED_CALLBACK_OBSERVED": "observed",
        "BARRIER_ONE_CONTEXT": "barrier-one-context",
        "BARRIER_ONE_CALLBACK": "barrier-one-callback",
        "BARRIER_ONE_SUBMITTER": "barrier-one-submitter",
        "BARRIER_TWO_CONTEXT": "barrier-two-context",
        "BARRIER_TWO_CALLBACK": "barrier-two-callback",
        "BARRIER_TWO_SUBMITTER": "barrier-two-submitter",
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
        exception_event_count=0,
        service_name=SERVICE,
        attributes={"durable.execution.arn": EXECUTION_ARN, **attributes},
        links=tuple(SpanLink(TRACE_ID, f"{link:016x}") for link in links),
    )


def _unhandled_trace(view: str, context_status: str = "ERROR", callback_status: str = "ERROR") -> Trace:
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


def _trace(view: str, context_status: str = "ERROR", callback_status: str = "ERROR") -> Trace:
    """Oracle fixture: first failure is caught, then two barriers force replay."""
    initial = _unhandled_trace(view, context_status, callback_status)
    spans = []
    for span in initial.spans:
        if span.name == "Workflow":
            span = replace(
                span,
                end_time=START + timedelta(seconds=11),
                status="OK",
                attributes={**span.attributes, "durable.execution.status": "SUCCEEDED"},
            )
        elif span.name == "Invocation" and not span.attributes["durable.invocation.first"]:
            span = replace(span, status="OK", attributes={**span.attributes, "durable.invocation.status": "PENDING"})
        elif span.span_id == f"{5:016x}":
            span = replace(span, end_time=START + timedelta(seconds=3.5))
        elif span.span_id == f"{7:016x}":
            span = replace(span, end_time=START + timedelta(seconds=3.4))
        spans.append(span)
    begin: float
    for identifier, begin, end, status in [(11, 6, 8, "PENDING"), (12, 9, 11, "SUCCEEDED")]:
        spans.append(
            _span(
                identifier,
                "Invocation",
                begin,
                end,
                None,
                "OK",
                {
                    "durable.invocation.first": False,
                    "durable.invocation.status": status,
                },
            )
        )
    marker = _operation("observed", "STEP", "Step", "otel-failed-callback-observed")
    links = (1,) if view == "invocation" else (3,)
    spans.extend(
        [
            _span(
                20,
                "otel-failed-callback-observed",
                3.6,
                3.9,
                3 if view == "invocation" else 1,
                "OK",
                {**marker, "durable.operation.status": "SUCCEEDED", "durable.attempt.number": 1},
                links,
            ),
            _span(
                21,
                "otel-failed-callback-observed attempt 1",
                3.7,
                3.8,
                20,
                "OK",
                {**marker, "durable.attempt.number": 1, "durable.attempt.outcome": "SUCCEEDED"},
                links,
            ),
        ]
    )
    for word, base, creating, completing, begin, finish in [
        ("one", 30, 3, 11, 4.0, 6.0),
        ("two", 40, 11, 12, 7.0, 9.0),
    ]:
        name = f"otel-failed-callback-barrier-{word}"
        context = _operation(f"barrier-{word}-context", "CONTEXT", "WaitForCallback", name)
        callback = _operation(f"barrier-{word}-callback", "CALLBACK", "Callback", name + " create callback id")
        step = _operation(f"barrier-{word}-submitter", "STEP", "Step", name + " submitter")
        if view == "invocation":
            spans.extend(
                [
                    _span(
                        base,
                        name,
                        begin + 0.1,
                        begin + 0.9,
                        creating,
                        "UNSET",
                        {**context, "durable.operation.status": "STARTED"},
                        (1,),
                    ),
                    _span(
                        base + 1,
                        name,
                        finish + 0.1,
                        finish + 0.9,
                        completing,
                        "OK",
                        {**context, "durable.operation.status": "SUCCEEDED"},
                        (base, 1),
                    ),
                    _span(
                        base + 2,
                        name + " create callback id",
                        begin + 0.2,
                        begin + 0.8,
                        base,
                        "UNSET",
                        {**callback, "durable.operation.status": "STARTED"},
                        (1,),
                    ),
                    _span(
                        base + 3,
                        name + " create callback id",
                        finish + 0.2,
                        finish + 0.8,
                        base + 1,
                        "OK",
                        {**callback, "durable.operation.status": "SUCCEEDED"},
                        (base + 2, 1),
                    ),
                ]
            )
        else:
            spans.extend(
                [
                    _span(
                        base + 1,
                        name,
                        begin + 0.1,
                        finish + 0.9,
                        1,
                        "OK",
                        {**context, "durable.operation.status": "SUCCEEDED"},
                        (completing,),
                    ),
                    _span(
                        base + 3,
                        name + " create callback id",
                        begin + 0.2,
                        finish + 0.8,
                        base + 1,
                        "OK",
                        {**callback, "durable.operation.status": "SUCCEEDED"},
                        (completing,),
                    ),
                ]
            )
        links = (1,) if view == "invocation" else (creating,)
        spans.extend(
            [
                _span(
                    base + 4,
                    name + " submitter",
                    begin + 0.3,
                    begin + 0.6,
                    base if view == "invocation" else base + 1,
                    "OK",
                    {**step, "durable.operation.status": "SUCCEEDED", "durable.attempt.number": 1},
                    links,
                ),
                _span(
                    base + 5,
                    name + " submitter attempt 1",
                    begin + 0.4,
                    begin + 0.5,
                    base + 4,
                    "OK",
                    {**step, "durable.attempt.number": 1, "durable.attempt.outcome": "SUCCEEDED"},
                    links,
                ),
            ]
        )
    return replace(initial, spans=tuple(spans))


def _observation_replay_segments(phases: tuple[int, ...]) -> Trace:
    trace = _trace("invocation")
    original = next(span for span in trace.spans if span.name == "otel-failed-callback-observed")
    replays = tuple(
        replace(
            original,
            span_id=f"{80 + phase:016x}",
            parent_span_id=f"{phase + 8:016x}",
            start_time=START + timedelta(seconds=(phase - 1) * 3 + 0.2),
            end_time=START + timedelta(seconds=(phase - 1) * 3 + 0.4),
            links=(SpanLink(TRACE_ID, original.span_id), SpanLink(TRACE_ID, f"{1:016x}")),
        )
        for phase in phases
    )
    return replace(trace, spans=(*trace.spans, *replays))


@pytest.mark.parametrize("phases", [(3,), (4,), (3, 4)])
def test_case25_allows_invocation_view_observation_replay_segments(phases: tuple[int, ...]) -> None:
    trace = _observation_replay_segments(phases)
    assert validate_trace(trace, _assertions("invocation"), _query()) == []


@pytest.mark.parametrize(
    "corruption",
    [
        "extra-attempt",
        "late-attempt",
        "callback-duplicate",
        "wrong-link",
        "wrong-phase",
        "changed-id",
        "duplicate-segment",
    ],
)
def test_case25_replay_segments_preserve_attempt_and_callback_uniqueness(corruption: str) -> None:
    trace = _observation_replay_segments((3, 4))
    spans = list(trace.spans)
    replay = next(span for span in spans if span.span_id == f"{83:016x}")
    attempt = next(span for span in spans if span.name == "otel-failed-callback-observed attempt 1")
    if corruption in {"extra-attempt", "late-attempt"}:
        late = replace(
            attempt,
            span_id=f"{85:016x}",
            parent_span_id=replay.span_id,
            start_time=replay.start_time,
            end_time=replay.end_time,
        )
        if corruption == "late-attempt":
            spans.remove(attempt)
        spans.append(late)
    elif corruption == "callback-duplicate":
        spans.append(next(span for span in spans if span.span_id == f"{7:016x}"))
    elif corruption == "wrong-link":
        spans[spans.index(replay)] = replace(replay, links=(SpanLink(TRACE_ID, f"{1:016x}"),))
    elif corruption == "wrong-phase":
        later = next(span for span in spans if span.span_id == f"{84:016x}")
        spans[spans.index(later)] = replace(
            later, parent_span_id=replay.parent_span_id, start_time=replay.start_time, end_time=replay.end_time
        )
    elif corruption == "changed-id":
        spans[spans.index(replay)] = replace(
            replay, attributes=dict(replay.attributes) | {"durable.operation.id": "different"}
        )
    else:
        spans.append(replay)
    assert validate_trace(replace(trace, spans=tuple(spans)), _assertions("invocation"), _query())


def test_case25_execution_view_still_exports_observation_only_once() -> None:
    trace = _trace("execution")
    observed = next(span for span in trace.spans if span.name == "otel-failed-callback-observed")
    errors = validate_trace(replace(trace, spans=(*trace.spans, observed)), _assertions("execution"), _query())
    assert any("must select exactly one" in error for error in errors), errors


@pytest.mark.parametrize("step_id,parent_id,start", [(8, 5, 3.2), (34, 31, 6.3), (44, 41, 9.3)])
def test_case25_allows_completed_submitter_replay_without_another_attempt(
    step_id: int, parent_id: int, start: float
) -> None:
    trace = _observation_replay_segments((3, 4))
    original = next(span for span in trace.spans if span.span_id == f"{step_id:016x}")
    replay = replace(
        original,
        span_id=f"{100 + step_id:016x}",
        parent_span_id=f"{parent_id:016x}",
        start_time=START + timedelta(seconds=start),
        end_time=START + timedelta(seconds=start + 0.1),
        links=(SpanLink(TRACE_ID, original.span_id), SpanLink(TRACE_ID, f"{1:016x}")),
    )
    replayed = replace(trace, spans=(*trace.spans, replay))
    assert validate_trace(replayed, _assertions("invocation"), _query()) == []
    attempt = next(
        span
        for span in trace.spans
        if span.parent_span_id == original.span_id and "durable.attempt.outcome" in span.attributes
    )
    repeated_attempt = replace(
        attempt,
        span_id=f"{200 + step_id:016x}",
        parent_span_id=replay.span_id,
        start_time=replay.start_time,
        end_time=replay.end_time,
    )
    assert validate_trace(
        replace(replayed, spans=(*replayed.spans, repeated_attempt)), _assertions("invocation"), _query()
    )


@pytest.mark.parametrize("view", ["invocation", "execution"])
def test_case25_distinguishes_all_generic_callback_and_submitter_names(view: str) -> None:
    trace = _trace(view)
    spans = []
    for span in trace.spans:
        kind = span.attributes.get("durable.operation.type")
        name = span.attributes.get("durable.operation.name", "")
        if kind == "CALLBACK":
            span = replace(span, name="CALLBACK")
        elif kind == "STEP" and name.endswith(" submitter"):
            span = replace(span, name="STEP attempt 1" if "durable.attempt.outcome" in span.attributes else "STEP")
        spans.append(span)
    assert validate_trace(replace(trace, spans=tuple(spans)), _assertions(view), _query()) == []


def test_case11_barrier_submitter_allows_replay_but_not_another_user_attempt() -> None:
    # Reuse the callback barrier fixture to isolate the two case-11 submitter assertions.
    trace = _trace("invocation")
    kept = {f"{number:016x}" for number in (1, 2, 3, 11, 30, 31, 34, 35)}
    old_name, new_name = "otel-failed-callback-barrier-one", "otel-invoke-replay-barrier"
    spans = tuple(
        replace(
            span,
            name=span.name.replace(old_name, new_name),
            attributes={
                key: value.replace(old_name, new_name) if isinstance(value, str) else value
                for key, value in span.attributes.items()
            },
        )
        for span in trace.spans
        if span.span_id in kept
    )
    original = next(span for span in spans if span.span_id == f"{34:016x}")
    replay = replace(
        original,
        span_id=f"{134:016x}",
        parent_span_id=f"{31:016x}",
        start_time=START + timedelta(seconds=6.3),
        end_time=START + timedelta(seconds=6.4),
        links=(SpanLink(TRACE_ID, original.span_id), SpanLink(TRACE_ID, f"{1:016x}")),
    )
    context = PlaceholderContext()
    for name, value in {
        "EXECUTION_ARN": EXECUTION_ARN,
        "SERVICE_NAME": SERVICE,
        "INVOKE_BARRIER_CONTEXT": "barrier-one-context",
        "INVOKE_BARRIER_SUBMITTER": "barrier-one-submitter",
    }.items():
        context.bind(name, value)
    assertions = {
        "span_assertions": context.substitute(
            [
                item
                for item in _requirement("invocation", 11)["TelemetryAssertions"]["span_assertions"]
                if item["expect"].get("attributes", {}).get("durable.operation.id") == "${INVOKE_BARRIER_SUBMITTER}"
            ]
        )
    }
    replayed = replace(trace, spans=(*spans, replay))
    assert validate_trace(replayed, assertions, _query()) == []
    attempt = next(span for span in spans if span.span_id == f"{35:016x}")
    repeated = replace(
        attempt,
        span_id=f"{135:016x}",
        parent_span_id=replay.span_id,
        start_time=replay.start_time,
        end_time=replay.end_time,
    )
    assert validate_trace(replace(replayed, spans=(*replayed.spans, repeated)), assertions, _query())


def _query() -> TelemetryQuery:
    return TelemetryQuery(EXECUTION_ARN, SERVICE, START, START + timedelta(seconds=11))


@pytest.mark.parametrize("view", ["invocation", "execution"])
def test_case25_requires_error_for_callback_without_error_details(view: str) -> None:
    assert validate_trace(_trace(view), _assertions(view), _query()) == []


@pytest.mark.parametrize("view", ["invocation", "execution"])
def test_case25_requires_successful_workflow_despite_failed_callback(view: str) -> None:
    trace = _trace(view)
    spans = tuple(
        replace(span, status="ERROR", attributes={**span.attributes, "durable.execution.status": "FAILED"})
        if span.name == "Workflow"
        else span
        for span in trace.spans
    )
    assert validate_trace(replace(trace, spans=spans), _assertions(view), _query())


@pytest.mark.parametrize("view", ["invocation", "execution"])
def test_case25_caught_failure_does_not_fail_its_invocation(view: str) -> None:
    trace = _trace(view)
    spans = tuple(
        replace(span, status="ERROR", attributes={**span.attributes, "durable.invocation.status": "FAILED"})
        if span.span_id == f"{3:016x}"
        else span
        for span in trace.spans
    )
    assert validate_trace(replace(trace, spans=spans), _assertions(view), _query())


@pytest.mark.parametrize("view", ["invocation", "execution"])
@pytest.mark.parametrize("corruption", ["missing-marker", "repeated-marker", "missing-final-invocation"])
def test_case25_requires_observation_once_and_both_later_replays(view: str, corruption: str) -> None:
    trace = _trace(view)
    marker = next(span for span in trace.spans if span.span_id == f"{21:016x}")
    if corruption == "missing-marker":
        spans = tuple(span for span in trace.spans if span is not marker)
    elif corruption == "repeated-marker":
        spans = (*trace.spans, marker)
    else:
        spans = tuple(span for span in trace.spans if span.span_id != f"{12:016x}")
    assert validate_trace(replace(trace, spans=spans), _assertions(view), _query())


@pytest.mark.parametrize("view", ["invocation", "execution"])
def test_case25_first_failed_leaf_cannot_be_delayed_until_later_replay(view: str) -> None:
    trace = _trace(view)
    spans = []
    for span in trace.spans:
        if span.span_id in {f"{5:016x}", f"{7:016x}"}:
            span = replace(
                span,
                start_time=span.start_time + timedelta(seconds=3),
                end_time=span.end_time + timedelta(seconds=3),
                links=span.links if view == "invocation" else (SpanLink(TRACE_ID, f"{11:016x}"),),
            )
            if view == "invocation" and span.span_id == f"{5:016x}":
                span = replace(span, parent_span_id=f"{11:016x}")
        spans.append(span)
    assert validate_trace(replace(trace, spans=tuple(spans)), _assertions(view), _query())


def test_case25_execution_does_not_order_backend_end_against_observation_clock() -> None:
    trace = _trace("execution")
    spans = tuple(
        replace(span, end_time=START + timedelta(seconds=4.3 if span.span_id == f"{5:016x}" else 4.2))
        if span.span_id in {f"{5:016x}", f"{7:016x}"}
        else span
        for span in trace.spans
    )
    # The logical failure is linked to invocation2. Its backend end can be
    # ahead of the observation attempt's SDK clock without changing that fact.
    assert validate_trace(replace(trace, spans=spans), _assertions("execution"), _query()) == []
    wrong_phase = tuple(
        replace(span, links=(SpanLink(TRACE_ID, f"{11:016x}"),)) if span.span_id in {f"{5:016x}", f"{7:016x}"} else span
        for span in spans
    )
    assert validate_trace(replace(trace, spans=wrong_phase), _assertions("execution"), _query())


def test_case25_initial_callback_scope_cannot_move_to_resumed_invocation() -> None:
    trace = _trace("invocation")
    initial_scope = {f"{number:016x}" for number in (4, 6, 8, 9)}
    spans = tuple(
        replace(
            span,
            parent_span_id=f"{3:016x}" if span.span_id == f"{4:016x}" else span.parent_span_id,
            start_time=START + timedelta(seconds=3 + (span.start_time - START).total_seconds() * 0.04),
            end_time=START + timedelta(seconds=3 + (span.end_time - START).total_seconds() * 0.04),
        )
        if span.span_id in initial_scope
        else span
        for span in trace.spans
    )
    errors = validate_trace(replace(trace, spans=spans), _assertions("invocation"), _query())
    assert any("parent.$occurrence" in error for error in errors), errors


@pytest.mark.parametrize("view", ["invocation", "execution"])
def test_case25_matches_independent_public_runner_history(view: str) -> None:
    # Sanitized producer history, not derived from the requirement under test.
    history = json.loads((Path(__file__).parent / "fixtures/caught_callback_failure_replay_history.json").read_text())
    requirement = _requirement(view)
    assert EventHistoryMatcher().match(requirement["ExpectedExecutionHistory"], history["Events"]).success
    assert requirement["ExpectedResult"] == {"ExecutionStatus": "SUCCEEDED", "Result": "caught/one/two"}
    assert [action["Operation"] for action in requirement["CallbackActions"]] == ["failure", "success", "success"]
    assert all(action["AfterInvocationCompleted"] is True for action in requirement["CallbackActions"])
    assert [action.get("Payload") for action in requirement["CallbackActions"]] == [None, "one", "two"]
    for event_id in [6, 9, 10, 15, 22, 25, 26]:
        incomplete = [event for event in history["Events"] if event["EventId"] != event_id]
        assert not EventHistoryMatcher().match(requirement["ExpectedExecutionHistory"], incomplete).success
    wrong_parent = deepcopy(history["Events"])
    wrong_parent[6]["ParentId"] = "unrelated-context"
    assert not EventHistoryMatcher().match(requirement["ExpectedExecutionHistory"], wrong_parent).success


@pytest.mark.parametrize("view", ["invocation", "execution"])
@pytest.mark.parametrize("callback_status", ["OK", "UNSET"])
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
    assert validate_trace(_unhandled_trace(view, callback_status="ERROR"), _assertions(view, 17), _query()) == []
    assert validate_trace(_unhandled_trace(view, callback_status="UNSET"), _assertions(view, 17), _query())


@pytest.mark.parametrize("view", ["invocation", "execution"])
@pytest.mark.parametrize("callback_status", ["OK", "UNSET"])
def test_case25_error_status_cannot_be_bypassed_by_xray_unset_disparity(view: str, callback_status: str) -> None:
    assert validate_trace(
        _trace(view, callback_status=callback_status),
        _assertions(view),
        _query(),
        feature_disparities=(BackendFeatureDisparity.UNSET_STATUS, BackendFeatureDisparity.EXCEPTION_EVENTS),
    )


@pytest.mark.parametrize("view", ["invocation", "execution"])
@pytest.mark.parametrize("exception_count", [0, 1, None])
@pytest.mark.parametrize("feature_disparities", [(), (BackendFeatureDisparity.EXCEPTION_EVENTS,)])
def test_case25_accepts_optional_callback_exception_events(
    view: str, exception_count: int | None, feature_disparities: tuple[BackendFeatureDisparity, ...]
) -> None:
    trace = _trace(view)
    spans = tuple(
        replace(span, exception_event_count=exception_count)
        if span.attributes.get("durable.operation.id") == "callback"
        and span.attributes.get("durable.operation.status") == "FAILED"
        else span
        for span in trace.spans
    )
    assert (
        validate_trace(
            replace(trace, spans=spans),
            _assertions(view),
            _query(),
            feature_disparities=feature_disparities,
        )
        == []
    )


class _DelayedCallbackBackend(PollingBackend):
    name = "delayed-callback"

    def __init__(self, initial: Trace, later: Trace) -> None:
        self.elapsed = 0.0
        self.lookups = 0
        self.initial = initial
        self.later = later
        super().__init__(monotonic=lambda: self.elapsed, sleep=self._sleep)

    def _sleep(self, seconds: float) -> None:
        self.elapsed += seconds

    def _lookup(self, query: TelemetryQuery) -> Trace:
        del query
        self.lookups += 1
        return self.initial if self.lookups <= 2 else self.later


@pytest.mark.parametrize("view", ["invocation", "execution"])
@pytest.mark.parametrize("late_record", ["stable", "duplicate", "exception"])
def test_case25_waits_for_stable_callback_evidence(view: str, late_record: str) -> None:
    initial = _trace(view)
    terminal = next(
        span
        for span in initial.spans
        if span.attributes.get("durable.operation.id") == "callback"
        and span.attributes.get("durable.operation.status") == "FAILED"
    )
    if late_record == "duplicate":
        later = replace(initial, spans=(*initial.spans, terminal))
    elif late_record == "exception":
        later = replace(
            initial,
            spans=tuple(replace(span, exception_event_count=1) if span is terminal else span for span in initial.spans),
        )
    else:
        later = initial
    backend = _DelayedCallbackBackend(initial, later)
    assertions = _assertions(view)
    actual = backend.find_trace(
        _query(),
        PollingPolicy(
            timeout_seconds=7,
            interval_seconds=1,
            max_attempts=8,
            quiescence_seconds=assertions.get("quiescence_seconds", 0),
        ),
        accept=lambda trace: not validate_trace(trace, assertions, _query()),
    )
    errors = validate_trace(actual, assertions, _query())
    if late_record == "stable":
        assert errors == []
        assert backend.elapsed == 5
    elif late_record == "exception":
        assert actual is later
        assert errors == []
        assert backend.elapsed == 7
    else:
        assert actual is later
        assert errors


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
    failed_context_events = [
        event for event in requirement["ExpectedExecutionHistory"] if event.get("Id") == "${FAILED_CALLBACK_CONTEXT}"
    ]
    assert len(failed_context_events) == 2
    assert all(event["Name"] == "otel-failed-callback" for event in failed_context_events)
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
