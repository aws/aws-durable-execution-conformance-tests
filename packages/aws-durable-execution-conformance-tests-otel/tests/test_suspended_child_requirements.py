# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Case 27 rejects loss of telemetry across a chained child's suspension."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from aws_durable_execution_conformance_tests.validate import discover_test_files, load_yaml_file
from aws_durable_execution_conformance_tests.variables import PlaceholderContext
from aws_durable_execution_conformance_tests_otel.extension import OtelExtension
from aws_durable_execution_conformance_tests_otel.model import (
    ExecutionTraceContext,
    Span,
    SpanLink,
    TelemetryQuery,
    Trace,
)
from aws_durable_execution_conformance_tests_otel.validators import validate_trace

from .requirement_assertions import assert_span_error, span_assertion_path

START = datetime(2026, 10, 23, tzinfo=UTC)
TRACE = "1" * 32
CALLER = "arn:caller"
CHILD = "arn:child"


def _assertions(view: str) -> dict:
    suite = next(s for s in OtelExtension().requirement_suites() if s.name == f"otel-{view}")
    requirement = load_yaml_file(discover_test_files(suite.root, suite="all")[f"otel-{view}-27"])
    context = PlaceholderContext()
    for key, value in {
        "EXECUTION_ARN": CALLER,
        "TARGET_EXECUTION_ARN": CHILD,
        "SERVICE_NAME": "test",
        "INVOKE1": "invoke",
    }.items():
        context.bind(key, value)
    return context.substitute(requirement["TelemetryAssertions"])


def _span(
    identifier: int, name: str, arn: str, start: float, end: float, parent: int, attrs: dict, status="OK", links=()
) -> Span:
    return Span(
        trace_id=TRACE,
        span_id=f"{identifier:016x}",
        name=name,
        start_time=START + timedelta(seconds=start),
        end_time=START + timedelta(seconds=end),
        parent_span_id=f"{parent:016x}",
        kind="INTERNAL",
        status=status,
        service_name="test",
        attributes={"durable.execution.arn": arn, **attrs},
        links=tuple(SpanLink(TRACE, f"{link:016x}") for link in links),
    )


def _trace(view: str) -> Trace:
    spans = [
        _span(1, "Workflow", CALLER, 0, 5, 999, {"durable.execution.status": "SUCCEEDED"}),
        _span(2, "Workflow", CHILD, 1, 4, 998, {"durable.execution.status": "SUCCEEDED"}),
    ]
    for identifier, arn, start, first in [
        (10, CALLER, 0, True),
        (11, CALLER, 4, False),
        (20, CHILD, 1, True),
        (21, CHILD, 3, False),
    ]:
        spans.append(
            _span(
                identifier,
                "Invocation",
                arn,
                start,
                start + 1,
                999 if arn == CALLER else 998,
                {
                    "durable.invocation.first": first,
                    "durable.invocation.status": "PENDING" if first else "SUCCEEDED",
                },
            )
        )
    for (
        identifier,
        name,
        arn,
        operation,
        subtype,
        workflow,
        first_invocation,
        last_invocation,
        operation_start,
        operation_end,
    ) in [
        (30, "otel-suspended-invoke", CALLER, "invoke", "ChainedInvoke", 1, 10, 11, 0.1, 4.5),
        (40, "otel-suspended-target-wait", CHILD, "wait", "Wait", 2, 20, 21, 1.1, 3.5),
    ]:
        attrs = {
            "durable.operation.id": operation,
            "durable.operation.name": name,
            "durable.operation.type": "CHAINED_INVOKE" if arn == CALLER else "WAIT",
            "durable.operation.subtype": subtype,
        }
        if view == "invocation":
            spans += [
                _span(
                    identifier,
                    name,
                    arn,
                    operation_start,
                    operation_start + 0.8,
                    first_invocation,
                    attrs | {"durable.operation.status": "STARTED"},
                    "UNSET",
                    (workflow,),
                ),
                _span(
                    identifier + 1,
                    name,
                    arn,
                    operation_end - 0.4,
                    operation_end,
                    last_invocation,
                    attrs | {"durable.operation.status": "SUCCEEDED"},
                    links=(identifier, workflow),
                ),
            ]
        else:
            spans.append(
                _span(
                    identifier,
                    name,
                    arn,
                    operation_start,
                    operation_end,
                    workflow,
                    attrs | {"durable.operation.status": "SUCCEEDED"},
                    links=(last_invocation,),
                )
            )
    return Trace(TRACE, tuple(spans))


def _validate(view: str, trace: Trace) -> list[str]:
    query = TelemetryQuery(
        CALLER,
        "test",
        START,
        START + timedelta(seconds=6),
        execution_trace_contexts={
            CHILD: ExecutionTraceContext(TRACE, f"{30:016x}", True),
        },
    )
    return validate_trace(trace, _assertions(view), query)


@pytest.mark.parametrize("view", ["invocation", "execution"])
def test_case27_accepts_child_suspension_and_resume(view: str) -> None:
    assert _validate(view, _trace(view)) == []


@pytest.mark.parametrize("view", ["invocation", "execution"])
@pytest.mark.parametrize(
    "mutation,diagnostic",
    [
        ("missing-resume", "select matched no spans"),
        ("echo-child", "select matched no spans"),
        ("wrong-first", "select matched no spans"),
        ("split-trace", "expect.same_trace_as: span 'Invocation' uses a different trace"),
        ("wrong-parent", "expect.parent.attributes.durable.execution.arn: expected 'arn:child'"),
        ("wrong-link", "expect.links: expected 2 item(s), found 1"),
        ("duplicate-wait", "select matched 2 spans; it must select exactly one"),
        ("missing-wait", "select matched no spans"),
        ("wrong-status", "expect.status: expected 'OK'"),
    ],
)
def test_case27_rejects_broken_child_continuations(view: str, mutation: str, diagnostic: str) -> None:
    trace = _trace(view)
    resumed = next(s for s in trace.spans if s.span_id == f"{21:016x}")
    initial = next(s for s in trace.spans if s.span_id == f"{20:016x}")
    wait = next(s for s in trace.spans if s.name == "otel-suspended-target-wait" and s.status == "OK")
    spans = list(trace.spans)
    if mutation == "missing-resume":
        spans.remove(resumed)
    elif mutation == "echo-child":
        spans = [s for s in spans if s != resumed and s.name != "otel-suspended-target-wait"]
        spans[spans.index(initial)] = replace(
            initial, attributes=dict(initial.attributes) | {"durable.invocation.status": "SUCCEEDED"}
        )
    elif mutation == "wrong-first":
        spans[spans.index(resumed)] = replace(
            resumed, attributes=dict(resumed.attributes) | {"durable.invocation.first": True}
        )
    elif mutation == "split-trace":
        spans[spans.index(resumed)] = replace(resumed, trace_id="2" * 32)
    elif mutation == "wrong-parent":
        spans[spans.index(wait)] = replace(wait, parent_span_id=f"{11 if view == 'invocation' else 1:016x}")
    elif mutation == "wrong-link":
        spans[spans.index(wait)] = replace(wait, links=(SpanLink(TRACE, f"{10:016x}"),))
    elif mutation == "duplicate-wait":
        spans.append(wait)
    elif mutation == "missing-wait":
        spans.remove(wait)
    else:
        spans[spans.index(wait)] = replace(wait, status="ERROR")
    errors = _validate(view, replace(trace, spans=tuple(spans)))
    assertions = _assertions(view)
    wait_selector: dict = {"name": "otel-suspended-target-wait"}
    if view == "invocation":
        wait_selector["attributes"] = {"durable.operation.status": "SUCCEEDED"}
    selector = wait_selector
    if mutation in {"missing-resume", "echo-child", "wrong-first", "split-trace"}:
        selector = {
            "name": "Invocation",
            "attributes": {"durable.execution.arn": CHILD, "durable.invocation.first": False},
        }
    if mutation == "wrong-link" and view == "execution":
        diagnostic = "expect.links[0].attributes.durable.execution.arn: expected 'arn:child'"
    assert_span_error(errors, assertions, selector, diagnostic)
    if mutation == "echo-child":
        # An echo child loses both its suspension and its durable wait.
        assert_span_error(
            errors,
            assertions,
            {"name": "Invocation", "attributes": {"durable.execution.arn": CHILD, "durable.invocation.first": True}},
            "select matched no spans",
        )
        assert_span_error(errors, assertions, wait_selector, "select matched no spans")


@pytest.mark.parametrize("view", ["invocation", "execution"])
def test_missing_resume_control_requires_its_own_selector_error(view: str, monkeypatch: pytest.MonkeyPatch) -> None:
    """Other missing-resume errors must not mask loss of the resumed selector check."""
    original_validate = validate_trace
    selector = {
        "name": "Invocation",
        "attributes": {"durable.execution.arn": CHILD, "durable.invocation.first": False},
    }

    def suppress_selector_error(trace: Trace, assertions: dict, query: TelemetryQuery) -> list[str]:
        errors = original_validate(trace, assertions, query)
        expected = f"{span_assertion_path(assertions, selector)}.select matched no spans"
        assert expected in errors, errors
        remaining = [error for error in errors if error != expected]
        assert any("expect.before matched no spans" in error for error in remaining), remaining
        return remaining

    monkeypatch.setattr(f"{__name__}.validate_trace", suppress_selector_error)
    with pytest.raises(AssertionError, match=r"Missing .*select matched no spans"):
        test_case27_rejects_broken_child_continuations(view, "missing-resume", "select matched no spans")


@pytest.mark.parametrize("view", ["invocation", "execution"])
def test_case27_does_not_compare_clocks_between_executions(view: str) -> None:
    trace = _trace(view)
    shifted = tuple(
        replace(s, start_time=s.start_time + timedelta(seconds=60), end_time=s.end_time + timedelta(seconds=60))
        if s.attributes["durable.execution.arn"] == CHILD
        else s
        for s in trace.spans
    )
    assert _validate(view, replace(trace, spans=shifted)) == []


@pytest.mark.parametrize("first", [True, False], ids=["initial", "resumed"])
def test_case27_rejects_both_wait_segments_in_one_child_invocation(first: bool) -> None:
    trace = _trace("invocation")
    parent = next(
        span
        for span in trace.spans
        if span.name == "Invocation"
        and span.attributes["durable.execution.arn"] == CHILD
        and span.attributes["durable.invocation.first"] is first
    )
    spans = list(trace.spans)
    waits = [span for span in spans if span.name == "otel-suspended-target-wait"]
    for index, wait in enumerate(waits):
        spans[spans.index(wait)] = replace(
            wait,
            parent_span_id=parent.span_id,
            start_time=parent.start_time + timedelta(seconds=0.1 + index * 0.4),
            end_time=parent.start_time + timedelta(seconds=0.3 + index * 0.4),
        )
    errors = _validate("invocation", replace(trace, spans=tuple(spans)))
    assert_span_error(
        errors,
        _assertions("invocation"),
        {
            "name": "otel-suspended-target-wait",
            "attributes": {"durable.operation.status": "SUCCEEDED" if first else "STARTED"},
        },
        "expect.parent.$occurrence: parent span is occurrence",
    )


def test_case27_rejects_changed_wait_operation_id_on_resume() -> None:
    trace = _trace("invocation")
    spans = tuple(
        replace(span, attributes=dict(span.attributes) | {"durable.operation.id": "different-wait"})
        if span.name == "otel-suspended-target-wait" and span.status == "OK"
        else span
        for span in trace.spans
    )
    errors = _validate("invocation", replace(trace, spans=spans))
    assert_span_error(
        errors,
        _assertions("invocation"),
        {"name": "otel-suspended-target-wait", "attributes": {"durable.operation.status": "SUCCEEDED"}},
        "expect.same_operation_as: spans have different durable.operation.id values",
    )


@pytest.mark.parametrize(
    "operation_name,service_end,wrong_invocation",
    [("otel-suspended-target-wait", 4.2, 20), ("otel-suspended-invoke", 5.2, 10)],
)
def test_case27_execution_preserves_external_phase_despite_workflow_clock_skew(
    operation_name: str, service_end: float, wrong_invocation: int
) -> None:
    trace = _trace("execution")
    spans = tuple(
        replace(span, end_time=START + timedelta(seconds=service_end)) if span.name == operation_name else span
        for span in trace.spans
    )
    assert _validate("execution", replace(trace, spans=spans)) == []
    wrong_phase = tuple(
        replace(span, links=(SpanLink(TRACE, f"{wrong_invocation:016x}"),)) if span.name == operation_name else span
        for span in spans
    )
    errors = _validate("execution", replace(trace, spans=wrong_phase))
    assert_span_error(
        errors,
        _assertions("execution"),
        {"name": operation_name},
        "expect.links[0].$occurrence: linked span is occurrence 1, expected 2",
    )
