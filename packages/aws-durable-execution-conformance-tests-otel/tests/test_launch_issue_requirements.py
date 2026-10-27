# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Regression evidence for user-visible OTel launch issues."""

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from aws_durable_execution_conformance_tests.history import EventHistoryMatcher
from aws_durable_execution_conformance_tests.validate import discover_test_files, load_yaml_file
from aws_durable_execution_conformance_tests.variables import PlaceholderContext
from aws_durable_execution_conformance_tests_otel.backends.collector import _merge_trace_files
from aws_durable_execution_conformance_tests_otel.extension import OtelExtension
from aws_durable_execution_conformance_tests_otel.model import Span, SpanLink, TelemetryQuery, Trace
from aws_durable_execution_conformance_tests_otel.polling import PollingBackend, PollingPolicy
from aws_durable_execution_conformance_tests_otel.validators import validate_trace

from .requirement_assertions import assert_span_error

TRACE_ID = "1" * 32
EXECUTION_ARN = "arn:test:execution:successful-replay"
SERVICE_NAME = "durable-execution-conformance"
START = datetime(2026, 10, 2, tzinfo=UTC)


def _requirement(suite: str, number: int) -> dict:
    root = next(item.root for item in OtelExtension().requirement_suites() if item.name == suite)
    files = discover_test_files(root, suite="all")
    return load_yaml_file(files[f"{suite}-{number}"])


def _replay_assertions(view: str = "execution") -> dict:
    placeholders = PlaceholderContext()
    for name, value in {
        "EXECUTION_ARN": EXECUTION_ARN,
        "SERVICE_NAME": SERVICE_NAME,
        "COMPLETED_STEP": "completed-before-wait",
        "REPLAY_WAIT": "resume-timer",
        "AFTER_STEP": "completed-after-wait",
    }.items():
        placeholders.bind(name, value)
    return placeholders.substitute(_requirement(f"otel-{view}", 21)["TelemetryAssertions"])


def _span(number: int, name: str, start: float, end: float, parent: int | None, attributes: dict, links=()) -> Span:
    return Span(
        trace_id=TRACE_ID,
        span_id=f"{number:016x}",
        name=name,
        start_time=START + timedelta(seconds=start),
        end_time=START + timedelta(seconds=end),
        parent_span_id=f"{parent:016x}" if parent is not None else "f" * 16,
        kind="INTERNAL",
        status="OK",
        service_name=SERVICE_NAME,
        attributes={"durable.execution.arn": EXECUTION_ARN, **attributes},
        links=tuple(SpanLink(trace_id=TRACE_ID, span_id=f"{link:016x}") for link in links),
    )


def _normal_successful_resume() -> Trace:
    """One completed step precedes suspension; only new work runs on resume."""
    before = {
        "durable.operation.id": "completed-before-wait",
        "durable.operation.type": "STEP",
        "durable.operation.subtype": "Step",
        "durable.operation.name": "otel-before-wait",
    }
    after = {
        "durable.operation.id": "completed-after-wait",
        "durable.operation.type": "STEP",
        "durable.operation.subtype": "Step",
        "durable.operation.name": "otel-after-wait",
    }
    return Trace(
        trace_id=TRACE_ID,
        spans=(
            _span(1, "Workflow", 0, 5, None, {"durable.execution.status": "SUCCEEDED"}),
            _span(
                2, "Invocation", 0, 2, None, {"durable.invocation.first": True, "durable.invocation.status": "PENDING"}
            ),
            _span(
                3,
                "Invocation",
                3,
                5,
                None,
                {"durable.invocation.first": False, "durable.invocation.status": "SUCCEEDED"},
            ),
            _span(4, "otel-before-wait", 0.5, 1, 1, before, links=(2,)),
            _span(
                5,
                "otel-before-wait attempt 1",
                0.6,
                0.9,
                4,
                {**before, "durable.attempt.outcome": "SUCCEEDED"},
                links=(2,),
            ),
            _span(
                6,
                "otel-replay-wait",
                1.5,
                3.5,
                1,
                {
                    "durable.operation.id": "resume-timer",
                    "durable.operation.type": "WAIT",
                    "durable.operation.subtype": "Wait",
                    "durable.operation.name": "otel-replay-wait",
                },
                links=(3,),
            ),
            _span(7, "otel-after-wait", 4, 4.5, 1, after, links=(3,)),
            _span(
                8,
                "otel-after-wait attempt 1",
                4.1,
                4.4,
                7,
                {**after, "durable.attempt.outcome": "SUCCEEDED"},
                links=(3,),
            ),
        ),
    )


def _query() -> TelemetryQuery:
    return TelemetryQuery(EXECUTION_ARN, SERVICE_NAME, START, START + timedelta(seconds=5))


def _normal_invocation_resume() -> Trace:
    trace = _normal_successful_resume()
    spans = []
    workflow_link = SpanLink(TRACE_ID, f"{1:016x}")
    for span in trace.spans:
        if span.name in {"Workflow", "Invocation"}:
            spans.append(span)
        elif span.name == "otel-replay-wait":
            spans.extend(
                [
                    replace(
                        span,
                        end_time=START + timedelta(seconds=1.9),
                        parent_span_id=f"{2:016x}",
                        status="UNSET",
                        attributes=dict(span.attributes) | {"durable.operation.status": "STARTED"},
                        links=(workflow_link,),
                    ),
                    replace(
                        span,
                        span_id=f"{9:016x}",
                        start_time=START + timedelta(seconds=3.1),
                        parent_span_id=f"{3:016x}",
                        attributes=dict(span.attributes) | {"durable.operation.status": "SUCCEEDED"},
                        links=(SpanLink(TRACE_ID, span.span_id), workflow_link),
                    ),
                ]
            )
        else:
            attributes = dict(span.attributes) | {"durable.attempt.number": 1}
            parent = span.parent_span_id
            if "durable.attempt.outcome" not in attributes:
                attributes["durable.operation.status"] = "SUCCEEDED"
                parent = f"{2 if span.name == 'otel-before-wait' else 3:016x}"
            spans.append(replace(span, attributes=attributes, parent_span_id=parent, links=(workflow_link,)))
    return replace(trace, spans=tuple(spans))


@pytest.mark.parametrize("first", [True, False], ids=["initial", "resumed"])
def test_case21_wait_segments_must_belong_to_distinct_invocation_phases(first: bool) -> None:
    trace = _normal_invocation_resume()
    assertions = _replay_assertions("invocation")
    assert validate_trace(trace, assertions, _query()) == []
    parent = next(
        span
        for span in trace.spans
        if span.name == "Invocation" and span.attributes["durable.invocation.first"] is first
    )
    spans = list(trace.spans)
    waits = [span for span in spans if span.name == "otel-replay-wait"]
    for index, wait in enumerate(waits):
        spans[spans.index(wait)] = replace(
            wait,
            parent_span_id=parent.span_id,
            start_time=parent.start_time + timedelta(seconds=0.1 + index * 0.4),
            end_time=parent.start_time + timedelta(seconds=0.3 + index * 0.4),
        )
    errors = validate_trace(replace(trace, spans=tuple(spans)), assertions, _query())
    assert_span_error(
        errors,
        assertions,
        {"name": "otel-replay-wait", "attributes": {"durable.operation.status": "SUCCEEDED" if first else "STARTED"}},
        "expect.parent.$occurrence: parent span is occurrence",
    )


def test_case21_user_attempt_must_belong_to_original_completed_step_segment() -> None:
    trace = _normal_invocation_resume()
    original = next(span for span in trace.spans if span.name == "otel-before-wait")
    replay = replace(
        original,
        span_id=f"{10:016x}",
        parent_span_id=f"{3:016x}",
        start_time=START + timedelta(seconds=3.55),
        end_time=START + timedelta(seconds=3.85),
        links=(SpanLink(TRACE_ID, original.span_id), SpanLink(TRACE_ID, f"{1:016x}")),
    )
    trace = replace(trace, spans=(*trace.spans, replay))
    assertions = _replay_assertions("invocation")
    assert validate_trace(trace, assertions, _query()) == []
    spans = tuple(
        replace(
            span,
            parent_span_id=replay.span_id,
            start_time=START + timedelta(seconds=3.6),
            end_time=START + timedelta(seconds=3.8),
        )
        if span.name == "otel-before-wait attempt 1"
        else span
        for span in trace.spans
    )
    errors = validate_trace(replace(trace, spans=spans), assertions, _query())
    assert_span_error(
        errors,
        assertions,
        {"name": "otel-before-wait attempt 1"},
        "expect.parent.$occurrence: parent span is occurrence 2, expected 1",
    )


def test_successful_replay_requirement_accepts_one_export_per_completed_operation() -> None:
    assert validate_trace(_normal_successful_resume(), _replay_assertions(), _query()) == []


@pytest.mark.parametrize("service_end", [4.7, 5.2])
def test_successful_replay_preserves_timer_phase_despite_clock_skew(service_end: float) -> None:
    trace = _normal_successful_resume()
    spans = tuple(
        replace(span, end_time=START + timedelta(seconds=service_end)) if span.name == "otel-replay-wait" else span
        for span in trace.spans
    )
    # The service end can exceed the next step or SDK Workflow end. Completion
    # history and the resumed Invocation link still establish the correct phase.
    assert validate_trace(replace(trace, spans=spans), _replay_assertions(), _query()) == []
    wrong_phase = tuple(
        replace(span, links=(SpanLink(TRACE_ID, f"{2:016x}"),)) if span.name == "otel-replay-wait" else span
        for span in spans
    )
    errors = validate_trace(replace(trace, spans=wrong_phase), _replay_assertions(), _query())
    assert_span_error(
        errors,
        _replay_assertions(),
        {"name": "otel-replay-wait"},
        "expect.links[0].$occurrence: linked span is occurrence 1, expected 2",
    )


@pytest.mark.parametrize("view", ["invocation", "execution"])
@pytest.mark.parametrize("mutation", [None, "missing-completion", "completion-after-step", "changed-id"])
def test_case21_history_establishes_timer_completion_before_next_step(view: str, mutation: str | None) -> None:
    # Sanitized service history from successful cloud run 40040364906.
    history = json.loads((Path(__file__).parent / "fixtures/completed_step_replay_history.json").read_text())
    events = history["Events"]
    if mutation == "missing-completion":
        events = [event for event in events if event["EventType"] != "WaitSucceeded"]
    elif mutation == "completion-after-step":
        for event in events:
            if event["EventId"] in (6, 7):
                event["EventId"] = 13 - event["EventId"]
    elif mutation == "changed-id":
        next(event for event in events if event["EventType"] == "WaitSucceeded")["Id"] = "different-wait"
    result = EventHistoryMatcher().match(_requirement(f"otel-{view}", 21)["ExpectedExecutionHistory"], events)
    assert result.success is (mutation is None), result.errors


@pytest.mark.parametrize("duplicate_index", [3, 4])
def test_successful_replay_requirement_rejects_duplicate_operation_or_user_attempt(duplicate_index: int) -> None:
    trace = _normal_successful_resume()
    # The old JS behavior can export the identical deterministic span again.
    # Use separate S3 objects to prove normalization does not hide that duplicate.
    merged = _merge_trace_files(
        [
            ("first-and-resumed.json", [trace]),
            ("replayed-completed-operation.json", [replace(trace, spans=(trace.spans[duplicate_index],))]),
        ],
        bucket="conformance-test-bucket",
    )[0]
    assert len(merged.spans) == len(trace.spans) + 1
    errors = validate_trace(merged, _replay_assertions(), _query())
    assert_span_error(
        errors,
        _replay_assertions(),
        {"name": trace.spans[duplicate_index].name},
        "select matched 2 spans; it must select exactly one",
    )


def test_replay_requirement_rejects_missing_pre_suspend_operation() -> None:
    trace = _normal_successful_resume()
    errors = validate_trace(replace(trace, spans=trace.spans[:3] + trace.spans[4:]), _replay_assertions(), _query())
    assert_span_error(errors, _replay_assertions(), {"name": "otel-before-wait"}, "select matched no spans")


def test_completed_step_replay_scenarios_match_across_views() -> None:
    invocation = _requirement("otel-invocation", 21)
    execution = _requirement("otel-execution", 21)
    for key in ("Input", "AsyncInvoke", "ExpectedExecutionHistory", "ExpectedResult"):
        assert invocation[key] == execution[key]
    assert execution["ExpectedResult"]["Result"] == "before-after"


def _bound_assertions(suite: str, number: int) -> dict:
    placeholders = PlaceholderContext()
    placeholders.bind("EXECUTION_ARN", EXECUTION_ARN)
    placeholders.bind("SERVICE_NAME", SERVICE_NAME)
    return placeholders.substitute(_requirement(suite, number)["TelemetryAssertions"])


@pytest.mark.parametrize("view", ["execution", "invocation"])
def test_invocation_retry_contract_rejects_old_python_alias_and_false_success(view: str) -> None:
    normal = _normal_successful_resume()
    workflow, first, resumed = normal.spans[:3]
    retrying = replace(first, status="UNSET", attributes={**first.attributes, "durable.invocation.status": "RETRYING"})
    trace = replace(normal, spans=(workflow, retrying, resumed))
    assertions = _bound_assertions(f"otel-{view}", 24)
    assert validate_trace(trace, assertions, _query()) == []
    old_alias = replace(retrying, attributes={**retrying.attributes, "durable.invocation.status": "RETRY"})
    errors = validate_trace(replace(trace, spans=(workflow, old_alias, resumed)), assertions, _query())
    assert_span_error(
        errors,
        assertions,
        {"name": "Invocation"},
        "expect_by_occurrence[0].attributes.durable.invocation.status: expected 'RETRYING'",
    )
    errors = validate_trace(
        replace(trace, spans=(workflow, replace(retrying, status="OK"), resumed)), assertions, _query()
    )
    assert_span_error(errors, assertions, {"name": "Invocation"}, "expect_by_occurrence[0].status: expected 'UNSET'")
    # Failed-invocation recovery may redeliver deterministic operation spans.
    # Case 24 must not accidentally impose case 21's successful-replay rule.
    recovered = replace(trace, spans=(*trace.spans, normal.spans[3], normal.spans[3]))
    assert validate_trace(recovered, assertions, _query()) == []


@pytest.mark.parametrize("view", ["execution", "invocation"])
def test_invocation_retry_contract_allows_a_checkpointed_step_retry_delay(view: str) -> None:
    normal = _normal_successful_resume()
    workflow, first, resumed = normal.spans[:3]
    retrying = replace(first, status="UNSET", attributes={**first.attributes, "durable.invocation.status": "RETRYING"})
    # Recovering an interrupted at-most-once step can checkpoint a retry timer.
    # That is a real PENDING invocation between the invocation failure and success.
    pending = replace(
        first,
        span_id="e" * 16,
        start_time=START + timedelta(seconds=2.2),
        end_time=START + timedelta(seconds=2.8),
        attributes={**first.attributes, "durable.invocation.first": False, "durable.invocation.status": "PENDING"},
    )
    trace = replace(normal, spans=(workflow, retrying, pending, resumed))
    assertions = _bound_assertions(f"otel-{view}", 24)
    assert validate_trace(trace, assertions, _query()) == []
    # The optional phase has a precise mapping and order, not an arbitrary extra span allowance.
    for status in ("ERROR", "UNSET"):
        errors = validate_trace(
            replace(trace, spans=(workflow, retrying, replace(pending, status=status), resumed)), assertions, _query()
        )
        assert_span_error(errors, assertions, {"name": "Invocation"}, "expect_by_occurrence[1].status: expected 'OK'")
    errors = validate_trace(replace(trace, spans=(workflow, retrying, pending, pending, resumed)), assertions, _query())
    assert_span_error(errors, assertions, {"name": "Invocation"}, "select matched 4 spans; expected 2 or 3")
    errors = validate_trace(replace(trace, spans=(workflow, pending, resumed)), assertions, _query())
    assert_span_error(
        errors,
        assertions,
        {"name": "Invocation"},
        "expect_by_occurrence[0].attributes.durable.invocation.status: expected 'RETRYING'",
    )
    assert (
        validate_trace(replace(trace, spans=(*trace.spans, normal.spans[3], normal.spans[3])), assertions, _query())
        == []
    )


@pytest.mark.parametrize("view", ["execution", "invocation"])
@pytest.mark.parametrize("unexpected_status", ["RETRY", "PENDING", "FAILED"])
def test_invocation_retry_contract_rejects_additional_lifecycle_spans(view: str, unexpected_status: str) -> None:
    normal = _normal_successful_resume()
    workflow, first, resumed = normal.spans[:3]
    retrying = replace(first, status="UNSET", attributes={**first.attributes, "durable.invocation.status": "RETRYING"})
    unexpected = replace(
        retrying,
        span_id="e" * 16,
        attributes={**retrying.attributes, "durable.invocation.status": unexpected_status},
    )
    trace = replace(normal, spans=(workflow, retrying, resumed, unexpected))
    assertions = _bound_assertions(f"otel-{view}", 24)
    errors = validate_trace(trace, assertions, _query())
    for diagnostic in (
        "expect_by_occurrence[1].status: expected 'OK'",
        "expect_by_occurrence[1].attributes.durable.invocation.first: expected False",
        "expect_by_occurrence[1].after: span 'Invocation'",
    ):
        assert_span_error(errors, assertions, {"name": "Invocation"}, diagnostic)
    if unexpected_status != "PENDING":
        assert_span_error(
            errors,
            assertions,
            {"name": "Invocation"},
            "expect_by_occurrence[1].attributes.durable.invocation.status: expected 'PENDING'",
        )


@pytest.mark.parametrize("view", ["execution", "invocation"])
@pytest.mark.parametrize(
    ("callback", "parent_name"),
    [
        ("step", "otel-context-step attempt 1"),
        ("child", "otel-context-child"),
        ("child-restored", "otel-context-child"),
        ("parallel-a", "otel-context-branch-a"),
        ("map-0", "otel-context-iteration-0"),
        ("handler-after-resume", "Invocation"),
    ],
)
def test_user_function_contract_rejects_orphans_and_wrong_active_scope(
    view: str, callback: str, parent_name: str
) -> None:
    assertion = next(
        item
        for item in _bound_assertions(f"otel-{view}", 22)["span_assertions"]
        if item["select"]["name"] == f"conformance.{callback}"
    )
    if callback == "handler-after-resume" and view == "execution":
        parent_name = "Workflow"
    parent = _span(1, parent_name, 0, 5, None, {"durable.invocation.first": False})
    user_span = replace(
        _span(2, f"conformance.{callback}", 1, 2, 1, {"conformance.callback": callback}),
        status="UNSET",
    )
    workflow = parent if parent_name == "Workflow" else _span(3, "Workflow", 0, 5, None, {})
    invocation = (
        parent
        if parent_name == "Invocation"
        else _span(4, "Invocation", 0, 5, None, {"durable.invocation.first": False})
    )
    others = tuple(item for item in (workflow, invocation) if item is not parent)
    trace = Trace(TRACE_ID, (parent, user_span, *others))
    focused = {"span_assertions": [assertion]}
    assert validate_trace(trace, focused, _query()) == []
    errors = validate_trace(
        replace(trace, spans=(parent, replace(user_span, parent_span_id=None), *others)), focused, _query()
    )
    selector = {"name": f"conformance.{callback}"}
    parent_path = "expect.parent.$any_of[0]" if callback == "handler-after-resume" else "expect.parent"
    assert_span_error(errors, focused, selector, f"{parent_path}: selected span has no parent")
    # A valid span in another branch is not a correct active context.
    wrong_scope = replace(
        parent,
        name="otel-context-branch-b" if callback == "parallel-a" else "unrelated-scope",
        attributes={**parent.attributes, "durable.operation.type": "CONTEXT"},
    )
    errors = validate_trace(replace(trace, spans=(wrong_scope, user_span, *others)), focused, _query())
    assert_span_error(errors, focused, selector, f"{parent_path}.name: expected {parent_name!r}")
    if callback == "handler-after-resume":
        assert_span_error(
            errors,
            focused,
            selector,
            "expect.parent.$any_of[1]: resolved parent span 'unrelated-scope'",
        )
    errors = validate_trace(
        replace(trace, spans=(parent, replace(user_span, trace_id="9" * 32), *others)), focused, _query()
    )
    assert_span_error(
        errors, focused, selector, f"expect.same_trace_as: span 'conformance.{callback}' uses a different trace"
    )


@pytest.mark.parametrize("view", ["execution", "invocation"])
def test_handler_restore_rejects_a_stale_user_span_as_ambient_parent(view: str) -> None:
    assertions = _bound_assertions(f"otel-{view}", 22)
    assertion = next(
        a for a in assertions["span_assertions"] if a["select"]["name"] == "conformance.handler-after-resume"
    )
    workflow = _span(1, "Workflow", 0, 5, None, {})
    invocation = _span(2, "Invocation", 3, 5, None, {"durable.invocation.first": False})
    stale_user = _span(3, "conformance.step", 3, 4, 2, {"conformance.callback": "step"})
    restored = replace(
        _span(4, "conformance.handler-after-resume", 4, 4, 3, {"conformance.callback": "handler-after-resume"}),
        status="UNSET",
    )
    trace = Trace(TRACE_ID, (workflow, invocation, stale_user, restored))
    focused = {"span_assertions": [assertion]}
    errors = validate_trace(trace, focused, _query())
    assert_span_error(
        errors,
        focused,
        {"name": "conformance.handler-after-resume"},
        "expect.parent.$any_of[1]: resolved parent matches the forbidden span selector",
    )


@pytest.mark.parametrize("view", ["execution", "invocation"])
@pytest.mark.parametrize("late_forbidden_parent", [False, True])
def test_context_polling_observes_forbidden_parent_after_initial_passing_snapshot(
    view: str, late_forbidden_parent: bool
) -> None:
    assertions = _bound_assertions(f"otel-{view}", 22)
    handler = next(
        a for a in assertions["span_assertions"] if a["select"]["name"] == "conformance.handler-after-resume"
    )
    focused = {"span_assertions": [handler]}
    workflow = _span(1, "Workflow", 0, 5, None, {})
    invocation = _span(2, "Invocation", 3, 5, None, {"durable.invocation.first": False})
    stale_user = _span(3, "conformance.stale-scope", 3, 5, 2, {"conformance.callback": "stale-scope"})
    restored = replace(
        _span(4, "conformance.handler-after-resume", 4, 4, 3, {"conformance.callback": "handler-after-resume"}),
        status="UNSET",
    )
    partial = Trace(TRACE_ID, (workflow, invocation, restored))
    complete = replace(partial, spans=(*partial.spans, stale_user))
    assert validate_trace(partial, focused, _query()) == []
    assert_span_error(
        validate_trace(complete, focused, _query()),
        focused,
        {"name": "conformance.handler-after-resume"},
        "expect.parent.$any_of[1]: resolved parent matches the forbidden span selector",
    )

    class IngestionBackend(PollingBackend):
        name = "context-ingestion"

        def __init__(self) -> None:
            self.now = 0.0
            self.lookups = 0
            super().__init__(monotonic=lambda: self.now, sleep=self.advance)

        def advance(self, seconds: float) -> None:
            self.now += seconds

        def _lookup(self, _query: TelemetryQuery) -> Trace | None:
            self.lookups += 1
            return complete if late_forbidden_parent and self.lookups > 1 else partial

    backend = IngestionBackend()
    quiescence = assertions.get("quiescence_seconds", 0)
    observed = backend.find_trace(
        _query(),
        PollingPolicy(timeout_seconds=10, interval_seconds=1, max_attempts=8, quiescence_seconds=quiescence),
        accept=lambda trace: not validate_trace(trace, focused, _query()),
    )
    errors = validate_trace(observed, focused, _query())
    if late_forbidden_parent:
        assert_span_error(
            errors,
            focused,
            {"name": "conformance.handler-after-resume"},
            "expect.parent.$any_of[1]: resolved parent matches the forbidden span selector",
        )
        assert backend.lookups > 1
    else:
        assert errors == []
        assert backend.now == quiescence


def test_invocation_replay_permitted_counts_require_distinct_linked_segments() -> None:
    placeholders = PlaceholderContext()
    for name, value in {
        "EXECUTION_ARN": EXECUTION_ARN,
        "SERVICE_NAME": SERVICE_NAME,
        "COMPLETED_STEP": "before",
    }.items():
        placeholders.bind(name, value)
    requirement = placeholders.substitute(_requirement("otel-invocation", 21)["TelemetryAssertions"])
    assertion = next(a for a in requirement["span_assertions"] if a["select"]["name"] == "otel-before-wait")
    # This test focuses on replay linkage/parentage, independent of the later step.
    assertion = replace_assertion_without_before(assertion)
    workflow = _span(1, "Workflow", 0, 5, None, {})
    initial = _span(2, "Invocation", 0, 2, None, {"durable.invocation.first": True})
    resumed = _span(3, "Invocation", 3, 5, None, {"durable.invocation.first": False})
    attributes = {
        "durable.operation.id": "before",
        "durable.operation.type": "STEP",
        "durable.operation.subtype": "Step",
        "durable.operation.name": "otel-before-wait",
        "durable.operation.status": "SUCCEEDED",
        "durable.attempt.number": 1,
    }
    first = _span(4, "otel-before-wait", 0.5, 1, 2, attributes, links=(1,))
    second = _span(5, "otel-before-wait", 3.1, 3.2, 3, attributes, links=(4, 1))
    focused = {"span_assertions": [assertion]}
    trace = Trace(TRACE_ID, (workflow, initial, resumed, first))
    assert validate_trace(trace, focused, _query()) == []
    assert validate_trace(replace(trace, spans=(*trace.spans, second)), focused, _query()) == []
    errors = validate_trace(replace(trace, spans=(*trace.spans, first)), focused, _query())
    for diagnostic in (
        "expect_by_occurrence[1].parent.attributes.durable.invocation.first: expected False",
        "expect_by_occurrence[1].links: expected 2 item(s), found 1",
    ):
        assert_span_error(errors, focused, {"name": "otel-before-wait"}, diagnostic)
    errors = validate_trace(replace(trace, spans=(*trace.spans, replace(second, links=()))), focused, _query())
    assert_span_error(
        errors, focused, {"name": "otel-before-wait"}, "expect_by_occurrence[1].links: expected 2 item(s), found 0"
    )
    errors = validate_trace(
        replace(trace, spans=(workflow, initial, resumed, replace(first, links=()))), focused, _query()
    )
    assert_span_error(
        errors, focused, {"name": "otel-before-wait"}, "expect_by_occurrence[0].links: expected 1 item(s), found 0"
    )


def replace_assertion_without_before(assertion: dict) -> dict:
    from copy import deepcopy

    result = deepcopy(assertion)
    result["expect"].pop("before", None)
    return result


@pytest.mark.parametrize("view", ["execution", "invocation"])
def test_callback_case_requires_initial_and_terminal_invocations_not_duplicate_initial_exports(view: str) -> None:
    assertions = _bound_assertions(f"otel-{view}", 23)
    focused = {"minimum_invocations": 2, "span_assertions": assertions["span_assertions"][:3]}
    base = _normal_successful_resume()
    workflow, initial, terminal = base.spans[:3]
    complete = replace(base, spans=(workflow, initial, terminal))
    assert validate_trace(complete, focused, _query()) == []
    duplicate_initial = replace(base, spans=(workflow, initial, initial))
    errors = validate_trace(duplicate_initial, focused, _query())
    assert_span_error(
        errors,
        focused,
        {"name": "Invocation", "attributes": {"durable.invocation.first": False}},
        "select matched no spans",
    )
