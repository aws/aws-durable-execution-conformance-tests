# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Oracle mutation controls; real SDK/cloud evidence is recorded separately."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from aws_durable_execution_conformance_tests.history import EventHistoryMatcher
from aws_durable_execution_conformance_tests.validate import discover_test_files, load_yaml_file
from aws_durable_execution_conformance_tests.variables import PlaceholderContext
from aws_durable_execution_conformance_tests_otel.extension import OtelExtension
from aws_durable_execution_conformance_tests_otel.model import Span, SpanLink, TelemetryQuery, Trace
from aws_durable_execution_conformance_tests_otel.validators import validate_trace

from .requirement_assertions import assert_span_error

TRACE = "1" * 32
ARN = "arn:test:external-replay"
START = datetime(2026, 10, 7, tzinfo=UTC)
NAMES = ["otel-external-target", "otel-external-barrier-one", "otel-external-barrier-two"]
SYMBOLS = ["TARGET", "BARRIER_ONE", "BARRIER_TWO"]


def requirement(view: str) -> dict:
    suite = next(s for s in OtelExtension().requirement_suites() if s.name == f"otel-{view}")
    return load_yaml_file(discover_test_files(suite.root, suite="all")[f"otel-{view}-26"])


@pytest.mark.parametrize("view", ["invocation", "execution"])
@pytest.mark.parametrize("event_type,event_id", [("CallbackStarted", 2), ("CallbackSucceeded", 4)])
def test_case26_history_rejects_nested_target_parent(view: str, event_type: str, event_id: int) -> None:
    expected = next(event for event in requirement(view)["ExpectedExecutionHistory"] if event["EventId"] == event_id)
    actual = {
        "EventId": event_id,
        "EventType": event_type,
        "SubType": "Callback",
        "Id": "root-target",
        "Name": "otel-external-target",
    }
    assert EventHistoryMatcher().match([expected], [actual]).success
    assert not EventHistoryMatcher().match([expected], [{**actual, "ParentId": "unexpected-child-context"}]).success


def assertions(view: str) -> dict:
    context = PlaceholderContext()
    context.bind("EXECUTION_ARN", ARN)
    context.bind("SERVICE_NAME", "conformance")
    context.bind("TARGET_OBSERVED", "observed")
    for stage, symbol in enumerate(SYMBOLS):
        context.bind(f"{symbol}_CONTEXT", f"ctx{stage}")
        context.bind(f"{symbol}_CALLBACK", f"cb{stage}")
        context.bind(f"{symbol}_SUBMITTER", f"step{stage}")
    return context.substitute(requirement(view)["TelemetryAssertions"])


def _callback_selector(view: str, identifier: str = "cb0", *, started: bool = False) -> dict:
    attributes = {"durable.operation.id": identifier}
    if view == "invocation":
        attributes["durable.operation.status"] = (
            "STARTED" if started else "${/^(?:SUCCEEDED|FAILED|TIMED_OUT|CANCELLED|STOPPED)$/}"
        )
    return {"attributes": attributes}


def span(
    identifier: int, name: str, start: float, end: float, parent: int, status: str, attributes: dict, links=()
) -> Span:
    return Span(
        trace_id=TRACE,
        span_id=f"{identifier:016x}",
        name=name,
        start_time=START + timedelta(seconds=start),
        end_time=START + timedelta(seconds=end),
        kind="INTERNAL",
        parent_span_id=f"{parent:016x}",
        status=status,
        attributes={"durable.execution.arn": ARN, **attributes},
        links=tuple(SpanLink(TRACE, f"{link:016x}") for link in links),
        service_name="conformance",
    )


def trace(view: str) -> Trace:
    spans = [span(1, "Workflow", 0, 11.5, 999, "OK", {"durable.execution.status": "SUCCEEDED"})]
    for i in range(4):
        spans.append(
            span(
                10 + i,
                "Invocation",
                3 * i,
                3 * i + 2,
                999,
                "OK",
                {"durable.invocation.first": i == 0, "durable.invocation.status": "PENDING" if i < 3 else "SUCCEEDED"},
            )
        )
    for stage, name in enumerate(NAMES):
        attrs = {
            "durable.operation.id": f"cb{stage}",
            "durable.operation.type": "CALLBACK",
            "durable.operation.subtype": "Callback",
            "durable.operation.name": name + " create callback id",
        }
        ctxattrs = {"durable.operation.id": f"ctx{stage}", "durable.operation.type": "CONTEXT"}
        created, completed = [0, 4.4, 7.35][stage], 3 * (stage + 1)
        if view == "invocation":
            spans.extend(
                [
                    span(
                        100 + stage * 2,
                        name,
                        created + 0.2,
                        3 * stage + 1.99,
                        10 + stage,
                        "UNSET",
                        ctxattrs | {"durable.operation.status": "STARTED"},
                    ),
                    span(
                        101 + stage * 2,
                        name,
                        completed + 0.2,
                        completed + 1.6,
                        11 + stage,
                        "OK",
                        ctxattrs | {"durable.operation.status": "SUCCEEDED"},
                    ),
                    span(
                        200 + stage * 2,
                        name + " create callback id",
                        created + 0.4,
                        3 * stage + 1.95,
                        100 + stage * 2,
                        "UNSET",
                        attrs | {"durable.operation.status": "STARTED"},
                        (1,),
                    ),
                    span(
                        201 + stage * 2,
                        name + " create callback id",
                        completed + 0.4,
                        completed + 1.4,
                        101 + stage * 2,
                        "OK",
                        attrs | {"durable.operation.status": "SUCCEEDED"},
                        (200 + stage * 2, 1),
                    ),
                ]
            )
        else:
            spans.extend(
                [
                    span(100 + stage, name, created + 0.2, completed + 1.6, 1, "OK", ctxattrs),
                    span(
                        200 + stage,
                        name + " create callback id",
                        created + 0.4,
                        completed + 1.4,
                        100 + stage,
                        "OK",
                        attrs | {"durable.operation.status": "SUCCEEDED"},
                        (11 + stage,),
                    ),
                ]
            )
    root_contexts = {f"{100:016x}", f"{101:016x}"} if view == "invocation" else {f"{100:016x}"}
    spans = [s for s in spans if s.span_id not in root_contexts]
    spans = [
        replace(
            s,
            name="otel-external-target",
            parent_span_id=f"{(10 if s.attributes.get('durable.operation.status') == 'STARTED' else 11) if view == 'invocation' else 1:016x}",
            attributes=dict(s.attributes) | {"durable.operation.name": "otel-external-target"},
        )
        if s.attributes.get("durable.operation.id") == "cb0"
        else s
        for s in spans
    ]
    observed = {
        "durable.operation.id": "observed",
        "durable.operation.type": "STEP",
        "durable.operation.subtype": "Step",
        "durable.operation.name": "otel-external-target-observed",
    }
    spans.extend(
        [
            span(
                300,
                "otel-external-target-observed",
                4.42,
                4.52,
                11 if view == "invocation" else 1,
                "OK",
                observed | {"durable.operation.status": "SUCCEEDED"},
            ),
            span(
                301,
                "otel-external-target-observed attempt 1",
                4.45,
                4.5,
                300,
                "OK",
                observed | {"durable.attempt.number": 1, "durable.attempt.outcome": "SUCCEEDED"},
                (1,) if view == "invocation" else (11,),
            ),
        ]
    )
    return Trace(TRACE, tuple(spans))


def validate(value: Trace, view: str) -> list[str]:
    return validate_trace(
        value, assertions(view), TelemetryQuery(ARN, "conformance", START, START + timedelta(seconds=12))
    )


@pytest.mark.parametrize("view", ["invocation", "execution"])
def test_first_completion_and_two_subsequent_replays_pass_oracle(view: str) -> None:
    assert validate(trace(view), view) == []


@pytest.mark.parametrize(
    "callback_id,timestamp,value,wrong_invocation",
    [
        ("cb0", "end_time", 4.7, 12),
        ("cb0", "end_time", 12.5, 12),
        ("cb1", "start_time", 4.4, 13),
        ("cb2", "start_time", 4.4, 12),
    ],
)
def test_execution_service_clock_skew_preserves_callback_phase(
    callback_id: str, timestamp: str, value: float, wrong_invocation: int
) -> None:
    original = trace("execution")
    spans = tuple(
        replace(
            item,
            start_time=START + timedelta(seconds=value) if timestamp == "start_time" else item.start_time,
            end_time=START + timedelta(seconds=value) if timestamp == "end_time" else item.end_time,
        )
        if item.attributes.get("durable.operation.id") == callback_id
        else item
        for item in original.spans
    )
    # Service callback timestamps may overlap the SDK observation attempt.
    # History and invocation links establish the phase, independent of clock skew.
    assert validate(replace(original, spans=spans), "execution") == []
    wrong_phase = tuple(
        replace(item, links=(SpanLink(TRACE, f"{wrong_invocation:016x}"),))
        if item.attributes.get("durable.operation.id") == callback_id
        else item
        for item in spans
    )
    errors = validate(replace(original, spans=wrong_phase), "execution")
    assert_span_error(
        errors,
        assertions("execution"),
        _callback_selector("execution", callback_id),
        "expect.links[0].$occurrence: linked span is occurrence",
    )


def test_initial_callback_cannot_be_exported_in_the_resumed_invocation() -> None:
    original = trace("invocation")
    spans = tuple(
        replace(
            item,
            parent_span_id=f"{11:016x}",
            start_time=START + timedelta(seconds=3.05),
            end_time=START + timedelta(seconds=3.2),
        )
        if item.attributes.get("durable.operation.id") == "cb0"
        and item.attributes.get("durable.operation.status") == "STARTED"
        else item
        for item in original.spans
    )
    errors = validate(replace(original, spans=spans), "invocation")
    assert_span_error(
        errors,
        assertions("invocation"),
        _callback_selector("invocation", started=True),
        "expect.parent.$occurrence: parent span is occurrence 2, expected 1",
    )


@pytest.mark.parametrize(
    "leaf,parent,begin,end",
    [
        (202, 103, 6.25, 6.35),
        (203, 102, 4.96, 4.98),
        (204, 105, 9.25, 9.35),
        (205, 104, 7.96, 7.98),
    ],
)
def test_case26_barrier_leaf_must_use_matching_context_phase(leaf: int, parent: int, begin: float, end: float) -> None:
    original = trace("invocation")
    spans = tuple(
        replace(
            item,
            parent_span_id=f"{parent:016x}",
            start_time=START + timedelta(seconds=begin),
            end_time=START + timedelta(seconds=end),
        )
        if item.span_id == f"{leaf:016x}"
        else item
        for item in original.spans
    )
    errors = validate(replace(original, spans=spans), "invocation")
    target = next(item for item in original.spans if item.span_id == f"{leaf:016x}")
    assert_span_error(
        errors,
        assertions("invocation"),
        _callback_selector("invocation", str(target.attributes["durable.operation.id"]), started=leaf % 2 == 0),
        "expect.parent.attributes.durable.operation.status: expected",
    )


def test_case26_barrier_context_cannot_move_its_leaf_into_another_invocation() -> None:
    original = trace("invocation")
    spans = []
    for item in original.spans:
        if item.span_id == f"{102:016x}":
            item = replace(
                item,
                parent_span_id=f"{12:016x}",
                start_time=START + timedelta(seconds=6.05),
                end_time=START + timedelta(seconds=6.3),
            )
        elif item.span_id == f"{202:016x}":
            item = replace(item, start_time=START + timedelta(seconds=6.1), end_time=START + timedelta(seconds=6.25))
        spans.append(item)
    errors = validate(replace(original, spans=tuple(spans)), "invocation")
    assert_span_error(
        errors,
        assertions("invocation"),
        _callback_selector("invocation", "cb1", started=True),
        "expect.descendant_of: unresolved parent",
    )


@pytest.mark.parametrize("view", ["invocation", "execution"])
@pytest.mark.parametrize(
    "corruption,diagnostic",
    [
        ("missing-first", "select matched no spans"),
        ("identical-duplicate", "select matched 2 spans; it must select exactly one"),
        ("duplicate-new-id", "select matched 2 spans; it must select exactly one"),
        ("wrong-status", "expect.status: expected 'OK'"),
        ("wrong-operation-status", "expect.attributes.durable.operation.status: expected 'SUCCEEDED'"),
        ("wrong-trace", "expect.same_trace_as: span 'otel-external-target' uses a different trace"),
        ("wrong-parent", "expect.parent: parent span is not present in the trace"),
        ("late-completion", "expect.before: span 'otel-external-target'"),
        ("missing-replay", "select matched 3 spans; expected 4"),
        ("extra-invocation", "select matched 5 spans; expected 4"),
    ],
)
def test_external_completion_mutations_fail(view: str, corruption: str, diagnostic: str) -> None:
    value = trace(view)
    target = next(
        s
        for s in value.spans
        if s.attributes.get("durable.operation.id") == "cb0"
        and s.attributes.get("durable.operation.status") == "SUCCEEDED"
    )
    spans = list(value.spans)
    if corruption == "missing-first":
        spans.remove(target)
    elif corruption == "identical-duplicate":
        spans.append(target)
    elif corruption == "duplicate-new-id":
        spans.append(replace(target, span_id="f" * 16))
    elif corruption == "missing-replay":
        spans = [s for s in spans if s.span_id != f"{12:016x}"]
    elif corruption == "extra-invocation":
        spans.append(replace(next(s for s in spans if s.name == "Invocation"), span_id="e" * 16))
    else:
        mutations = {
            "wrong-status": replace(target, status="UNSET"),
            "wrong-operation-status": replace(
                target, attributes=dict(target.attributes) | {"durable.operation.status": "FAILED"}
            ),
            "wrong-trace": replace(target, trace_id="2" * 32),
            "wrong-parent": replace(target, parent_span_id=f"{999:016x}"),
            "late-completion": replace(
                target,
                start_time=START + timedelta(seconds=6.4),
                end_time=START + timedelta(seconds=7.4),
                links=(SpanLink(TRACE, f"{12:016x}"),) if view == "execution" else target.links,
            ),
        }
        spans = [mutations[corruption] if s is target else s for s in spans]
    errors = validate(replace(value, spans=tuple(spans)), view)
    selector = (
        {"name": "Invocation"} if corruption in {"missing-replay", "extra-invocation"} else _callback_selector(view)
    )
    if corruption == "late-completion" and view == "execution":
        diagnostic = "expect.links[0].$occurrence: linked span is occurrence 3, expected 2"
    assert_span_error(errors, assertions(view), selector, diagnostic)


@pytest.mark.parametrize("view", ["invocation", "execution"])
def test_contract_requires_three_real_callback_deliveries_and_four_phases(view: str) -> None:
    req = requirement(view)
    assert req["CallbackActions"][0]["CallbackName"] == "otel-external-target"
    assert req["Input"] == {"scenario": "external-callback-completion-replay"}
    assert len(req["CallbackActions"]) == 3
    assert [action["Payload"] for action in req["CallbackActions"]] == ["target", "one", "two"]
    assert all(
        action["Operation"] == "success" and action["AfterInvocationCompleted"] is True
        for action in req["CallbackActions"]
    )
    events = req["ExpectedExecutionHistory"]
    assert [e["EventId"] for e in events if e["EventType"] == "InvocationCompleted"] == [3, 11, 18, 21]
    assert len({e["Id"] for e in events if e["EventType"] == "CallbackStarted"}) == 3
    assert req["TelemetryAssertions"]["quiescence_seconds"] == 5
    assert req["ExpectedResult"] == {"ExecutionStatus": "SUCCEEDED", "Result": "target/one/two"}


@pytest.mark.parametrize("parent,start,end", [(10, 1.96, 1.99), (12, 3.4, 4.4), (12, 6.4, 7.4)])
def test_target_terminal_must_belong_to_first_completion_invocation(parent: int, start: float, end: float) -> None:
    value = trace("invocation")
    target = next(
        s
        for s in value.spans
        if s.attributes.get("durable.operation.id") == "cb0"
        and s.attributes.get("durable.operation.status") == "SUCCEEDED"
    )
    bad = replace(
        target,
        parent_span_id=f"{parent:016x}",
        start_time=START + timedelta(seconds=start),
        end_time=START + timedelta(seconds=end),
    )
    errors = validate(replace(value, spans=tuple(bad if s is target else s for s in value.spans)), "invocation")
    assert_span_error(
        errors,
        assertions("invocation"),
        _callback_selector("invocation"),
        "expect.parent.$occurrence: parent span is occurrence",
    )


@pytest.mark.parametrize("occurrence", [None, True, 0, -1, "2"])
def test_parent_occurrence_is_strictly_positive_integer(occurrence: object) -> None:
    config = assertions("invocation")
    item = next(
        item
        for item in config["span_assertions"]
        if item["expect"].get("attributes", {}).get("durable.operation.id") == "cb0"
        and item["expect"]["attributes"].get("durable.operation.status") == "SUCCEEDED"
    )
    item["expect"]["parent"]["$occurrence"] = occurrence
    errors = validate_trace(
        trace("invocation"), config, TelemetryQuery(ARN, "conformance", START, START + timedelta(seconds=12))
    )
    assert_span_error(
        errors, config, _callback_selector("invocation"), "expect.parent.$occurrence must be a positive integer"
    )


def test_parent_occurrence_cannot_be_bypassed_by_unresolved_alternative() -> None:
    config = assertions("invocation")
    item = next(
        item
        for item in config["span_assertions"]
        if item["expect"].get("attributes", {}).get("durable.operation.id") == "cb0"
        and item["expect"]["attributes"].get("durable.operation.status") == "SUCCEEDED"
    )
    valid = item["expect"]["parent"]
    item["expect"]["parent"] = {"$any_of": [valid, {"name": "Invocation", "$allow_unresolved": True, "$occurrence": 2}]}
    errors = validate_trace(
        trace("invocation"), config, TelemetryQuery(ARN, "conformance", START, START + timedelta(seconds=12))
    )
    assert_span_error(
        errors,
        config,
        _callback_selector("invocation"),
        "expect.parent.$any_of[1].$occurrence requires a resolved parent",
    )


@pytest.mark.parametrize("occurrence", [0, True, "2"])
def test_invalid_parent_occurrence_in_unused_alternative_still_fails(occurrence: object) -> None:
    config = assertions("invocation")
    item = next(
        item
        for item in config["span_assertions"]
        if item["expect"].get("attributes", {}).get("durable.operation.id") == "cb0"
        and item["expect"]["attributes"].get("durable.operation.status") == "SUCCEEDED"
    )
    valid = item["expect"]["parent"]
    item["expect"]["parent"] = {"$any_of": [valid, {**valid, "$occurrence": occurrence}]}
    errors = validate_trace(
        trace("invocation"), config, TelemetryQuery(ARN, "conformance", START, START + timedelta(seconds=12))
    )
    assert_span_error(
        errors,
        config,
        _callback_selector("invocation"),
        "expect.parent.$any_of[1].$occurrence must be a positive integer",
    )


def test_parent_occurrence_out_of_range_is_validation_error() -> None:
    config = assertions("invocation")
    item = next(
        item
        for item in config["span_assertions"]
        if item["expect"].get("attributes", {}).get("durable.operation.id") == "cb0"
        and item["expect"]["attributes"].get("durable.operation.status") == "SUCCEEDED"
    )
    item["expect"]["parent"]["$occurrence"] = 99
    errors = validate_trace(
        trace("invocation"), config, TelemetryQuery(ARN, "conformance", START, START + timedelta(seconds=12))
    )
    assert_span_error(
        errors,
        config,
        _callback_selector("invocation"),
        "expect.parent.$occurrence: parent span is occurrence 2, expected 99",
    )


def test_parent_occurrence_ignores_other_trace_id_collision_and_accepts_matchers() -> None:
    from aws_durable_execution_conformance_tests_otel.model import span_to_dict
    from aws_durable_execution_conformance_tests_otel.validators import _parent_expectation_errors

    value = trace("invocation")
    target = next(
        s
        for s in value.spans
        if s.attributes.get("durable.operation.id") == "cb0"
        and s.attributes.get("durable.operation.status") == "SUCCEEDED"
    )
    parent = next(s for s in value.spans if s.span_id == target.parent_span_id)
    collision = replace(
        parent, trace_id="0" * 32, start_time=START - timedelta(seconds=10), end_time=START - timedelta(seconds=8)
    )
    by_id: dict = {}
    for item in (*value.spans, collision, parent):
        by_id.setdefault(item.span_id, []).append((item, span_to_dict(item)))
    expected = {
        "name": "${/^Invocation$/}",
        "attributes": {"durable.execution.arn": {"$any_of": [ARN, "arn:other"]}},
        "status": {"$any_of": ["OK", "UNSET"]},
        "$occurrence": 2,
    }
    assert _parent_expectation_errors(expected, target, by_id, path="parent", feature_disparities=()) == []
