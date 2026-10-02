# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Regression evidence for user-visible OTel launch issues."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from aws_durable_execution_conformance_tests.validate import discover_test_files, load_yaml_file
from aws_durable_execution_conformance_tests.variables import PlaceholderContext
from aws_durable_execution_conformance_tests_otel.backends.collector import _merge_trace_files
from aws_durable_execution_conformance_tests_otel.extension import OtelExtension
from aws_durable_execution_conformance_tests_otel.model import Span, SpanLink, TelemetryQuery, Trace
from aws_durable_execution_conformance_tests_otel.validators import validate_trace

TRACE_ID = "1" * 32
EXECUTION_ARN = "arn:test:execution:successful-replay"
SERVICE_NAME = "durable-execution-conformance"
START = datetime(2026, 10, 2, tzinfo=UTC)


def _requirement(suite: str, number: int) -> dict:
    root = next(item.root for item in OtelExtension().requirement_suites() if item.name == suite)
    files = discover_test_files(root, suite="all")
    return load_yaml_file(files[f"{suite}-{number}"])


def _replay_assertions() -> dict:
    placeholders = PlaceholderContext()
    for name, value in {
        "EXECUTION_ARN": EXECUTION_ARN,
        "SERVICE_NAME": SERVICE_NAME,
        "COMPLETED_STEP": "completed-before-wait",
        "REPLAY_WAIT": "resume-timer",
        "AFTER_STEP": "completed-after-wait",
    }.items():
        placeholders.bind(name, value)
    return placeholders.substitute(_requirement("otel-execution", 21)["TelemetryAssertions"])


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


def test_successful_replay_requirement_accepts_one_export_per_completed_operation() -> None:
    assert validate_trace(_normal_successful_resume(), _replay_assertions(), _query()) == []


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
    assert errors, "normal successful replay must not export a completed operation or rerun its user attempt"
    assert any("matched 2 spans; it must select exactly one" in error for error in errors)


def test_replay_requirement_rejects_missing_pre_suspend_operation() -> None:
    trace = _normal_successful_resume()
    assert validate_trace(replace(trace, spans=trace.spans[:3] + trace.spans[4:]), _replay_assertions(), _query())


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
    assert validate_trace(replace(trace, spans=(workflow, old_alias, resumed)), assertions, _query())
    assert validate_trace(
        replace(trace, spans=(workflow, replace(retrying, status="OK"), resumed)), assertions, _query()
    )
    # Failed-invocation recovery may redeliver deterministic operation spans.
    # Case 24 must not accidentally impose case 21's successful-replay rule.
    recovered = replace(trace, spans=(*trace.spans, normal.spans[3], normal.spans[3]))
    assert validate_trace(recovered, assertions, _query()) == []


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
    assert validate_trace(
        replace(trace, spans=(parent, replace(user_span, parent_span_id=None), *others)), focused, _query()
    )
    # A valid span in another branch is not a correct active context.
    wrong_scope = replace(
        parent,
        name="otel-context-branch-b" if callback == "parallel-a" else "unrelated-scope",
        attributes={**parent.attributes, "durable.operation.type": "CONTEXT"},
    )
    assert validate_trace(replace(trace, spans=(wrong_scope, user_span, *others)), focused, _query())
    assert validate_trace(
        replace(trace, spans=(parent, replace(user_span, trace_id="9" * 32), *others)), focused, _query()
    )
