# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Oracle mutation controls; real SDK/cloud evidence is recorded separately."""

from dataclasses import replace
from datetime import UTC, datetime, timedelta

import pytest

from aws_durable_execution_conformance_tests.validate import discover_test_files, load_yaml_file
from aws_durable_execution_conformance_tests.variables import PlaceholderContext
from aws_durable_execution_conformance_tests_otel.extension import OtelExtension
from aws_durable_execution_conformance_tests_otel.model import Span, SpanLink, TelemetryQuery, Trace
from aws_durable_execution_conformance_tests_otel.validators import validate_trace

TRACE = "1" * 32
ARN = "arn:test:external-replay"
START = datetime(2026, 10, 7, tzinfo=UTC)
NAMES = ["otel-external-target", "otel-external-barrier-one", "otel-external-barrier-two"]
SYMBOLS = ["TARGET", "BARRIER_ONE", "BARRIER_TWO"]


def requirement(view: str) -> dict:
    suite = next(s for s in OtelExtension().requirement_suites() if s.name == f"otel-{view}")
    return load_yaml_file(discover_test_files(suite.root, suite="all")[f"otel-{view}-26"])


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
                    span(100 + stage * 2, name, created + 0.2, 3 * stage + 1.99, 10 + stage, "UNSET", ctxattrs),
                    span(101 + stage * 2, name, completed + 0.2, completed + 1.6, 11 + stage, "OK", ctxattrs),
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


@pytest.mark.parametrize("view", ["invocation", "execution"])
@pytest.mark.parametrize(
    "corruption",
    [
        "missing-first",
        "identical-duplicate",
        "duplicate-new-id",
        "wrong-status",
        "wrong-operation-status",
        "wrong-trace",
        "wrong-parent",
        "late-completion",
        "missing-replay",
        "extra-invocation",
    ],
)
def test_external_completion_mutations_fail(view: str, corruption: str) -> None:
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
                target, start_time=START + timedelta(seconds=6.4), end_time=START + timedelta(seconds=7.4)
            ),
        }
        spans = [mutations[corruption] if s is target else s for s in spans]
    assert validate(replace(value, spans=tuple(spans)), view)


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
