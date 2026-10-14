# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The child service carrier is independent of outgoing header construction."""

import json
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from aws_durable_execution_conformance_tests.extensions import ValidationContext
from aws_durable_execution_conformance_tests_otel.chained_invoke import (
    execution_trace_context,
    load_execution_trace_contexts,
)
from aws_durable_execution_conformance_tests_otel.extension import OtelExtension
from aws_durable_execution_conformance_tests_otel.model import ExecutionTraceContext, Span, TelemetryQuery, Trace
from aws_durable_execution_conformance_tests_otel.polling import PollingBackend
from aws_durable_execution_conformance_tests_otel.validators import validate_trace

ROOT = "1-11111111-111111111111111111111111"
HEADER = f"Root={ROOT};Parent={'2' * 16};Sampled=1"
CHILD = "arn:target"
PARENT_SELECTOR = {"name": "otel-invoke", "attributes": {"durable.operation.status": "STARTED"}}
ASSERTIONS = {
    "allowed_execution_arns": ["arn:caller", CHILD],
    "chained_invokes": [{"child_execution_arn": CHILD, "parent": PARENT_SELECTOR, "sampled": True}],
}


def _fixture() -> tuple[Trace, TelemetryQuery]:
    now = datetime(2026, 10, 14, tzinfo=UTC)
    invoke = Span(
        trace_id="1" * 32,
        span_id="2" * 16,
        name="otel-invoke",
        start_time=now,
        end_time=now,
        attributes={"durable.execution.arn": "arn:caller", "durable.operation.status": "STARTED"},
    )
    child = replace(
        invoke,
        span_id="3" * 16,
        name="Workflow",
        parent_span_id="f" * 16,
        attributes={"durable.execution.arn": CHILD},
    )
    invocation = replace(child, span_id="4" * 16, name="Invocation")
    query = TelemetryQuery(
        "arn:caller",
        "service",
        now,
        now,
        execution_trace_contexts={CHILD: execution_trace_context({"TraceHeader": {"XAmznTraceId": HEADER}})},
    )
    return Trace(invoke.trace_id, (invoke, child, invocation)), query


@pytest.mark.parametrize("sampled", [False, True])
def test_reads_explicit_service_sampling_decision(sampled: bool) -> None:
    header = HEADER.replace("Sampled=1", f"Sampled={int(sampled)}")
    assert execution_trace_context({"TraceHeader": {"XAmznTraceId": header}}) == ExecutionTraceContext(
        "1" * 32, "2" * 16, sampled
    )


@pytest.mark.parametrize(
    "header",
    [
        None,
        "",
        "Parent=2222222222222222;Sampled=1",
        f"Root={ROOT};Sampled=1",
        HEADER.replace("Sampled=1", "Sampled=?"),
        HEADER + ";Parent=3333333333333333",
        HEADER.replace("2222222222222222", "0000000000000000"),
    ],
)
def test_missing_or_ambiguous_service_context_never_passes(header: object) -> None:
    with pytest.raises(ValueError, match="GetDurableExecution"):
        execution_trace_context({"TraceHeader": {"XAmznTraceId": header}})


def test_reads_actual_child_arn_from_history_without_an_invoke_or_header_injection() -> None:
    client = Mock()
    client.get_durable_execution.return_value = {"TraceHeader": {"XAmznTraceId": HEADER}}
    history = {
        "Events": [{"EventType": "ChainedInvokeStarted", "ChainedInvokeStartedDetails": {"DurableExecutionArn": CHILD}}]
    }
    result = load_execution_trace_contexts(ASSERTIONS["chained_invokes"], history, client)
    client.get_durable_execution.assert_called_once_with(DurableExecutionArn=CHILD)
    assert list(result) == [CHILD]
    assert result[CHILD].parent_span_id == "2" * 16


@pytest.mark.parametrize("events", [[], [{"EventType": "ExecutionStarted"}]])
def test_does_not_read_unbound_child_arns(events: list[dict]) -> None:
    client = Mock()
    with pytest.raises(ValueError, match="exactly one actual ChainedInvokeStarted"):
        load_execution_trace_contexts(ASSERTIONS["chained_invokes"], {"Events": events}, client)
    client.get_durable_execution.assert_not_called()


@pytest.mark.parametrize("field", ["Root", "Parent", "Sampled", "missing"])
def test_same_root_or_exported_spans_do_not_replace_exact_service_context(field: str) -> None:
    trace, query = _fixture()
    carrier = query.execution_trace_contexts[CHILD]
    if field == "Root":
        carrier = replace(carrier, trace_id="9" * 32)
    elif field == "Parent":
        carrier = replace(carrier, parent_span_id="9" * 16)
    elif field == "Sampled":
        carrier = replace(carrier, sampled=False)
    query = replace(query, execution_trace_contexts={} if field == "missing" else {CHILD: carrier})
    errors = validate_trace(trace, ASSERTIONS, query)
    assert any(field in error for error in errors), errors


def test_received_parent_must_be_first_segment_even_when_replay_span_has_same_trace() -> None:
    trace, query = _fixture()
    replay = replace(
        trace.spans[0],
        span_id="5" * 16,
        attributes={"durable.execution.arn": "arn:caller", "durable.operation.status": "SUCCEEDED"},
    )
    carrier = replace(query.execution_trace_contexts[CHILD], parent_span_id=replay.span_id)
    errors = validate_trace(
        replace(trace, spans=(*trace.spans, replay)),
        ASSERTIONS,
        replace(query, execution_trace_contexts={CHILD: carrier}),
    )
    assert any("Parent differs from the selected first invoke" in error for error in errors), errors


def test_service_carrier_qualifies_partial_sdk_export_but_not_full_backend_ancestry() -> None:
    trace, query = _fixture()
    assert validate_trace(trace, ASSERTIONS, query) == []
    errors = validate_trace(trace, ASSERTIONS, query, full_trace_ancestry=True)
    assert len(errors) == 2 and all("unresolved parent" in error for error in errors), errors
    infrastructure = replace(trace.spans[0], span_id="f" * 16, name="Lambda", parent_span_id="2" * 16, attributes={})
    assert (
        validate_trace(
            replace(trace, spans=(*trace.spans, infrastructure)), ASSERTIONS, query, full_trace_ancestry=True
        )
        == []
    )
    disconnected = replace(infrastructure, parent_span_id=None)
    errors = validate_trace(
        replace(trace, spans=(*trace.spans, disconnected)), ASSERTIONS, query, full_trace_ancestry=True
    )
    assert len(errors) == 2 and all("does not reach ancestor" in error for error in errors), errors


@pytest.mark.parametrize("full_ancestry", [False, True])
@pytest.mark.parametrize("wrong_parent", [False, True])
def test_extension_reads_and_preserves_actual_service_evidence_on_pass_and_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, full_ancestry: bool, wrong_parent: bool
) -> None:
    trace, _ = _fixture()
    if full_ancestry:
        infrastructure = replace(
            trace.spans[0], span_id="f" * 16, name="Lambda", parent_span_id="2" * 16, attributes={}
        )
        trace = replace(trace, spans=(*trace.spans, infrastructure))

    class Backend(PollingBackend):
        name = "test"
        provides_infrastructure_ancestry = full_ancestry

        def _lookup(self, _query: TelemetryQuery) -> Trace:
            return trace

    backend = Backend(monotonic=lambda: 0, sleep=lambda _delay: None)
    monkeypatch.setattr(
        OtelExtension,
        "_backends",
        staticmethod(lambda: {"test": SimpleNamespace(create=lambda *_args, **_kwargs: backend)}),
    )
    client = Mock()
    header = HEADER.replace("2" * 16, "5" * 16) if wrong_parent else HEADER
    client.get_durable_execution.return_value = {"TraceHeader": {"XAmznTraceId": header}}
    context = ValidationContext(
        description_id="otel-invocation-11",
        function_name="caller",
        execution_arn="arn:caller",
        invocation_started_at_ms=1,
        invocation_finished_at_ms=2,
        region="us-west-2",
        language="python",
        requirement={"TelemetryAssertions": ASSERTIONS},
        execution_history={
            "Events": [
                {"EventType": "ChainedInvokeStarted", "ChainedInvokeStartedDetails": {"DurableExecutionArn": CHILD}}
            ]
        },
        output_dir=tmp_path,
        options={
            "otel_backend": "test",
            "otel_service_name": "service",
            "otel_poll_timeout": 1,
            "otel_poll_interval": 0,
            "otel_poll_attempts": 1,
            "otel_write_trace_artifact": False,
        },
        aws_clients={"lambda": client},
    )
    errors = OtelExtension().validate_telemetry(context)
    assert bool(errors) is wrong_parent, errors
    client.get_durable_execution.assert_called_once_with(DurableExecutionArn=CHILD)
    evidence = json.loads((tmp_path / "otel-invocation-11-otel.json").read_text())
    assert evidence["full_trace_ancestry_checked"] is full_ancestry
    assert evidence["execution_trace_contexts"][CHILD] == {
        "trace_id": "1" * 32,
        "parent_span_id": ("5" if wrong_parent else "2") * 16,
        "sampled": True,
    }
    assert len(evidence["spans"]) == len(trace.spans)
