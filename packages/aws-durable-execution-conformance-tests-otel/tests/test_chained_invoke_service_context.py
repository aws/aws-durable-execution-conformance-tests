# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""The child service carrier is independent of outgoing header construction."""

import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from aws_durable_execution_conformance_tests.extensions import ValidationContext
from aws_durable_execution_conformance_tests.validate import load_yaml_file
from aws_durable_execution_conformance_tests.variables import PlaceholderContext
from aws_durable_execution_conformance_tests_otel.chained_invoke import (
    execution_trace_context,
    load_execution_trace_contexts,
)
from aws_durable_execution_conformance_tests_otel.extension import OtelExtension
from aws_durable_execution_conformance_tests_otel.model import (
    ExecutionTraceContext,
    Span,
    SpanLink,
    TelemetryQuery,
    Trace,
)
from aws_durable_execution_conformance_tests_otel.polling import (
    BackendError,
    BackendFeatureDisparity,
    PollingBackend,
    RetryableBackendError,
)
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


def test_case11_service_invoke_end_may_exceed_sdk_workflow_clock() -> None:
    suite = next(s for s in OtelExtension().requirement_suites() if s.name == "otel-execution")
    requirement = load_yaml_file(str(suite.root / "otel-execution-11.yaml"))
    context = PlaceholderContext()
    for name, value in {"EXECUTION_ARN": "arn:caller", "SERVICE_NAME": "service", "INVOKE1": "invoke"}.items():
        context.bind(name, value)
    assertion = context.substitute(
        next(
            item
            for item in requirement["TelemetryAssertions"]["span_assertions"]
            if item["select"]["name"] == "otel-invoke"
        )
    )
    trace, query = _fixture()
    original = trace.spans[0]
    start = original.start_time
    workflow = replace(
        original,
        name="Workflow",
        span_id="5" * 16,
        end_time=start + timedelta(seconds=4),
        kind="INTERNAL",
        status="OK",
        service_name="service",
        attributes={"durable.execution.arn": "arn:caller", "durable.execution.status": "SUCCEEDED"},
    )
    first = replace(
        workflow,
        name="Invocation",
        span_id="6" * 16,
        end_time=start + timedelta(seconds=1),
        attributes={"durable.execution.arn": "arn:caller"},
    )
    second = replace(
        first, span_id="7" * 16, start_time=start + timedelta(seconds=2), end_time=start + timedelta(seconds=4)
    )
    invoke = replace(
        original,
        parent_span_id=workflow.span_id,
        end_time=start + timedelta(seconds=4.2),
        kind="INTERNAL",
        status="OK",
        service_name="service",
        links=(SpanLink(original.trace_id, second.span_id),),
        attributes={
            "durable.execution.arn": "arn:caller",
            "durable.operation.id": "invoke",
            "durable.operation.type": "CHAINED_INVOKE",
            "durable.operation.subtype": "ChainedInvoke",
            "durable.operation.name": "otel-invoke",
            "durable.operation.status": "SUCCEEDED",
        },
    )
    trace = replace(trace, spans=(workflow, first, second, invoke))
    assertions = {"span_assertions": [assertion]}
    assert validate_trace(trace, assertions, query) == []
    wrong_parent = replace(invoke, parent_span_id=second.span_id)
    assert validate_trace(replace(trace, spans=(workflow, first, second, wrong_parent)), assertions, query)
    wrong_phase = replace(invoke, links=(SpanLink(original.trace_id, first.span_id),))
    assert validate_trace(replace(trace, spans=(workflow, first, second, wrong_phase)), assertions, query)


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


@pytest.mark.parametrize("normalized", [False, True])
def test_chained_parent_status_selects_first_segment_with_declared_disparity(normalized: bool) -> None:
    trace, query = _fixture()
    first = replace(trace.spans[0], status="OK" if normalized else "UNSET")
    replay = replace(
        first,
        span_id="5" * 16,
        status="OK",
        start_time=first.start_time + timedelta(seconds=1),
        end_time=first.end_time + timedelta(seconds=1),
        attributes={**first.attributes, "durable.operation.status": "SUCCEEDED"},
    )
    trace = replace(trace, spans=(replay, first, *trace.spans[1:]))
    assertions = {
        **ASSERTIONS,
        "chained_invokes": [
            {"child_execution_arn": CHILD, "parent": {"name": "otel-invoke", "status": "UNSET"}, "sampled": True}
        ],
    }
    disparities = (BackendFeatureDisparity.UNSET_STATUS,)
    if normalized:
        assert validate_trace(trace, assertions, query) == [
            "chained_invokes[0].parent must select exactly one span; found 0"
        ]
    assert validate_trace(trace, assertions, query, feature_disparities=disparities) == []
    wrong_carrier = replace(query.execution_trace_contexts[CHILD], parent_span_id=replay.span_id)
    errors = validate_trace(
        trace,
        assertions,
        replace(query, execution_trace_contexts={CHILD: wrong_carrier}),
        feature_disparities=disparities,
    )
    assert errors == ["chained_invokes[0]: child service Parent differs from the selected first invoke span ID"]


def test_chained_parent_prefers_exact_status_over_an_earlier_fallback_match() -> None:
    trace, query = _fixture()
    fallback = replace(
        trace.spans[0],
        span_id="5" * 16,
        status="OK",
        start_time=trace.spans[0].start_time - timedelta(seconds=1),
        end_time=trace.spans[0].end_time - timedelta(seconds=1),
    )
    assertions = {
        **ASSERTIONS,
        "chained_invokes": [
            {"child_execution_arn": CHILD, "parent": {"name": "otel-invoke", "status": "UNSET"}, "sampled": True}
        ],
    }
    assert (
        validate_trace(
            replace(trace, spans=(fallback, *trace.spans)),
            assertions,
            query,
            feature_disparities=(BackendFeatureDisparity.UNSET_STATUS,),
        )
        == []
    )


@pytest.mark.parametrize("status", ["*", "${/.*/}", {"$any_of": ["UNSET", "OK"]}])
def test_chained_parent_keeps_ambiguous_status_matchers_strict(status: object) -> None:
    trace, query = _fixture()
    replay = replace(trace.spans[0], span_id="5" * 16, status="OK")
    assertions = {
        **ASSERTIONS,
        "chained_invokes": [
            {"child_execution_arn": CHILD, "parent": {"name": "otel-invoke", "status": status}, "sampled": True}
        ],
    }
    assert validate_trace(
        replace(trace, spans=(*trace.spans, replay)),
        assertions,
        query,
        feature_disparities=(BackendFeatureDisparity.UNSET_STATUS,),
    ) == ["chained_invokes[0].parent must select exactly one span; found 2"]


@pytest.mark.parametrize("field", ["Root", "Sampled", "missing"])
def test_chained_parent_status_disparity_keeps_actual_service_context_strict(field: str) -> None:
    trace, query = _fixture()
    trace = replace(trace, spans=(replace(trace.spans[0], status="OK"), *trace.spans[1:]))
    assertions = {
        **ASSERTIONS,
        "chained_invokes": [
            {"child_execution_arn": CHILD, "parent": {**PARENT_SELECTOR, "status": "UNSET"}, "sampled": True}
        ],
    }
    carrier = query.execution_trace_contexts[CHILD]
    if field == "Root":
        carrier = replace(carrier, trace_id="9" * 32)
    elif field == "Sampled":
        carrier = replace(carrier, sampled=False)
    errors = validate_trace(
        trace,
        assertions,
        replace(query, execution_trace_contexts={} if field == "missing" else {CHILD: carrier}),
        feature_disparities=(BackendFeatureDisparity.UNSET_STATUS,),
    )
    assert any(field in error for error in errors), errors
    assert not any("must select exactly one" in error for error in errors), errors


def test_chained_parent_status_disparity_keeps_full_backend_ancestry_strict() -> None:
    trace, query = _fixture()
    first = replace(trace.spans[0], status="OK")
    infrastructure = replace(first, span_id="f" * 16, name="Lambda", parent_span_id=first.span_id, attributes={})
    assertions = {
        **ASSERTIONS,
        "chained_invokes": [
            {"child_execution_arn": CHILD, "parent": {**PARENT_SELECTOR, "status": "UNSET"}, "sampled": True}
        ],
    }
    trace = replace(trace, spans=(first, *trace.spans[1:], infrastructure))
    disparities = (BackendFeatureDisparity.UNSET_STATUS,)
    assert validate_trace(trace, assertions, query, feature_disparities=disparities, full_trace_ancestry=True) == []
    errors = validate_trace(
        replace(trace, spans=(*trace.spans[:-1], replace(infrastructure, parent_span_id=None))),
        assertions,
        query,
        feature_disparities=disparities,
        full_trace_ancestry=True,
    )
    assert len(errors) == 2 and all("does not reach ancestor" in error for error in errors), errors


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


@pytest.mark.parametrize("view", ["execution", "invocation"])
@pytest.mark.parametrize("parent_kind", ["first", "platform", "unresolved", "replay", "other-invoke", "invocation"])
def test_catalog_target_parent_keeps_sdk_identity_even_with_correct_service_carrier(
    view: str, parent_kind: str
) -> None:
    suite = next(suite for suite in OtelExtension().requirement_suites() if suite.name == f"otel-{view}")
    requirement = load_yaml_file(str(suite.root / f"otel-{view}-11.yaml"))
    context = PlaceholderContext()
    for name, value in {"EXECUTION_ARN": "arn:caller", "TARGET_EXECUTION_ARN": CHILD, "INVOKE1": "invoke-1"}.items():
        context.bind(name, value)
    catalog = context.substitute(requirement["TelemetryAssertions"])
    target = next(
        item
        for item in catalog["span_assertions"]
        if item["select"].get("name") == "Workflow"
        and item["select"].get("attributes", {}).get("durable.execution.arn") == CHILD
    )
    assertions = {
        "allowed_execution_arns": catalog["allowed_execution_arns"],
        "chained_invokes": catalog["chained_invokes"],
        "span_assertions": {
            "select": {"name": "Workflow", "attributes": {"durable.execution.arn": CHILD}},
            "expect": {"parent": target["expect"]["parent"], "same_trace_as": target["expect"]["same_trace_as"]},
        },
    }
    trace, query = _fixture()
    first = replace(
        trace.spans[0],
        attributes={
            "durable.execution.arn": "arn:caller",
            "durable.operation.id": "invoke-1",
            "durable.operation.status": "STARTED" if view == "invocation" else "SUCCEEDED",
        },
    )
    other = replace(first, span_id="5" * 16)
    if parent_kind == "platform":
        other = replace(other, name="Lambda", attributes={})
    elif parent_kind == "replay":
        other = replace(other, attributes={**first.attributes, "durable.operation.status": "SUCCEEDED"})
    elif parent_kind == "other-invoke":
        other = replace(other, attributes={**first.attributes, "durable.operation.id": "invoke-other"})
    elif parent_kind == "invocation":
        other = replace(other, name="Invocation", attributes={"durable.execution.arn": "arn:caller"})
    # A logical execution-view invoke must remain unique; replay duplicates are
    # already rejected by the service-parent selector's exact-one requirement.
    child = replace(
        trace.spans[1],
        parent_span_id=first.span_id if parent_kind == "first" else other.span_id,
        start_time=first.end_time + timedelta(seconds=2),
        end_time=first.end_time + timedelta(seconds=3),
    )
    spans: tuple[Span, ...] = (first, child, trace.spans[2])
    if parent_kind not in {"first", "unresolved"}:
        spans += (other,)
    errors = validate_trace(replace(trace, spans=spans), assertions, query)
    if parent_kind in {"first", "platform", "unresolved"}:
        assert errors == [], errors
    else:
        assert errors, "A correct service carrier must not hide incorrect resolved SDK parentage"
        if not (view == "execution" and parent_kind == "replay"):
            assert any("parent" in error for error in errors), errors


@pytest.mark.parametrize("full_ancestry", [False, True])
@pytest.mark.parametrize("wrong_parent", [False, True])
@pytest.mark.parametrize("has_trace", [False, True])
def test_extension_reads_and_preserves_actual_service_evidence_on_pass_and_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, full_ancestry: bool, wrong_parent: bool, has_trace: bool
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

        def _lookup(self, _query: TelemetryQuery) -> Trace | None:
            return trace if has_trace else None

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
    assert bool(errors) is (wrong_parent or not has_trace), errors
    client.get_durable_execution.assert_called_once_with(DurableExecutionArn=CHILD)
    evidence = json.loads((tmp_path / "otel-invocation-11-otel.json").read_text())
    assert evidence["full_trace_ancestry_checked"] is (full_ancestry and has_trace)
    assert evidence["execution_trace_contexts"][CHILD] == {
        "trace_id": "1" * 32,
        "parent_span_id": ("5" if wrong_parent else "2") * 16,
        "sampled": True,
    }
    if has_trace:
        assert len(evidence["spans"]) == len(trace.spans)
    else:
        assert evidence["trace_unavailable"] is True
        assert "trace_id" not in evidence
        assert "spans" not in evidence


@pytest.mark.parametrize(
    "shape",
    [
        "complete",
        "ambiguous-parent",
        "missing-Workflow",
        "missing-Invocation",
        "missing-both",
        "ambiguous-after-complete",
    ],
)
def test_artifact_reports_only_ancestry_checks_that_ran(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, shape: str
) -> None:
    trace, _ = _fixture()
    infrastructure = replace(trace.spans[0], span_id="f" * 16, name="Lambda", parent_span_id="2" * 16, attributes={})
    complete = replace(trace, spans=(*trace.spans, infrastructure))
    trace = complete
    if shape.startswith("ambiguous"):
        trace = replace(trace, spans=(*trace.spans, replace(trace.spans[0], span_id="6" * 16)))
    elif shape.startswith("missing"):
        missing = {"Workflow", "Invocation"} if shape == "missing-both" else {shape.removeprefix("missing-")}
        trace = replace(trace, spans=tuple(span for span in trace.spans if span.name not in missing))

    class Backend(PollingBackend):
        name = "test"
        provides_infrastructure_ancestry = True
        attempts = 0

        def _lookup(self, _query: TelemetryQuery) -> Trace | None:
            self.attempts += 1
            return complete if shape == "ambiguous-after-complete" and self.attempts == 1 else trace

    backend = Backend(monotonic=lambda: 0, sleep=lambda _delay: None)
    monkeypatch.setattr(
        OtelExtension,
        "_backends",
        staticmethod(lambda: {"test": SimpleNamespace(create=lambda *_args, **_kwargs: backend)}),
    )
    client = Mock()
    client.get_durable_execution.return_value = {"TraceHeader": {"XAmznTraceId": HEADER}}
    context = ValidationContext(
        description_id="otel-invocation-11",
        function_name="caller",
        execution_arn="arn:caller",
        invocation_started_at_ms=1,
        invocation_finished_at_ms=2,
        region="us-west-2",
        language="python",
        requirement={
            "TelemetryAssertions": {**ASSERTIONS, "quiescence_seconds": 1 if shape == "ambiguous-after-complete" else 0}
        },
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
            "otel_poll_attempts": 2 if shape == "ambiguous-after-complete" else 1,
        },
        aws_clients={"lambda": client},
    )
    errors = OtelExtension().validate_telemetry(context)
    if shape == "complete":
        assert errors == []
    elif shape.startswith("ambiguous"):
        assert any("parent must select exactly one span; found 2" in error for error in errors)
    else:
        assert any("child Workflow and Invocation telemetry are required" in error for error in errors)
    evidence = json.loads((tmp_path / "otel-invocation-11-otel.json").read_text())
    assert evidence["full_trace_ancestry_checked"] is (shape == "complete")
    assert len(evidence["spans"]) == len(trace.spans)


@pytest.mark.parametrize("error_type", [BackendError, RetryableBackendError])
@pytest.mark.parametrize("has_service_context", [False, True])
def test_extension_preserves_service_context_after_backend_lookup_failure(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, error_type: type[BackendError], has_service_context: bool
) -> None:
    class FailingBackend(PollingBackend):
        name = "failing"
        provides_infrastructure_ancestry = True

        def _lookup(self, _query: TelemetryQuery) -> Trace | None:
            raise error_type("backend lookup failed")

    backend = FailingBackend(monotonic=lambda: 0, sleep=lambda _delay: None)
    monkeypatch.setattr(
        OtelExtension,
        "_backends",
        staticmethod(lambda: {"failing": SimpleNamespace(create=lambda *_args, **_kwargs: backend)}),
    )
    client = Mock()
    client.get_durable_execution.return_value = {"TraceHeader": {"XAmznTraceId": HEADER}}
    context = ValidationContext(
        description_id="otel-invocation-11",
        function_name="caller",
        execution_arn="arn:caller",
        invocation_started_at_ms=1,
        invocation_finished_at_ms=2,
        region="us-west-2",
        language="python",
        requirement={"TelemetryAssertions": ASSERTIONS if has_service_context else {}},
        execution_history={
            "Events": [
                {"EventType": "ChainedInvokeStarted", "ChainedInvokeStartedDetails": {"DurableExecutionArn": CHILD}}
            ]
        },
        output_dir=tmp_path,
        options={
            "otel_backend": "failing",
            "otel_service_name": "service",
            "otel_poll_timeout": 1,
            "otel_poll_interval": 0,
            "otel_poll_attempts": 1,
            "otel_write_trace_artifact": False,
        },
        aws_clients={"lambda": client},
    )
    assert OtelExtension().validate_telemetry(context) == [
        "OpenTelemetry backend validation failed: backend lookup failed"
    ]
    artifact = tmp_path / "otel-invocation-11-otel.json"
    if has_service_context:
        client.get_durable_execution.assert_called_once_with(DurableExecutionArn=CHILD)
        assert json.loads(artifact.read_text()) == {
            "trace_unavailable": True,
            "execution_trace_contexts": {CHILD: {"trace_id": "1" * 32, "parent_span_id": "2" * 16, "sampled": True}},
            "full_trace_ancestry_checked": False,
        }
    else:
        client.get_durable_execution.assert_not_called()
        assert not artifact.exists()
