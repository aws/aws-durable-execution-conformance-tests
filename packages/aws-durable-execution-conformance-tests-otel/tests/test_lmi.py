# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Positive controls and regression-sensitivity checks for LMI telemetry."""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from typing import Any, Literal

import pytest

from aws_durable_execution_conformance_tests_otel.lmi import validate_lmi_telemetry
from aws_durable_execution_conformance_tests_otel.model import Span, span_to_dict


def evidence(
    runtime: str = "javascript",
    requirement: str = "lmi-lifecycle-7",
    *,
    supplied_attempts: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    attempts: list[dict[str, Any]] = [
        {
            "requestId": request_id,
            "executionArn": f"arn:execution:{request_id}",
            "environmentId": "environment-1",
            "workerId": f"worker-{index}" if runtime == "python" else "worker-1",
            "processId": f"process-{index}" if runtime == "python" else "process-1",
            "startedAtMs": 1000,
            "endedAtMs": 2000,
        }
        for index, request_id in enumerate(("victim", "peer"), start=1)
    ]
    if requirement == "lmi-lifecycle-8":
        attempts.append(
            {
                **attempts[0],
                "requestId": "warm",
                "executionArn": "arn:execution:warm",
                "startedAtMs": 2100,
                "endedAtMs": 2200,
            }
        )
    if supplied_attempts is not None:
        attempts = supplied_attempts
    request_ids = [attempt["requestId"] for attempt in attempts]
    source = {"javascript": "invocation-context", "java": "runtime-caller-context", "python": "process-environment"}[
        runtime
    ]
    views = {}
    for view in ("invocation", "execution"):
        carriers = []
        spans = []
        assertions = {}
        for index, attempt in enumerate(attempts, start=1):
            trace_id = str(index) * 32
            carriers.append(
                {
                    "requestId": attempt["requestId"],
                    "environmentId": attempt["environmentId"],
                    "processId": attempt["processId"],
                    "workerId": attempt["workerId"],
                    "source": source,
                    "header": f"Root=1-{trace_id[:8]}-{trace_id[8:]};Parent={'a' * 16};Sampled=1",
                    "expectedSampled": True,
                }
            )
            name = "Invocation" if view == "invocation" else "Workflow"
            span = Span(
                trace_id=trace_id,
                span_id=str(index) * 16,
                name=name,
                start_time=datetime.fromtimestamp(attempt["startedAtMs"] / 1000, UTC),
                end_time=datetime.fromtimestamp(attempt["endedAtMs"] / 1000, UTC),
                parent_span_id="a" * 16,
                status="OK",
                attributes={
                    "durable.execution.arn": attempt["executionArn"],
                    "faas.invocation_id": attempt["requestId"],
                    "durable.invocation.first": True,
                },
            )
            spans.append({"requestId": attempt["requestId"], "span": span_to_dict(span)})
            assertions[attempt["executionArn"]] = {
                "require_unique_root_per_trace": True,
                "span_assertions": [{"select": {"name": name}, "expect": {"status": "OK"}}],
            }
        views[view] = {
            "collection": {"status": "complete", "scope": "cohort", "requestIds": request_ids},
            "carriers": carriers,
            "spans": spans,
            "assertions": assertions,
            "exportOutcomes": [{"requestId": request_id, "status": "delivered"} for request_id in request_ids],
        }
    provider_ids = {attempt["processId"] for attempt in attempts}
    telemetry = {
        "views": views,
        "providers": [
            {
                "environmentId": "environment-1",
                "processId": process_id,
                "providerId": f"provider-{process_id}",
                "exporterId": f"exporter-{process_id}",
                "createdAtMs": 0,
            }
            for process_id in provider_ids
        ],
        "providerBindings": [
            {"requestId": attempt["requestId"], "providerId": f"provider-{attempt['processId']}"}
            for attempt in attempts
        ],
        "flushes": [
            {
                "requestId": attempt["requestId"],
                "startedAtMs": attempt["endedAtMs"] - 1,
                "finishedAtMs": attempt["endedAtMs"],
                "budgetMs": 150,
                "outcome": "completed",
            }
            for attempt in attempts
        ],
    }
    return {
        "runtime": runtime,
        "requirementId": requirement,
        "artifact": {"plugin": "sha256:plugin-1"},
        "selectedRequests": {attempt.get("role", attempt["requestId"]): attempt["requestId"] for attempt in attempts},
        "observationEndedAtMs": max(attempt["endedAtMs"] for attempt in attempts) + 100,
        "attempts": attempts,
        "telemetry": telemetry,
    }


@pytest.mark.parametrize("runtime", ["javascript", "java", "python"])
@pytest.mark.parametrize("requirement", ["lmi-lifecycle-7", "lmi-lifecycle-8"])
@pytest.mark.parametrize("phase", ["observations", "final"])
def test_valid_cohort_uses_both_existing_views(
    runtime: str, requirement: str, phase: Literal["observations", "final"]
) -> None:
    assert validate_lmi_telemetry(evidence(runtime, requirement), phase=phase) == []


@pytest.mark.parametrize("view", ["invocation", "execution"])
def test_swapped_execution_attribution_is_checked_before_filtering(view: str) -> None:
    data = evidence()
    spans = data["telemetry"]["views"][view]["spans"]
    spans[0]["span"]["attributes"]["durable.execution.arn"] = "arn:execution:peer"
    assert any("execution attribution" in error for error in validate_lmi_telemetry(data))


def test_late_activity_retains_original_request() -> None:
    data = evidence()
    view = data["telemetry"]["views"]["invocation"]
    late = deepcopy(view["spans"][0])
    late["span"]["span_id"] = "f" * 16
    late["span"]["attributes"]["faas.invocation_id"] = "peer"
    view["spans"].append(late)
    assert any("another invocation's request ID" in error for error in validate_lmi_telemetry(data))


@pytest.mark.parametrize("missing", ["invocation", "execution"])
def test_missing_view_cannot_pass(missing: str) -> None:
    data = evidence()
    del data["telemetry"]["views"][missing]
    assert any(f"missing {missing} view" in error for error in validate_lmi_telemetry(data))


@pytest.mark.parametrize("status", ["missing", "export-error", "timeout", "partial"])
def test_collection_failure_never_means_unsampled(status: str) -> None:
    data = evidence()
    data["telemetry"]["views"]["invocation"]["collection"]["status"] = status
    assert any("collection did not complete" in error for error in validate_lmi_telemetry(data))


def test_filtered_collection_cannot_hide_peer_spans() -> None:
    data = evidence()
    data["telemetry"]["views"]["invocation"]["collection"]["requestIds"] = ["victim"]
    assert any("without execution filtering" in error for error in validate_lmi_telemetry(data))


@pytest.mark.parametrize("source", ["input", "compatibility", "process-environment"])
def test_node_carrier_must_come_from_runtime_context(source: str) -> None:
    data = evidence()
    data["telemetry"]["views"]["invocation"]["carriers"][0]["source"] = source
    assert any("genuine javascript runtime carrier" in error for error in validate_lmi_telemetry(data))


@pytest.mark.parametrize(
    "header",
    [None, "", "Root=broken", "Root=1-11111111-111111111111111111111111;Parent=aaaaaaaaaaaaaaaa;Sampled=1;Sampled=0"],
)
def test_bad_runtime_carriers_remain_incomplete(header: Any) -> None:
    data = evidence()
    data["telemetry"]["views"]["invocation"]["carriers"][0]["header"] = header
    assert validate_lmi_telemetry(data)[0].startswith("INCOMPLETE:")


def test_worker_substitution_is_not_runtime_carrier_evidence() -> None:
    data = evidence("python")
    data["telemetry"]["views"]["execution"]["carriers"][0]["processId"] = "replacement-process"
    assert any("wrong processId" in error for error in validate_lmi_telemetry(data))


def test_genuine_parent_correlation_is_required() -> None:
    data = evidence()
    data["telemetry"]["views"]["invocation"]["spans"][0]["span"]["parent_span_id"] = "b" * 16
    assert any("does not reach the runtime parent" in error for error in validate_lmi_telemetry(data))


def test_observed_runtime_ancestry_supports_auto_instrumentation() -> None:
    data = evidence()
    view = data["telemetry"]["views"]["invocation"]
    sdk = view["spans"][0]["span"]
    ambient = {**deepcopy(sdk), "name": "lambda runtime", "span_id": "b" * 16, "attributes": {}}
    sdk["parent_span_id"] = ambient["span_id"]
    view["infrastructureSpans"] = [ambient]
    assert validate_lmi_telemetry(data) == []


def test_ordinary_span_assertions_still_run() -> None:
    data = evidence()
    data["telemetry"]["views"]["execution"]["spans"][0]["span"]["status"] = "ERROR"
    assert any("status" in error for error in validate_lmi_telemetry(data))


def test_missing_assertions_cannot_appear_as_coverage() -> None:
    data = evidence()
    data["telemetry"]["views"]["execution"]["assertions"] = {}
    assert any("missing ordinary span assertions" in error for error in validate_lmi_telemetry(data))


def test_explicit_unsampled_fixture_is_distinct_from_missing_evidence() -> None:
    data = evidence(requirement="lmi-lifecycle-8")
    view = data["telemetry"]["views"]["invocation"]
    view["carriers"][0]["header"] = view["carriers"][0]["header"].replace("Sampled=1", "Sampled=0")
    view["carriers"][0]["expectedSampled"] = False
    view["spans"].pop(0)
    view["exportOutcomes"][0]["status"] = "unsampled"
    assert validate_lmi_telemetry(data) == []
    data["requirementId"] = "lmi-lifecycle-7"
    assert any("requires sampled delivery" in error for error in validate_lmi_telemetry(data))


@pytest.mark.parametrize(
    "status,fixture,flush",
    [
        ("export-error", "bounded-export-error", "error"),
        ("timeout-loss", "bounded-flush-timeout", "timeout"),
    ],
)
def test_bounded_export_fixture_preserves_healthy_peer(status: str, fixture: str, flush: str) -> None:
    data = evidence(requirement="lmi-lifecycle-8")
    view = data["telemetry"]["views"]["invocation"]
    view["spans"].pop(0)
    view["exportOutcomes"][0].update(status=status, fixture=fixture, error="observed transport failure")
    data["telemetry"]["flushes"][0].update(outcome=flush, error="observed transport failure")
    assert validate_lmi_telemetry(data) == []
    data["telemetry"]["flushes"][0]["outcome"] = "completed"
    assert any("no matching bounded flush" in error for error in validate_lmi_telemetry(data))


def test_export_loss_cannot_establish_attribution() -> None:
    data = evidence()
    data["telemetry"]["views"]["invocation"]["exportOutcomes"][0].update(
        status="export-error",
        fixture="bounded-export-error",
        error="observed error",
    )
    assert any("cannot establish attribution coverage" in error for error in validate_lmi_telemetry(data))


def test_invocation_cannot_replace_process_provider() -> None:
    data = evidence(requirement="lmi-lifecycle-8")
    provider = {**data["telemetry"]["providers"][0], "providerId": "replacement"}
    data["telemetry"]["providers"].append(provider)
    data["telemetry"]["providerBindings"][1]["providerId"] = "replacement"
    assert any("replaced their shared provider" in error for error in validate_lmi_telemetry(data))


def test_provider_cannot_shutdown_while_peer_is_active() -> None:
    data = evidence(requirement="lmi-lifecycle-8")
    data["telemetry"]["providers"][0]["shutdownAtMs"] = 1500
    assert any("shut down while its invocation was active" in error for error in validate_lmi_telemetry(data))


def test_unbounded_flush_fails_declared_budget() -> None:
    data = evidence(requirement="lmi-lifecycle-8")
    data["telemetry"]["flushes"][0]["finishedAtMs"] = 3000
    assert any("exceeded its declared budget" in error for error in validate_lmi_telemetry(data))


@pytest.mark.parametrize("field", ["plugin", "views", "attempts"])
def test_missing_identity_or_observations_fail_closed(field: str) -> None:
    data = evidence()
    if field == "plugin":
        data["artifact"].pop(field)
    elif field == "views":
        data["telemetry"].pop(field)
    else:
        data.pop(field)
    assert validate_lmi_telemetry(data)[0].startswith("INCOMPLETE:")


def test_collector_cohort_keeps_spans_with_wrong_execution_attribution() -> None:
    import io
    import json

    from aws_durable_execution_conformance_tests_otel.backends.collector import CollectorBackend
    from aws_durable_execution_conformance_tests_otel.model import TelemetryQuery

    document = {
        "resourceSpans": [
            {
                "scopeSpans": [
                    {
                        "spans": [
                            {
                                "traceId": str(index) * 32,
                                "spanId": str(index) * 16,
                                "name": "Invocation",
                                "startTimeUnixNano": "1767225600000000000",
                                "endTimeUnixNano": "1767225601000000000",
                                "attributes": [{"key": "durable.execution.arn", "value": {"stringValue": arn}}],
                            }
                            for index, arn in enumerate(("arn:expected", "arn:wrong-execution"), start=1)
                        ]
                    }
                ]
            }
        ]
    }

    class Client:
        def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
            assert kwargs == {"Bucket": "test-bucket", "Prefix": "run/case/"}
            return {"Contents": [{"Key": "run/case/traces_1.json"}]}

        def get_object(self, **kwargs: Any) -> dict[str, Any]:
            return {"Body": io.BytesIO(json.dumps(document).encode())}

    backend = CollectorBackend(Client(), "test-bucket", "run/case/")
    traces = backend.collect_cohort()
    assert {span.attributes["durable.execution.arn"] for trace in traces for span in trace.spans} == {
        "arn:expected",
        "arn:wrong-execution",
    }
    now = datetime(2026, 1, 1, tzinfo=UTC)
    ordinary = backend._lookup(TelemetryQuery("arn:expected", "test", now, now))
    assert ordinary is not None
    assert len(ordinary.spans) == 1


def test_collector_cohort_requires_resource_ownership_scope() -> None:
    from aws_durable_execution_conformance_tests_otel.backends.collector import CollectorBackend
    from aws_durable_execution_conformance_tests_otel.polling import BackendError

    with pytest.raises(BackendError, match="owned S3 prefix"):
        CollectorBackend(object(), "shared-bucket").collect_cohort()


def test_collector_incomplete_pagination_cannot_be_complete_cohort() -> None:
    from aws_durable_execution_conformance_tests_otel.backends.collector import CollectorBackend
    from aws_durable_execution_conformance_tests_otel.polling import BackendError

    class Client:
        def list_objects_v2(self, **kwargs: Any) -> dict[str, Any]:
            return {"IsTruncated": True}

    with pytest.raises(BackendError, match="truncated"):
        CollectorBackend(Client(), "bucket", "run/case/").collect_cohort()


def test_user_span_can_inherit_execution_attribution_from_observed_parent() -> None:
    data = evidence()
    view = data["telemetry"]["views"]["invocation"]
    user = deepcopy(view["spans"][0])
    user["span"].update(name="user code", span_id="f" * 16, parent_span_id="1" * 16, attributes={})
    view["spans"].append(user)
    assert validate_lmi_telemetry(data) == []
    user["span"]["parent_span_id"] = "b" * 16
    assert any("lacks observed execution ancestry" in error for error in validate_lmi_telemetry(data))


def test_cross_execution_parent_is_rejected_even_on_a_shared_runtime_trace() -> None:
    data = evidence()
    view = data["telemetry"]["views"]["invocation"]
    view["spans"][1]["span"]["trace_id"] = "1" * 32
    view["carriers"][1]["header"] = view["carriers"][0]["header"]
    view["spans"][0]["span"]["parent_span_id"] = "2" * 16
    assert any("parent belonging to a peer execution" in error for error in validate_lmi_telemetry(data))


def test_sampling_is_checked_against_configured_expectation() -> None:
    data = evidence()
    data["telemetry"]["views"]["execution"]["carriers"][0]["expectedSampled"] = False
    assert any("differs from the configured decision" in error for error in validate_lmi_telemetry(data))


def combined_evidence(runtime: str, requirement: str) -> dict[str, Any]:
    """Reuse the core lifecycle control and attach canonical OTel observations."""
    import runpy
    from pathlib import Path

    core_tests = Path(__file__).parents[2] / "aws-durable-execution-conformance-tests/tests/test_lmi_evidence.py"
    core = runpy.run_path(str(core_tests))["warm_evidence"](runtime)
    core["requirementId"] = requirement
    core["artifact"]["plugin"] = "sha256:plugin-1"
    core["telemetry"] = evidence(runtime, requirement, supplied_attempts=core["attempts"])["telemetry"]
    late_event = next(event for event in core["events"] if event["kind"] == "late_activity")
    late_event["telemetry"] = {}
    for name, view in core["telemetry"]["views"].items():
        late = deepcopy(view["spans"][0])
        late["span"].update(name="delayed task", span_id="f" * 16, parent_span_id="1" * 16)
        late["span"]["start_time"] = datetime.fromtimestamp(0.06, UTC).isoformat()
        late["span"]["end_time"] = datetime.fromtimestamp(0.065, UTC).isoformat()
        view["spans"].append(late)
        late_event["telemetry"][name] = {"traceId": late["span"]["trace_id"], "spanId": late["span"]["span_id"]}
    return core


@pytest.mark.parametrize("runtime", ["javascript", "java", "python"])
@pytest.mark.parametrize("requirement", ["lmi-lifecycle-7", "lmi-lifecycle-8"])
@pytest.mark.parametrize("phase", ["observations", "final"])
def test_combined_lifecycle_and_otel_evidence_accepts_original_overlap_and_warm_reuse(
    runtime: str, requirement: str, phase: Literal["observations", "final"]
) -> None:
    from aws_durable_execution_conformance_tests.lmi.evidence import validate_evidence

    data = combined_evidence(runtime, requirement)
    assert validate_evidence(data, phase=phase) == []
    assert validate_lmi_telemetry(data, phase=phase) == []


def test_combined_telemetry_cannot_replace_missing_native_lifecycle_evidence() -> None:
    from aws_durable_execution_conformance_tests.lmi.evidence import validate_evidence

    data = combined_evidence("python", "lmi-lifecycle-7")
    data["events"] = [event for event in data["events"] if event["kind"] != "late_activity"]
    assert any("late_activity" in error for error in validate_evidence(data, phase="observations"))
    assert validate_lmi_telemetry(data, phase="observations") == []


def test_combined_warm_late_span_keeps_original_request_attribution() -> None:
    from aws_durable_execution_conformance_tests.lmi.evidence import validate_evidence

    data = combined_evidence("javascript", "lmi-lifecycle-7")
    spans = data["telemetry"]["views"]["invocation"]["spans"]
    late = next(record for record in spans if record["span"]["name"] == "delayed task")
    assert validate_evidence(data, phase="observations") == []
    assert validate_lmi_telemetry(data, phase="observations") == []
    late["span"]["attributes"]["faas.invocation_id"] = "request-warm"
    assert any(
        "another invocation's request ID" in error for error in validate_lmi_telemetry(data, phase="observations")
    )


def test_old_span_cannot_satisfy_current_lifecycle_window() -> None:
    data = combined_evidence("java", "lmi-lifecycle-7")
    span = data["telemetry"]["views"]["invocation"]["spans"][0]["span"]
    span["start_time"] = datetime.fromtimestamp(0, UTC).isoformat()
    assert any("outside its lifecycle observation window" in error for error in validate_lmi_telemetry(data))


def test_flush_cannot_be_counted_from_an_unrelated_observation_window() -> None:
    data = combined_evidence("python", "lmi-lifecycle-8")
    flush = data["telemetry"]["flushes"][0]
    flush["startedAtMs"] += 1000
    flush["finishedAtMs"] += 1000
    assert any("outside its lifecycle observation window" in error for error in validate_lmi_telemetry(data))


def test_provider_lifetime_requires_sequential_reuse_not_only_overlap() -> None:
    data = evidence(requirement="lmi-lifecycle-8")
    data["attempts"][-1]["startedAtMs"] = 1000
    assert any("sequential worker/process reuse" in error for error in validate_lmi_telemetry(data))


@pytest.mark.parametrize("view", ["invocation", "execution"])
@pytest.mark.parametrize("corruption", ["missing-marker", "missing-span", "wrong-owner", "old-span"])
def test_late_activity_must_bind_actual_original_invocation_span(view: str, corruption: str) -> None:
    data = combined_evidence("python", "lmi-lifecycle-7")
    event = next(event for event in data["events"] if event["kind"] == "late_activity")
    records = data["telemetry"]["views"][view]["spans"]
    late = next(record for record in records if record["span"]["name"] == "delayed task")
    if corruption == "missing-marker":
        del event["telemetry"][view]
    elif corruption == "missing-span":
        records.remove(late)
    elif corruption == "wrong-owner":
        late["requestId"] = "request-warm"
    else:
        late["span"]["start_time"] = datetime.fromtimestamp(0.01, UTC).isoformat()
        late["span"]["end_time"] = datetime.fromtimestamp(0.02, UTC).isoformat()
    assert any("late_activity" in error for error in validate_lmi_telemetry(data, phase="observations"))


def test_late_probe_does_not_impose_exactly_once_export() -> None:
    data = combined_evidence("javascript", "lmi-lifecycle-7")
    spans = data["telemetry"]["views"]["invocation"]["spans"]
    spans.append(deepcopy(next(record for record in spans if record["span"]["name"] == "delayed task")))
    assert validate_lmi_telemetry(data, phase="observations") == []
