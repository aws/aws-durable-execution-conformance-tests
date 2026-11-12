# SPDX-FileCopyrightText: 2026-present Amazon.com, Inc. or its affiliates.
#
# SPDX-License-Identifier: Apache-2.0
"""Export mode controls are the same for JavaScript, Java and Python."""

from __future__ import annotations

from copy import deepcopy
from datetime import UTC, datetime
from typing import Any

import pytest

from aws_durable_execution_conformance_tests.lmi.evidence import validate_common_evidence
from aws_durable_execution_conformance_tests_otel.lmi_exports import (
    EXPORT_MODES,
    EXPORT_SCENARIOS,
    validate_export_lifecycle,
)
from aws_durable_execution_conformance_tests_otel.model import Span, span_to_dict


def build_evidence(
    scenario: str, runtime: str = "python", native: int = 2, view: str = "invocation", export_mode: str = "sync"
) -> dict[str, Any]:
    """Complete synthetic evidence, including actual-vs-wait completion states."""
    attempts: list[dict[str, Any]] = [
        {
            "requestId": f"request-{role}",
            "executionArn": f"arn:execution:{role}",
            "role": role,
            "attempt": 1,
            "environmentId": "environment-1",
            "processId": "process-1",
            "workerId": "worker-1",
            "handlerOwnerId": "handler-1",
            "threadId": f"thread-{role}",
            "startedAtMs": 100 + index * 100,
            "endedAtMs": 160 if scenario == "held-export" and export_mode == "sync" else 140 + index * 100,
            "outcome": "SUCCEEDED",
        }
        for index, role in enumerate(("victim", "warm") if scenario == "export-failure-recovery" else ("victim",))
    ]
    events: list[dict[str, Any]] = []
    spans, carriers, faults = [], [], []
    outcomes: dict[tuple[str, str], str] = {}

    def event(attempt: dict[str, Any], kind: str, when: float, **details: Any) -> None:
        events.append(
            {
                "requestId": attempt["requestId"],
                "ownerRequestId": attempt["requestId"],
                "executionArn": attempt["executionArn"],
                "kind": kind,
                "atMs": when,
                "providerId": "provider-1",
                "exporterId": "exporter-1",
                **details,
            }
        )

    for index, attempt in enumerate(attempts):
        offset = index * 100
        export_id, flush_id = f"export-{index}", f"flush-{index}"
        failed = scenario == "export-failure-recovery" and index == 0
        outcome = "failed" if failed else "succeeded"
        outcomes[("export", export_id)] = outcome
        completed = (150 if scenario == "held-export" else 130) + offset
        event(attempt, "wrapper_enter", attempt["startedAtMs"])
        event(attempt, "export_started", 120 + offset, exportId=export_id)
        event(
            attempt,
            "export_completed",
            completed,
            exportId=export_id,
            outcome=outcome,
            **({"error": "observed export failure"} if failed else {}),
        )
        event(attempt, "wrapper_return", attempt["endedAtMs"], outcome="SUCCEEDED")
        if scenario == "held-export":
            event(attempt, "export_held", 121, exportId=export_id, gateId="gate-1")
            event(attempt, "export_released", 145, exportId=export_id, gateId="gate-1", mode="normal")
            faults.append({"id": "gate-1", "ownerCaseId": "case-1", "releasedAtMs": 145})
        if export_mode == "async-bounded-flush":
            held = scenario == "held-export"
            outcomes[("flush", flush_id)] = outcome
            event(attempt, "flush_started", 122 + offset, flushId=flush_id, pendingExportIds=[export_id])
            event(
                attempt,
                "flush_wait_completed",
                132 + offset,
                flushId=flush_id,
                outcome="timeout" if held else "error" if failed else "completed",
                pendingExportIds=[export_id] if held else [],
                **({"error": "observed flush error"} if failed else {}),
            )
            event(attempt, "flush_completed", completed + 1, flushId=flush_id, outcome=outcome, pendingExportIds=[])
        trace_id = f"{index + 1:032x}"
        header = f"Root=1-{trace_id[:8]}-{trace_id[8:]};Parent={'a' * 16};Sampled=1"
        carriers.append(
            {
                "requestId": attempt["requestId"],
                "environmentId": attempt["environmentId"],
                "workerId": attempt["workerId"],
                "processId": attempt["processId"],
                "source": {
                    "javascript": "invocation-context",
                    "java": "runtime-caller-context",
                    "python": "process-environment",
                }[runtime],
                "header": header,
                "expectedSampled": True,
            }
        )
        for number, name in enumerate(("Invocation", "Workflow"), start=1):
            span = Span(
                trace_id=trace_id,
                span_id=f"{index * 2 + number:016x}",
                name=name,
                start_time=datetime.fromtimestamp((101 + offset) / 1000, UTC),
                end_time=datetime.fromtimestamp((119 + offset) / 1000, UTC),
                parent_span_id="a" * 16,
                status="OK",
                attributes={"durable.execution.arn": attempt["executionArn"], "durable.invocation.first": True},
            )
            spans.append({"requestId": attempt["requestId"], "exportId": export_id, "span": span_to_dict(span)})
    for attempt in attempts:
        local = sorted(
            (item for item in events if item["requestId"] == attempt["requestId"]), key=lambda item: item["atMs"]
        )
        for sequence, item in enumerate(local):
            item["sequence"] = sequence
    resources = [{"kind": "invocation", "id": attempt["requestId"], "ownerCaseId": "case-1"} for attempt in attempts]
    resources.extend(
        {"kind": "execution", "id": attempt["executionArn"], "ownerCaseId": "case-1"} for attempt in attempts
    )
    resources.extend({"kind": kind, "id": identity, "ownerCaseId": "case-1"} for kind, identity in outcomes)
    expectations = {attempt["executionArn"]: {"status": "SUCCEEDED", "result": attempt["role"]} for attempt in attempts}
    return {
        "schemaVersion": 1,
        "coverageProfile": "export-lifecycle",
        "runtime": runtime,
        "requirementId": "lmi-lifecycle-16",
        "scenario": scenario,
        "otelView": view,
        "exportMode": export_mode,
        "runId": "run-1",
        "caseId": "case-1",
        "cohortId": "cohort-1",
        "artifact": {"sdk": "sdk-commit", "conformance": "conformance-commit", "plugin": "plugin-commit"},
        "profile": {
            "functionArn": f"arn:function:c{native}",
            "nativeConcurrency": native,
            "timeoutMs": 1000,
            "configurationDigest": "config-1",
        },
        "attempts": attempts,
        "selectedRequests": {attempt["role"]: attempt["requestId"] for attempt in attempts},
        "events": events,
        "observationEndedAtMs": 400,
        "sdkRequests": [],
        "expectedHistories": expectations,
        "histories": [
            {"executionArn": arn, "ownerCaseId": "case-1", "operations": [], **expected}
            for arn, expected in expectations.items()
        ],
        "collection": {
            "watermarks": {"lifecycle": 400, "sdkRequests": 400, "history": 400, "exportLifecycle": 400},
            "evidenceUris": ["s3://example/run-1/case-1/raw"],
        },
        "exportLifecycle": {
            "budgets": {
                "exportCompletionMs": 60,
                "observationGraceMs": 10,
                **({"flushWaitMs": 15} if export_mode == "async-bounded-flush" else {}),
            }
        },
        "resourcesCreated": resources,
        "faultsCreated": faults,
        "retirement": {
            "retiredAtMs": 420,
            "resources": [
                {
                    **resource,
                    "settledAtMs": 410,
                    "evidenceUri": "s3://example/run-1/case-1/settled",
                    "outcome": "QUIESCENT"
                    if resource["kind"] == "invocation"
                    else "SUCCEEDED"
                    if resource["kind"] == "execution"
                    else outcomes[(resource["kind"], resource["id"])],
                }
                for resource in resources
            ],
            "faults": [{**fault, "mode": "normal"} for fault in faults],
        },
        "telemetry": {
            "providers": [
                {
                    "providerId": "provider-1",
                    "exporterId": "exporter-1",
                    "environmentId": "environment-1",
                    "processId": "process-1",
                    "createdAtMs": 101,
                }
            ],
            "providerBindings": [
                {"requestId": attempt["requestId"], "providerId": "provider-1"} for attempt in attempts
            ],
            "views": {
                view: {
                    "collection": {
                        "status": "complete",
                        "scope": "cohort",
                        "requestIds": [attempt["requestId"] for attempt in attempts],
                    },
                    "carriers": carriers,
                    "spans": spans,
                }
            },
        },
    }


def find(value: dict[str, Any], kind: str, request: str = "request-victim") -> dict[str, Any]:
    return next(event for event in value["events"] if event["kind"] == kind and event["requestId"] == request)


def sequence_events(value: dict[str, Any]) -> None:
    """Update synthetic semantic ordering after moving boundary timestamps."""
    for attempt in value["attempts"]:
        local = sorted(
            (event for event in value["events"] if event["requestId"] == attempt["requestId"]),
            key=lambda event: (event["atMs"], event["sequence"]),
        )
        for sequence, event in enumerate(local):
            event["sequence"] = sequence


@pytest.mark.parametrize("scenario", EXPORT_SCENARIOS)
@pytest.mark.parametrize("runtime", ["javascript", "java", "python"])
@pytest.mark.parametrize("native", [1, 2])
@pytest.mark.parametrize("view", ["invocation", "execution"])
@pytest.mark.parametrize("export_mode", EXPORT_MODES)
def test_all_72_cells_validate_modes_and_shared_ownership(
    scenario: str, runtime: str, native: int, view: str, export_mode: str
) -> None:
    value = build_evidence(scenario, runtime, native, view, export_mode)
    assert validate_common_evidence(value, requirement_ids=("lmi-lifecycle-16",)) == []
    assert validate_export_lifecycle(value, phase="observations") == []
    assert validate_export_lifecycle(value) == []


def test_async_success_can_settle_after_wrapper_return() -> None:
    value = build_evidence("export-success", export_mode="async-bounded-flush")
    find(value, "export_completed")["atMs"] = 160
    find(value, "flush_completed")["atMs"] = 161
    find(value, "flush_wait_completed").update(outcome="timeout", pendingExportIds=["export-0"])
    sequence_events(value)
    assert validate_export_lifecycle(value) == []


def test_sync_timeout_signal_cannot_bound_an_earlier_blocked_export() -> None:
    value = build_evidence("held-export", export_mode="async-bounded-flush")
    value["exportMode"] = "sync"
    assert any("synchronous export did not settle" in error for error in validate_export_lifecycle(value))


def test_async_mode_requires_actual_wait_and_completion_boundaries() -> None:
    value = build_evidence("export-success")
    value["exportMode"] = "async-bounded-flush"
    value["exportLifecycle"]["budgets"]["flushWaitMs"] = 15
    assert any("separate flush start" in error for error in validate_export_lifecycle(value))


@pytest.mark.parametrize("missing", ["export_completed", "flush_completed"])
def test_missing_completion_has_an_independent_final_retirement_failure(missing: str) -> None:
    value = build_evidence("held-export", export_mode="async-bounded-flush")
    value["events"] = [event for event in value["events"] if event["kind"] != missing]
    before = validate_export_lifecycle(value, phase="observations")
    after = validate_export_lifecycle(value)
    assert before
    assert any("retirement" in error and error not in before for error in after)


def test_bounded_wait_timeout_is_not_underlying_flush_completion() -> None:
    value = build_evidence("held-export", export_mode="async-bounded-flush")
    find(value, "flush_completed")["atMs"] = find(value, "flush_wait_completed")["atMs"]
    assert validate_export_lifecycle(value)


def test_flush_wait_completion_cannot_hide_pending_exports() -> None:
    value = build_evidence("held-export", export_mode="async-bounded-flush")
    find(value, "flush_wait_completed").update(outcome="completed", pendingExportIds=[])
    assert any("pendingExportIds" in error for error in validate_export_lifecycle(value))


@pytest.mark.parametrize("budget", ["flushWaitMs", "exportCompletionMs"])
def test_wait_and_actual_completion_have_separate_immutable_budgets(budget: str) -> None:
    value = build_evidence("held-export", export_mode="async-bounded-flush")
    value["exportLifecycle"]["budgets"][budget] = 5
    assert any(budget in error for error in validate_export_lifecycle(value))


@pytest.mark.parametrize("mutation", ["wrong-owner", "wrong-outcome", "early-retire", "missing-inventory"])
def test_retirement_preserves_owned_completion(mutation: str) -> None:
    value = build_evidence("export-failure-recovery", export_mode="async-bounded-flush")
    record = next(item for item in value["retirement"]["resources"] if item["kind"] == "export")
    if mutation == "wrong-owner":
        record["ownerCaseId"] = "peer-case"
    elif mutation == "wrong-outcome":
        record["outcome"] = "succeeded"
    elif mutation == "early-retire":
        record["settledAtMs"] = 121
    else:
        value["resourcesCreated"] = [item for item in value["resourcesCreated"] if item["id"] != record["id"]]
    assert any("retirement" in error for error in validate_export_lifecycle(value))


def test_export_completion_cannot_substitute_a_healthy_request() -> None:
    value = build_evidence("export-failure-recovery")
    completed = find(value, "export_completed")
    completed.update(requestId="request-warm", ownerRequestId="request-warm", executionArn="arn:execution:warm")
    assert any("substituted" in error for error in validate_export_lifecycle(value))


def test_recovery_cannot_be_a_retry_or_new_provider() -> None:
    value = build_evidence("export-failure-recovery")
    value["attempts"][1]["executionArn"] = value["attempts"][0]["executionArn"]
    assert any("retry" in error for error in validate_export_lifecycle(value))


def test_provider_is_reused_without_prescribing_owner_threads() -> None:
    value = build_evidence("export-failure-recovery", runtime="java")
    value["attempts"][1]["threadId"] = "different-java-owner-thread"
    assert validate_export_lifecycle(value) == []
    value["telemetry"]["providers"][0]["shutdownAtMs"] = 150
    assert any("shut down" in error for error in validate_export_lifecycle(value))


@pytest.mark.parametrize("mode", EXPORT_MODES)
def test_held_gate_release_cannot_be_emergency_cleanup(mode: str) -> None:
    value = build_evidence("held-export", export_mode=mode)
    find(value, "export_released")["mode"] = "emergency"
    assert any("normal measurement release" in error for error in validate_export_lifecycle(value))


def test_async_held_fixture_proves_wrapper_return_before_release() -> None:
    value = build_evidence("held-export", export_mode="async-bounded-flush")
    find(value, "export_released")["atMs"] = 135
    value["faultsCreated"][0]["releasedAtMs"] = 135
    sequence_events(value)
    assert any("remained held" in error for error in validate_export_lifecycle(value))


def test_missing_export_collection_cannot_look_like_quiescence() -> None:
    value = build_evidence("export-success")
    value["collection"]["watermarks"]["exportLifecycle"] = 130
    assert any("complete observation window" in error for error in validate_export_lifecycle(value))


def test_logical_span_reexport_does_not_impose_exactly_once_delivery() -> None:
    value = build_evidence("export-success")
    spans = value["telemetry"]["views"]["invocation"]["spans"]
    spans.append(deepcopy(spans[0]))
    assert validate_export_lifecycle(value) == []


def test_async_flush_can_start_after_its_exports_already_completed() -> None:
    value = build_evidence("export-success", export_mode="async-bounded-flush")
    find(value, "flush_started").update(atMs=135, pendingExportIds=[])
    find(value, "flush_completed")["atMs"] = 136
    find(value, "flush_wait_completed")["atMs"] = 137
    sequence_events(value)
    assert validate_export_lifecycle(value) == []


@pytest.mark.parametrize("premature", ["underlying-completion", "wait-completion"])
def test_no_pending_exports_is_not_proof_of_flush_completion(premature: str) -> None:
    value = build_evidence("export-success", export_mode="async-bounded-flush")
    find(value, "flush_started").update(atMs=135, pendingExportIds=[])
    find(value, "flush_completed")["atMs"] = 136
    find(value, "flush_wait_completed")["atMs"] = 137
    if premature == "underlying-completion":
        find(value, "flush_completed")["atMs"] = 134
    else:
        find(value, "flush_completed")["atMs"] = 138
    sequence_events(value)
    assert any("flush work" in error or "work was still pending" in error for error in validate_export_lifecycle(value))


def test_timeout_cannot_relabel_completed_flush_even_when_pending_is_empty() -> None:
    value = build_evidence("export-success", export_mode="async-bounded-flush")
    find(value, "flush_started").update(atMs=135, pendingExportIds=[])
    find(value, "flush_completed")["atMs"] = 136
    find(value, "flush_wait_completed").update(atMs=137, outcome="timeout", pendingExportIds=[])
    sequence_events(value)
    assert any("already completed flush work" in error for error in validate_export_lifecycle(value))


@pytest.mark.parametrize("completed_first", [True, False])
def test_equal_millisecond_sync_completion_respects_actual_boundary_order(completed_first: bool) -> None:
    value = build_evidence("export-success")
    completed, wrapper = find(value, "export_completed"), find(value, "wrapper_return")
    completed["atMs"] = wrapper["atMs"]
    sequence_events(value)
    if not completed_first:
        completed["sequence"], wrapper["sequence"] = wrapper["sequence"], completed["sequence"]
    errors = validate_export_lifecycle(value)
    if completed_first:
        assert errors == []
    else:
        assert any("synchronous export did not settle" in error for error in errors)


@pytest.mark.parametrize("export_first", [True, False])
@pytest.mark.parametrize("pending_snapshot", [False, True])
def test_equal_millisecond_flush_completion_uses_actual_export_order(
    export_first: bool, pending_snapshot: bool
) -> None:
    value = build_evidence("export-success", export_mode="async-bounded-flush")
    exported, flushed = find(value, "export_completed"), find(value, "flush_completed")
    flushed["atMs"] = exported["atMs"]
    sequence_events(value)
    if not export_first:
        exported["sequence"], flushed["sequence"] = flushed["sequence"], exported["sequence"]
    flushed["pendingExportIds"] = ["export-0"] if pending_snapshot else []
    errors = validate_export_lifecycle(value)
    assert bool(errors) == (not export_first or pending_snapshot)


def test_equal_millisecond_flush_start_includes_already_admitted_export() -> None:
    value = build_evidence("export-success", export_mode="async-bounded-flush")
    accepted, flushed = find(value, "export_started"), find(value, "flush_started")
    accepted["atMs"] = flushed["atMs"]
    sequence_events(value)
    assert validate_export_lifecycle(value) == []
    accepted["sequence"], flushed["sequence"] = flushed["sequence"], accepted["sequence"]
    flushed["pendingExportIds"] = []
    assert any("admitted more export work" in error for error in validate_export_lifecycle(value))


def test_sequence_cannot_reverse_actual_timestamp_progress() -> None:
    value = build_evidence("export-success")
    completed, wrapper = find(value, "export_completed"), find(value, "wrapper_return")
    completed["sequence"], wrapper["sequence"] = wrapper["sequence"], completed["sequence"]
    assert any("sequence contradicts" in error for error in validate_export_lifecycle(value))


def test_independent_request_sequences_do_not_establish_causal_order() -> None:
    from aws_durable_execution_conformance_tests_otel.lmi_exports import _compare_events

    with pytest.raises(ValueError, match="different requests"):
        _compare_events(
            {"requestId": "first", "atMs": 130, "sequence": 999}, {"requestId": "second", "atMs": 130, "sequence": 0}
        )


@pytest.mark.parametrize("mode", EXPORT_MODES)
def test_export_cannot_complete_before_its_canonical_spans_end(mode: str) -> None:
    value = build_evidence("export-success", export_mode=mode)
    span = value["telemetry"]["views"]["invocation"]["spans"][0]["span"]
    span["end_time"] = datetime.fromtimestamp(0.300, UTC).isoformat()
    assert any("span ended after its export" in error for error in validate_export_lifecycle(value))


def test_async_export_can_complete_after_wrapper_when_spans_end_before_completion() -> None:
    value = build_evidence("held-export", export_mode="async-bounded-flush")
    completed = find(value, "export_completed")["atMs"]
    assert completed > find(value, "wrapper_return")["atMs"]
    for record in value["telemetry"]["views"]["invocation"]["spans"]:
        record["span"]["end_time"] = datetime.fromtimestamp(completed / 1000, UTC).isoformat()
    assert validate_export_lifecycle(value) == []
